package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

const (
	mysqlDSN = "root@tcp(localhost:3306)"
	dbName   = "anomaly_test"
)

func main() {
	createDatabase()

	readUncommitted()

	readCommitted()

	repeatableRead()
}

func createDatabase() {
	db, err := sql.Open("mysql", mysqlDSN+"/sys")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if _, err := db.Exec("DROP DATABASE IF EXISTS " + dbName); err != nil {
		panic(err)
	}

	if _, err := db.Exec("CREATE DATABASE " + dbName); err != nil {
		panic(err)
	}

	{
		db, closeDB := openDB()
		defer closeDB()

		if _, err := db.Exec("CREATE TABLE IF NOT EXISTS test (value INT)"); err != nil {
			panic(err)
		}
	}
}

func openDB() (*sql.DB, func()) {
	db, err := sql.Open("mysql", mysqlDSN+"/"+dbName)
	if err != nil {
		panic(err)
	}

	return db, func() {
		db.Close()
	}
}

func setupDB() (*sql.DB, func()) {
	db, closeDB := openDB()

	tx, err := db.BeginTx(context.TODO(), nil)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	exec(tx, "DELETE FROM test")
	commitTx(tx)

	return db, closeDB
}

func readUncommitted() {
	tx1, closeTx1 := beginTx(nil)
	defer closeTx1()

	tx2, closeTx2 := beginTx(&sql.TxOptions{Isolation: sql.LevelReadUncommitted})
	defer closeTx2()
	exec(tx2, "SELECT * FROM test")

	writeValue := 1
	exec(tx1, "INSERT INTO test (value) VALUES (?)", writeValue)

	// ReadUncommited はtx1 のトランザクションがコミットされる前にtx2 で読み込むことができる
	value, ok := queryRow[int](tx2, "SELECT * FROM test LIMIT 1")
	if !ok {
		fmt.Println("readUncommitted: not found")
	} else {
		fmt.Println("readUncommitted:", *value)
	}
}

func readCommitted() {
	tx1, closeTx1 := beginTx(nil)
	defer closeTx1()

	tx2, closeTx2 := beginTx(&sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	defer closeTx2()
	exec(tx2, "SELECT * FROM test")

	writeValue := 2
	exec(tx1, "INSERT INTO test (value) VALUES (?)", writeValue)

	value1, ok := queryRow[int](tx2, "SELECT * FROM test LIMIT 1")
	if !ok {
		fmt.Println("readCommitted: uncommitted: not found")
	} else {
		fmt.Println("readCommitted: uncommitted: ", *value1)
	}

	commitTx(tx1)

	// ReadCommitted はtx1 のトランザクションがコミットされるとtx2 で読み込むことができてしまう
	value2, ok := queryRow[int](tx2, "SELECT * FROM test LIMIT 1")
	if !ok {
		fmt.Println("readCommitted: committed: not found")
	} else {
		fmt.Println("readCommitted: committed: ", *value2)
	}
}

func repeatableRead() {
	tx1, closeTx1 := beginTx(&sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	defer closeTx1()

	tx2, closeTx2 := beginTx(&sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	defer closeTx2()
	exec(tx2, "SELECT * FROM test")

	writeValue := 3
	exec(tx1, "INSERT INTO test (value) VALUES (?)", writeValue)
	commitTx(tx1)

	// RepeatableRead はtx1 のトランザクションがコミットされてもtx2 で読み込むことができない
	value2, ok := queryRow[int](tx2, "SELECT * FROM test WHERE value = ?", writeValue)
	if !ok {
		fmt.Println("repeatableRead: not found")
	} else {
		fmt.Println("repeatableRead: ", *value2)
	}
}

func beginTx(options *sql.TxOptions) (*sql.Tx, func()) {
	db, closeDB := setupDB()
	tx, err := db.BeginTx(context.TODO(), options)
	if err != nil {
		panic(err)
	}
	return tx, func() {
		tx.Rollback()
		closeDB()
	}
}

func commitTx(tx *sql.Tx) {
	if err := tx.Commit(); err != nil {
		panic(err)
	}
}

func exec(db *sql.Tx, query string, args ...any) {
	if _, err := db.Exec(query, args...); err != nil {
		panic(err)
	}
}

func queryRow[T any](db *sql.Tx, query string, args ...any) (*T, bool) {
	row := db.QueryRow(query, args...)
	var value T
	if err := row.Scan(&value); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false
		}
		panic(err)
	}
	return &value, true
}
