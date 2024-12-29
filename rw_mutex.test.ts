import { RWMutex } from "./rw_mutex.ts";
import {
  assertEquals,
} from "https://deno.land/std@0.224.0/assert/assert_equals.ts";
import { delay } from "https://deno.land/std@0.224.0/async/delay.ts";
import { describe, test } from "https://deno.land/std@0.224.0/testing/bdd.ts";

describe("rw_mutex", () => {
  test("when write lock is acquired, write is blocked", async () => {
    const mutex = new RWMutex();

    // txn 1: acquire write lock
    const release1 = await mutex.acquireWrite();

    let txn2Finished = false;
    (async () => {
      // txn 2: acquire write lock
      const release2 = await mutex.acquireWrite();
      release2();
      txn2Finished = true;
    })();

    // txn 2 is blocked
    assertEquals(txn2Finished, false);

    release1();

    await delay(10);

    // txn 2 is unblocked
    assertEquals(txn2Finished, true);
  });

  Deno.test("when write lock is acquired, read is blocked", async () => {
    const mutex = new RWMutex();

    // txn 1: acquire write lock
    const release1 = await mutex.acquireWrite();

    let txn2Finished = false;
    (async () => {
      // txn 2: acquire read lock
      const release2 = await mutex.acquireRead();
      release2();
      txn2Finished = true;
    })();

    // txn 2 is blocked
    assertEquals(txn2Finished, false);

    release1();

    await delay(10);

    // txn 2 is unblocked
    assertEquals(txn2Finished, true);
  });

  Deno.test("when read lock is acquired, write is blocked", async () => {
    const mutex = new RWMutex();

    // txn 1: acquire read lock
    const release1 = await mutex.acquireRead();

    let txn2Finished = false;
    (async () => {
      // txn 2: acquire write lock
      const release2 = await mutex.acquireWrite();
      release2();
      txn2Finished = true;
    })();

    // txn 2 is blocked
    assertEquals(txn2Finished, false);

    release1();

    await delay(10);

    // txn 2 is unblocked
    assertEquals(txn2Finished, true);
  });

  Deno.test("when read lock is acquired, read is not blocked", async () => {
    const mutex = new RWMutex();

    // txn 1: acquire read lock
    const release1 = await mutex.acquireRead();

    let txn2Finished = false;
    (async () => {
      // txn 2: acquire read lock
      const release2 = await mutex.acquireRead();
      release2();
      txn2Finished = true;
    })();

    release1();

    await delay(10);

    // txn 2 is unblocked
    assertEquals(txn2Finished, true);
  });
});
