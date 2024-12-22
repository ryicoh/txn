import { RWMutex } from "./rw_mutex.ts";

type LockType = "shared" | "exclusive"

type LockRecord = {
    txnId: number
    key: string
    type: LockType
}

type LockKey = string

type LockTable = Record<LockKey, LockRecord>

const lockTable: LockTable = {}

const mutexTable = new Map<string, RWMutex>()

export const getLocks = (): LockRecord[] => {
    return Object.values(lockTable)
}

export const acquireLock = async (txnId: number, {
    key,
    type
}: {
    key: string,
    type: LockType
}): Promise<() => void> => {
    lockTable[txnId] = { txnId, key, type }

    let mutex = mutexTable.get(key)
    if (mutex) {
        let release: () => void
        if (type === "exclusive") {
            release = await mutex.acquireWrite()
        } else {
            release = await mutex.acquireRead()
        }
        release()
    } 

    // コードをわかりやすくするためにMutexを使い回さず、
    // 毎回Mutexを作成する
    mutex = new RWMutex()
    let release: () => void
    if (type === "exclusive") {
        release = await mutex.acquireWrite()
    } else {
        release = await mutex.acquireRead()
    }
    mutexTable.set(key, mutex)

    return () => {
        release()
        mutexTable.delete(key)
        delete lockTable[txnId]
    }
}