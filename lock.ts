import { RWMutex } from "./rw_mutex.ts";
import { TxnId } from "./txn_id.ts";

type LockType = "shared" | "exclusive";

type LockRecord = {
  txnId: number;
  key: string;
  type: LockType;
  release?: () => void;
};

type LockTable = Record<TxnId, Map<string, LockRecord>>;

const lockTable: LockTable = {};

const mutexTable = new Map<string, RWMutex>();

export const getLocks = (): LockRecord[] => {
  return Object.values(lockTable)
    .flatMap((locks) => Array.from(locks.values()))
    .map((lock) => {
      const { release: _, ...rest } = lock;
      return rest;
    });
};

export const acquireLock = async (txnId: number, {
  key,
  type,
}: {
  key: string;
  type: LockType;
}): Promise<() => void> => {
  const locks = lockTable[txnId] ?? new Map();
  const prevLock = locks.get(key);
  if (prevLock) {
    // 排他ロックがすでに取得済みならOK
    switch (prevLock.type) {
      case "exclusive":
        // 排他ロックはすでに取得済み
        return () => {};
      case "shared":
        switch (type) {
          case "exclusive":
            // 排他ロックの方が強いのでアップグレードする必要がある
            prevLock.release?.();
            locks.delete(key);
            break;
          case "shared":
            // 共有ロックはすでに取得済み
            return () => {};
          default:
            throw new Error("invalid lock type");
        }
        break;
      default:
        throw new Error("invalid lock type");
    }
  }

  const lock: LockRecord = { txnId, key, type };
  locks.set(key, lock);
  lockTable[txnId] = locks;

  let mutex = mutexTable.get(key);
  if (mutex) {
    let release: () => void;
    switch (type) {
      case "exclusive":
        release = await mutex.acquireWrite();
        break;
      case "shared":
        release = await mutex.acquireRead();
        break;
      default:
        throw new Error("invalid lock type");
    }
    release();
  }

  // コードをわかりやすくするためにMutexを使い回さず、
  // 毎回Mutexを作成する
  mutex = new RWMutex();
  let release: () => void;
  if (type === "exclusive") {
    release = await mutex.acquireWrite();
  } else {
    release = await mutex.acquireRead();
  }
  lock.release = release;
  mutexTable.set(key, mutex);

  return () => {
    release();
    mutexTable.delete(key);
    delete lockTable[txnId];
  };
};

export const clearLock = () => {
  for (const key in lockTable) {
    delete lockTable[key];
  }
  for (const key of mutexTable.keys()) {
    mutexTable.delete(key);
  }
};
