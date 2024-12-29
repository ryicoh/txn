import { acquireLock } from "./lock.ts";
import { getTxnId } from "./txn_id.ts";

type VersionedValue = {
  version: number;
  value: string;
  writeTxnId: number | null;
  commitTxnId: number | null;
};

type Txn = {
  id: number;
  rollBacked: boolean;
  versionedValues: VersionedValue[];
  releases: (() => void)[];
};

const db: Record<string, VersionedValue[]> = {};

export const beginTxn = () => {
  const txn: Txn = {
    id: getTxnId(),
    rollBacked: false,
    versionedValues: [],
    releases: [],
  };

  return {
    get: async (key: string) => await read(txn, key),
    set: async (key: string, value: string) => await write(txn, key, value),
    commit: () => commit(txn),
    rollback: () => {
      txn.rollBacked = true;
      txn.releases.forEach((release) => release());
    },
  };
};

const read = async (txn: Txn, key: string): Promise<string | null> => {
  if (txn.rollBacked) {
    throw new Error("transaction is roll backed");
  }
  const release = await acquireLock(txn.id, { key, type: "shared" });
  txn.releases.push(release);

  return db[key]?.findLast((v) => {
    // read uncommitted value
    if (v.writeTxnId === txn.id) {
      return true;
    }

    // read committed value
    if (v.commitTxnId !== null && v.commitTxnId < txn.id) {
      return true;
    }

    return false;
  })?.value ?? null;
};

const write = async (txn: Txn, key: string, value: string) => {
  if (txn.rollBacked) {
    throw new Error("transaction is roll backed");
  }

  const release = await acquireLock(txn.id, { key, type: "exclusive" });
  txn.releases.push(release);

  const values = db[key] ?? [];
  const versionedValue = {
    version: values.length + 1,
    value,
    writeTxnId: txn.id,
    commitTxnId: null,
  } satisfies VersionedValue;

  txn.versionedValues.push(versionedValue);
  values.push(versionedValue);
  db[key] = values;
};

const commit = (txn: Txn): void => {
  if (txn.rollBacked) {
    throw new Error("transaction is roll backed");
  }

  const commitTxnId = getTxnId();

  txn.versionedValues.forEach((v) => {
    v.commitTxnId = commitTxnId;
  });

  txn.releases.forEach((release) => release());
};

export const clearDB = () => {
  for (const key in db) {
    delete db[key];
  }
};
