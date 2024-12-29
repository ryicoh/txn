import { beginTxn, clearDB } from "./db.ts";
import {
  assertEquals,
} from "https://deno.land/std@0.224.0/assert/assert_equals.ts";
import {
  beforeEach,
  describe,
  test,
} from "https://deno.land/std@0.224.0/testing/bdd.ts";
import { clearLock } from "./lock.ts";

describe("db", () => {
  beforeEach(() => {
    clearDB();
    clearLock();
  });

  test("read no exist value", async () => {
    const tx = beginTxn();
    assertEquals(await tx.get("k1"), null);
    tx.rollback();
  });

  test("read uncommitted value", async () => {
    const tx = beginTxn();
    await tx.set("k1", "1");
    assertEquals(await tx.get("k1"), "1");
    tx.rollback();
  });

  test("read rollbacked value", async () => {
    const tx1 = beginTxn();
    await tx1.set("k1", "1");
    assertEquals(await tx1.get("k1"), "1");
    tx1.rollback();

    const tx2 = beginTxn();
    assertEquals(await tx2.get("k1"), null);
    tx2.rollback();
  });

  test("dirty read anomaly", async () => {
    { // pre-condition
      const tx = beginTxn();
      await tx.set("k1", "1");
      tx.commit();
    }

    const tx1 = beginTxn();
    const tx2 = beginTxn();

    await tx1.set("k1", "10");
    await tx1.set("k2", "20");

    {
      assertEquals(await tx1.get("k1"), "10");
      assertEquals(await tx1.get("k2"), "20");
    }

    const tx2GetPromise = (async () => {
      // cannot read uncommitted values
      assertEquals(await tx2.get("k1"), "1");
      assertEquals(await tx2.get("k2"), null);
    })();

    tx1.commit();
    await tx2GetPromise;
    tx2.rollback();
  });

  test("fuzzy read", async () => {
    { // pre-condition
      const tx = beginTxn();
      await tx.set("k1", "1");
      tx.commit();
    }

    const tx1 = beginTxn();
    const tx2 = beginTxn();
    {
      await tx1.set("k1", "10");
      await tx1.set("k2", "20");
      tx1.commit();
    }

    assertEquals(await tx2.get("k1"), "1");
    assertEquals(await tx2.get("k2"), null);
    tx2.rollback();
  });
});
