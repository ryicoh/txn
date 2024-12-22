import { getTxnId } from "./txn_id.ts"
import { acquireLock, getLocks } from "./lock.ts"
import {
    assertEquals,
} from "https://deno.land/std@0.224.0/assert/assert_equals.ts"

Deno.test("acquire and release lock", async () => {
    const txnId = getTxnId()
    const release = await acquireLock(txnId, { key: "key1", type: "exclusive" })

    assertEquals(getLocks(), [
        { txnId, key: "key1", type: "exclusive" }
    ])

    release()

    assertEquals(getLocks(), [])
})

Deno.test("two transactions", async () => {
    const txnId1 = getTxnId()
    const txnId2 = getTxnId()

    const release1 = await acquireLock(txnId1, { key: "key1", type: "exclusive" })
    const release2 = await acquireLock(txnId2, { key: "key2", type: "exclusive" })

    assertEquals(getLocks(), [
        { txnId: txnId1, key: "key1", type: "exclusive" },
        { txnId: txnId2, key: "key2", type: "exclusive" }
    ])

    release1()

    assertEquals(getLocks(), [ { txnId: txnId2, key: "key2", type: "exclusive" } ])

    release2()

    assertEquals(getLocks(), [])
})

Deno.test("two transactions with same key", async () => {
    const txnId1 = getTxnId()
    const txnId2 = getTxnId()

    const release1 = await acquireLock(txnId1, { key: "key1", type: "exclusive" })
    const acquire2Promise = acquireLock(txnId2, { key: "key1", type: "exclusive" })

    assertEquals(getLocks(), [
        { txnId: txnId1, key: "key1", type: "exclusive" },
        { txnId: txnId2, key: "key1", type: "exclusive" }
    ])

    release1()

    assertEquals(getLocks(), [ { txnId: txnId2, key: "key1", type: "exclusive" } ])

    const release2 = await acquire2Promise
    release2()

    assertEquals(getLocks(), [])
})

Deno.test("two share locks with same key", async () => {
    const txnId1 = getTxnId()
    const txnId2 = getTxnId()

    const release1 = await acquireLock(txnId1, { key: "key1", type: "shared" })
    const release2 = await acquireLock(txnId2, { key: "key1", type: "shared" })

    assertEquals(getLocks(), [
        { txnId: txnId1, key: "key1", type: "shared" },
        { txnId: txnId2, key: "key1", type: "shared" }
    ])

    release2()

    assertEquals(getLocks(), [ { txnId: txnId1, key: "key1", type: "shared" } ])

    release1()

    assertEquals(getLocks(), [])
})