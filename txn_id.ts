
let currentTxnId = 1

export const getTxnId = () => {
    return currentTxnId++
}