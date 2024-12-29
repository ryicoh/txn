export type TxnId = number;

let currentTxnId: TxnId = 1;

export const getTxnId = (): TxnId => {
  return currentTxnId++;
};
