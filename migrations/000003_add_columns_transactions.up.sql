ALTER TABLE transactions
ADD COLUMN source_id BIGINT REFERENCES accounts(id),
ADD COLUMN destination_id BIGINT REFERENCES accounts(id),
ADD COLUMN amount BIGINT NULL CHECK (amount > 0);

UPDATE transactions t
SET
    amount = dst.amount,
    destination_id = dst.account_id,
    source_id = src.account_id
FROM ledger_entries src, ledger_entries dst
WHERE
    dst.amount > 0
    AND dst.transaction_id = t.id
    AND src.amount < 0 
    AND src.transaction_id = t.id;


ALTER TABLE transactions ALTER COLUMN amount SET NOT NULL;