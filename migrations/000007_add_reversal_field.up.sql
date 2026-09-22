-- since now transactions have not just "transfer" type, but "deposit", "withdrawal" and "reversal"
ALTER TABLE transactions ALTER COLUMN type DROP DEFAULT;

ALTER TABLE transactions
ADD COLUMN reversal_of BIGINT REFERENCES transactions(id);

CREATE UNIQUE INDEX unique_reversal_of ON transactions(reversal_of)
WHERE reversal_of IS NOT NULL;