ALTER TABLE transactions ALTER COLUMN type SET DEFAULT 'transfer';

DROP INDEX IF EXISTS unique_reversal_of;

ALTER TABLE transactions DROP COLUMN IF EXISTS reversal_of;