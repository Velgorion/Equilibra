-- This index is using for transactions history selection ordered by id
CREATE INDEX CONCURRENTLY idx_ledger_entries_pagination ON ledger_entries (account_id, id);