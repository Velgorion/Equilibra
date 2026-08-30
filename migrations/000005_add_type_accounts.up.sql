ALTER TABLE accounts
ADD COLUMN type TEXT NOT NULL DEFAULT 'user' CHECK (type IN ('user', 'system')),
ADD COLUMN code TEXT UNIQUE;

ALTER TABLE transactions
ADD COLUMN type TEXT NOT NULL DEFAULT 'transfer' CHECK (
    type IN (
        'transfer',
        'deposit',
        'withdrawal',
        'reversal'
    )
);

ALTER TABLE transactions ALTER COLUMN source_id SET NOT NULL;

ALTER TABLE transactions ALTER COLUMN destination_id SET NOT NULL;