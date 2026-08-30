-- System accounts represent the boundary of the system: money entering from the
-- outside world (deposits) leaves an IN account, money leaving the system
-- (withdrawals) arrives at an OUT account. Keeping them as ordinary rows in
-- accounts lets every operation stay a balanced pair of ledger entries.
INSERT INTO accounts (owner, currency, type, code) VALUES
    ('ATM settlement, incoming',      'RUB', 'system', 'ATM_IN_RUB'),
    ('ATM settlement, outgoing',      'RUB', 'system', 'ATM_OUT_RUB'),
    ('Interbank transfers, incoming', 'RUB', 'system', 'BANK_IN_RUB'),
    ('Interbank transfers, outgoing', 'RUB', 'system', 'BANK_OUT_RUB'),
    ('ATM settlement, incoming',      'USD', 'system', 'ATM_IN_USD'),
    ('ATM settlement, outgoing',      'USD', 'system', 'ATM_OUT_USD'),
    ('Interbank transfers, incoming', 'USD', 'system', 'BANK_IN_USD'),
    ('Interbank transfers, outgoing', 'USD', 'system', 'BANK_OUT_USD')
ON CONFLICT (code) DO NOTHING;
    