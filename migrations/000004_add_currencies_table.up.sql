CREATE TABLE currencies (
    code TEXT PRIMARY KEY
);

INSERT INTO currencies (code) VALUES ('RUB'), ('USD')
ON CONFLICT (code) DO NOTHING;

ALTER TABLE accounts
ADD CONSTRAINT fk_accounts_currency
FOREIGN KEY (currency)
REFERENCES currencies(code);
