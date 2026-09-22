package storage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	commitTimeout   = time.Second
	rollbackTimeout = time.Minute
)

type Querier interface {
	GetAccount(ctx context.Context, id int64) (Account, error)
	GetAccountForUpdate(ctx context.Context, id int64) (Account, error)
	GetAccountBalance(ctx context.Context, id int64) (int64, error)
	GetSystemAccountByCode(ctx context.Context, code string) (Account, error)
	CreateTransaction(ctx context.Context, t Transaction) (Transaction, error)
	GetTransaction(ctx context.Context, key string) (Transaction, error)
	GetTransactionByID(ctx context.Context, transactionID int64) (Transaction, error)
	CreateLedgerEntry(ctx context.Context, transactionID, accountID, amount int64) error
	UpdateTransactionStatus(ctx context.Context, transactionID int64, status string) error
}

type queries struct {
	db dbtx
}

var _ Querier = (*queries)(nil)

func (q *queries) GetAccountForUpdate(ctx context.Context, accountID int64) (Account, error) {
	var account Account

	err := q.db.QueryRow(ctx, `
			SELECT id, owner, currency, type, COALESCE(code, ''), created_at
			FROM accounts
			WHERE id = $1 FOR UPDATE;
	`, accountID).Scan(&account.ID, &account.Owner, &account.Currency,
		&account.Type, &account.Code, &account.CreatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Account{}, ErrNotFound
		}
		return Account{}, err
	}

	return account, nil
}

func (q *queries) GetAccount(ctx context.Context, accountID int64) (Account, error) {
	var account Account

	err := q.db.QueryRow(ctx, `
			SELECT id, owner, currency, type, COALESCE(code, ''), created_at
			FROM accounts
			WHERE id = $1;
	`, accountID).Scan(&account.ID, &account.Owner, &account.Currency,
		&account.Type, &account.Code, &account.CreatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Account{}, ErrNotFound
		}
		return Account{}, err
	}

	return account, nil
}

func (q *queries) GetSystemAccountByCode(ctx context.Context, code string) (Account, error) {
	var account Account

	err := q.db.QueryRow(ctx, `
			SELECT id, owner, currency, type, COALESCE(code, ''), created_at
			FROM accounts
			WHERE code = $1;
	`, code).Scan(&account.ID, &account.Owner, &account.Currency,
		&account.Type, &account.Code, &account.CreatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Account{}, ErrNotFound
		}
		return Account{}, err
	}

	return account, nil
}

func (q *queries) GetAccountBalance(ctx context.Context, accountID int64) (int64, error) {
	var balance int64

	err := q.db.QueryRow(ctx, `
			SELECT COALESCE(SUM(amount), 0)
			FROM ledger_entries
			WHERE account_id = $1;
	`, accountID).Scan(&balance)

	return balance, err
}

func (q *queries) CreateTransaction(ctx context.Context, transaction Transaction) (Transaction, error) {
	created := transaction
	created.Status = "completed"

	err := q.db.QueryRow(ctx, `
			INSERT INTO transactions (source_id, destination_id, amount, 
			idempotency_key, type, reversal_of, status)
			VALUES ($1, $2, $3, $4, $5, $6, 'completed')
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING id, created_at;
	`, transaction.SourceID, transaction.DestinationID, transaction.Amount,
		transaction.IdempotencyKey, transaction.Type, transaction.ReversalOf).Scan(&created.ID, &created.CreatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Transaction{}, ErrDuplicateKey
		}
		return Transaction{}, err
	}

	return created, nil
}

func (q *queries) GetTransaction(ctx context.Context, idempotencyKey string) (Transaction, error) {
	var t Transaction

	err := q.db.QueryRow(ctx, `
			SELECT id, idempotency_key, status, type, created_at, 
			source_id, destination_id, amount, reversal_of
			FROM transactions
			WHERE idempotency_key = $1;
	`, idempotencyKey).Scan(&t.ID, &t.IdempotencyKey, &t.Status, &t.Type, &t.CreatedAt,
		&t.SourceID, &t.DestinationID, &t.Amount, &t.ReversalOf)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Transaction{}, ErrNotFound
		}
		return Transaction{}, err
	}

	return t, nil
}

func (q *queries) GetTransactionByID(ctx context.Context, transactionID int64) (Transaction, error) {
	var t Transaction

	err := q.db.QueryRow(ctx, `
			SELECT id, idempotency_key, status, type, created_at,
			source_id, destination_id, amount, reversal_of
			FROM transactions
			WHERE id = $1
			FOR UPDATE;
	`, transactionID).Scan(&t.ID, &t.IdempotencyKey, &t.Status, &t.Type, &t.CreatedAt,
		&t.SourceID, &t.DestinationID, &t.Amount, &t.ReversalOf)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Transaction{}, ErrNotFound
		}
		return Transaction{}, err
	}

	return t, nil
}

func (q *queries) UpdateTransactionStatus(ctx context.Context, transactionID int64, status string) error {
	res, err := q.db.Exec(ctx, `
		UPDATE transactions
		SET status = $1
		WHERE id = $2 AND status = 'completed';
	`, status, transactionID)

	if res.RowsAffected() == 0 {
		return ErrNotFound
	}

	return err
}

func (q *queries) CreateLedgerEntry(ctx context.Context, transactionID, accountID, amount int64) error {
	_, err := q.db.Exec(ctx, `
			INSERT INTO ledger_entries (transaction_id, account_id, amount)
			VALUES ($1, $2, $3);
	`, transactionID, accountID, amount)

	return err
}
