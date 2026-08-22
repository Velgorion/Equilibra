package storage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound     = errors.New("record not found")
	ErrDuplicateKey = errors.New("duplicate idempotency key")
)

type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Storage struct {
	db *pgxpool.Pool
}

type Transaction struct {
	ID                      int64
	SourceID, DestinationID int64
	Amount                  int64
	IdempotencyKey          string
	Status                  string
	CreatedAt               time.Time
}

func New(db *pgxpool.Pool) *Storage {
	return &Storage{
		db: db,
	}
}

func (s *Storage) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return s.db.BeginTx(ctx, pgx.TxOptions{})
}

func (s *Storage) GetAccountCurrencyForUpdate(ctx context.Context, tx DBTX, accountID int64) (string, error) {
	var currency string

	err := tx.QueryRow(ctx, `
			SELECT currency FROM accounts WHERE id = $1 FOR UPDATE;		
	`, accountID).Scan(&currency)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}

	return currency, nil
}

func (s *Storage) GetAccountCurrency(ctx context.Context, tx DBTX, accountID int64) (string, error) {
	var currency string

	err := tx.QueryRow(ctx, `
			SELECT currency FROM accounts WHERE id = $1;		
	`, accountID).Scan(&currency)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}

	return currency, nil
}

func (s *Storage) GetAccountBalance(ctx context.Context, tx DBTX, accountID int64) (int64, error) {
	var balance int64

	err := tx.QueryRow(ctx, `
			SELECT COALESCE(SUM(amount), 0)
			FROM ledger_entries
			WHERE account_id = $1;
	`, accountID).Scan(&balance)

	return balance, err
}

func (s *Storage) CreateTransaction(ctx context.Context, tx DBTX, transaction Transaction) (int64, error) {
	var id int64

	err := tx.QueryRow(ctx, `
			INSERT INTO transactions (source_id, destination_id, amount, idempotency_key, status)
			VALUES ($1, $2, $3, $4, 'completed')
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING id;
	`, transaction.SourceID, transaction.DestinationID, transaction.Amount, transaction.IdempotencyKey).Scan(&id)

	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrDuplicateKey
	}

	return id, err
}

func (s *Storage) GetTransaction(ctx context.Context, tx DBTX, idempotencyKey string) (Transaction, error) {
	var t Transaction

	err := tx.QueryRow(ctx, `
			SELECT id, idempotency_key, status, created_at, source_id, destination_id, amount
			FROM transactions
			WHERE idempotency_key = $1;
	`, idempotencyKey).Scan(&t.ID, &t.IdempotencyKey, &t.Status, &t.CreatedAt,
		&t.SourceID, &t.DestinationID, &t.Amount)

	return t, err
}

func (s *Storage) CreateLedgerEntry(ctx context.Context, tx DBTX, transactionID, accountID int64, amount int64) error {
	_, err := tx.Exec(ctx, `
			INSERT INTO ledger_entries (transaction_id, account_id, amount)
			VALUES ($1, $2, $3);
	`, transactionID, accountID, amount)

	return err
}
