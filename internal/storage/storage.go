package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound = errors.New("record not found")
)

type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Storage struct {
	db *pgxpool.Pool
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

func (s *Storage) CreateTransaction(ctx context.Context, tx DBTX, idempotencyKey string) (int64, error) {
	var id int64

	err := tx.QueryRow(ctx, `
			INSERT INTO transactions (idempotency_key, status)
			VALUES ($1, 'completed')
			RETURNING id;
	`, idempotencyKey).Scan(&id)

	if err != nil {
		return 0, err
	}

	return id, nil
}

func (s *Storage) CreateLedgerEntry(ctx context.Context, tx DBTX, transactionID, accountID int64, amount int64) error {
	_, err := tx.Exec(ctx, `
			INSERT INTO ledger_entries (transaction_id, account_id, amount)
			VALUES ($1, $2, $3);
	`, transactionID, accountID, amount)

	return err
}
