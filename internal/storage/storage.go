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

type Account struct {
	ID        int64
	Owner     string
	Currency  string
	Type      string
	Code      string
	CreatedAt time.Time
}

func New(db *pgxpool.Pool) *Storage {
	return &Storage{
		db: db,
	}
}

func (s *Storage) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return s.db.BeginTx(ctx, pgx.TxOptions{})
}

func (s *Storage) GetAccountForUpdate(ctx context.Context, tx DBTX, accountID int64) (Account, error) {
	var account Account

	err := tx.QueryRow(ctx, `
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

func (s *Storage) GetAccountBalance(ctx context.Context, tx DBTX, accountID int64) (int64, error) {
	var balance int64

	err := tx.QueryRow(ctx, `
			SELECT COALESCE(SUM(amount), 0)
			FROM ledger_entries
			WHERE account_id = $1;
	`, accountID).Scan(&balance)

	return balance, err
}

func (s *Storage) CreateTransaction(ctx context.Context, tx DBTX, transaction Transaction) (Transaction, error) {
	created := transaction
	created.Status = "completed"

	err := tx.QueryRow(ctx, `
			INSERT INTO transactions (source_id, destination_id, amount, idempotency_key, status)
			VALUES ($1, $2, $3, $4, 'completed')
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING id, created_at;
	`, transaction.SourceID, transaction.DestinationID, transaction.Amount,
		transaction.IdempotencyKey).Scan(&created.ID, &created.CreatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Transaction{}, ErrDuplicateKey
		}
		return Transaction{}, err
	}

	return created, nil
}

func (s *Storage) GetTransaction(ctx context.Context, tx DBTX, idempotencyKey string) (Transaction, error) {
	var t Transaction

	err := tx.QueryRow(ctx, `
			SELECT id, idempotency_key, status, created_at, source_id, destination_id, amount
			FROM transactions
			WHERE idempotency_key = $1;
	`, idempotencyKey).Scan(&t.ID, &t.IdempotencyKey, &t.Status, &t.CreatedAt,
		&t.SourceID, &t.DestinationID, &t.Amount)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Transaction{}, ErrNotFound
		}
		return Transaction{}, err
	}

	return t, nil
}

func (s *Storage) CreateLedgerEntry(ctx context.Context, tx DBTX, transactionID, accountID int64, amount int64) error {
	_, err := tx.Exec(ctx, `
			INSERT INTO ledger_entries (transaction_id, account_id, amount)
			VALUES ($1, $2, $3);
	`, transactionID, accountID, amount)

	return err
}
