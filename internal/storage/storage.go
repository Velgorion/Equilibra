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

type dbtx interface {
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
	Type                    string

	// ReversalOf is set only for a reversal and indicates the transaction it reverses
	ReversalOf *int64

	CreatedAt time.Time
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

func (s *Storage) WithTx(ctx context.Context, fn func(q Querier) error) error {
	// Create and begin database transaction
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}

	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	// Fn receive "queries" type with tx inside and all necessary methods.
	// All logic is encapsulated within "fn" function
	err = fn(&queries{tx})
	if err != nil {
		return err
	}

	commitCtx, cancel := context.WithTimeout(context.Background(), commitTimeout)
	defer cancel()

	return tx.Commit(commitCtx)
}
