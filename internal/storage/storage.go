package storage

import (
	"context"
	"errors"
	"time"

	"github.com/Velgorion/equilibra/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	commitTimeout   = time.Second
	rollbackTimeout = time.Minute

	pingTimeout = 5 * time.Second
)

var (
	ErrNotFound     = errors.New("record not found")
	ErrDuplicateKey = errors.New("duplicate idempotency key")
)

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

type LedgerEntry struct {
	ID            int64
	TransactionID int64
	Amount        int64
	CreatedAt     time.Time
	SourceID      int64
	DestinationID int64
	Type          string

	// ReversalOf is set only for a reversal and indicates the transaction it reverses
	ReversalOf *int64
}

type OutboxEvent struct {
	ID           int64
	EventID      string
	EventType    string
	PartitionKey string
	Payload      []byte
}

// Open creates a connection pool configured from cfg and checks that the
// database is reachable
func Open(ctx context.Context, cfg config.Database) (*pgxpool.Pool, error) {
	dbConfig, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, err
	}

	dbConfig.MaxConnLifetime = cfg.MaxConnLifetime
	dbConfig.MaxConnIdleTime = cfg.MaxConnIdleTime
	dbConfig.MaxConns = cfg.MaxConns
	dbConfig.MinConns = cfg.MinConns
	dbConfig.MinIdleConns = cfg.MinIdleConns
	dbConfig.MaxConnLifetimeJitter = cfg.MaxConnLifetimeJitter

	dbpool, err := pgxpool.NewWithConfig(ctx, dbConfig)
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()

	if err := dbpool.Ping(pingCtx); err != nil {
		dbpool.Close()
		return nil, err
	}

	return dbpool, nil
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
