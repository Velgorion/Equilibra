package storage

import (
	"context"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var (
	testPool *pgxpool.Pool
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) (status int) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx, "postgres:15-alpine",
		postgres.WithDatabase("test_db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(30*time.Second)),
	)

	if err != nil {
		log.Println(err)
		return 1
	}

	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate pgContainer: %s\n", err)
		}
	}()

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Println(err)
		return 1
	}

	// databaseURL is a dsn for golang-migrate, whose scheme must start with 'pgx5' instead of 'postgres'
	databaseURL := "pgx5" + strings.TrimPrefix(dsn, "postgres")
	migrations, err := migrate.New("file://../../migrations", databaseURL)

	if err != nil {
		log.Println(err)
		return 1
	}

	if err := migrations.Up(); err != nil {
		log.Println(err)
		return 1
	}

	testPool, err = openTestDB(dsn)
	if err != nil {
		log.Println(err)
		return 1
	}
	defer testPool.Close()

	return m.Run()
}

func setupTest(t *testing.T) *pgxpool.Pool {
	t.Helper()

	// Clean tables before every test
	_, err := testPool.Exec(t.Context(), `
		TRUNCATE ledger_entries, transactions, outbox RESTART IDENTITY CASCADE;
	`)
	require.NoError(t, err)

	_, err = testPool.Exec(t.Context(), `
		DELETE FROM accounts WHERE type = 'user';
	`)
	require.NoError(t, err)

	return testPool
}

func openTestDB(dsn string) (*pgxpool.Pool, error) {
	dbConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	dbpool, err := pgxpool.NewWithConfig(context.Background(), dbConfig)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = dbpool.Ping(ctx)
	if err != nil {
		dbpool.Close()
		return nil, err
	}

	return dbpool, nil
}

func CreateTestAccount(ctx context.Context, pool *pgxpool.Pool, owner, currency string) (int64, error) {
	var id int64

	err := pool.QueryRow(ctx, `
		INSERT INTO accounts (owner, currency)
		VALUES ($1, $2)
		RETURNING id;
	`, owner, currency).Scan(&id)

	return id, err
}

func query(t *testing.T, store *Storage, fn func(q Querier) error) error {
	t.Helper()

	return store.WithTx(t.Context(), fn)
}

func TestGetAccountBalanceOnEmptyAccount(t *testing.T) {
	pgPool := setupTest(t)
	store := New(pgPool)

	accountID, err := CreateTestAccount(t.Context(), pgPool, "Vladimir", "RUB")
	require.NoError(t, err)

	// SUM over zero rows is NULL, so COALESCE must turn it into 0 instead of failing
	var balance int64
	err = query(t, store, func(q Querier) error {
		var err error
		balance, err = q.GetAccountBalance(t.Context(), accountID)
		return err
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), balance)
}

func TestCreateTransactionDuplicateKey(t *testing.T) {
	pgPool := setupTest(t)
	store := New(pgPool)

	sourceID, err := CreateTestAccount(t.Context(), pgPool, "Alice", "RUB")
	require.NoError(t, err)

	destinationID, err := CreateTestAccount(t.Context(), pgPool, "Bob", "RUB")
	require.NoError(t, err)

	transaction := Transaction{
		SourceID:       sourceID,
		DestinationID:  destinationID,
		Amount:         100,
		IdempotencyKey: "duplicate",
		Type:           "transfer",
	}

	var created Transaction
	err = query(t, store, func(q Querier) error {
		var err error
		created, err = q.CreateTransaction(t.Context(), transaction)
		return err
	})
	require.NoError(t, err)
	require.NotZero(t, created.ID)
	require.NotZero(t, created.CreatedAt)

	// ON CONFLICT DO NOTHING returns no rows, so function must return the ErrDuplicateKey
	err = query(t, store, func(q Querier) error {
		_, err := q.CreateTransaction(t.Context(), transaction)
		return err
	})
	require.ErrorIs(t, err, ErrDuplicateKey)
}

func TestGetTransactionNotFound(t *testing.T) {
	pgPool := setupTest(t)
	store := New(pgPool)

	err := query(t, store, func(q Querier) error {
		_, err := q.GetTransaction(t.Context(), "missing")
		return err
	})
	require.ErrorIs(t, err, ErrNotFound)
}

func TestOutboxEvents(t *testing.T) {

	const (
		limitEvents = 2
	)

	pgPool := setupTest(t)
	store := New(pgPool)

	err := query(t, store, func(q Querier) error {
		for i := 1; i <= 3; i++ {
			if err := q.CreateOutboxEvent(t.Context(), "test.event", "1", map[string]int{"n": i}); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	var events []OutboxEvent

	err = query(t, store, func(q Querier) error {
		var err error
		events, err = q.GetUnpublishedOutboxEvents(t.Context(), limitEvents)
		return err
	})
	require.NoError(t, err)

	require.Equal(t, len(events), 2)
	require.Equal(t, events[0].EventType, "test.event")
	require.Equal(t, events[0].PartitionKey, "1")

	require.Equal(t, events[1].EventType, "test.event")
	require.Equal(t, events[1].PartitionKey, "1")

	// Mark events as published
	ids := []int64{events[0].ID, events[1].ID}

	err = query(t, store, func(q Querier) error {
		return q.MarkOutboxEventsPublished(t.Context(), ids)
	})
	require.NoError(t, err)

	err = query(t, store, func(q Querier) error {
		var err error
		events, err = q.GetUnpublishedOutboxEvents(t.Context(), limitEvents)
		return err
	})
	require.NoError(t, err)

	// After the marking, only one event is unpublished
	require.Equal(t, len(events), 1)
}
