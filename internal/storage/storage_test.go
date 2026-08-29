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
		TRUNCATE ledger_entries, transactions, accounts RESTART IDENTITY CASCADE;
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

func CreateTestAccount(ctx context.Context, tx DBTX, owner, currency string) (int64, error) {
	var id int64

	err := tx.QueryRow(ctx, `
		INSERT INTO accounts (owner, currency)
		VALUES ($1, $2)
		RETURNING id;
	`, owner, currency).Scan(&id)

	return id, err
}

func TestGetAccountBalanceOnEmptyAccount(t *testing.T) {
	pgPool := setupTest(t)
	store := New(pgPool)

	accountID, err := CreateTestAccount(t.Context(), pgPool, "Vladimir", "RUB")
	require.NoError(t, err)

	// SUM over zero rows is NULL, so COALESCE must turn it into 0 instead of failing
	balance, err := store.GetAccountBalance(t.Context(), pgPool, accountID)
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
	}

	created, err := store.CreateTransaction(t.Context(), pgPool, transaction)
	require.NoError(t, err)
	require.NotZero(t, created.ID)
	require.NotZero(t, created.CreatedAt)

	// ON CONFLICT DO NOTHING returns no rows, so function must return the ErrDuplicateKey
	_, err = store.CreateTransaction(t.Context(), pgPool, transaction)
	require.ErrorIs(t, err, ErrDuplicateKey)
}

func TestGetTransactionNotFound(t *testing.T) {
	pgPool := setupTest(t)
	store := New(pgPool)

	_, err := store.GetTransaction(t.Context(), pgPool, "missing")
	require.ErrorIs(t, err, ErrNotFound)
}
