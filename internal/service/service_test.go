package service

import (
	"context"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Velgorion/equilibra/internal/storage"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stretchr/testify/require"

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

func newTestService(pool *pgxpool.Pool) *Service {
	return &Service{
		storage: storage.New(pool),
	}
}

func createTestAccount(ctx context.Context, tx storage.DBTX, owner, currency string) (int64, error) {
	var id int64

	err := tx.QueryRow(ctx, `
		INSERT INTO accounts (owner, currency)
		VALUES ($1, $2)
		RETURNING id;
	`, owner, currency).Scan(&id)

	return id, err
}

func createTestTransaction(ctx context.Context, tx storage.DBTX,
	sourceID, destinationID int64, amount int64, idempotencyKey string) (int64, error) {

	var id int64

	err := tx.QueryRow(ctx, `
			INSERT INTO transactions (source_id, destination_id, amount, idempotency_key, status)
			VALUES ($1, $2, $3, $4, 'completed')
			RETURNING id;
	`, sourceID, destinationID, amount, idempotencyKey).Scan(&id)

	if err != nil {
		return 0, err
	}

	_, err = tx.Exec(ctx, `
			INSERT INTO ledger_entries (transaction_id, account_id, amount)
			VALUES ($1, $2, $3);
	`, id, destinationID, amount)

	if err != nil {
		return 0, err
	}

	_, err = tx.Exec(ctx, `
			INSERT INTO ledger_entries (transaction_id, account_id, amount)
			VALUES ($1, $2, $3);
	`, id, sourceID, -amount)

	return id, err
}

func TestTransfer(t *testing.T) {

	creatingTwoAccounts := func(t *testing.T, pool *pgxpool.Pool) (from, to int64) {
		aliceID, err := createTestAccount(t.Context(), pool, "Alice", "RUB")
		require.NoError(t, err)
		require.NotZero(t, aliceID)

		bobID, err := createTestAccount(t.Context(), pool, "Bob", "RUB")
		require.NoError(t, err)
		require.NotZero(t, bobID)

		return aliceID, bobID
	}

	creatingSameAccount := func(t *testing.T, pool *pgxpool.Pool) (from, to int64) {
		aliceID, err := createTestAccount(t.Context(), pool, "Alice", "RUB")
		require.NoError(t, err)
		require.NotZero(t, aliceID)

		return aliceID, aliceID
	}

	sameCurrencies := func(t *testing.T, pool *pgxpool.Pool) (from, to int64) {
		aliceID, err := createTestAccount(t.Context(), pool, "Alice", "RUB")
		require.NoError(t, err)
		require.NotZero(t, aliceID)

		bobID, err := createTestAccount(t.Context(), pool, "Bob", "RUB")
		require.NoError(t, err)
		require.NotZero(t, bobID)

		transactionID, err := createTestTransaction(t.Context(), pool, bobID, aliceID, 100, "test_transaction")
		require.NoError(t, err)
		require.NotZero(t, transactionID)

		return aliceID, bobID
	}

	diffCurrencies := func(t *testing.T, pool *pgxpool.Pool) (from, to int64) {
		aliceID, err := createTestAccount(t.Context(), pool, "Alice", "RUB")
		require.NoError(t, err)
		require.NotZero(t, aliceID)

		strangerID, err := createTestAccount(t.Context(), pool, "Stranger", "USD")
		require.NoError(t, err)
		require.NotZero(t, strangerID)

		return aliceID, strangerID
	}

	tests := []struct {
		name           string
		amount         int64
		idempotencyKey string
		setup          func(t *testing.T, pool *pgxpool.Pool) (from, to int64)
		wantErr        error
	}{
		{
			name:           "Negative amount",
			amount:         -10,
			idempotencyKey: "key1",
			setup:          creatingTwoAccounts,
			wantErr:        ErrInvalidAmount,
		},
		{
			name:           "Same account ID",
			amount:         100,
			idempotencyKey: "key2",
			setup:          creatingSameAccount,
			wantErr:        ErrSameAccount,
		},
		{
			name:           "Different account currencies",
			amount:         100,
			idempotencyKey: "key3",
			setup:          diffCurrencies,
			wantErr:        ErrCurrencyMismatch,
		},
		{
			name:           "Not enough balance",
			amount:         200,
			idempotencyKey: "key4",
			setup:          sameCurrencies,
			wantErr:        ErrNotEnough,
		},
		{
			name:           "Successfull transaction",
			amount:         100,
			idempotencyKey: "key5",
			setup:          sameCurrencies,
			wantErr:        nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pgPool := setupTest(t)
			from, to := tt.setup(t, pgPool)
			s := newTestService(pgPool)

			oldBalanceFrom, err := s.storage.GetAccountBalance(t.Context(), pgPool, from)
			require.NoError(t, err)

			oldBalanceTo, err := s.storage.GetAccountBalance(t.Context(), pgPool, to)
			require.NoError(t, err)

			res, err := s.Transfer(t.Context(), from, to, tt.amount, tt.idempotencyKey)
			require.ErrorIs(t, err, tt.wantErr)
			if tt.wantErr != nil {
				require.Nil(t, res)
				return
			}

			require.Equal(t, to, res.DestinationID)
			require.Equal(t, from, res.SourceID)
			require.Equal(t, tt.amount, res.Amount)
			require.Equal(t, tt.idempotencyKey, res.IdempotencyKey)

			newBalanceFrom, err := s.storage.GetAccountBalance(t.Context(), pgPool, from)
			require.NoError(t, err)
			require.Equal(t, oldBalanceFrom-res.Amount, newBalanceFrom)

			newBalanceTo, err := s.storage.GetAccountBalance(t.Context(), pgPool, to)
			require.NoError(t, err)
			require.Equal(t, oldBalanceTo+res.Amount, newBalanceTo)

			countEntries, err := countLedgerEntries(t.Context(), pgPool, res.TransactionID)
			require.NoError(t, err)
			// We expect exactly 2 ledger_entries for every unique transaction
			require.Equal(t, 2, countEntries)

			sumOfEntries, err := sumLedgerEntries(t.Context(), pgPool)
			require.NoError(t, err)
			// We expect the zero sum of all ledger entries, as it should be in a double-entry ledger service
			require.Equal(t, 0, sumOfEntries)
		})
	}
}

func countLedgerEntries(ctx context.Context, tx storage.DBTX, transactionID int64) (int, error) {
	var count int

	err := tx.QueryRow(ctx, `
		SELECT count(*)
		FROM ledger_entries
		WHERE transaction_id = $1;
	`, transactionID).Scan(&count)

	return count, err
}

func sumLedgerEntries(ctx context.Context, tx storage.DBTX) (int, error) {
	var sum int

	err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount), 0)
		FROM ledger_entries;
	`).Scan(&sum)

	return sum, err
}
