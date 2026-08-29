package service

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
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

	oneAccount := func(t *testing.T, pool *pgxpool.Pool) (from, to int64) {
		aliceID, err := createTestAccount(t.Context(), pool, "Alice", "RUB")
		require.NoError(t, err)
		require.NotZero(t, aliceID)

		const nonexistentID = 0

		return aliceID, nonexistentID
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
		{
			name:           "Nonexistent destination account with less id",
			amount:         100,
			idempotencyKey: "key6",
			setup:          oneAccount,
			wantErr:        ErrDestinationAccountNotFound,
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

func TestTransferIdempotency(t *testing.T) {

	const (
		idemKey string = "key"
	)

	pgPool := setupTest(t)
	s := newTestService(pgPool)

	aliceID, err := createTestAccount(t.Context(), pgPool, "Alice", "RUB")
	require.NoError(t, err)
	require.NotZero(t, aliceID)

	bobID, err := createTestAccount(t.Context(), pgPool, "Bob", "RUB")
	require.NoError(t, err)
	require.NotZero(t, bobID)

	// create user "Vovan" for id mismatch in tests
	vovanID, err := createTestAccount(t.Context(), pgPool, "Vovan", "RUB")
	require.NoError(t, err)
	require.NotZero(t, vovanID)

	transactionID, err := createTestTransaction(t.Context(), pgPool, bobID, aliceID, 100, "test_transaction")
	require.NoError(t, err)
	require.NotZero(t, transactionID)

	res, err := s.Transfer(t.Context(), aliceID, bobID, 99, idemKey)
	require.NoError(t, err)
	require.NotNil(t, res)

	aliceBalance, err := s.storage.GetAccountBalance(t.Context(), pgPool, aliceID)
	require.NoError(t, err)

	bobBalance, err := s.storage.GetAccountBalance(t.Context(), pgPool, bobID)
	require.NoError(t, err)

	tests := []struct {
		name           string
		from           int64
		to             int64
		amount         int64
		idempotencyKey string
		wantErr        error
	}{
		{
			name:           "Same transaction",
			from:           aliceID,
			to:             bobID,
			amount:         99,
			idempotencyKey: idemKey,
			wantErr:        nil,
		},
		{
			name:           "Amount mismatch",
			from:           aliceID,
			to:             bobID,
			amount:         88,
			idempotencyKey: idemKey,
			wantErr:        ErrTransactionsMismatch,
		},
		{
			name:           "Destination mismatch",
			from:           aliceID,
			to:             vovanID,
			amount:         99,
			idempotencyKey: idemKey,
			wantErr:        ErrTransactionsMismatch,
		},
		{
			name:           "Source mismatch",
			from:           vovanID,
			to:             bobID,
			amount:         99,
			idempotencyKey: idemKey,
			wantErr:        ErrTransactionsMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRes, err := s.Transfer(t.Context(), tt.from, tt.to, tt.amount, tt.idempotencyKey)
			require.ErrorIs(t, err, tt.wantErr)
			if tt.wantErr == nil {
				require.Equal(t, res, testRes)

				againAliceBalance, err := s.storage.GetAccountBalance(t.Context(), pgPool, aliceID)
				require.NoError(t, err)
				require.Equal(t, aliceBalance, againAliceBalance)

				againBobBalance, err := s.storage.GetAccountBalance(t.Context(), pgPool, bobID)
				require.NoError(t, err)
				require.Equal(t, bobBalance, againBobBalance)
			}

			countEntries, err := countLedgerEntries(t.Context(), pgPool, res.TransactionID)
			require.NoError(t, err)
			require.Equal(t, 2, countEntries)

			sumOfEntries, err := sumLedgerEntries(t.Context(), pgPool)
			require.NoError(t, err)
			require.Equal(t, 0, sumOfEntries)
		})
	}
}

func TestTranferConcurrentSameDirection(t *testing.T) {
	pgPool := setupTest(t)
	s := newTestService(pgPool)

	// Initital state: alice balance = 1000, bob balance = 0
	aliceID, err := createTestAccount(t.Context(), pgPool, "Alice", "RUB")
	require.NoError(t, err)
	require.NotZero(t, aliceID)

	bobID, err := createTestAccount(t.Context(), pgPool, "Bob", "RUB")
	require.NoError(t, err)
	require.NotZero(t, bobID)

	transactionID, err := createTestTransaction(t.Context(), pgPool, bobID, aliceID, 1000, "test_transaction")
	require.NoError(t, err)
	require.NotZero(t, transactionID)

	aliceBalance, err := s.storage.GetAccountBalance(t.Context(), pgPool, aliceID)
	require.NoError(t, err)

	bobBalance, err := s.storage.GetAccountBalance(t.Context(), pgPool, bobID)
	require.NoError(t, err)

	// channel for receiving errors from Tranfer
	errCh := make(chan error)

	var (
		wg        sync.WaitGroup
		successes int
		failures  int
	)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			key := strconv.Itoa(n)
			_, err := s.Transfer(t.Context(), aliceID, bobID, 100, key)
			errCh <- err
		}(i)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		switch err {
		case ErrNotEnough:
			failures++
		case nil:
			successes++
		default:
			t.Fatalf("got unexpected error: %v", err)
		}
	}

	// number of successull transactions with amount of 100 with initital balance = 1000
	require.Equal(t, 10, successes)
	// left number of fail transactions
	require.Equal(t, 90, failures)

	newAliceBalance, err := s.storage.GetAccountBalance(t.Context(), pgPool, aliceID)
	require.NoError(t, err)
	require.Equal(t, aliceBalance-1000, newAliceBalance)

	newBobBalance, err := s.storage.GetAccountBalance(t.Context(), pgPool, bobID)
	require.NoError(t, err)
	require.Equal(t, bobBalance+1000, newBobBalance)

	sumOfEntries, err := sumLedgerEntries(t.Context(), pgPool)
	require.NoError(t, err)
	require.Equal(t, 0, sumOfEntries)
}

func TestTranferConcurrentOppositeDirection(t *testing.T) {
	pgPool := setupTest(t)
	s := newTestService(pgPool)

	aliceID, err := createTestAccount(t.Context(), pgPool, "Alice", "RUB")
	require.NoError(t, err)
	require.NotZero(t, aliceID)

	bobID, err := createTestAccount(t.Context(), pgPool, "Bob", "RUB")
	require.NoError(t, err)
	require.NotZero(t, bobID)

	// system account is the source of money entering the system
	// Initital state: alice balance = 1000, bob balance = 1000
	systemID, err := createTestAccount(t.Context(), pgPool, "System", "RUB")
	require.NoError(t, err)
	require.NotZero(t, systemID)

	transactionID1, err := createTestTransaction(t.Context(), pgPool, systemID, aliceID, 1000, "test_transaction_1")
	require.NoError(t, err)
	require.NotZero(t, transactionID1)

	transactionID2, err := createTestTransaction(t.Context(), pgPool, systemID, bobID, 1000, "test_transaction_2")
	require.NoError(t, err)
	require.NotZero(t, transactionID2)

	type entry struct {
		err       error
		fromAlice bool
	}

	// channel for receiving errors from Tranfer
	errCh := make(chan entry)

	var (
		wg             sync.WaitGroup
		aliceSuccesses int
		bobSuccesses   int
	)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			key := strconv.Itoa(n)
			_, err := s.Transfer(t.Context(), aliceID, bobID, 100, key)
			errCh <- entry{err: err, fromAlice: true}
		}(i)
	}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			key := strconv.Itoa(n)
			_, err := s.Transfer(t.Context(), bobID, aliceID, 100, key)
			errCh <- entry{err: err, fromAlice: false}
		}(i + 50)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for e := range errCh {
		switch e.err {
		case ErrNotEnough:
		case nil:
			if e.fromAlice {
				aliceSuccesses++
			} else {
				bobSuccesses++
			}
		default:
			t.Fatalf("got unexpected error: %v", e.err)
		}
	}

	sumOfEntries, err := sumLedgerEntries(t.Context(), pgPool)
	require.NoError(t, err)
	require.Equal(t, 0, sumOfEntries)

	aliceBalance, err := s.storage.GetAccountBalance(t.Context(), pgPool, aliceID)
	require.NoError(t, err)
	require.Equal(t, int64(1000-100*aliceSuccesses+100*bobSuccesses), aliceBalance)

	bobBalance, err := s.storage.GetAccountBalance(t.Context(), pgPool, bobID)
	require.NoError(t, err)
	require.Equal(t, int64(1000-100*bobSuccesses+100*aliceSuccesses), bobBalance)

	// no transfer may push an account below zero
	require.GreaterOrEqual(t, aliceBalance, int64(0))
	require.GreaterOrEqual(t, bobBalance, int64(0))
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
