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
		TRUNCATE ledger_entries, transactions RESTART IDENTITY CASCADE;
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

func newTestService(pool *pgxpool.Pool) *Service {
	return &Service{
		storage: storage.New(pool),
	}
}

func createTestAccount(ctx context.Context, pool *pgxpool.Pool, owner, currency string) (int64, error) {
	var id int64

	err := pool.QueryRow(ctx, `
		INSERT INTO accounts (owner, currency)
		VALUES ($1, $2)
		RETURNING id;
	`, owner, currency).Scan(&id)

	return id, err
}

// fundAccount gives an account money through a deposi from a system account.
// Tests that only need a funded balance use this function
// instead of assembling a transaction and its ledger entries by hand.
func fundAccount(t *testing.T, s *Service, accountID, amount int64, idempotencyKey string) {
	t.Helper()

	_, err := s.Deposit(t.Context(), accountID, amount, "BANK", idempotencyKey)
	require.NoError(t, err)
}

// testTransactionParams describes a fixture transaction.
type testTransactionParams struct {
	SourceID       int64
	DestinationID  int64
	Amount         int64
	IdempotencyKey string
	Type           string
}

// createTestTransaction writes a completed transaction together with its pair of
// ledger entries, bypassing the service so that a fixture never depends on the
// code under test.
func createTestTransaction(ctx context.Context, pool *pgxpool.Pool, p testTransactionParams) (int64, error) {
	if p.Type == "" {
		p.Type = txTypeTransfer
	}

	var id int64

	err := pool.QueryRow(ctx, `
			INSERT INTO transactions (source_id, destination_id, amount, idempotency_key, type, status)
			VALUES ($1, $2, $3, $4, $5, 'completed')
			RETURNING id;
	`, p.SourceID, p.DestinationID, p.Amount, p.IdempotencyKey, p.Type).Scan(&id)

	if err != nil {
		return 0, err
	}

	_, err = pool.Exec(ctx, `
			INSERT INTO ledger_entries (transaction_id, account_id, amount)
			VALUES ($1, $2, $3);
	`, id, p.DestinationID, p.Amount)

	if err != nil {
		return 0, err
	}

	_, err = pool.Exec(ctx, `
			INSERT INTO ledger_entries (transaction_id, account_id, amount)
			VALUES ($1, $2, $3);
	`, id, p.SourceID, -p.Amount)

	return id, err
}

func balanceOf(t *testing.T, store Storage, id int64) (int64, error) {
	var balance int64
	err := store.WithTx(t.Context(), func(q storage.Querier) error {
		var err error
		balance, err = q.GetAccountBalance(t.Context(), id)
		return err
	})

	if err != nil {
		return 0, err
	}

	return balance, nil
}

func countLedgerEntries(ctx context.Context, pool *pgxpool.Pool, transactionID int64) (int, error) {
	var count int

	err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM ledger_entries
		WHERE transaction_id = $1;
	`, transactionID).Scan(&count)

	return count, err
}

func sumLedgerEntries(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var sum int

	err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount), 0)
		FROM ledger_entries;
	`).Scan(&sum)

	return sum, err
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

		fundAccount(t, newTestService(pool), aliceID, 100, "test_transaction")

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

			oldBalanceFrom, err := balanceOf(t, s.storage, from)
			require.NoError(t, err)

			oldBalanceTo, err := balanceOf(t, s.storage, to)
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

			newBalanceFrom, err := balanceOf(t, s.storage, from)
			require.NoError(t, err)
			require.Equal(t, oldBalanceFrom-res.Amount, newBalanceFrom)

			newBalanceTo, err := balanceOf(t, s.storage, to)
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

func TestDeposit(t *testing.T) {
	pgPool := setupTest(t)
	s := newTestService(pgPool)

	aliceID, err := createTestAccount(t.Context(), pgPool, "Alice", "RUB")
	require.NoError(t, err)

	res, err := s.Deposit(t.Context(), aliceID, 100, "BANK", "key")
	require.NoError(t, err)

	require.Equal(t, txTypeDeposit, res.Type)
	require.Equal(t, aliceID, res.DestinationID)

	balance, err := balanceOf(t, s.storage, aliceID)
	require.NoError(t, err)
	require.Equal(t, int64(100), balance)

	var code, typ string
	err = pgPool.QueryRow(t.Context(), `
		SELECT a.code, t.type
		FROM transactions t
		JOIN accounts a ON a.id = t.source_id
		WHERE t.id = $1;
	`, res.TransactionID).Scan(&code, &typ)

	require.NoError(t, err)
	require.Equal(t, "BANK_IN_RUB", code)
	require.Equal(t, txTypeDeposit, typ)

	sumOfEntries, err := sumLedgerEntries(t.Context(), pgPool)
	require.NoError(t, err)
	require.Equal(t, 0, sumOfEntries)
}

func TestWithdraw(t *testing.T) {
	pgPool := setupTest(t)
	s := newTestService(pgPool)

	aliceID, err := createTestAccount(t.Context(), pgPool, "Alice", "RUB")
	require.NoError(t, err)

	// fund the account first: a withdrawal needs money to take
	_, err = s.Deposit(t.Context(), aliceID, 500, "BANK", "deposit-key")
	require.NoError(t, err)

	res, err := s.Withdraw(t.Context(), aliceID, 200, "ATM", "withdraw-key")
	require.NoError(t, err)

	require.Equal(t, txTypeWithdrawal, res.Type)
	require.Equal(t, aliceID, res.SourceID)

	balance, err := balanceOf(t, s.storage, aliceID)
	require.NoError(t, err)
	require.Equal(t, int64(300), balance)

	// the destination must be the outgoing system account of the same currency
	var code, typ string
	err = pgPool.QueryRow(t.Context(), `
		SELECT a.code, t.type
		FROM transactions t
		JOIN accounts a ON a.id = t.destination_id
		WHERE t.id = $1;
	`, res.TransactionID).Scan(&code, &typ)

	require.NoError(t, err)
	require.Equal(t, "ATM_OUT_RUB", code)
	require.Equal(t, txTypeWithdrawal, typ)

	sumOfEntries, err := sumLedgerEntries(t.Context(), pgPool)
	require.NoError(t, err)
	require.Equal(t, 0, sumOfEntries)
}

func TestWithdrawErrors(t *testing.T) {
	pgPool := setupTest(t)
	s := newTestService(pgPool)

	aliceID, err := createTestAccount(t.Context(), pgPool, "Alice", "RUB")
	require.NoError(t, err)

	var systemID int64
	err = pgPool.QueryRow(t.Context(), `
		SELECT id FROM accounts WHERE code = 'ATM_OUT_RUB';
	`).Scan(&systemID)
	require.NoError(t, err)

	tests := []struct {
		name        string
		sourceID    int64
		amount      int64
		destination string
		wantErr     error
	}{
		{
			name:        "Negative amount",
			sourceID:    aliceID,
			amount:      -10,
			destination: "ATM",
			wantErr:     ErrInvalidAmount,
		},
		{
			name:        "Source account does not exist",
			sourceID:    999999,
			amount:      100,
			destination: "ATM",
			wantErr:     ErrSourceAccountNotFound,
		},
		{
			name:        "Source account is a system one",
			sourceID:    systemID,
			amount:      100,
			destination: "ATM",
			wantErr:     ErrInvalidWithdrawSource,
		},
		{
			name:        "Unknown destination channel",
			sourceID:    aliceID,
			amount:      100,
			destination: "CARRIER_PIGEON",
			wantErr:     ErrSystemAccountNotFound,
		},
		{
			name:        "Not enough balance",
			sourceID:    aliceID,
			amount:      100,
			destination: "ATM",
			wantErr:     ErrNotEnough,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := s.Withdraw(t.Context(), tt.sourceID, tt.amount, tt.destination, tt.name)

			require.ErrorIs(t, err, tt.wantErr)
			require.Nil(t, res)
		})
	}

	// none of the refused attempts may have written anything
	var countEntries int64
	err = pgPool.QueryRow(t.Context(), `SELECT COUNT(*) FROM ledger_entries;`).Scan(&countEntries)
	require.NoError(t, err)
	require.Zero(t, countEntries)
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

	fundAccount(t, s, aliceID, 100, "test_transaction")

	res, err := s.Transfer(t.Context(), aliceID, bobID, 99, idemKey)
	require.NoError(t, err)
	require.NotNil(t, res)

	aliceBalance, err := balanceOf(t, s.storage, aliceID)
	require.NoError(t, err)

	bobBalance, err := balanceOf(t, s.storage, bobID)
	require.NoError(t, err)

	// transfer checks transaction fields for a match
	// if a transaction with the same idempotency key already exists
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

				againAliceBalance, err := balanceOf(t, s.storage, aliceID)
				require.NoError(t, err)
				require.Equal(t, aliceBalance, againAliceBalance)

				againBobBalance, err := balanceOf(t, s.storage, bobID)
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

	fundAccount(t, s, aliceID, 1000, "test_transaction")

	aliceBalance, err := balanceOf(t, s.storage, aliceID)
	require.NoError(t, err)

	bobBalance, err := balanceOf(t, s.storage, bobID)
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

	newAliceBalance, err := balanceOf(t, s.storage, aliceID)
	require.NoError(t, err)
	require.Equal(t, aliceBalance-1000, newAliceBalance)

	newBobBalance, err := balanceOf(t, s.storage, bobID)
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

	transactionID1, err := createTestTransaction(t.Context(), pgPool, testTransactionParams{
		SourceID: systemID, DestinationID: aliceID, Amount: 1000, IdempotencyKey: "test_transaction_1",
	})
	require.NoError(t, err)
	require.NotZero(t, transactionID1)

	transactionID2, err := createTestTransaction(t.Context(), pgPool, testTransactionParams{
		SourceID: systemID, DestinationID: bobID, Amount: 1000, IdempotencyKey: "test_transaction_2",
	})
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

	aliceBalance, err := balanceOf(t, s.storage, aliceID)
	require.NoError(t, err)
	require.Equal(t, int64(1000-100*aliceSuccesses+100*bobSuccesses), aliceBalance)

	bobBalance, err := balanceOf(t, s.storage, bobID)
	require.NoError(t, err)
	require.Equal(t, int64(1000-100*bobSuccesses+100*aliceSuccesses), bobBalance)

	// no transfer may push an account below zero
	require.GreaterOrEqual(t, aliceBalance, int64(0))
	require.GreaterOrEqual(t, bobBalance, int64(0))
}

func TestReverse(t *testing.T) {
	pgPool := setupTest(t)
	s := newTestService(pgPool)

	aliceID, err := createTestAccount(t.Context(), pgPool, "Alice", "RUB")
	require.NoError(t, err)
	require.NotZero(t, aliceID)

	bobID, err := createTestAccount(t.Context(), pgPool, "Bob", "RUB")
	require.NoError(t, err)
	require.NotZero(t, bobID)

	transactionID, err := createTestTransaction(t.Context(), pgPool, testTransactionParams{
		SourceID: bobID, DestinationID: aliceID, Amount: 100, IdempotencyKey: "test_transaction",
	})
	require.NoError(t, err)
	require.NotZero(t, transactionID)

	aliceBalance, err := balanceOf(t, s.storage, aliceID)
	require.Equal(t, int64(100), aliceBalance)
	require.NoError(t, err)

	bobBalance, err := balanceOf(t, s.storage, bobID)
	require.Equal(t, int64(-100), bobBalance)
	require.NoError(t, err)

	res, err := s.Reverse(t.Context(), transactionID, "key")
	require.NoError(t, err)

	require.Equal(t, txTypeReversal, res.Type)
	require.Equal(t, txStatusCompleted, res.Status)

	// In total we should have 4 ledger entries
	var countEntries int64
	err = pgPool.QueryRow(t.Context(), `
		SELECT COUNT(*)
		FROM ledger_entries;
	`).Scan(&countEntries)

	require.NoError(t, err)
	require.Equal(t, int64(4), countEntries)

	sumOfEntries, err := sumLedgerEntries(t.Context(), pgPool)
	require.NoError(t, err)
	require.Equal(t, 0, sumOfEntries)

	properAliceBalance, err := balanceOf(t, s.storage, aliceID)
	require.NoError(t, err)

	properBobBalance, err := balanceOf(t, s.storage, bobID)
	require.NoError(t, err)

	// Balances returned to the values before the reversed transaction
	require.Equal(t, int64(0), properAliceBalance)
	require.Equal(t, int64(0), properBobBalance)

	var reversedTx storage.Transaction
	err = pgPool.QueryRow(t.Context(), `
		SELECT id, status, type, reversal_of
		FROM transactions
		WHERE id = $1;
	`, transactionID).Scan(&reversedTx.ID, &reversedTx.Status,
		&reversedTx.Type, &reversedTx.ReversalOf)

	require.NoError(t, err)
	// The reversed transaction now have the 'reversed' status
	require.Equal(t, txStatusReversed, reversedTx.Status)

	var reversalTx storage.Transaction
	err = pgPool.QueryRow(t.Context(), `
		SELECT id, status, type, reversal_of
		FROM transactions
		WHERE id = $1;
	`, res.TransactionID).Scan(&reversalTx.ID, &reversalTx.Status,
		&reversalTx.Type, &reversalTx.ReversalOf)

	require.NoError(t, err)
	require.Equal(t, txTypeReversal, reversalTx.Type)

	// reversalTx.ReversalOf must contain the ID of the transaction being reversed
	require.Equal(t, reversedTx.ID, *reversalTx.ReversalOf)

	// A second reversal of the same transaction must be refused: the row is read
	// FOR UPDATE, so this call sees the status left by the first one.
	secondRes, err := s.Reverse(t.Context(), transactionID, "another-key")
	require.ErrorIs(t, err, ErrReverseIncompletedTx)
	require.Nil(t, secondRes)

	// nothing was written by the refused attempt
	err = pgPool.QueryRow(t.Context(), `
		SELECT COUNT(*)
		FROM ledger_entries;
	`).Scan(&countEntries)

	require.NoError(t, err)
	require.Equal(t, int64(5), countEntries)
}

func TestReverseAllowNegative(t *testing.T) {
	pgPool := setupTest(t)
	s := newTestService(pgPool)

	aliceID, err := createTestAccount(t.Context(), pgPool, "Alice", "RUB")
	require.NoError(t, err)
	require.NotZero(t, aliceID)

	bobID, err := createTestAccount(t.Context(), pgPool, "Bob", "RUB")
	require.NoError(t, err)
	require.NotZero(t, bobID)

	transactionID_1, err := createTestTransaction(t.Context(), pgPool, testTransactionParams{
		SourceID: bobID, DestinationID: aliceID, Amount: 100, IdempotencyKey: "test_transaction_1",
	})
	require.NoError(t, err)
	require.NotZero(t, transactionID_1)

	transactionID_2, err := createTestTransaction(t.Context(), pgPool, testTransactionParams{
		SourceID: aliceID, DestinationID: bobID, Amount: 100, IdempotencyKey: "test_transaction_2",
	})
	require.NoError(t, err)
	require.NotZero(t, transactionID_2)

	aliceBalance, err := balanceOf(t, s.storage, aliceID)
	require.NoError(t, err)
	// Alice now has a zero balance
	require.Equal(t, int64(0), aliceBalance)

	// Then we decided to reverse the first transaction,
	// which would lead us to negative balance on Alice's account
	res, err := s.Reverse(t.Context(), transactionID_1, "key")
	require.NoError(t, err)
	require.Equal(t, txTypeReversal, res.Type)
	require.Equal(t, txStatusCompleted, res.Status)

	negativeAliceBalance, err := balanceOf(t, s.storage, aliceID)
	require.NoError(t, err)

	// But the 'allowNegative' condition in the transfer permits a negative balance
	// due to a reversing transaction
	require.Equal(t, int64(-100), negativeAliceBalance)
}
