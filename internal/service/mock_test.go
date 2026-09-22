package service

import (
	"context"
	"testing"

	"github.com/Velgorion/equilibra/internal/storage"
	"github.com/stretchr/testify/require"
)

// mockQuerier implements storage.Querier with a function field per method.
// This provides a flexible way to define the function's logic.
type mockQuerier struct {
	t *testing.T

	getAccountFn              func(ctx context.Context, id int64) (storage.Account, error)
	getAccountForUpdateFn     func(ctx context.Context, id int64) (storage.Account, error)
	getAccountBalanceFn       func(ctx context.Context, id int64) (int64, error)
	getSystemAccountByCodeFn  func(ctx context.Context, code string) (storage.Account, error)
	createTransactionFn       func(ctx context.Context, t storage.Transaction) (storage.Transaction, error)
	getTransactionFn          func(ctx context.Context, key string) (storage.Transaction, error)
	getTransactionByIDFn      func(ctx context.Context, transactionID int64) (storage.Transaction, error)
	createLedgerEntryFn       func(ctx context.Context, transactionID, accountID, amount int64) error
	updateTransactionStatusFn func(ctx context.Context, transactionID int64, status string) error

	// ledgerEntries records every entry written, so a test can assert that a
	// rejected operation moved no money at all.
	ledgerEntries []ledgerEntry

	// statusUpdates records every status change, so a test can assert that a
	// reversal marked its original transaction and nothing else.
	statusUpdates []statusUpdate
}

var _ storage.Querier = (*mockQuerier)(nil)

type ledgerEntry struct {
	TransactionID int64
	AccountID     int64
	Amount        int64
}

type statusUpdate struct {
	TransactionID int64
	Status        string
}

func (m *mockQuerier) GetAccount(ctx context.Context, id int64) (storage.Account, error) {
	if m.getAccountFn == nil {
		m.t.Fatalf("unexpected call: GetAccount(%d)", id)
	}

	return m.getAccountFn(ctx, id)
}

func (m *mockQuerier) GetAccountForUpdate(ctx context.Context, id int64) (storage.Account, error) {
	if m.getAccountForUpdateFn == nil {
		m.t.Fatalf("unexpected call: GetAccountForUpdate(%d)", id)
	}

	return m.getAccountForUpdateFn(ctx, id)
}

func (m *mockQuerier) GetAccountBalance(ctx context.Context, id int64) (int64, error) {
	if m.getAccountBalanceFn == nil {
		m.t.Fatalf("unexpected call: GetAccountBalance(%d)", id)
	}

	return m.getAccountBalanceFn(ctx, id)
}

func (m *mockQuerier) GetSystemAccountByCode(ctx context.Context, code string) (storage.Account, error) {
	if m.getSystemAccountByCodeFn == nil {
		m.t.Fatalf("unexpected call: GetSystemAccountByCode(%q)", code)
	}

	return m.getSystemAccountByCodeFn(ctx, code)
}

func (m *mockQuerier) CreateTransaction(ctx context.Context, t storage.Transaction) (storage.Transaction, error) {
	if m.createTransactionFn == nil {
		m.t.Fatalf("unexpected call: CreateTransaction(%+v)", t)
	}

	return m.createTransactionFn(ctx, t)
}

func (m *mockQuerier) GetTransaction(ctx context.Context, key string) (storage.Transaction, error) {
	if m.getTransactionFn == nil {
		m.t.Fatalf("unexpected call: GetTransaction(%q)", key)
	}

	return m.getTransactionFn(ctx, key)
}

func (m *mockQuerier) GetTransactionByID(ctx context.Context, transactionID int64) (storage.Transaction, error) {
	if m.getTransactionByIDFn == nil {
		m.t.Fatalf("unexpected call: GetTransactionByID(%d)", transactionID)
	}

	return m.getTransactionByIDFn(ctx, transactionID)
}

func (m *mockQuerier) UpdateTransactionStatus(ctx context.Context, transactionID int64, status string) error {
	m.statusUpdates = append(m.statusUpdates, statusUpdate{transactionID, status})

	if m.updateTransactionStatusFn == nil {
		return nil
	}

	return m.updateTransactionStatusFn(ctx, transactionID, status)
}

func (m *mockQuerier) CreateLedgerEntry(ctx context.Context, transactionID, accountID, amount int64) error {
	m.ledgerEntries = append(m.ledgerEntries, ledgerEntry{transactionID, accountID, amount})

	if m.createLedgerEntryFn == nil {
		return nil
	}

	return m.createLedgerEntryFn(ctx, transactionID, accountID, amount)
}

// mockStorage implements the Storage interface of this package
type mockStorage struct {
	querier   *mockQuerier
	committed bool
}

var _ Storage = (*mockStorage)(nil)

func (m *mockStorage) WithTx(ctx context.Context, fn func(q storage.Querier) error) error {

	if err := fn(m.querier); err != nil {
		return err
	}

	m.committed = true
	return nil
}

func newMockService(t *testing.T) (*Service, *mockQuerier, *mockStorage) {
	t.Helper()

	q := &mockQuerier{t: t}
	st := &mockStorage{querier: q}

	return New(st), q, st
}

func account(id int64, currency, accountType string) storage.Account {
	return storage.Account{ID: id, Currency: currency, Type: accountType}
}

func TestUnitTransferRejectsSystemAccounts(t *testing.T) {
	s, q, st := newMockService(t)

	q.getAccountFn = func(ctx context.Context, id int64) (storage.Account, error) {
		if id < 10 {
			return account(id, "RUB", accountTypeSystem), nil
		}
		return account(id, "RUB", accountTypeUser), nil
	}

	res, err := s.Transfer(t.Context(), 5, 10, 100, "key")
	require.ErrorIs(t, err, ErrSystemAccountNotAllowed)
	require.False(t, st.committed)
	require.Empty(t, q.ledgerEntries)
	require.Nil(t, res)
}

func TestUnitAmount(t *testing.T) {
	s, q, st := newMockService(t)

	res, err := s.Transfer(t.Context(), 1, 2, -99, "key")
	require.ErrorIs(t, err, ErrInvalidAmount)
	require.Empty(t, q.ledgerEntries)
	require.False(t, st.committed)
	require.Nil(t, res)
}

func TestUnitSameAccount(t *testing.T) {
	s, q, st := newMockService(t)

	res, err := s.Transfer(t.Context(), 1, 1, 100, "key")
	require.ErrorIs(t, err, ErrSameAccount)
	require.Empty(t, q.ledgerEntries)
	require.False(t, st.committed)
	require.Nil(t, res)
}

func TestUnitCurrencyMismatch(t *testing.T) {
	s, q, st := newMockService(t)

	q.getAccountFn = func(ctx context.Context, id int64) (storage.Account, error) {
		if id == 1 {
			return account(id, "USD", accountTypeUser), nil
		}
		return account(id, "RUB", accountTypeUser), nil
	}

	q.getAccountForUpdateFn = q.getAccountFn

	res, err := s.Transfer(t.Context(), 1, 2, 100, "key")
	require.ErrorIs(t, err, ErrCurrencyMismatch)
	require.Empty(t, q.ledgerEntries)
	require.False(t, st.committed)
	require.Nil(t, res)
}

func TestUnitDeposit(t *testing.T) {
	s, q, st := newMockService(t)

	const (
		wantCode        = "BANK_IN_RUB"
		systemAccountID = int64(1)
		userAccountID   = int64(10)
	)

	q.getAccountFn = func(ctx context.Context, id int64) (storage.Account, error) {
		return account(id, "RUB", accountTypeUser), nil
	}

	q.getSystemAccountByCodeFn = func(ctx context.Context, code string) (storage.Account, error) {
		if code != wantCode {
			return storage.Account{}, ErrSystemAccountNotFound
		}
		return account(systemAccountID, "RUB", accountTypeSystem), nil
	}

	q.getAccountForUpdateFn = func(ctx context.Context, id int64) (storage.Account, error) {
		if id == systemAccountID {
			return account(id, "RUB", accountTypeSystem), nil
		}
		return account(id, "RUB", accountTypeUser), nil
	}

	q.createTransactionFn = func(ctx context.Context, tr storage.Transaction) (storage.Transaction, error) {
		tr.ID = 1
		tr.Status = "completed"
		return tr, nil
	}

	q.getAccountBalanceFn = func(ctx context.Context, id int64) (int64, error) {
		return 1000, nil
	}

	res, err := s.Deposit(t.Context(), userAccountID, 100, "BANK", "key")

	require.NoError(t, err)
	require.True(t, st.committed)
	require.Equal(t, systemAccountID, res.SourceID)
	require.Equal(t, userAccountID, res.DestinationID)

	require.Len(t, q.ledgerEntries, 2)
	require.Equal(t, int64(-100), q.ledgerEntries[0].Amount)
	require.Equal(t, int64(100), q.ledgerEntries[1].Amount)
}

func TestUnitReverseErrors(t *testing.T) {
	const (
		reversedID = int64(1)
		reversalID = int64(2)
		missingID  = int64(3)
	)

	tests := []struct {
		name    string
		id      int64
		wantErr error
	}{
		{
			name:    "Already reversed",
			id:      reversedID,
			wantErr: ErrReverseIncompletedTx,
		},
		{
			name:    "Reversal of a reversal",
			id:      reversalID,
			wantErr: ErrReverseReversalTx,
		},
		{
			name:    "Transaction does not exist",
			id:      missingID,
			wantErr: ErrTransactionNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, q, st := newMockService(t)

			q.getTransactionByIDFn = func(ctx context.Context, transactionID int64) (storage.Transaction, error) {
				switch transactionID {
				case reversedID:
					return storage.Transaction{Status: txStatusReversed, Type: txTypeTransfer}, nil
				case reversalID:
					return storage.Transaction{Status: txStatusCompleted, Type: txTypeReversal}, nil
				default:
					return storage.Transaction{}, ErrTransactionNotFound
				}
			}

			res, err := s.Reverse(t.Context(), tt.id, "key")

			require.ErrorIs(t, err, tt.wantErr)
			require.Nil(t, res)
			require.False(t, st.committed)
			require.Empty(t, q.ledgerEntries)
			require.Empty(t, q.statusUpdates)
		})
	}
}

func TestUnitReverse(t *testing.T) {
	s, q, st := newMockService(t)

	const (
		sourceAccount      = int64(20)
		destinationAccount = int64(21)

		originalTransactionID = int64(1)
		reversalTransactionID = int64(99)
	)

	q.getTransactionByIDFn = func(ctx context.Context, transactionID int64) (storage.Transaction, error) {
		return storage.Transaction{ID: originalTransactionID,
			SourceID:       sourceAccount,
			DestinationID:  destinationAccount,
			Amount:         100,
			IdempotencyKey: "key",
			Status:         txStatusCompleted,
			Type:           txTypeTransfer,
		}, nil
	}

	q.getAccountForUpdateFn = func(ctx context.Context, id int64) (storage.Account, error) {
		return storage.Account{ID: id, Currency: "RUB"}, nil
	}

	q.createTransactionFn = func(ctx context.Context, tr storage.Transaction) (storage.Transaction, error) {
		tr.ID = reversalTransactionID
		tr.Status = txStatusCompleted
		return tr, nil
	}

	q.getAccountBalanceFn = func(ctx context.Context, id int64) (int64, error) {
		return 1000, nil
	}

	res, err := s.Reverse(t.Context(), originalTransactionID, "key")
	require.NoError(t, err)
	require.True(t, st.committed)

	require.Equal(t, reversalTransactionID, res.TransactionID)
	require.Equal(t, txTypeReversal, res.Type)

	// During the reversal operation the destination account must become
	// the source account, and vice versa.
	require.Equal(t, sourceAccount, res.DestinationID)
	require.Equal(t, destinationAccount, res.SourceID)

	require.Equal(t, []ledgerEntry{
		{TransactionID: reversalTransactionID, AccountID: destinationAccount, Amount: -100},
		{TransactionID: reversalTransactionID, AccountID: sourceAccount, Amount: 100},
	}, q.ledgerEntries)

	// the status is written to the original transaction
	require.Equal(t, []statusUpdate{
		{TransactionID: originalTransactionID, Status: txStatusReversed},
	}, q.statusUpdates)
}
