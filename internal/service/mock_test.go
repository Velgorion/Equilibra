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

	getAccountFn             func(ctx context.Context, id int64) (storage.Account, error)
	getAccountForUpdateFn    func(ctx context.Context, id int64) (storage.Account, error)
	getAccountBalanceFn      func(ctx context.Context, id int64) (int64, error)
	getSystemAccountByCodeFn func(ctx context.Context, code string) (storage.Account, error)
	createTransactionFn      func(ctx context.Context, t storage.Transaction) (storage.Transaction, error)
	getTransactionFn         func(ctx context.Context, key string) (storage.Transaction, error)
	createLedgerEntryFn      func(ctx context.Context, transactionID, accountID, amount int64) error

	// ledgerEntries records every entry written, so a test can assert that a
	// rejected operation moved no money at all.
	ledgerEntries []ledgerEntry
}

var _ storage.Querier = (*mockQuerier)(nil)

type ledgerEntry struct {
	TransactionID int64
	AccountID     int64
	Amount        int64
}

var _ storage.Querier = (*mockQuerier)(nil)

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
