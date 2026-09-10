package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Velgorion/equilibra/internal/storage"
)

var (
	ErrNotEnough                  = errors.New("not enough balance to write off this amount")
	ErrInvalidAmount              = errors.New("invalid amount")
	ErrSameAccount                = errors.New("transfer to the same account")
	ErrCurrencyMismatch           = errors.New("the currencies of the accounts do not match")
	ErrSourceAccountNotFound      = errors.New("source account not found")
	ErrDestinationAccountNotFound = errors.New("destination account not found")
	ErrTransactionsMismatch       = errors.New("values in the transaction with the same idempotencyKey have changed")
	ErrSystemAccountNotAllowed    = errors.New("the transfer takes place only between users")
	ErrInvalidDepositDestination  = errors.New("destination account in deposit operation must be user type")
	ErrInvalidDepositSource       = errors.New("source account in deposit operation must be user type")
	ErrSystemAccountNotFound      = errors.New("the system account not found")
)

const (
	accountTypeUser   = "user"
	accountTypeSystem = "system"
)

type Storage interface {
	WithTx(ctx context.Context, fn func(q storage.Querier) error) error
}

type Service struct {
	storage Storage
}

type Result struct {
	TransactionID           int64
	SourceID, DestinationID int64
	Amount                  int64
	IdempotencyKey          string
	Status                  string
	CreatedAt               time.Time
}

func New(s Storage) *Service {
	return &Service{storage: s}
}

// Transfer create transaction between two user type accounts
func (s *Service) Transfer(ctx context.Context, fromAccountID, toAccountID int64,
	amount int64, idempotencyKey string) (*Result, error) {

	var res *Result
	err := s.storage.WithTx(ctx, func(q storage.Querier) error {

		fromAccount, err := q.GetAccount(ctx, fromAccountID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return ErrSourceAccountNotFound
			}
			return err
		}

		toAccount, err := q.GetAccount(ctx, toAccountID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return ErrDestinationAccountNotFound
			}
			return err
		}

		if fromAccount.Type != accountTypeUser || toAccount.Type != accountTypeUser {
			return ErrSystemAccountNotAllowed
		}

		result, err := s.transfer(ctx, q, fromAccountID, toAccountID, amount, idempotencyKey)
		if err != nil {
			return err
		}

		res = result

		return err
	})

	return res, err

}

// Deposit create transaction with a system account as a source
// source parameter to identify the source of the transaction ("ATM" or "BANK")
func (s *Service) Deposit(ctx context.Context, destinationID int64,
	amount int64, source string, idempotencyKey string) (*Result, error) {

	var res *Result
	err := s.storage.WithTx(ctx, func(q storage.Querier) error {

		account, err := q.GetAccount(ctx, destinationID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return ErrDestinationAccountNotFound
			}
			return err
		}

		if account.Type != accountTypeUser {
			return ErrInvalidDepositDestination
		}

		sysAccountCode := source + "_IN_" + account.Currency

		sysAccount, err := q.GetSystemAccountByCode(ctx, sysAccountCode)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return ErrSystemAccountNotFound
			}
			return err
		}

		if sysAccount.Type != accountTypeSystem {
			return ErrSystemAccountNotFound
		}

		result, err := s.transfer(ctx, q, sysAccount.ID, destinationID, amount, idempotencyKey)
		if err != nil {
			return err
		}

		res = result

		return err
	})

	return res, err
}

// Withdraw create transaction with a system account as a destination
// destination parameter to identify the destination of the transaction ("ATM" or "BANK")
func (s *Service) Withdraw(ctx context.Context, sourceID int64,
	amount int64, destination string, idempotencyKey string) (*Result, error) {

	var res *Result
	err := s.storage.WithTx(ctx, func(q storage.Querier) error {

		account, err := q.GetAccount(ctx, sourceID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return ErrSourceAccountNotFound
			}
			return err
		}

		if account.Type != accountTypeUser {
			return ErrInvalidDepositSource
		}

		sysAccountCode := destination + "_IN_" + account.Currency

		sysAccount, err := q.GetSystemAccountByCode(ctx, sysAccountCode)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return ErrSystemAccountNotFound
			}
			return err
		}

		if sysAccount.Type != accountTypeSystem {
			return ErrSystemAccountNotFound
		}

		result, err := s.transfer(ctx, q, sysAccount.ID, sourceID, amount, idempotencyKey)
		if err != nil {
			return err
		}

		res = result

		return err
	})

	return res, err
}

func (s *Service) transfer(ctx context.Context, q storage.Querier, fromAccountID, toAccountID int64,
	amount int64, idempotencyKey string) (*Result, error) {

	if amount <= 0 {
		return nil, ErrInvalidAmount
	}

	if fromAccountID == toAccountID {
		return nil, ErrSameAccount
	}

	// Implement ordered locking to prevent deadlock in case of concurrent and opposite transactions
	first, second := min(fromAccountID, toAccountID), max(fromAccountID, toAccountID)

	firstAccount, err := q.GetAccountForUpdate(ctx, first)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			if first == fromAccountID {
				return nil, ErrSourceAccountNotFound
			}
			return nil, ErrDestinationAccountNotFound
		}
		return nil, fmt.Errorf("lock account %d: %w", first, err)
	}

	secondAccount, err := q.GetAccountForUpdate(ctx, second)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			if second == fromAccountID {
				return nil, ErrSourceAccountNotFound
			}
			return nil, ErrDestinationAccountNotFound
		}
		return nil, fmt.Errorf("lock account %d: %w", second, err)
	}

	sourceAccount, destinationAccount := firstAccount, secondAccount
	if first != fromAccountID {
		sourceAccount, destinationAccount = secondAccount, firstAccount
	}

	if sourceAccount.Currency != destinationAccount.Currency {
		return nil, ErrCurrencyMismatch
	}

	created, err := q.CreateTransaction(ctx, storage.Transaction{
		SourceID: fromAccountID, DestinationID: toAccountID, Amount: amount, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		if errors.Is(err, storage.ErrDuplicateKey) {
			prevTransaction, err := q.GetTransaction(ctx, idempotencyKey)
			if err != nil {
				return nil, err
			}

			// return the result of the transaction with the same key if the arguments match
			if compareTransactions(storage.Transaction{SourceID: fromAccountID, DestinationID: toAccountID, Amount: amount,
				IdempotencyKey: idempotencyKey}, prevTransaction) {

				return newResult(prevTransaction), nil
			}

			// otherwise it's a error
			return nil, ErrTransactionsMismatch
		}

		return nil, err
	}

	balanceFrom, err := q.GetAccountBalance(ctx, fromAccountID)
	if err != nil {
		return nil, err
	}

	if balanceFrom-amount < 0 {
		return nil, ErrNotEnough
	}

	if err := q.CreateLedgerEntry(ctx, created.ID, fromAccountID, -amount); err != nil {
		return nil, err
	}

	if err := q.CreateLedgerEntry(ctx, created.ID, toAccountID, amount); err != nil {
		return nil, err
	}

	return newResult(created), nil
}

func newResult(t storage.Transaction) *Result {
	return &Result{
		TransactionID:  t.ID,
		SourceID:       t.SourceID,
		DestinationID:  t.DestinationID,
		Amount:         t.Amount,
		IdempotencyKey: t.IdempotencyKey,
		Status:         t.Status,
		CreatedAt:      t.CreatedAt,
	}
}

func compareTransactions(curr, prev storage.Transaction) bool {
	return curr.SourceID == prev.SourceID &&
		curr.DestinationID == prev.DestinationID &&
		curr.Amount == prev.Amount
}
