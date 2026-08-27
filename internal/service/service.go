package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Velgorion/equilibra/internal/storage"
	"github.com/jackc/pgx/v5"
)

var (
	ErrNotEnough                  = errors.New("not enough balance to write off this amount")
	ErrInvalidAmount              = errors.New("invalid amount")
	ErrSameAccount                = errors.New("transfer to the same account")
	ErrCurrencyMismatch           = errors.New("the currencies of the accounts do not match")
	ErrSourceAccountNotFound      = errors.New("source account not found")
	ErrDestinationAccountNotFound = errors.New("destination account not found")
	ErrTransactionsMismatch       = errors.New("values in the transaction with the same idempotencyKey have changed")
)

type Storage interface {
	BeginTx(ctx context.Context) (pgx.Tx, error)
	GetAccountBalance(ctx context.Context, tx storage.DBTX, accountID int64) (int64, error)
	CreateTransaction(ctx context.Context, tx storage.DBTX, transaction storage.Transaction) (storage.Transaction, error)
	CreateLedgerEntry(ctx context.Context, tx storage.DBTX, transactionID, accountID int64, amount int64) error
	GetAccountCurrencyForUpdate(ctx context.Context, tx storage.DBTX, accountID int64) (string, error)
	GetAccountCurrency(ctx context.Context, tx storage.DBTX, accountID int64) (string, error)
	GetTransaction(ctx context.Context, tx storage.DBTX, idempotencyKey string) (storage.Transaction, error)
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

func (s *Service) Transfer(ctx context.Context, fromAccountID, toAccountID int64,
	amount int64, idempotencyKey string) (*Result, error) {

	if amount <= 0 {
		return nil, ErrInvalidAmount
	}

	if fromAccountID == toAccountID {
		return nil, ErrSameAccount
	}

	tx, err := s.storage.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	created, err := s.storage.CreateTransaction(ctx, tx, storage.Transaction{
		SourceID: fromAccountID, DestinationID: toAccountID, Amount: amount, IdempotencyKey: idempotencyKey,
	})
	if err != nil {

		if errors.Is(err, storage.ErrDuplicateKey) {
			prevTransaction, err := s.storage.GetTransaction(ctx, tx, idempotencyKey)
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

	destinationCurrency, err := s.storage.GetAccountCurrency(ctx, tx, toAccountID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, ErrDestinationAccountNotFound
		}
		return nil, fmt.Errorf("get destination currency: %w", err)
	}

	sourceCurrency, err := s.storage.GetAccountCurrencyForUpdate(ctx, tx, fromAccountID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, ErrSourceAccountNotFound
		}
		return nil, fmt.Errorf("get source currency: %w", err)
	}

	if sourceCurrency != destinationCurrency {
		return nil, ErrCurrencyMismatch
	}

	balanceFrom, err := s.storage.GetAccountBalance(ctx, tx, fromAccountID)
	if err != nil {
		return nil, err
	}

	newBalance := balanceFrom - amount

	if newBalance < 0 {
		return nil, ErrNotEnough
	}

	err = s.storage.CreateLedgerEntry(ctx, tx, created.ID, fromAccountID, -amount)
	if err != nil {
		return nil, err
	}

	err = s.storage.CreateLedgerEntry(ctx, tx, created.ID, toAccountID, amount)
	if err != nil {
		return nil, err
	}

	commitCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := tx.Commit(commitCtx); err != nil {
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
