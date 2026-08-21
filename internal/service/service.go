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
	ErrNotEnough        = errors.New("not enough balance to write off this amount")
	ErrInvalidAmount    = errors.New("invalid amount")
	ErrSameAccount      = errors.New("transfer to the same account")
	ErrCurrencyMismatch = errors.New("the currencies of the accounts do not match")
)

type Storage interface {
	BeginTx(ctx context.Context) (pgx.Tx, error)
	GetAccountBalance(ctx context.Context, tx storage.DBTX, accountID int64) (int64, error)
	CreateTransaction(ctx context.Context, tx storage.DBTX, idempotencyKey string) (int64, error)
	CreateLedgerEntry(ctx context.Context, tx storage.DBTX, transactionID, accountID int64, amount int64) error
	GetAccountCurrencyForUpdate(ctx context.Context, tx storage.DBTX, accountID int64) (string, error)
	GetAccountCurrency(ctx context.Context, tx storage.DBTX, accountID int64) (string, error)
}

type Service struct {
	storage Storage
}

func New(s Storage) *Service {
	return &Service{storage: s}
}

func (s *Service) Transfer(ctx context.Context, fromAccountID, toAccountID int64,
	amount int64, idempotencyKey string) error {

	if amount <= 0 {
		return ErrInvalidAmount
	}

	if fromAccountID == toAccountID {
		return ErrSameAccount
	}

	tx, err := s.storage.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	destinationCurrency, err := s.storage.GetAccountCurrency(ctx, tx, toAccountID)
	if err != nil {
		return fmt.Errorf("destination account %d: %w", toAccountID, err)
	}

	sourceCurrency, err := s.storage.GetAccountCurrencyForUpdate(ctx, tx, fromAccountID)
	if err != nil {
		return fmt.Errorf("source account %d: %w", fromAccountID, err)

	}

	if sourceCurrency != destinationCurrency {
		return ErrCurrencyMismatch
	}

	balanceFrom, err := s.storage.GetAccountBalance(ctx, tx, fromAccountID)
	if err != nil {
		return err
	}

	newBalance := balanceFrom - amount

	if newBalance < 0 {
		return ErrNotEnough
	}

	transactionID, err := s.storage.CreateTransaction(ctx, tx, idempotencyKey)
	if err != nil {
		return err
	}

	err = s.storage.CreateLedgerEntry(ctx, tx, transactionID, fromAccountID, -amount)
	if err != nil {
		return err
	}

	err = s.storage.CreateLedgerEntry(ctx, tx, transactionID, toAccountID, amount)
	if err != nil {
		return err
	}

	commitCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	return tx.Commit(commitCtx)
}
