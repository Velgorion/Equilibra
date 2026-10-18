package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	lockOutboxNumber = 1001
)

type Querier interface {
	GetAccount(ctx context.Context, id int64) (Account, error)
	GetAccountForUpdate(ctx context.Context, id int64) (Account, error)
	GetAccountBalance(ctx context.Context, id int64) (int64, error)
	GetAccountHistory(ctx context.Context, accountID int64, limit, before int64) ([]LedgerEntry, error)
	GetSystemAccountByCode(ctx context.Context, code string) (Account, error)
	CreateTransaction(ctx context.Context, t Transaction) (Transaction, error)
	GetTransaction(ctx context.Context, key string) (Transaction, error)
	GetTransactionByID(ctx context.Context, transactionID int64) (Transaction, error)
	CreateLedgerEntry(ctx context.Context, transactionID, accountID, amount int64) error
	UpdateTransactionStatus(ctx context.Context, transactionID int64, status string) error
	CreateOutboxEvent(ctx context.Context, eventType, partitionKey string, payload any) error
	TryLockOutbox(ctx context.Context) (bool, error)
	GetUnpublishedOutboxEvents(ctx context.Context, limit int) ([]OutboxEvent, error)
	MarkOutboxEventsPublished(ctx context.Context, ids []int64) error
}

type dbtx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type queries struct {
	db dbtx
}

var _ Querier = (*queries)(nil)

func (q *queries) GetAccountForUpdate(ctx context.Context, accountID int64) (Account, error) {
	var account Account

	err := q.db.QueryRow(ctx, `
			SELECT id, owner, currency, type, COALESCE(code, ''), created_at
			FROM accounts
			WHERE id = $1 FOR UPDATE;
	`, accountID).Scan(&account.ID, &account.Owner, &account.Currency,
		&account.Type, &account.Code, &account.CreatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Account{}, ErrNotFound
		}
		return Account{}, err
	}

	return account, nil
}

func (q *queries) GetAccount(ctx context.Context, accountID int64) (Account, error) {
	var account Account

	err := q.db.QueryRow(ctx, `
			SELECT id, owner, currency, type, COALESCE(code, ''), created_at
			FROM accounts
			WHERE id = $1;
	`, accountID).Scan(&account.ID, &account.Owner, &account.Currency,
		&account.Type, &account.Code, &account.CreatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Account{}, ErrNotFound
		}
		return Account{}, err
	}

	return account, nil
}

func (q *queries) GetSystemAccountByCode(ctx context.Context, code string) (Account, error) {
	var account Account

	err := q.db.QueryRow(ctx, `
			SELECT id, owner, currency, type, COALESCE(code, ''), created_at
			FROM accounts
			WHERE code = $1;
	`, code).Scan(&account.ID, &account.Owner, &account.Currency,
		&account.Type, &account.Code, &account.CreatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Account{}, ErrNotFound
		}
		return Account{}, err
	}

	return account, nil
}

func (q *queries) GetAccountBalance(ctx context.Context, accountID int64) (int64, error) {
	var balance int64

	err := q.db.QueryRow(ctx, `
			SELECT COALESCE(SUM(amount), 0)
			FROM ledger_entries
			WHERE account_id = $1;
	`, accountID).Scan(&balance)

	return balance, err
}

func (q *queries) CreateTransaction(ctx context.Context, transaction Transaction) (Transaction, error) {
	created := transaction
	created.Status = "completed"

	err := q.db.QueryRow(ctx, `
			INSERT INTO transactions (source_id, destination_id, amount, 
			idempotency_key, type, reversal_of, status)
			VALUES ($1, $2, $3, $4, $5, $6, 'completed')
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING id, created_at;
	`, transaction.SourceID, transaction.DestinationID, transaction.Amount,
		transaction.IdempotencyKey, transaction.Type, transaction.ReversalOf).Scan(&created.ID, &created.CreatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Transaction{}, ErrDuplicateKey
		}
		return Transaction{}, err
	}

	return created, nil
}

func (q *queries) GetTransaction(ctx context.Context, idempotencyKey string) (Transaction, error) {
	var t Transaction

	err := q.db.QueryRow(ctx, `
			SELECT id, idempotency_key, status, type, created_at, 
			source_id, destination_id, amount, reversal_of
			FROM transactions
			WHERE idempotency_key = $1;
	`, idempotencyKey).Scan(&t.ID, &t.IdempotencyKey, &t.Status, &t.Type, &t.CreatedAt,
		&t.SourceID, &t.DestinationID, &t.Amount, &t.ReversalOf)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Transaction{}, ErrNotFound
		}
		return Transaction{}, err
	}

	return t, nil
}

func (q *queries) GetTransactionByID(ctx context.Context, transactionID int64) (Transaction, error) {
	var t Transaction

	err := q.db.QueryRow(ctx, `
			SELECT id, idempotency_key, status, type, created_at,
			source_id, destination_id, amount, reversal_of
			FROM transactions
			WHERE id = $1
			FOR UPDATE;
	`, transactionID).Scan(&t.ID, &t.IdempotencyKey, &t.Status, &t.Type, &t.CreatedAt,
		&t.SourceID, &t.DestinationID, &t.Amount, &t.ReversalOf)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Transaction{}, ErrNotFound
		}
		return Transaction{}, err
	}

	return t, nil
}

func (q *queries) UpdateTransactionStatus(ctx context.Context, transactionID int64, status string) error {
	tag, err := q.db.Exec(ctx, `
		UPDATE transactions
		SET status = $1
		WHERE id = $2 AND status = 'completed';
	`, status, transactionID)

	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return err
}

func (q *queries) CreateLedgerEntry(ctx context.Context, transactionID, accountID, amount int64) error {
	_, err := q.db.Exec(ctx, `
			INSERT INTO ledger_entries (transaction_id, account_id, amount)
			VALUES ($1, $2, $3);
	`, transactionID, accountID, amount)

	return err
}

func (q *queries) GetAccountHistory(ctx context.Context, accountID int64,
	limit, before int64) ([]LedgerEntry, error) {

	if before == 0 {
		before = math.MaxInt64
	}

	rows, err := q.db.Query(ctx, `
		SELECT l.id, l.amount, l.created_at,
			t.id, t.source_id, t.destination_id, t.type, t.reversal_of
		FROM ledger_entries l
		JOIN transactions t
		ON l.transaction_id = t.id
		WHERE l.account_id = $1 AND l.id < $2
		ORDER BY l.id DESC LIMIT $3
	`, accountID, before, limit)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var entries []LedgerEntry

	for rows.Next() {
		var e LedgerEntry

		if err := rows.Scan(&e.ID, &e.Amount, &e.CreatedAt, &e.TransactionID, &e.SourceID,
			&e.DestinationID, &e.Type, &e.ReversalOf); err != nil {
			return nil, err
		}

		entries = append(entries, e)
	}

	return entries, rows.Err()
}

func (q *queries) CreateOutboxEvent(ctx context.Context, eventType, partitionKey string, payload any) error {
	js, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal event payload: %w", err)
	}

	_, err = q.db.Exec(ctx, `
		INSERT INTO outbox (event_type, partition_key, payload)
		VALUES ($1, $2, $3);
	`, eventType, partitionKey, js)

	return err
}

func (q *queries) TryLockOutbox(ctx context.Context) (bool, error) {
	var isOpen bool

	err := q.db.QueryRow(ctx, `
		SELECT pg_try_advisory_xact_lock($1)
	`, lockOutboxNumber).Scan(&isOpen)

	return isOpen, err
}

func (q *queries) GetUnpublishedOutboxEvents(ctx context.Context, limit int) ([]OutboxEvent, error) {
	rows, err := q.db.Query(ctx, `
		SELECT id, event_id::text, event_type, partition_key, payload
		FROM outbox
		WHERE published_at is NULL
		ORDER BY id
		LIMIT $1;
	`, limit)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []OutboxEvent

	for rows.Next() {
		var e OutboxEvent

		if err := rows.Scan(&e.ID, &e.EventID, &e.EventType,
			&e.PartitionKey, &e.Payload); err != nil {
			return nil, err
		}

		events = append(events, e)
	}

	return events, rows.Err()
}

func (q *queries) MarkOutboxEventsPublished(ctx context.Context, ids []int64) error {
	_, err := q.db.Exec(ctx, `
		UPDATE outbox SET published_at = now() WHERE id = ANY($1);
	`, ids)

	return err
}
