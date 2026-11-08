// Package tracing wraps the service in a span per call, with the business data
// of the call as span attributes. The service itself knows nothing about tracing.
package tracing

import (
	"context"

	"github.com/Velgorion/equilibra/internal/service"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/Velgorion/equilibra/internal/service/tracing"

type EquilibraService interface {
	Transfer(ctx context.Context, fromAccountID, toAccountID int64,
		amount int64, idempotencyKey string) (*service.Result, error)

	Deposit(ctx context.Context, destinationID int64,
		amount int64, source string, idempotencyKey string) (*service.Result, error)

	Withdraw(ctx context.Context, sourceID int64,
		amount int64, destination string, idempotencyKey string) (*service.Result, error)

	Reverse(ctx context.Context, transactionID int64,
		idempotencyKey string) (*service.Result, error)

	AccountBalance(ctx context.Context, accountID int64) (*service.AccountBalance, error)

	AccountTransactionsHistory(ctx context.Context, accountID int64,
		limit, before int64) (service.Statement, error)
}

type tracedService struct {
	inner EquilibraService
}

func NewTracedService(inner EquilibraService) EquilibraService {
	return &tracedService{inner: inner}
}

// The idempotency key is never recorded: whoever knows it can replay the operation.

func (t *tracedService) Transfer(ctx context.Context, fromAccountID, toAccountID int64,
	amount int64, idempotencyKey string) (*service.Result, error) {

	ctx, span := start(ctx, "service.Transfer",
		attribute.Int64("account.from", fromAccountID),
		attribute.Int64("account.to", toAccountID),
		attribute.Int64("amount", amount),
	)
	defer span.End()

	res, err := t.inner.Transfer(ctx, fromAccountID, toAccountID, amount, idempotencyKey)
	finishResult(span, res, err)

	return res, err
}

func (t *tracedService) Deposit(ctx context.Context, destinationID int64,
	amount int64, source string, idempotencyKey string) (*service.Result, error) {

	ctx, span := start(ctx, "service.Deposit",
		attribute.Int64("account.to", destinationID),
		attribute.Int64("amount", amount),
		attribute.String("deposit.source", source),
	)
	defer span.End()

	res, err := t.inner.Deposit(ctx, destinationID, amount, source, idempotencyKey)
	finishResult(span, res, err)

	return res, err
}

func (t *tracedService) Withdraw(ctx context.Context, sourceID int64,
	amount int64, destination string, idempotencyKey string) (*service.Result, error) {

	ctx, span := start(ctx, "service.Withdraw",
		attribute.Int64("account.from", sourceID),
		attribute.Int64("amount", amount),
		attribute.String("withdrawal.destination", destination),
	)
	defer span.End()

	res, err := t.inner.Withdraw(ctx, sourceID, amount, destination, idempotencyKey)
	finishResult(span, res, err)

	return res, err
}

func (t *tracedService) Reverse(ctx context.Context, transactionID int64,
	idempotencyKey string) (*service.Result, error) {

	ctx, span := start(ctx, "service.Reverse",
		attribute.Int64("transaction.reversed_id", transactionID),
	)
	defer span.End()

	res, err := t.inner.Reverse(ctx, transactionID, idempotencyKey)
	finishResult(span, res, err)

	return res, err
}

func (t *tracedService) AccountBalance(ctx context.Context, accountID int64) (*service.AccountBalance, error) {
	ctx, span := start(ctx, "service.AccountBalance",
		attribute.Int64("account.id", accountID),
	)
	defer span.End()

	// the balance itself stays out of the trace: it is the account owner's data,
	// and the trace is read by far more people than the owner
	res, err := t.inner.AccountBalance(ctx, accountID)
	finish(span, err)

	return res, err
}

func (t *tracedService) AccountTransactionsHistory(ctx context.Context, accountID int64,
	limit, before int64) (service.Statement, error) {

	ctx, span := start(ctx, "service.AccountTransactionsHistory",
		attribute.Int64("account.id", accountID),
		attribute.Int64("page.limit", limit),
		attribute.Int64("page.before", before),
	)
	defer span.End()

	res, err := t.inner.AccountTransactionsHistory(ctx, accountID, limit, before)
	if err == nil {
		span.SetAttributes(
			attribute.Int("page.entries", len(res.Entries)),
			attribute.Int64("page.next_cursor", res.NextCursor),
		)
	}
	finish(span, err)

	return res, err
}

func start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return otel.Tracer(tracerName).Start(ctx, name, trace.WithAttributes(attrs...))
}

// finish marks the span as failed if err is set
func finish(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// finishResult records what a money operation ended with.
func finishResult(span trace.Span, res *service.Result, err error) {
	finish(span, err)
	if err != nil || res == nil {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.Int64("transaction.id", res.TransactionID),
		attribute.String("transaction.type", res.Type),
		attribute.String("transaction.status", res.Status),
		attribute.Bool("transaction.replayed", res.Replayed),
	}
	if res.ReversalOf != nil {
		attrs = append(attrs, attribute.Int64("transaction.reversal_of", *res.ReversalOf))
	}

	span.SetAttributes(attrs...)
}
