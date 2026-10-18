package relay

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/Velgorion/equilibra/internal/storage"
)

var (
	ErrOutboxLocked = errors.New("outbox table is locked")
)

type Storage interface {
	WithTx(ctx context.Context, fn func(q storage.Querier) error) error
}

type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

type Message struct {
	Key     string
	Value   []byte
	Headers map[string]string
}

type Relay struct {
	store     Storage
	publisher Publisher
	logger    *slog.Logger
	send      SendingParams
}

type SendingParams struct {
	BatchSize int
	Interval  time.Duration
}

func New(s Storage, p Publisher, sp SendingParams, l *slog.Logger) *Relay {
	return &Relay{
		store:     s,
		publisher: p,
		logger:    l,
		send:      sp,
	}
}

func (r *Relay) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		n, err := r.publishBatch(ctx)
		if err != nil {
			r.logger.Error("publish outbox batch", "err", err)
		}

		// a full batch means there is a backlog: go for the next one right away
		if err == nil && n == r.send.BatchSize {
			continue
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.send.Interval):
		}
	}
}

func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	var sent int

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err := r.store.WithTx(ctx, func(q storage.Querier) error {
		var events []storage.OutboxEvent

		if isOpen, err := q.TryLockOutbox(ctx); err != nil {
			return err
		} else if !isOpen {
			return ErrOutboxLocked
		}

		var err error
		if events, err = q.GetUnpublishedOutboxEvents(ctx, r.send.BatchSize); err != nil {
			return err
		}

		if len(events) == 0 {
			return nil
		}

		messages, ids := disassembleEvents(events)
		if err = r.publisher.Publish(ctx, messages); err != nil {
			return err
		}

		if err = q.MarkOutboxEventsPublished(ctx, ids); err != nil {
			return err
		}

		sent = len(messages)

		return nil
	})

	if err != nil {
		if errors.Is(err, ErrOutboxLocked) {
			return 0, nil
		}
		return 0, err
	}

	return sent, nil
}

func disassembleEvents(events []storage.OutboxEvent) (messages []Message, ids []int64) {
	messages = make([]Message, 0, len(events))
	ids = make([]int64, 0, len(events))

	for _, e := range events {
		messages = append(messages, Message{
			Key:   e.PartitionKey,
			Value: e.Payload,
			Headers: map[string]string{
				"event_id":   e.EventID,
				"event_type": e.EventType,
			},
		})

		ids = append(ids, e.ID)
	}

	return messages, ids
}
