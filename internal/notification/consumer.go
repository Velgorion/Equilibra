package notification

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Handler interface {
	Handle(ctx context.Context, eventID string, payload []byte) error
}

type Consumer struct {
	client        *kgo.Client
	handler       Handler
	logger        *slog.Logger
	retryInterval time.Duration // pause before retry after a temporal error
}

func NewConsumer(client *kgo.Client, h Handler, l *slog.Logger) *Consumer {
	return &Consumer{
		client:        client,
		handler:       h,
		logger:        l,
		retryInterval: 1 * time.Second,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}

		fetches.EachError(func(t string, i int32, err error) {
			c.logger.Warn("fetch err topic", "topic", t, "partition", i, "error", err)
		})

		var done []*kgo.Record
		for _, r := range fetches.Records() {
			if err := c.handle(ctx, r); err != nil {
				break
			}
			done = append(done, r)
		}

		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err := c.client.CommitRecords(commitCtx, done...)
		cancel()

		if err != nil {
			c.logger.Warn("commit records offset", "error", err)
		}

		if ctx.Err() != nil {
			return nil
		}
	}
}

func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) error {
	var eventID string
	for _, r := range rec.Headers {
		if r.Key == "event_id" {
			eventID = string(r.Value)
		}
	}

	for {
		err := c.handler.Handle(ctx, eventID, rec.Value)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return err

		case <-time.After(c.retryInterval):
		}
	}
}
