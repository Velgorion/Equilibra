package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/Velgorion/equilibra/internal/events"
)

type Sender interface {
	Send(accountID int64, msg []byte)
}

// Publisher publish the notifications for the accountID for futher sending by copies of notification
type Publisher interface {
	Publish(ctx context.Context, accountID int64, msg []byte) error
}

// notification is the message sent to the browser over WebSocket.
type notification struct {
	EventID string `json:"event_id"`
	events.EntryPosted
}

// Processor is a core component tasked with checking events for duplicates (using Deduper)
// and publishing them out as notifications (using Publisher).
type Processor struct {
	dedup         Deduper
	publisher     Publisher
	logger        *slog.Logger
	retryInterval time.Duration
	metrics       *Metrics
}

// NewProcessor creates a Processor.
func NewProcessor(d Deduper, p Publisher, m *Metrics, l *slog.Logger) *Processor {
	return &Processor{
		dedup:         d,
		publisher:     p,
		logger:        l,
		retryInterval: 1 * time.Second,
		metrics:       m,
	}
}

// Handle publishes the notification for the event with the given id, unless it was already published.
//
// A nil error means the record is finished and its offset can be committed: the
// notification was published, it had been published before, or the record is unusable and
// was skipped. An error means a temporary failure, such as Redis being down: the
// caller must retry the same record and must not commit it.
//
// Handle blocks while another owner holds the event's reservation, and returns
// the context's error if ctx is cancelled meanwhile.
func (p *Processor) Handle(ctx context.Context, eventID string, payload []byte) error {

	if eventID == "" {
		p.metrics.events.WithLabelValues(labelValueNoEventID).Inc()

		p.logger.Error("empty EventID")
		return nil
	}

claim:
	for {
		// Checks for duplicates
		res, err := p.dedup.Claim(ctx, eventID)
		if err != nil {
			return err
		}

		switch res {
		case ClaimAcquired:
			break claim

		case ClaimDone:
			p.metrics.events.WithLabelValues(labelValueDuplicate).Inc()

			p.logger.Debug("skip already notified event", "event_id", eventID)
			return nil

		// Trying to wait for p.retryInterval
		case ClaimBusy:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.retryInterval):
			}
		default:
			return fmt.Errorf("unexpected claim result: %d", res)
		}
	}

	var entryPosted events.EntryPosted

	err := json.Unmarshal(payload, &entryPosted)
	if err != nil {
		p.logger.Warn("unmarshal payload into events.EntryPosted", "event_id", eventID, "error", err)
		err = p.dedup.Done(ctx, eventID)
		if err != nil {
			return err
		}

		p.metrics.events.WithLabelValues(labelValueMalformed).Inc()

		return nil
	}

	// Compose the notification
	msg, err := json.Marshal(&notification{EventID: eventID, EntryPosted: entryPosted})
	if err != nil {
		return err
	}

	// Publish notification
	err = p.publisher.Publish(ctx, entryPosted.AccountID, msg)
	if err != nil {
		return err
	}

	p.metrics.events.WithLabelValues(labelValuePublished).Inc()

	p.logger.Debug("notification published", "event_id", eventID, "account_id", entryPosted.AccountID)

	// Mark event as notified
	err = p.dedup.Done(ctx, eventID)
	if err != nil {
		p.logger.Warn("mark event as published", "event_id", eventID, "error", err)
		return nil
	}

	return nil
}
