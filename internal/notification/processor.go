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

// notification is the message sent to the browser over WebSocket.
type notification struct {
	EventID string `json:"event_id"`
	events.EntryPosted
}

// Processor is a core component tasked with checking events for duplicates (using Deduper)
// and sending them out as notifications (using Sender).
type Processor struct {
	dedup         Deduper
	sender        Sender
	logger        *slog.Logger
	retryInterval time.Duration
}

// NewProcessor creates a Processor.
func NewProcessor(d Deduper, s Sender, l *slog.Logger) *Processor {
	return &Processor{
		dedup:         d,
		sender:        s,
		logger:        l,
		retryInterval: 1 * time.Second,
	}
}

// Handle delivers the event with the given id to the connections of its account,
// unless it was already delivered.
//
// A nil error means the record is finished and its offset can be committed: the
// notification was sent, it had been sent before, or the record is unusable and
// was skipped. An error means a temporary failure, such as Redis being down: the
// caller must retry the same record and must not commit it.
//
// Handle blocks while another owner holds the event's reservation, and returns
// the context's error if ctx is cancelled meanwhile.
func (p *Processor) Handle(ctx context.Context, eventID string, payload []byte) error {

	if eventID == "" {
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
		return p.dedup.Done(ctx, eventID)
	}

	// Compose the notification
	msg, err := json.Marshal(&notification{EventID: eventID, EntryPosted: entryPosted})
	if err != nil {
		return err
	}

	// Send notification to the connection of AccountID
	p.sender.Send(entryPosted.AccountID, msg)
	p.logger.Debug("notification sent", "event_id", eventID, "account_id", entryPosted.AccountID)

	// Mark event as notified
	err = p.dedup.Done(ctx, eventID)
	if err != nil {
		p.logger.Warn("mark event as notified", "event_id", eventID, "error", err)
		return nil
	}

	return nil
}
