package notification

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/Velgorion/equilibra/internal/events"
	"github.com/stretchr/testify/require"
)

// fakeDeduper plays a scripted sequence of claim results and records the calls.
type fakeDeduper struct {
	// set by a test
	results  []ClaimResult // returned in order; the last one repeats once they run out
	claimErr error
	doneErr  error

	// recorded by the fake
	claims int
	dones  int
}

var _ Deduper = (*fakeDeduper)(nil)

func (d *fakeDeduper) Claim(ctx context.Context, eventID string) (ClaimResult, error) {
	d.claims++
	if d.claimErr != nil {
		return ClaimUnknown, d.claimErr
	}

	i := min(d.claims, len(d.results)) - 1
	return d.results[i], nil
}

func (d *fakeDeduper) Done(ctx context.Context, eventID string) error {
	d.dones++
	return d.doneErr
}

type sendCall struct {
	accountID int64
	msg       []byte
}

// fakeSender records every notification handed to it.
type fakeSender struct {
	calls []sendCall
}

var _ Sender = (*fakeSender)(nil)

func (s *fakeSender) Send(accountID int64, msg []byte) {
	s.calls = append(s.calls, sendCall{accountID: accountID, msg: msg})
}

const testEventID = "event_id"

func newTestProcessor() (*Processor, *fakeDeduper, *fakeSender) {
	d := &fakeDeduper{results: []ClaimResult{ClaimAcquired}}
	s := &fakeSender{}

	p := NewProcessor(d, s, slog.New(slog.DiscardHandler))
	p.retryInterval = time.Millisecond

	return p, d, s
}

// entryPayload is the body of an entry.posted event as the relay publishes it.
func entryPayload(t *testing.T, accountID int64) []byte {
	t.Helper()

	data, err := json.Marshal(events.EntryPosted{
		AccountID:       accountID,
		TransactionID:   42,
		TransactionType: "transfer",
		Amount:          500,
		Currency:        "RUB",
		CounterpartyID:  9,
		OccurredAt:      time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	return data
}

func TestUnitHandleSendsNewEvent(t *testing.T) {
	p, d, s := newTestProcessor()

	err := p.Handle(t.Context(), testEventID, entryPayload(t, 10))
	require.NoError(t, err)

	require.Len(t, s.calls, 1)
	require.Equal(t, int64(10), s.calls[0].accountID)

	var got map[string]any
	require.NoError(t, json.Unmarshal(s.calls[0].msg, &got))
	require.Equal(t, testEventID, got["event_id"])
	require.EqualValues(t, 10, got["account_id"])
	require.EqualValues(t, 500, got["amount"])
	require.EqualValues(t, 9, got["counterparty_id"])

	require.Equal(t, 1, d.dones)
}

func TestUnitHandleSkipsDoneEvent(t *testing.T) {
	p, d, s := newTestProcessor()
	d.results = []ClaimResult{ClaimDone}

	err := p.Handle(t.Context(), testEventID, entryPayload(t, 10))
	require.NoError(t, err)

	// a repeat of an event that was already sent
	require.Empty(t, s.calls)
	require.Zero(t, d.dones)
}

func TestUnitHandleWaitsForBusyEvent(t *testing.T) {
	p, d, s := newTestProcessor()

	d.results = []ClaimResult{ClaimBusy, ClaimBusy, ClaimAcquired}

	err := p.Handle(t.Context(), testEventID, entryPayload(t, 10))
	require.NoError(t, err)

	require.Equal(t, 3, d.claims)
	require.Len(t, s.calls, 1)
}

func TestUnitHandleStopsWaitingOnCancel(t *testing.T) {
	p, d, s := newTestProcessor()
	d.results = []ClaimResult{ClaimBusy}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := p.Handle(ctx, testEventID, entryPayload(t, 10))

	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, s.calls)
	require.Zero(t, d.dones)
}

func TestUnitHandleClaimFails(t *testing.T) {
	p, d, s := newTestProcessor()
	d.claimErr = errors.New("redis is down")

	err := p.Handle(t.Context(), testEventID, entryPayload(t, 10))

	require.ErrorIs(t, err, d.claimErr)
	require.Empty(t, s.calls)
}

func TestUnitHandleUnexpectedClaimResult(t *testing.T) {
	p, d, s := newTestProcessor()
	d.results = []ClaimResult{ClaimUnknown}

	err := p.Handle(t.Context(), testEventID, entryPayload(t, 10))

	require.Error(t, err)
	require.Empty(t, s.calls)
}

func TestUnitHandleSkipsBadPayload(t *testing.T) {
	p, d, s := newTestProcessor()

	err := p.Handle(t.Context(), testEventID, []byte(`{not json`))

	// a broken message will never get better: skip it instead of blocking the partition
	require.NoError(t, err)
	require.Empty(t, s.calls)

	require.Equal(t, 1, d.dones)
}

func TestUnitHandleSkipsEmptyEventID(t *testing.T) {
	p, d, s := newTestProcessor()

	err := p.Handle(t.Context(), "", entryPayload(t, 10))

	require.NoError(t, err)
	require.Zero(t, d.claims)
	require.Empty(t, s.calls)
}

func TestUnitHandleDoneFailsAfterSend(t *testing.T) {
	p, d, s := newTestProcessor()
	d.doneErr = errors.New("redis is down")

	err := p.Handle(t.Context(), testEventID, entryPayload(t, 10))

	// the notification is already delivered: returning the error would make the
	// consumer retry the record and send it a second time
	require.NoError(t, err)
	require.Len(t, s.calls, 1)
}
