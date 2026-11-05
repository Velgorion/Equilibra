package notification

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/Velgorion/equilibra/internal/events"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

type publishCall struct {
	accountID int64
	msg       []byte
}

// fakePublisher records every notification handed to it, or fails with err if it is set.
type fakePublisher struct {
	err   error
	calls []publishCall
}

var _ Publisher = (*fakePublisher)(nil)

func (pub *fakePublisher) Publish(ctx context.Context, accountID int64, msg []byte) error {
	if pub.err != nil {
		return pub.err
	}
	pub.calls = append(pub.calls, publishCall{accountID: accountID, msg: msg})
	return nil
}

const testEventID = "event_id"

func newTestProcessor() (*Processor, *fakeDeduper, *fakePublisher) {
	d := &fakeDeduper{results: []ClaimResult{ClaimAcquired}}
	pub := &fakePublisher{}

	p := NewProcessor(d, pub, NewMetrics(prometheus.NewRegistry(), NewHub()), slog.New(slog.DiscardHandler))
	p.retryInterval = time.Millisecond

	return p, d, pub
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

// results is the expected value of notification_events_total for each result.
// Any result left out is expected to be 0.
type results map[string]float64

// requireEvents checks every result, not only the expected one, so that an event
// counted under the wrong result is caught too.
func requireEvents(t *testing.T, m *Metrics, want results) {
	t.Helper()

	for _, result := range []string{labelValuePublished, labelValueDuplicate, labelValueMalformed, labelValueNoEventID} {
		got := testutil.ToFloat64(m.events.WithLabelValues(result))
		require.Equal(t, want[result], got, "notification_events_total{result=%q}", result)
	}
}

func TestUnitHandleSendsNewEvent(t *testing.T) {
	p, d, pub := newTestProcessor()

	err := p.Handle(t.Context(), testEventID, entryPayload(t, 10))
	require.NoError(t, err)

	require.Len(t, pub.calls, 1)
	require.Equal(t, int64(10), pub.calls[0].accountID)

	var got map[string]any
	require.NoError(t, json.Unmarshal(pub.calls[0].msg, &got))
	require.Equal(t, testEventID, got["event_id"])
	require.EqualValues(t, 10, got["account_id"])
	require.EqualValues(t, 500, got["amount"])
	require.EqualValues(t, 9, got["counterparty_id"])

	require.Equal(t, 1, d.dones)
	requireEvents(t, p.metrics, results{labelValuePublished: 1})
}

func TestUnitHandleSkipsDoneEvent(t *testing.T) {
	p, d, pub := newTestProcessor()
	d.results = []ClaimResult{ClaimDone}

	err := p.Handle(t.Context(), testEventID, entryPayload(t, 10))
	require.NoError(t, err)

	// a repeat of an event that was already sent
	require.Empty(t, pub.calls)
	require.Zero(t, d.dones)
	requireEvents(t, p.metrics, results{labelValueDuplicate: 1})
}

func TestUnitHandleWaitsForBusyEvent(t *testing.T) {
	p, d, pub := newTestProcessor()

	d.results = []ClaimResult{ClaimBusy, ClaimBusy, ClaimAcquired}

	err := p.Handle(t.Context(), testEventID, entryPayload(t, 10))
	require.NoError(t, err)

	require.Equal(t, 3, d.claims)
	require.Len(t, pub.calls, 1)

	requireEvents(t, p.metrics, results{labelValuePublished: 1})
}

func TestUnitHandleStopsWaitingOnCancel(t *testing.T) {
	p, d, pub := newTestProcessor()
	d.results = []ClaimResult{ClaimBusy}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := p.Handle(ctx, testEventID, entryPayload(t, 10))

	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, pub.calls)
	require.Zero(t, d.dones)
	requireEvents(t, p.metrics, results{})
}

func TestUnitHandleClaimFails(t *testing.T) {
	p, d, pub := newTestProcessor()
	d.claimErr = errors.New("redis is down")

	err := p.Handle(t.Context(), testEventID, entryPayload(t, 10))

	require.ErrorIs(t, err, d.claimErr)
	require.Empty(t, pub.calls)
	requireEvents(t, p.metrics, results{})
}

func TestUnitHandleUnexpectedClaimResult(t *testing.T) {
	p, d, pub := newTestProcessor()
	d.results = []ClaimResult{ClaimUnknown}

	err := p.Handle(t.Context(), testEventID, entryPayload(t, 10))

	require.Error(t, err)
	require.Empty(t, pub.calls)
	requireEvents(t, p.metrics, results{})
}

func TestUnitHandleSkipsBadPayload(t *testing.T) {
	p, d, pub := newTestProcessor()

	err := p.Handle(t.Context(), testEventID, []byte(`{not json`))

	// a broken message will never get better: skip it instead of blocking the partition
	require.NoError(t, err)
	require.Empty(t, pub.calls)

	require.Equal(t, 1, d.dones)
	requireEvents(t, p.metrics, results{labelValueMalformed: 1})
}

func TestUnitHandleSkipsEmptyEventID(t *testing.T) {
	p, d, pub := newTestProcessor()

	err := p.Handle(t.Context(), "", entryPayload(t, 10))

	require.NoError(t, err)
	require.Zero(t, d.claims)
	require.Empty(t, pub.calls)
	requireEvents(t, p.metrics, results{labelValueNoEventID: 1})
}

func TestUnitHandleDoneFailsAfterSend(t *testing.T) {
	p, d, pub := newTestProcessor()
	d.doneErr = errors.New("redis is down")

	err := p.Handle(t.Context(), testEventID, entryPayload(t, 10))

	// the notification is already delivered: returning the error would make the
	// consumer retry the record and send it a second time
	require.NoError(t, err)
	require.Len(t, pub.calls, 1)

	// the notification went out, so it counts as published
	requireEvents(t, p.metrics, results{labelValuePublished: 1})
}

func TestUnitHandlePublishFails(t *testing.T) {
	p, d, pub := newTestProcessor()
	pub.err = errors.New("redis is down")

	err := p.Handle(t.Context(), testEventID, entryPayload(t, 10))

	// nobody got the notification: the consumer must retry the record, so the
	// event may not be marked as notified
	require.ErrorIs(t, err, pub.err)
	require.Zero(t, d.dones)
	requireEvents(t, p.metrics, results{})
}

func TestUnitHandleBadPayloadDoneFails(t *testing.T) {
	p, d, pub := newTestProcessor()
	d.doneErr = errors.New("redis is down")

	err := p.Handle(t.Context(), testEventID, []byte(`{not json`))

	// the consumer retries the record and the event comes here again, so it may
	// be counted as malformed only once the skip is recorded
	require.ErrorIs(t, err, d.doneErr)
	require.Empty(t, pub.calls)
	requireEvents(t, p.metrics, results{})
}
