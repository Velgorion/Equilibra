package relay

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/Velgorion/equilibra/internal/storage"
	"github.com/stretchr/testify/require"
)

// fakeQuerier implements only the outbox methods the relay is allowed to use.
type fakeQuerier struct {
	storage.Querier

	// set by a test
	lockGranted bool
	events      []storage.OutboxEvent

	// recorded by the fake
	lockCalled bool
	readCalled bool
	markedIDs  []int64
}

var _ storage.Querier = (*fakeQuerier)(nil)

func (q *fakeQuerier) TryLockOutbox(ctx context.Context) (bool, error) {
	q.lockCalled = true
	return q.lockGranted, nil
}

func (q *fakeQuerier) GetUnpublishedOutboxEvents(ctx context.Context, limit int) ([]storage.OutboxEvent, error) {
	q.readCalled = true
	if limit < len(q.events) {
		return q.events[:limit], nil
	}
	return q.events, nil
}

func (q *fakeQuerier) MarkOutboxEventsPublished(ctx context.Context, ids []int64) error {
	q.markedIDs = append(q.markedIDs, ids...)
	return nil
}

// fakePublisher records the messages it receives, or fails with err if it is set.
type fakePublisher struct {
	err      error
	calls    int
	messages []Message
}

var _ Publisher = (*fakePublisher)(nil)

func (p *fakePublisher) Publish(ctx context.Context, msgs []Message) error {
	p.calls++
	if p.err != nil {
		return p.err
	}
	p.messages = append(p.messages, msgs...)
	return nil
}

type fakeStore struct {
	querier   *fakeQuerier
	committed bool
}

var _ Storage = (*fakeStore)(nil)

func (s *fakeStore) WithTx(ctx context.Context, fn func(q storage.Querier) error) error {
	if err := fn(s.querier); err != nil {
		return err
	}
	s.committed = true
	return nil
}

func newTestRelay(t *testing.T) (*Relay, *fakeStore, *fakeQuerier, *fakePublisher) {
	t.Helper()

	q := &fakeQuerier{lockGranted: true}
	st := &fakeStore{querier: q}
	p := &fakePublisher{}
	r := New(st, p, SendingParams{BatchSize: 100, Interval: time.Millisecond},
		slog.New(slog.DiscardHandler))

	return r, st, q, p
}

func outboxEvent(id int64, eventID, key string) storage.OutboxEvent {
	return storage.OutboxEvent{
		ID:           id,
		EventID:      eventID,
		EventType:    "entry.posted",
		PartitionKey: key,
		Payload:      []byte(`{"n":` + eventID + `}`),
	}
}

func TestUnitPublishBatch(t *testing.T) {
	r, st, q, p := newTestRelay(t)

	q.events = []storage.OutboxEvent{
		outboxEvent(1, "11", "10"),
		outboxEvent(2, "22", "20"),
	}

	n, err := r.publishBatch(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.True(t, st.committed)

	// every event becomes a message with the partition key, the raw payload
	// and the envelope in the headers, in the order of the outbox
	require.Equal(t, []Message{
		{
			Key:     "10",
			Value:   []byte(`{"n":11}`),
			Headers: map[string]string{"event_id": "11", "event_type": "entry.posted"},
		},
		{
			Key:     "20",
			Value:   []byte(`{"n":22}`),
			Headers: map[string]string{"event_id": "22", "event_type": "entry.posted"},
		},
	}, p.messages)

	require.Equal(t, []int64{1, 2}, q.markedIDs)
}

func TestUnitPublishBatchPublishFails(t *testing.T) {
	r, st, q, p := newTestRelay(t)

	q.events = []storage.OutboxEvent{outboxEvent(1, "11", "10")}
	p.err = errors.New("broker is down")

	n, err := r.publishBatch(t.Context())
	require.ErrorIs(t, err, p.err)
	require.Zero(t, n)

	// nothing may be marked as published
	require.Empty(t, q.markedIDs)
	require.False(t, st.committed)
}

func TestUnitPublishBatchLockNotGranted(t *testing.T) {
	r, st, q, p := newTestRelay(t)

	q.lockGranted = false
	q.events = []storage.OutboxEvent{outboxEvent(1, "11", "10")}

	n, err := r.publishBatch(t.Context())

	// this is not an error, just a skipped turn
	require.NoError(t, err)
	require.Zero(t, n)

	require.True(t, q.lockCalled)
	require.False(t, q.readCalled)
	require.Zero(t, p.calls)
	require.Empty(t, q.markedIDs)
	require.False(t, st.committed)
}

func TestUnitPublishBatchRespectsBatchSize(t *testing.T) {
	r, _, q, p := newTestRelay(t)

	r.send.BatchSize = 2
	q.events = []storage.OutboxEvent{
		outboxEvent(1, "11", "10"),
		outboxEvent(2, "22", "10"),
		outboxEvent(3, "33", "10"),
	}

	n, err := r.publishBatch(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Len(t, p.messages, 2)
	require.Equal(t, []int64{1, 2}, q.markedIDs)
}

func TestUnitRunStopsWhenCancelled(t *testing.T) {
	r, _, q, _ := newTestRelay(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.NoError(t, r.Run(ctx))

	// a cancelled relay must not start a new batch
	require.False(t, q.lockCalled)
}
