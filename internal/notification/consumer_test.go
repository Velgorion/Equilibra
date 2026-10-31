package notification

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	consumerTestTopic = "ledger.entries"
	consumerTestGroup = "notification"

	consumerTestTimeout = 5 * time.Second
)

var errTemporary = errors.New("redis is down")

// fakeHandler records every call and stops the consumer once it has seen enough.
type fakeHandler struct {
	// set by a test
	fail             func(eventID string, attempt int) bool // true makes this call fail
	stopAfterHandled int                                    // cancel after this many successful calls
	stopAfterCalls   int                                    // cancel after this many calls of any kind
	cancel           context.CancelFunc

	// recorded by the fake
	calls    []string // every call in order, failed ones included
	handled  []string // successfully handled event ids in order
	attempts map[string]int
}

var _ Handler = (*fakeHandler)(nil)

func (h *fakeHandler) Handle(ctx context.Context, eventID string, payload []byte) error {
	h.calls = append(h.calls, eventID)
	h.attempts[eventID]++

	defer func() {
		// a zero threshold means "not set", so it never stops the consumer
		stopByCalls := h.stopAfterCalls > 0 && len(h.calls) == h.stopAfterCalls
		stopByHandled := h.stopAfterHandled > 0 && len(h.handled) == h.stopAfterHandled
		if stopByCalls || stopByHandled {
			h.cancel()
		}
	}()

	if h.fail != nil && h.fail(eventID, h.attempts[eventID]) {
		return errTemporary
	}

	h.handled = append(h.handled, eventID)
	return nil
}

func newFakeCluster(t *testing.T) []string {
	t.Helper()

	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, consumerTestTopic))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	return cluster.ListenAddrs()
}

// produce writes one entry.posted record per event id, the way the relay does.
func produce(t *testing.T, brokers []string, eventIDs ...string) {
	t.Helper()

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(consumerTestTopic))
	require.NoError(t, err)
	defer producer.Close()

	for _, id := range eventIDs {
		err := producer.ProduceSync(t.Context(), &kgo.Record{
			Key:     []byte("10"),
			Value:   entryPayload(t, 10),
			Headers: []kgo.RecordHeader{{Key: "event_id", Value: []byte(id)}},
		}).FirstErr()
		require.NoError(t, err)
	}
}

// runConsumer runs one member of the consumer group until the handler stops it,
// then closes the client so that the member leaves the group before the test
// starts the next one.
func runConsumer(t *testing.T, brokers []string, h *fakeHandler) {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(consumerTestGroup),
		kgo.ConsumeTopics(consumerTestTopic),
		kgo.DisableAutoCommit(),
	)
	require.NoError(t, err)
	defer client.Close()

	ctx, cancel := context.WithTimeout(t.Context(), consumerTestTimeout)
	defer cancel()

	h.cancel = cancel
	if h.attempts == nil {
		h.attempts = make(map[string]int)
	}

	c := NewConsumer(client, h, slog.New(slog.DiscardHandler))
	c.retryInterval = time.Millisecond

	require.NoError(t, c.Run(ctx))
	require.NotErrorIs(t, ctx.Err(), context.DeadlineExceeded)
}

func TestConsumerHandlesRecordsInOrder(t *testing.T) {
	brokers := newFakeCluster(t)
	produce(t, brokers, "e1", "e2", "e3")

	h := &fakeHandler{stopAfterHandled: 3}
	runConsumer(t, brokers, h)

	// the event id is taken from the record header, in the order of the partition
	require.Equal(t, []string{"e1", "e2", "e3"}, h.handled)
}

func TestConsumerRetriesTheSameRecord(t *testing.T) {
	brokers := newFakeCluster(t)
	produce(t, brokers, "e1", "e2")

	// the first record hits a temporary failure twice, then goes through
	h := &fakeHandler{
		fail:             func(id string, attempt int) bool { return id == "e1" && attempt <= 2 },
		stopAfterHandled: 2,
	}
	runConsumer(t, brokers, h)

	// e1 is retried until it succeeds, and e2 waits for it: skipping e1 would
	// lose its notification
	require.Equal(t, []string{"e1", "e1", "e1", "e2"}, h.calls)
	require.Equal(t, []string{"e1", "e2"}, h.handled)
}

func TestConsumerCommitsHandledRecords(t *testing.T) {
	brokers := newFakeCluster(t)
	produce(t, brokers, "e1", "e2", "e3")

	first := &fakeHandler{stopAfterHandled: 3}
	runConsumer(t, brokers, first)
	require.Equal(t, []string{"e1", "e2", "e3"}, first.handled)

	produce(t, brokers, "e4")

	// the next member of the group starts from the committed offset: had the
	// first one not committed, it would get e1 again instead of e4
	second := &fakeHandler{stopAfterHandled: 1}
	runConsumer(t, brokers, second)
	require.Equal(t, []string{"e4"}, second.handled)
}

func TestConsumerStopCommitsOnlyHandledRecords(t *testing.T) {
	brokers := newFakeCluster(t)
	produce(t, brokers, "e1", "e2", "e3")

	// e1 goes through, e2 keeps failing, and the service is stopped while it
	// is being retried
	first := &fakeHandler{
		fail:           func(id string, _ int) bool { return id == "e2" },
		stopAfterCalls: 3, // e1, e2, e2
	}
	runConsumer(t, brokers, first)
	require.Equal(t, []string{"e1"}, first.handled)

	// only e1 may be committed. Committing the whole fetch would make e2 and e3
	// count as read and their notifications would be lost
	second := &fakeHandler{stopAfterHandled: 2}
	runConsumer(t, brokers, second)
	require.Equal(t, []string{"e2", "e3"}, second.handled)
}

func TestConsumerRecordWithoutEventID(t *testing.T) {
	brokers := newFakeCluster(t)

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(consumerTestTopic))
	require.NoError(t, err)
	defer producer.Close()

	require.NoError(t, producer.ProduceSync(t.Context(), &kgo.Record{Value: entryPayload(t, 10)}).FirstErr())

	h := &fakeHandler{stopAfterHandled: 1}
	runConsumer(t, brokers, h)

	// a missing header reaches the handler as an empty id, and the processor
	// decides what to do with it
	require.Equal(t, []string{""}, h.handled)
}
