package notification

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const (
	pubsubTestTimeout = 5 * time.Second
	pubsubTestTick    = 5 * time.Millisecond
)

type sendCall struct {
	accountID int64
	msg       []byte
}

// fakeSender records every message handed to it. Send is called from the
// subscriber's goroutine.
type fakeSender struct {
	mu    sync.Mutex
	calls []sendCall
}

var _ Sender = (*fakeSender)(nil)

func (s *fakeSender) Send(accountID int64, msg []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls = append(s.calls, sendCall{accountID: accountID, msg: msg})
}

// sent returns a copy of the calls, so the caller can read it without the lock.
func (s *fakeSender) sent() []sendCall {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]sendCall{}, s.calls...)
}

func newTestRedisClient(t *testing.T, mr *miniredis.Miniredis) *redis.Client {
	t.Helper()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { rdb.Close() })

	return rdb
}

// startSubscriber runs a subscriber in its own goroutine and returns once it is
// subscribed
func startSubscriber(t *testing.T, mr *miniredis.Miniredis, sender Sender) (context.CancelFunc, <-chan error) {
	t.Helper()

	s := NewSubscriber(newTestRedisClient(t, mr), sender, slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	require.Eventually(t, func() bool {
		return mr.PubSubNumSub(notificationsChannel)[notificationsChannel] == 1
	}, pubsubTestTimeout, pubsubTestTick)

	return cancel, errCh
}

func waitResult(t *testing.T, errCh <-chan error) error {
	t.Helper()

	select {
	case err := <-errCh:
		return err
	case <-time.After(pubsubTestTimeout):
		t.Fatal("subscriber did not stop")
		return nil
	}
}

func TestPubSubDeliversMessage(t *testing.T) {
	mr := miniredis.RunT(t)
	sender := &fakeSender{}
	startSubscriber(t, mr, sender)

	pub := NewRedisPublisher(newTestRedisClient(t, mr))
	msg := entryPayload(t, 10)

	require.NoError(t, pub.Publish(t.Context(), 10, msg))

	require.Eventually(t, func() bool { return len(sender.sent()) == 1 }, pubsubTestTimeout, pubsubTestTick)

	// the message reaches the hub as it was published, for the same account
	got := sender.sent()[0]
	require.Equal(t, int64(10), got.accountID)
	require.JSONEq(t, string(msg), string(got.msg))
}

func TestPubSubSkipsMalformedMessage(t *testing.T) {
	mr := miniredis.RunT(t)
	sender := &fakeSender{}
	startSubscriber(t, mr, sender)

	// RedisPublisher always writes a valid envelope, so the broken one goes
	// straight to Redis
	mr.Publish(notificationsChannel, "{not json")

	pub := NewRedisPublisher(newTestRedisClient(t, mr))
	require.NoError(t, pub.Publish(t.Context(), 20, entryPayload(t, 20)))

	// the subscriber survives the broken message and handles the next one
	require.Eventually(t, func() bool { return len(sender.sent()) == 1 }, pubsubTestTimeout, pubsubTestTick)
	require.Equal(t, int64(20), sender.sent()[0].accountID)
}

func TestPubSubPublishWithoutSubscribers(t *testing.T) {
	mr := miniredis.RunT(t)
	pub := NewRedisPublisher(newTestRedisClient(t, mr))

	// no copy of the service is running, so there is no one to notify
	require.NoError(t, pub.Publish(t.Context(), 10, entryPayload(t, 10)))
}

func TestPubSubPublishRedisUnreachable(t *testing.T) {
	mr := miniredis.RunT(t)
	pub := NewRedisPublisher(newTestRedisClient(t, mr))
	mr.Close()

	// the processor must see the failure, otherwise it marks the event as
	// notified and the notification is lost
	require.Error(t, pub.Publish(t.Context(), 10, entryPayload(t, 10)))
}

func TestSubscriberStopsOnCancel(t *testing.T) {
	mr := miniredis.RunT(t)
	cancel, errCh := startSubscriber(t, mr, &fakeSender{})

	cancel()

	require.NoError(t, waitResult(t, errCh))

	// the subscription is closed
	require.Eventually(t, func() bool {
		return mr.PubSubNumSub(notificationsChannel)[notificationsChannel] == 0
	}, pubsubTestTimeout, pubsubTestTick)
}

func TestSubscriberCancelledBeforeSubscribe(t *testing.T) {
	mr := miniredis.RunT(t)
	s := NewSubscriber(newTestRedisClient(t, mr), &fakeSender{}, slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.NoError(t, s.Run(ctx))
}

func TestSubscriberRedisUnreachable(t *testing.T) {
	mr := miniredis.RunT(t)
	s := NewSubscriber(newTestRedisClient(t, mr), &fakeSender{}, slog.New(slog.DiscardHandler))
	mr.Close()

	ctx, cancel := context.WithTimeout(t.Context(), pubsubTestTimeout)
	defer cancel()

	require.Error(t, s.Run(ctx))
}
