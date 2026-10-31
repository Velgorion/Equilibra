package notification

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestDedup(t *testing.T) {
	const (
		eventID = "eventID"
	)

	mr, r := newTestDeduper(t)

	// Upon the first access, we create an entry with this eventID key,
	// so we get ClaimAcquired
	res, err := r.Claim(t.Context(), eventID)
	require.NoError(t, err)
	require.Equal(t, ClaimAcquired, res)

	val, err := mr.Get(dedupKey(eventID))
	require.NoError(t, err)
	require.Equal(t, stateProcessing, val)

	// the reservation must be short: if its owner dies, the event is retried soon
	require.Equal(t, leaseTTL, mr.TTL(dedupKey(eventID)))

	// Now there is an entry in Redis with a 30-second expiration time,
	// so we get ClaimBusy
	res, err = r.Claim(t.Context(), eventID)
	require.NoError(t, err)
	require.Equal(t, ClaimBusy, res)

	// Mark the entry as Done
	err = r.Done(t.Context(), eventID)
	require.NoError(t, err)

	// After marking get ClaimDone
	res, err = r.Claim(t.Context(), eventID)
	require.NoError(t, err)
	require.Equal(t, ClaimDone, res)

	val, err = mr.Get(dedupKey(eventID))
	require.NoError(t, err)
	require.Equal(t, stateDone, val)

	// Done replaces the short reservation with the long dedup window
	require.Equal(t, doneTTL, mr.TTL(dedupKey(eventID)))
}

func TestDedupExpirationOfReservation(t *testing.T) {
	const (
		eventID = "eventID"
	)

	mr, r := newTestDeduper(t)

	res, err := r.Claim(t.Context(), eventID)
	require.NoError(t, err)
	require.Equal(t, ClaimAcquired, res)

	mr.FastForward(leaseTTL + time.Second)

	// Once the reservation period has expired,
	// we can once again create an entry with this key
	res, err = r.Claim(t.Context(), eventID)
	require.NoError(t, err)
	require.Equal(t, ClaimAcquired, res)
}

func TestDedupExpirationOfExpirationTime(t *testing.T) {
	const (
		eventID = "eventID"
	)

	mr, r := newTestDeduper(t)

	res, err := r.Claim(t.Context(), eventID)
	require.NoError(t, err)
	require.Equal(t, ClaimAcquired, res)

	require.NoError(t, r.Done(t.Context(), eventID))

	// a repeat inside the window is filtered out
	mr.FastForward(doneTTL - time.Minute)
	res, err = r.Claim(t.Context(), eventID)
	require.NoError(t, err)
	require.Equal(t, ClaimDone, res)

	// after the window the event is forgotten and can be claimed again
	mr.FastForward(2 * time.Minute)

	res, err = r.Claim(t.Context(), eventID)
	require.NoError(t, err)
	require.Equal(t, ClaimAcquired, res)
}

func TestDedupDifferentKeys(t *testing.T) {
	const (
		eventID1 = "event1"
		eventID2 = "event2"
	)

	_, r := newTestDeduper(t)

	res, err := r.Claim(t.Context(), eventID1)
	require.NoError(t, err)
	require.Equal(t, ClaimAcquired, res)

	res, err = r.Claim(t.Context(), eventID2)
	require.NoError(t, err)
	require.Equal(t, ClaimAcquired, res)
}

func TestDedupUnreachable(t *testing.T) {
	const (
		eventID = "eventID"
	)

	mr, r := newTestDeduper(t)

	mr.Close()

	res, err := r.Claim(t.Context(), eventID)
	require.Error(t, err)
	require.Equal(t, ClaimUnknown, res)
}

func newTestDeduper(t *testing.T) (*miniredis.Miniredis, *RedisDeduper) {
	t.Helper()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { rdb.Close() })

	return mr, NewRedisDeduper(rdb)
}
