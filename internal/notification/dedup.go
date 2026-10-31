package notification

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type ClaimResult int

const (
	ClaimUnknown ClaimResult = iota
	ClaimAcquired
	ClaimDone
	ClaimBusy
)

const (
	keyPrefix = "notified:"

	// leaseTTL is how long a claimed event stays reserved. If its owner dies
	// before sending, the reservation expires and the event can be claimed again.
	leaseTTL = 30 * time.Second

	// doneTTL is the deduplication window: a repeat of a sent event arriving
	// later than this is no longer recognised.
	doneTTL = 24 * time.Hour

	// values stored under the key; Claim tells the two states apart
	stateProcessing = "processing"
	stateDone       = "done"
)

type Deduper interface {
	Claim(ctx context.Context, eventID string) (ClaimResult, error)
	Done(ctx context.Context, eventID string) error
}

type RedisDeduper struct {
	rdb *redis.Client
}

func NewRedisDeduper(rdb *redis.Client) *RedisDeduper {
	return &RedisDeduper{
		rdb: rdb,
	}
}

func (r *RedisDeduper) Claim(ctx context.Context, eventID string) (ClaimResult, error) {
	key := dedupKey(eventID)

	ok, err := r.rdb.SetNX(ctx, key, stateProcessing, leaseTTL).Result()
	if err != nil {
		return ClaimUnknown, err
	}

	if ok {
		return ClaimAcquired, nil
	}

	val, err := r.rdb.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ClaimBusy, nil
		}
		return ClaimUnknown, err
	}

	if val == stateDone {
		return ClaimDone, nil
	}

	return ClaimBusy, nil
}

func (r *RedisDeduper) Done(ctx context.Context, eventID string) error {
	return r.rdb.Set(ctx, dedupKey(eventID), stateDone, doneTTL).Err()
}

func dedupKey(eventID string) string {
	return keyPrefix + eventID
}
