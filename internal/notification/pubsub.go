package notification

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

const (
	notificationsChannel = "notifications"
)

type envelope struct {
	AccountID int64           `json:"account_id"`
	Message   json.RawMessage `json:"message"`
}

type RedisPublisher struct {
	rdb *redis.Client
}

var _ Publisher = (*RedisPublisher)(nil)

func NewRedisPublisher(rdb *redis.Client) *RedisPublisher {
	return &RedisPublisher{rdb: rdb}
}

func (r *RedisPublisher) Publish(ctx context.Context, accountID int64, msg []byte) error {
	data, err := json.Marshal(&envelope{AccountID: accountID, Message: msg})
	if err != nil {
		return err
	}

	return r.rdb.Publish(ctx, notificationsChannel, data).Err()
}

type Subscriber struct {
	rdb    *redis.Client
	sender Sender
	logger *slog.Logger
}

func NewSubscriber(rdb *redis.Client, s Sender, l *slog.Logger) *Subscriber {
	return &Subscriber{rdb: rdb, sender: s, logger: l}
}

func (s *Subscriber) Run(ctx context.Context) error {
	pubsub := s.rdb.Subscribe(ctx, notificationsChannel)
	defer pubsub.Close()

	_, err := pubsub.Receive(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	ch := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return nil

		case msg, ok := <-ch:
			if !ok {
				return errors.New("subscription closed")
			}

			var e envelope
			if err := json.Unmarshal([]byte(msg.Payload), &e); err != nil {
				s.logger.Warn("skip malformed message", "payload",
					msg.Payload, "channel", msg.Channel, "error", err.Error())

				continue
			}

			s.sender.Send(e.AccountID, e.Message)
		}
	}
}
