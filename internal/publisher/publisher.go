package publisher

import (
	"context"

	"github.com/Velgorion/equilibra/internal/relay"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaProducer struct {
	client *kgo.Client
}

func New(client *kgo.Client) *KafkaProducer {
	return &KafkaProducer{
		client: client,
	}
}

func (k *KafkaProducer) Publish(ctx context.Context, msgs []relay.Message) error {
	if err := k.client.ProduceSync(ctx, newRecords(msgs)...).FirstErr(); err != nil {
		return err
	}

	return nil
}

func newRecords(msgs []relay.Message) []*kgo.Record {
	recs := make([]*kgo.Record, 0, len(msgs))

	for _, m := range msgs {
		headers := make([]kgo.RecordHeader, 0, len(m.Headers))

		for k, v := range m.Headers {
			headers = append(headers, kgo.RecordHeader{
				Key:   k,
				Value: []byte(v),
			})
		}

		recs = append(recs, &kgo.Record{
			Key:     []byte(m.Key),
			Value:   m.Value,
			Headers: headers,
		})
	}

	return recs
}
