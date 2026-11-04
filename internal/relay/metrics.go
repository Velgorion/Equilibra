package relay

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	published prometheus.Counter   // relay_events_published_total
	errors    prometheus.Counter   // relay_batch_errors_total
	duration  prometheus.Histogram // relay_batch_publish_duration_seconds
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		published: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "relay_events_published_total",
			Help: "Outbox events published to Kafka.",
		}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "relay_batch_errors_total",
			Help: "Outbox batches that failed.",
		}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "relay_batch_publish_duration_seconds",
			Help: "Time distribution for publishing a batch of events to Kafka.",
		}),
	}

	reg.MustRegister(
		m.published,
		m.errors,
		m.duration,
	)

	return m
}
