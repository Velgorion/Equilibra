package notification

import "github.com/prometheus/client_golang/prometheus"

const (
	labelValuePublished = "published"
	labelValueDuplicate = "duplicate"
	labelValueMalformed = "malformed"
	labelValueNoEventID = "no_event_id"
)

type Metrics struct {
	events *prometheus.CounterVec // notification_events_total{result}
	errors prometheus.Counter     // notification_handle_errors_total
}

func NewMetrics(reg prometheus.Registerer, hub *Hub) *Metrics {
	m := &Metrics{
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_events_total",
			Help: "Events read from Kafka, partitioned by how they were handled.",
		}, []string{"result"}),

		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "notification_handle_errors_total",
			Help: "Notification handles of events that failed",
		}),
	}

	m.events.WithLabelValues(labelValuePublished)
	m.events.WithLabelValues(labelValueDuplicate)
	m.events.WithLabelValues(labelValueMalformed)
	m.events.WithLabelValues(labelValueNoEventID)

	reg.MustRegister(
		prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name: "notification_ws_connections",
				Help: "Current number of WebSocket-connections to the Notification",
			},
			func() float64 {
				return float64(hub.Connections())
			}),
		m.errors,
		m.events,
	)

	return m
}
