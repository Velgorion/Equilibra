package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Velgorion/equilibra/internal/config"
	"github.com/Velgorion/equilibra/internal/publisher"
	"github.com/Velgorion/equilibra/internal/relay"
	"github.com/Velgorion/equilibra/internal/storage"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kslog"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	_ = godotenv.Load() // ignore the error in the sense that we won't have a .env file in prod, only while local development

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("component", "relay")

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger.Info("config loaded")

	db, err := storage.Open(context.Background(), cfg.Database)
	if err != nil {
		return err
	}
	defer db.Close()

	logger.Info("database connection pool established")

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Kafka.Brokers...),
		kgo.DefaultProduceTopic(cfg.Kafka.Topic),
		kgo.WithLogger(kslog.New(slog.New(slog.NewTextHandler(os.Stdout, nil)).With("component", "kafka"))),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	store := storage.New(db)

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name: "relay_outbox_oldest_unpublished_age_seconds",
				Help: "Age in seconds of the oldest unpublished event in the Outbox table",
			},
			func() float64 {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				var secs float64
				err := store.WithTx(ctx, func(q storage.Querier) error {
					var err error
					secs, err = q.OldestUnpublishedOutboxAge(ctx)
					return err
				})

				if err != nil {
					logger.Warn("compute outbox age", "error", err)
					return math.NaN()
				}

				return secs
			},
		),
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	r := relay.New(
		store,
		publisher.New(client),
		relay.SendingParams{
			BatchSize: cfg.Relay.BatchSize,
			Interval:  cfg.Relay.Interval,
		},
		logger,
		relay.NewMetrics(reg),
	)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Relay.MetricsPort),
		Handler:      mux,
		IdleTimeout:  time.Minute,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		ErrorLog:     slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		err := r.Run(gctx)
		if err == nil && gctx.Err() == nil {
			return errors.New("relay stopped unexpectedly")
		}
		return err
	})

	g.Go(func() error {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	g.Go(func() error {
		<-gctx.Done()

		logger.Info("shutting down server")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})

	logger.Info("relay is running", "addr", srv.Addr)
	return g.Wait()
}
