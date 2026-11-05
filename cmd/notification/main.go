package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Velgorion/equilibra/internal/config"
	"github.com/Velgorion/equilibra/internal/notification"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

const (
	pingTimeout = 5 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	_ = godotenv.Load() // ignore the error in the sense that we won't have a .env file in prod, only while local development

	cfg, err := config.LoadNotification()
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).With("component", "notification")

	logger.Info("config loaded")

	rdb, err := openRedis(cfg)
	if err != nil {
		return err
	}
	defer rdb.Close()

	logger.Info("redis is ready")

	client, err := openKafka(cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	logger.Info("kafka client is ready")

	hub := notification.NewHub()
	dedup := notification.NewRedisDeduper(rdb)
	publisher := notification.NewRedisPublisher(rdb)
	subscriber := notification.NewSubscriber(rdb, hub, logger)

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	metrics := notification.NewMetrics(reg, hub)

	proc := notification.NewProcessor(dedup, publisher, metrics, logger)

	consumer := notification.NewConsumer(client, proc, metrics, logger)

	// Register WSHandler for the 'GET /v1/ws' route
	notificationMux := http.NewServeMux()
	notificationMux.Handle("GET /v1/ws", notification.NewWSHandler(hub, logger))

	// Not configure other timeouts in the way that it's fine for a WebSocket connection
	// to be idle for a long time
	notificationSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Notification.Port),
		ReadHeaderTimeout: 5 * time.Second,
		Handler:           notificationMux,
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	metricsSrv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Notification.MetricsPort),
		Handler:      metricsMux,
		IdleTimeout:  time.Minute,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		ErrorLog:     slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	// Cancel the ctx when one the listed signal arrives
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Create errgroup for the concurrent operation of our services
	// (consumer, subscriber, server, shutdown) and an error occur in any one of them
	// will cause the cancellation of gctx
	g, gctx := errgroup.WithContext(ctx)

	// Start the consumer in the errgroup
	g.Go(func() error {
		err := consumer.Run(gctx)
		if err == nil && gctx.Err() == nil {
			return errors.New("consumer stopped unexpectedly")
		}
		return err
	})

	// Run the subscriber in the errgroup
	g.Go(func() error {
		err := subscriber.Run(gctx)
		if err == nil && gctx.Err() == nil {
			return errors.New("subscriber stopped unexpectedly")
		}

		return err
	})

	// Start the server in the errgroup
	g.Go(func() error {
		if err := notificationSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	// Start the metrics server in the errgroup
	g.Go(func() error {
		if err := metricsSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	// Wait for the cancellation of context, then close the ws-connections and stop
	// both servers
	g.Go(func() error {
		<-gctx.Done()

		logger.Info("shutting down server")

		hub.CloseAll()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		return errors.Join(
			notificationSrv.Shutdown(shutdownCtx),
			metricsSrv.Shutdown(shutdownCtx),
		)
	})

	logger.Info("notification is running", "addr", notificationSrv.Addr)
	return g.Wait()
}

func openKafka(cfg config.NotificationConfig) (*kgo.Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Kafka.Brokers...),
		kgo.ConsumerGroup(cfg.Notification.ConsumerGroup),
		kgo.ConsumeTopics(cfg.Kafka.Topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()

	if err := client.Ping(pingCtx); err != nil {
		return nil, err
	}

	return client, nil
}

func openRedis(cfg config.NotificationConfig) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr})

	pingCtx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()

	if err := rdb.Ping(pingCtx).Err(); err != nil {
		return nil, err
	}

	return rdb, nil
}
