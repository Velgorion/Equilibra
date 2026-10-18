package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Velgorion/equilibra/internal/config"
	"github.com/Velgorion/equilibra/internal/publisher"
	"github.com/Velgorion/equilibra/internal/relay"
	"github.com/Velgorion/equilibra/internal/storage"
	"github.com/joho/godotenv"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kslog"
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

	r := relay.New(
		storage.New(db),
		publisher.New(client),
		relay.SendingParams{
			BatchSize: cfg.Relay.BatchSize,
			Interval:  cfg.Relay.Interval,
		},
		logger,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	return r.Run(ctx)
}
