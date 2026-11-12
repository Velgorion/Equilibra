package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/Velgorion/equilibra/internal/config"
	"github.com/Velgorion/equilibra/internal/handler"
	"github.com/Velgorion/equilibra/internal/logging"
	"github.com/Velgorion/equilibra/internal/service"
	"github.com/Velgorion/equilibra/internal/service/tracing"
	"github.com/Velgorion/equilibra/internal/storage"
	"github.com/Velgorion/equilibra/internal/telemetry"
	"github.com/exaring/otelpgx"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/joho/godotenv"
)

const serviceName = "api"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	_ = godotenv.Load() // ignore the error in the sense that we won't have a .env file in prod, only while local development

	logger := slog.New(logging.WithTrace(slog.NewJSONHandler(os.Stdout, nil))).With("component", serviceName)

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger.Info("config loaded")

	db, err := storage.Open(context.Background(), cfg.Database,
		storage.WithQueryTracer(otelpgx.NewTracer(otelpgx.WithQuerySpanNamePrefix())))

	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	logger.Info("database connection pool established")

	tracerShutdown, err := telemetry.InitTracer(context.Background(), telemetry.Config{
		ServiceName:       serviceName,
		CollectorEndpoint: cfg.Tracing.CollectorEndpoint,
		ServiceVersion:    cfg.Version,
		Environment:       cfg.Env,
		SamplingRatio:     cfg.Tracing.SamplingRatio,
	})

	if err != nil {
		return fmt.Errorf("init oltp tracer: %w", err)
	}

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracerShutdown(shutdownCtx); err != nil {
			logger.Error("shutdown tracer", "error", err)
		}
	}()

	store := storage.New(db)
	svc := tracing.NewTracedService(service.New(store))
	api := handler.New(svc, handler.BuildInfo{Env: cfg.Env, Version: cfg.Version}, logger)

	return serve(cfg, logger, otelhttp.NewHandler(api.Routes(), "api"))
}
