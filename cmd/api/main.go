package main

import (
	"context"
	"log"
	"log/slog"
	"os"

	"github.com/Velgorion/equilibra/internal/config"
	"github.com/Velgorion/equilibra/internal/handler"
	"github.com/Velgorion/equilibra/internal/service"
	"github.com/Velgorion/equilibra/internal/storage"

	"github.com/joho/godotenv"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	_ = godotenv.Load() // ignore the error in the sense that we won't have a .env file in prod, only while local development

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("component", "api")

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

	store := storage.New(db)
	svc := service.New(store)
	api := handler.New(svc, handler.BuildInfo{Env: cfg.Env, Version: cfg.Version}, logger)

	return serve(cfg, logger, api.Routes())
}
