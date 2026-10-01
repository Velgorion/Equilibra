package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/Velgorion/equilibra/internal/config"
	"github.com/Velgorion/equilibra/internal/handler"
	"github.com/Velgorion/equilibra/internal/service"
	"github.com/Velgorion/equilibra/internal/storage"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	_ = godotenv.Load() // ignore the error in the sense that we won't have a .env file in prod, only while local development

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger.Info("config loaded")

	db, err := openDB(cfg)
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

func openDB(cfg config.Config) (*pgxpool.Pool, error) {
	dbConfig, err := pgxpool.ParseConfig(cfg.Database.DSN)
	if err != nil {
		return nil, err
	}

	dbConfig.MaxConnLifetime = cfg.Database.MaxConnLifetime
	dbConfig.MaxConnIdleTime = cfg.Database.MaxConnIdleTime
	dbConfig.MaxConns = cfg.Database.MaxConns
	dbConfig.MinConns = cfg.Database.MinConns
	dbConfig.MinIdleConns = cfg.Database.MinIdleConns
	dbConfig.MaxConnLifetimeJitter = cfg.Database.MaxConnLifetimeJitter

	dbpool, err := pgxpool.NewWithConfig(context.Background(), dbConfig)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = dbpool.Ping(ctx)
	if err != nil {
		dbpool.Close()
		return nil, err
	}

	return dbpool, nil
}
