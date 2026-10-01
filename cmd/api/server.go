package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Velgorion/equilibra/internal/config"
)

const shutdownTimeout = 30 * time.Second

func serve(cfg config.Config, logger *slog.Logger, h http.Handler) error {
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      h,
		IdleTimeout:  time.Minute,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		ErrorLog:     slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	shutdownError := make(chan error)

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

		sig := <-quit
		logger.Info("shutting down server", "signal", sig)

		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			shutdownError <- err
			return
		}

		shutdownError <- nil
	}()

	logger.Info("starting server", "addr", srv.Addr, "env", cfg.Env)

	err := srv.ListenAndServe()

	// Calling Shutdown() on the server makes ListenAndServe return
	// http.ErrServerClosed, which means the graceful shutdown has started and is
	// not an error.
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	if err := <-shutdownError; err != nil {
		return err
	}

	logger.Info("server stopped", "addr", srv.Addr)

	return nil
}
