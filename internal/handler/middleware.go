package handler

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func (a *API) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		start := time.Now()

		next.ServeHTTP(ww, r)

		status := ww.Status()

		level := slog.LevelInfo
		if status >= 500 {
			level = slog.LevelError
		}

		a.logger.Log(r.Context(), level, "request",
			"method", r.Method,
			"route", chi.RouteContext(r.Context()).RoutePattern(),
			"status", status,
			"duration_ms", float64(time.Since(start).Microseconds())/1000,
		)

	})
}
