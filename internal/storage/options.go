package storage

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Option interface {
	apply(*pgxpool.Config)
}

func WithQueryTracer(tracer pgx.QueryTracer) Option {
	return tracerOption{tracer: tracer}
}

type tracerOption struct {
	tracer pgx.QueryTracer
}

func (t tracerOption) apply(cfg *pgxpool.Config) {
	cfg.ConnConfig.Tracer = t.tracer
}
