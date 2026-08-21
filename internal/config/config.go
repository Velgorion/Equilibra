package config

import (
	"time"

	"github.com/caarlos0/env/v11"
)

type Database struct {
	DSN                   string        `env:"DB_DSN,required,notEmpty"`
	MaxConns              int32         `env:"DB_MAX_CONNS" envDefault:"10"`
	MinConns              int32         `env:"DB_MIN_CONNS" envDefault:"0"`
	MinIdleConns          int32         `env:"DB_MIN_IDLE_CONNS" envDefault:"0"`
	MaxConnLifetime       time.Duration `env:"DB_MAX_CONN_LIFETIME" envDefault:"1h"`
	MaxConnIdleTime       time.Duration `env:"DB_MAX_CONN_IDLE_TIME" envDefault:"30m"`
	MaxConnLifetimeJitter time.Duration `env:"DB_MAX_CONN_LIFETIME_JITTER" envDefault:"5m"`
}

type Config struct {
	Database Database
}

func Load() (Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
