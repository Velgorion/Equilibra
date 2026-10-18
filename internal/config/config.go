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

type Kafka struct {
	Brokers []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"kafka:19092"`
	Topic   string   `env:"KAFKA_TOPIC" envDefault:"ledger.entries"`
}

// Relay service will take events from the outbox table and send them to Kafka
type Relay struct {
	// The interval at which the relay will receive new events
	Interval time.Duration `env:"RELAY_INTERVAL" envDefault:"500ms"`

	// How many events the relay will take at one time
	BatchSize int `env:"RELAY_BATCH_SIZE" envDefault:"100"`
}

type Config struct {
	Database Database
	Kafka    Kafka
	Relay    Relay
	Env      string `env:"ENV,required"`
	Version  string `env:"VERSION,required"`
	Port     int    `env:"PORT" envDefault:"8080"`
}

func Load() (Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
