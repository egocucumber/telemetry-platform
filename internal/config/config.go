package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type Common struct {
	ServiceName string `env:"SERVICE_NAME"`
	Env         string `env:"ENV" envDefault:"dev"`
	LogLevel    string `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat   string `env:"LOG_FORMAT" envDefault:"json"`

	AdminAddr string `env:"ADMIN_ADDR" envDefault:":9090"`

	OTLPEndpoint string  `env:"OTLP_ENDPOINT" envDefault:""`
	TraceSample  float64 `env:"TRACE_SAMPLE" envDefault:"1.0"`

	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"15s"`
}

type Kafka struct {
	Brokers  []string `env:"KAFKA_BROKERS" envDefault:"localhost:9092" envSeparator:","`
	ClientID string   `env:"KAFKA_CLIENT_ID" envDefault:""`
}

type Redis struct {
	Addr     string `env:"REDIS_ADDR" envDefault:"localhost:6379"`
	Password string `env:"REDIS_PASSWORD" envDefault:""`
	DB       int    `env:"REDIS_DB" envDefault:"0"`
}

type Postgres struct {
	DSN      string `env:"POSTGRES_DSN" envDefault:"postgres://telemetry:telemetry@localhost:5432/telemetry?sslmode=disable"`
	MaxConns int32  `env:"POSTGRES_MAX_CONNS" envDefault:"8"`
	Migrate  bool   `env:"POSTGRES_MIGRATE" envDefault:"false"`
}

func Load[T any](cfg *T) error {
	if err := env.Parse(cfg); err != nil {
		return fmt.Errorf("parse env: %w", err)
	}
	return nil
}

func MustLoad[T any]() *T {
	var cfg T
	if err := Load(&cfg); err != nil {
		panic(err)
	}
	return &cfg
}
