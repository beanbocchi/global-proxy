// Package config loads application configuration via viper (env overrides a YAML
// file). Shared by the gateway and worker binaries — one config, no per-binary
// variants. No direct-DB credentials: all global data is reached over PostgREST.
package config

import (
	"log/slog"
	"os"
	"strings"

	"github.com/thinkonmay/global-proxy/api/pkg/validator"

	"github.com/spf13/viper"
)

type Config struct {
	Port      string    `mapstructure:"port" validate:"required,numeric"`
	Log       Log       `mapstructure:"log"`
	PostgREST  PostgREST  `mapstructure:"postgrest"`  // global data over HTTP (no direct DB)
	Upstreams  Upstreams  `mapstructure:"upstreams"`  // extra reverse-proxy targets (gateway = Kong)
	Nats       Nats       `mapstructure:"nats"`       // event bus (JetStream)
	ClickHouse ClickHouse `mapstructure:"clickhouse"` // analytics sink (the worker)
}

// Upstreams are the non-PostgREST targets the gateway reverse-proxies, replacing
// Kong's service routes for what this stack actually runs. Empty => route off.
type Upstreams struct {
	Meta   string `mapstructure:"meta"`   // postgres-meta — /pg/* (Studio schema browse)
	Studio string `mapstructure:"studio"` // dashboard UI — "/" catch-all
}

// ClickHouse is the analytics store — only the worker's usage sink connects to it.
type ClickHouse struct {
	Addr     string `mapstructure:"addr"`     // host:port (native, e.g. clickhouse:9000)
	Database string `mapstructure:"database"` // default: analytics
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

// PostgREST is how services reach global data — over HTTP, never a direct DB
// connection. No Postgres credentials live in this config.
type PostgREST struct {
	URL        string `mapstructure:"url" validate:"required,url"`
	AnonKey    string `mapstructure:"anonKey"`    // public catalog reads / proxy default
	ServiceKey string `mapstructure:"serviceKey"` // privileged writes
}

type Nats struct {
	URL string `mapstructure:"url" validate:"required"` // nats://host:port
}

type Log struct {
	Level     string `mapstructure:"level"  validate:"required,oneof=debug info warn error"`
	Format    string `mapstructure:"format" validate:"required,oneof=json text"`
	AddSource bool   `mapstructure:"addSource"`
}

// NewConfig reads config.yaml (searched in . and ./config), overlays APP_* env
// vars, then validates. Invalid or missing required fields return an error.
func NewConfig() (*Config, error) {
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("./config")

	v.SetEnvPrefix("APP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	if err := validator.Validate(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// SetupLogger sets the process-wide slog.Default from the config. Shared by the
// gateway and worker binaries.
func (c *Config) SetupLogger() {
	var level slog.Level
	switch c.Log.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:     level,
		AddSource: c.Log.AddSource,
	})))
}
