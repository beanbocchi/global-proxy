package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/thinkonmay/global-proxy/api/config"
	"github.com/thinkonmay/global-proxy/api/internal/gateway/handler"
	"github.com/thinkonmay/global-proxy/api/pkg/bus"
	busnats "github.com/thinkonmay/global-proxy/api/pkg/bus/nats"
	"github.com/thinkonmay/global-proxy/api/pkg/guard"
	"github.com/thinkonmay/global-proxy/api/shared/model"
)

func main() {
	if err := Run(); err != nil {
		log.Fatal(err)
	}
}

// Run loads config, wires the app, serves HTTP, and blocks until a signal
// triggers graceful shutdown.
func Run() error {
	cfg, err := config.NewConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	cfg.SetupLogger()

	// Guard (breaker + bulkhead) for the /rest/v1 proxy, per host — fail fast,
	// never hang (TDD §2.1.1 / P11).
	bt := guard.New(nil, guard.Config{MaxFailures: 5, Cooldown: 30 * time.Second, MaxConcurrent: 64})

	eventBus, err := busnats.New([]string{cfg.Nats.URL}, slog.Default())
	if err != nil {
		return fmt.Errorf("connect nats bus: %w", err)
	}
	defer func() { _ = eventBus.Close() }()

	// Fan SSE events off the bus out to connected clients. One fixed group =
	// correct for a single gateway instance; multi-replica needs a unique group
	// per process so every replica gets a copy.
	hub := NewSSEHub()
	bus.Subscribe(eventBus, model.TopicSSE, "gateway-sse", hub.Dispatch)

	h := handler.NewHandler(eventBus)
	srv := &http.Server{Addr: ":" + cfg.Port, Handler: newMux(h, hub, cfg.PostgREST, cfg.Upstreams, bt)}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("starting HTTP server", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		slog.Info("shutting down HTTP server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
