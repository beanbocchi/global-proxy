package main

import (
	"net/http"

	"github.com/thinkonmay/global-proxy/api/config"
	"github.com/thinkonmay/global-proxy/api/internal/gateway/handler"
	"github.com/thinkonmay/global-proxy/api/pkg/guard"
)

// inbound rate-limit defaults (per client IP). Promote to config when tuned.
const (
	rateRPS   = 50  // sustained req/s per client
	rateBurst = 100 // burst allowance
)

// Inbound IP policy (populate from config/env later). Whitelisted IPs are marked
// trusted and skip ALL guard (rate limit + outbound breaker/bulkhead); blacklisted
// IPs get 403.
var (
	ipWhitelist = []string{} // e.g. "127.0.0.1", "10.0.0.0/8" — admin / health checks
	ipBlacklist = []string{}
)

// newMux builds the gateway router on the stdlib ServeMux (Go 1.22 method+path
// patterns). rt is the circuit-breaking transport for the /rest/v1 proxy.
func newMux(h *handler.Handler, hub *SSEHub, prCfg config.PostgREST, up config.Upstreams, rt http.RoundTripper) http.Handler {
	mux := http.NewServeMux()

	h.Route(mux)

	// Live event stream: clients subscribe here, the bus feeds hub.Dispatch.
	mux.HandleFunc("GET /sse", hub.Serve)

	// Supabase-compatible passthrough, replacing Kong's service routes:
	//   /rest/v1/*, /graphql/v1 → PostgREST; /pg/* → postgres-meta; / → Studio.
	registerRestProxy(mux, prCfg, rt)
	registerUpstreams(mux, up, rt)

	// Inbound guard chain (outermost first): deny blacklisted IPs → CORS/preflight
	// → mark whitelisted IPs trusted → per-IP rate limit (skips trusted).
	return guard.Chain(mux,
		guard.Denylist(guard.IPSet(ipBlacklist...)),
		withCORS,
		guard.Allowlist(guard.IPSet(ipWhitelist...)),
		guard.RateLimit(guard.RateLimitConfig{RPS: rateRPS, Burst: rateBurst}),
	)
}
