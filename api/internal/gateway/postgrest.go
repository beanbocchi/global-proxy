package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/thinkonmay/global-proxy/api/config"
	"github.com/thinkonmay/global-proxy/api/pkg/guard"
)

// restPrefix is the Supabase-compatible REST path the gateway proxies to PostgREST.
const restPrefix = "/rest/v1"

// graphqlPath is Supabase's GraphQL endpoint; PostgREST serves it via pg_graphql
// at /rpc/graphql.
const graphqlPath = "/graphql/v1"

// proxyTimeout bounds each proxied API request (TDD §2.1.1).
const proxyTimeout = 5 * time.Second

// newProxy builds a reverse proxy to rawURL over the shared circuit-breaking
// transport rt. director runs after scheme/host are rewritten, for path rewrite
// and header injection. Returns nil (route should be skipped) if rawURL is empty
// or unparseable.
func newProxy(rawURL string, rt http.RoundTripper, director func(*http.Request)) *httputil.ReverseProxy {
	if rawURL == "" {
		return nil
	}
	target, err := url.Parse(rawURL)
	if err != nil {
		slog.Error("invalid upstream url, route disabled", "url", rawURL, "err", err)
		return nil
	}
	return &httputil.ReverseProxy{
		Transport: rt,
		Director: func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			req.Host = target.Host
			if director != nil {
				director(req)
			}
		},
		ErrorHandler: proxyError,
	}
}

// proxyError maps a guard rejection to 503 {"global_unavailable":true} and any
// other upstream failure to 502 — never a hung proxy (P11).
func proxyError(w http.ResponseWriter, _ *http.Request, err error) {
	if guard.Rejected(err) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"global_unavailable":true}`))
		return
	}
	slog.Error("proxy error", "err", err)
	w.WriteHeader(http.StatusBadGateway)
}

// timed wraps an http.Handler with a per-request timeout context.
func timed(h http.Handler, d time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()
		h.ServeHTTP(w, r.WithContext(ctx))
	}
}

// injectAnon adds anon credentials when the client supplied none — the lean
// replacement for Kong's anon role mapping (P0-A). No key-auth/ACL validation.
func injectAnon(cfg config.PostgREST, req *http.Request) {
	if cfg.AnonKey != "" && req.Header.Get("apikey") == "" {
		req.Header.Set("apikey", cfg.AnonKey)
		req.Header.Set("Authorization", "Bearer "+cfg.AnonKey)
	}
}

// registerRestProxy wires /rest/v1/* and /graphql/v1 to PostgREST, replacing
// Kong's rest-v1 and graphql-v1 services.
func registerRestProxy(mux *http.ServeMux, cfg config.PostgREST, rt http.RoundTripper) {
	rest := newProxy(cfg.URL, rt, func(req *http.Request) {
		req.URL.Path = strings.TrimPrefix(req.URL.Path, restPrefix)
		injectAnon(cfg, req)
	})
	if rest == nil {
		slog.Error("postgrest url invalid, /rest/v1 and /graphql/v1 disabled")
		return
	}
	mux.Handle(restPrefix+"/", timed(rest, proxyTimeout))

	// GraphQL: rewrite to PostgREST's pg_graphql RPC.
	graphql := newProxy(cfg.URL, rt, func(req *http.Request) {
		req.URL.Path = "/rpc/graphql"
		injectAnon(cfg, req)
	})
	mux.Handle(graphqlPath, timed(graphql, proxyTimeout))
}
