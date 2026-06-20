package main

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/thinkonmay/global-proxy/api/config"
)

// metaPrefix maps to Kong's /pg route → postgres-meta. Kong gated this admin-only
// (ACL "admin"); lightweight mode does NOT — open until key-auth/ACL is added.
const metaPrefix = "/pg"

// registerUpstreams wires the non-PostgREST Kong service routes this stack runs:
// /pg/* → postgres-meta, and "/" → Studio (catch-all dashboard). Each is skipped
// when its upstream URL is empty.
func registerUpstreams(mux *http.ServeMux, up config.Upstreams, rt http.RoundTripper) {
	if meta := newProxy(up.Meta, rt, func(req *http.Request) {
		req.URL.Path = strings.TrimPrefix(req.URL.Path, metaPrefix)
	}); meta != nil {
		mux.Handle(metaPrefix+"/", timed(meta, proxyTimeout))
	} else if up.Meta != "" {
		slog.Error("meta upstream invalid, /pg/* disabled")
	}

	// Studio is the catch-all dashboard, matching Kong's "/" route. No timeout
	// wrap — it serves UI assets, not bounded API calls. Registered last; "/"
	// only matches paths no more-specific pattern claimed.
	if studio := newProxy(up.Studio, rt, nil); studio != nil {
		mux.Handle("/", studio)
	} else if up.Studio != "" {
		slog.Error("studio upstream invalid, / disabled")
	}
}
