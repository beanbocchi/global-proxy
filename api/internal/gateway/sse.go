package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thinkonmay/global-proxy/api/shared/model"
)

const (
	sseClientBuffer = 16               // per-client queue; full => event dropped for that client
	sseHeartbeat    = 25 * time.Second // keepalive comment so idle conns/proxies stay open
)

// sseClient is one connected SSE stream: a queue drained by its own Serve loop.
type sseClient struct {
	recipient string
	ch        chan model.SSEMsg
}

// SSEHub routes events to connected clients by recipient. Live, best-effort: a
// client that can't keep up drops events rather than stalling the bus.
type SSEHub struct {
	mu      sync.RWMutex
	clients map[*sseClient]struct{}
	seq     atomic.Uint64
}

func NewSSEHub() *SSEHub {
	return &SSEHub{clients: make(map[*sseClient]struct{})}
}

// Dispatch is the bus handler: it routes one event to every matching client
// (recipient targets one user's streams; empty broadcasts to all). The send is
// non-blocking — a client whose buffer is full drops the event rather than
// stalling the bus.
func (h *SSEHub) Dispatch(_ context.Context, e model.SSEMsg) error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		if e.Recipient != "" && e.Recipient != c.recipient {
			continue
		}
		select {
		case c.ch <- e:
		default: // slow client: drop, don't block the bus
		}
	}
	return nil
}

// Serve holds the connection open and streams events for the requesting user
// until the client disconnects.
func (h *SSEHub) Serve(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	c := h.add(r.URL.Query().Get("user")) // TODO: derive recipient from auth, not the client
	defer h.remove(c)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ticker := time.NewTicker(sseHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case e := <-c.ch:
			h.writeMsg(w, e)
			flusher.Flush()
		case <-ticker.C:
			_, _ = fmt.Fprint(w, ":keepalive\n\n")
			flusher.Flush()
		}
	}
}

// writeMsg renders one message in the SSE wire format (tdd §2.4.1):
//
//	id:{seq}\n data:{json}\n \n
func (h *SSEHub) writeMsg(w io.Writer, e model.SSEMsg) {
	data, err := json.Marshal(e)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(w, "id:%d\ndata:%s\n\n", h.seq.Add(1), data)
}

func (h *SSEHub) add(recipient string) *sseClient {
	c := &sseClient{recipient: recipient, ch: make(chan model.SSEMsg, sseClientBuffer)}
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	return c
}

func (h *SSEHub) remove(c *sseClient) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

// Clients reports the number of connected streams.
func (h *SSEHub) Clients() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}
