// Package admin implements the control plane for the dashboard:
// session-cookie authentication (auth.go), the REST handlers under /api
// (handlers.go) and the log ring buffer with its SSE broadcaster (logs.go).
package admin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Ring and broadcaster sizes: the ring keeps the last 1000 entries in RAM;
// every SSE subscriber gets a 256-entry buffer and is disconnected — never
// allowed to slow the data plane — when it falls behind.
const (
	ringSize  = 1000
	subBuffer = 256
)

// keepaliveEvery is the SSE ":keepalive" comment interval.
const keepaliveEvery = 20 * time.Second

// Entry is one broadcast/ring record: the SSE event name and its JSON data
// (already encoded once, so every subscriber reuses the same bytes).
type Entry struct {
	Event string
	Data  json.RawMessage
}

// RequestEntry is the `event: request` payload.
type RequestEntry struct {
	Ts       int64  `json:"ts"` // unix seconds
	RID      string `json:"rid"`
	Acc      string `json:"acc"` // account id ("" for a fallback-channel attempt)
	Name     string `json:"name,omitempty"`
	Channel  string `json:"channel,omitempty"` // "A" | "B"
	ModelIn  string `json:"model_in"`
	ModelOut string `json:"model_out"`
	Stream   bool   `json:"stream"`
	Status   int    `json:"status"`
	Attempts int    `json:"attempts"`
	MS       int64  `json:"ms"`
	InTok    int64  `json:"in_tok"`
	OutTok   int64  `json:"out_tok"`
}

// FailoverEntry is the `event: failover` payload.
type FailoverEntry struct {
	Ts   int64  `json:"ts"`
	RID  string `json:"rid"`
	From string `json:"from"` // account id
	To   string `json:"to"`   // account id
	Kind string `json:"kind,omitempty"`
}

// StateEntry is the `event: state` payload.
type StateEntry struct {
	Ts    int64  `json:"ts"`
	Acc   string `json:"acc"`
	From  string `json:"from"`
	To    string `json:"to"`
	Until int64  `json:"until,omitempty"` // unix seconds, 0 = none
	Kind  string `json:"kind,omitempty"`
}

// ClaimEntry is the `event: claim` payload.
type ClaimEntry struct {
	Ts              int64  `json:"ts"`
	Acc             string `json:"acc"`
	OK              bool   `json:"ok"`
	Kind            string `json:"kind"`
	CaptchaRequired bool   `json:"captcha_required"`
	// NextClaimAt is data.plan.ends_at of the claim response (unix seconds,
	// 0 when the body carried none): the next-claim instant upstream itself
	// computed.
	NextClaimAt int64 `json:"next_claim_at,omitempty"`
}

// ClaimJobEntry is one claim-all progress frame — the `event: claim_job`
// payload: phase start (with the account total), tick after each account,
// done/aborted when the pass ends.
type ClaimJobEntry struct {
	Phase string `json:"phase"`
	Done  int    `json:"done"`
	Total int    `json:"total"`
}

// subscriber is one SSE client. When Publish cannot fit an entry into the
// buffer the subscriber is slow: it is removed and its channel closed, which
// ends its streaming handler.
type subscriber struct {
	ch chan Entry
}

// Logs is the in-RAM request log: a fixed ring for GET /api/logs plus a
// fan-out broadcaster for GET /api/logs/stream.
type Logs struct {
	mu      sync.Mutex
	ring    [ringSize]Entry
	start   int
	count   int
	subs    map[*subscriber]struct{}
	stopped bool

	failovers atomic.Int64
	startedAt time.Time
}

// NewLogs creates the log hub. startedAt anchors uptime_seconds in /api/stats.
func NewLogs() *Logs {
	return &Logs{
		subs:      make(map[*subscriber]struct{}),
		startedAt: time.Now(),
	}
}

// Request records a completed /v1 request (`event: request`).
func (l *Logs) Request(e RequestEntry) { l.publish("request", e) }

// Failover records an account switch inside one request (`event: failover`) and
// bumps the failovers_total counter.
func (l *Logs) Failover(e FailoverEntry) {
	l.failovers.Add(1)
	l.publish("failover", e)
}

// State records an account state transition (`event: state`), fed from
// Pool.SetEventHandler.
func (l *Logs) State(e StateEntry) { l.publish("state", e) }

// Claim records one account's claim result (`event: claim`), fed from
// Claimer.SetClaimHandler.
func (l *Logs) Claim(e ClaimEntry) { l.publish("claim", e) }

// ClaimJob publishes one claim-all progress frame (`event: claim_job`),
// fed from Claimer.SetJobHandler.
func (l *Logs) ClaimJob(e ClaimJobEntry) { l.publish("claim_job", e) }

func (l *Logs) publish(event string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	e := Entry{Event: event, Data: data}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return
	}
	if l.count < ringSize {
		l.ring[(l.start+l.count)%ringSize] = e
		l.count++
	} else {
		l.ring[l.start] = e // overwrite the oldest
		l.start = (l.start + 1) % ringSize
	}
	for sub := range l.subs {
		select {
		case sub.ch <- e:
		default:
			// Slow subscriber (256-entry buffer full): disconnect instead of
			// blocking the data plane.
			delete(l.subs, sub)
			close(sub.ch)
		}
	}
}

// Subscribe registers an SSE subscriber. The returned cancel func unregisters
// it; a subscriber disconnected for being slow observes a closed channel.
func (l *Logs) Subscribe() (<-chan Entry, func()) {
	sub := &subscriber{ch: make(chan Entry, subBuffer)}
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		ch := make(chan Entry)
		close(ch)
		return ch, func() {}
	}
	l.subs[sub] = struct{}{}
	l.mu.Unlock()
	return sub.ch, func() {
		l.mu.Lock()
		if _, ok := l.subs[sub]; ok {
			delete(l.subs, sub)
			close(sub.ch)
		}
		l.mu.Unlock()
	}
}

// Snapshot returns the newest min(limit, ring) entries, oldest first.
func (l *Logs) Snapshot(limit int) []Entry {
	if limit <= 0 || limit > ringSize {
		limit = ringSize
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	n := l.count
	if limit < n {
		n = limit
	}
	out := make([]Entry, 0, n)
	for i := l.count - n; i < l.count; i++ {
		out = append(out, l.ring[(l.start+i)%ringSize])
	}
	return out
}

// Reset clears the ring buffer (DELETE /api/logs).
func (l *Logs) Reset() {
	l.mu.Lock()
	l.start, l.count = 0, 0
	l.mu.Unlock()
}

// FailoversTotal reports the failovers_total counter.
func (l *Logs) FailoversTotal() int64 { return l.failovers.Load() }

// UptimeSeconds reports process uptime for /api/stats.
func (l *Logs) UptimeSeconds() int64 {
	return int64(time.Since(l.startedAt).Seconds())
}

// Close disconnects every subscriber and freezes the hub (used at shutdown).
func (l *Logs) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return
	}
	l.stopped = true
	for sub := range l.subs {
		close(sub.ch)
	}
	l.subs = make(map[*subscriber]struct{})
}

// ServeStream is GET /api/logs/stream: an SSE stream of
// request/failover/state events with a ":keepalive" comment every 20s.
func (l *Logs) ServeStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":"streaming unsupported"}`, http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	events, cancel := l.Subscribe()
	defer cancel()

	keepalive := time.NewTicker(keepaliveEvery)
	defer keepalive.Stop()

	for {
		select {
		case e, ok := <-events:
			if !ok {
				return // disconnected (slow subscriber) or hub closed
			}
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Event, e.Data); err != nil {
				return
			}
			flusher.Flush()
		case <-keepalive.C:
			if _, err := w.Write([]byte(":keepalive\n\n")); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// --- request id ---------------------------------------------------------------
type ctxKey int

const requestIDKey ctxKey = 1

// NewRequestID mints a short request id ("r_" + 8 hex chars) for log entries.
func NewRequestID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("r_%d", time.Now().UnixNano())
	}
	return "r_" + hex.EncodeToString(b[:])
}

// WithRequestID attaches the request id to a context (server middleware).
func WithRequestID(ctx context.Context, rid string) context.Context {
	return context.WithValue(ctx, requestIDKey, rid)
}

// RequestIDFrom returns the request id of the current request ("" if the
// middleware was bypassed).
func RequestIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey).(string)
	return v
}
