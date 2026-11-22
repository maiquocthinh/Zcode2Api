// e2e_test.go — the acceptance suite. Everything runs against httptest
// servers only: a fake upstream and a real gateway built with
// server.NewServer. No real network is ever contacted.
//
// The gateway under test is the hybrid: channel A is the pool of free JWT
// accounts (zcode-plan origin, Bearer JWT + the ZCode source headers),
// channel B is the optional FALLBACK_API_KEY used only when the pool returns
// ErrPoolExhausted.
//
// Scenario → test mapping:
//
// 1. Chat non-stream via channel A → TestE2EScenario1_NonStreamSuccessChannelA
// 2. Chat stream SSE passthrough, flush per chunk → TestE2EScenario2_StreamFlushPerChunk
// 3. HTTP 200 + code 1005 → EXHAUSTED + retry (plain JSON body) →
//    TestE2EScenario3_PureJSON1005Failover; SSE error event →
//    TestE2E2_FailoverOn1005FirstEvent
// 4. 3012 → RISK_COOLDOWN + retry → TestE2EScenario4_RiskBlock3012Retry
// 5. 1006 → DISABLED + retry → TestE2EScenario5_TokenDead1006Retry
// 6. Upstream error mid-stream → the error event is forwarded verbatim, no
//    retry → TestE2E3_MidStreamErrorForwardedNoRetry
// 7. Pool exhausted + FALLBACK_API_KEY → channel B →
//    TestE2E11_FallbackChannelBWhenPoolExhausted
// 8. Pool exhausted, no fallback → 429 + Retry-After →
//    TestE2E4_AllAccountsCooling429WithRetryAfter
// 9. Wrong client key → 401; STRICT_MODELS → 404 → TestE2EClientAuth401 +
//    TestE2EScenario9_StrictModels404
// 10. Claim 3007 without solver / with a fake solver →
//     TestE2E12_ClaimEndpoint3007WithoutSolver + TestE2E13_ClaimEndpointWithFakeSolver
//
// Extra: TestE2EScenario7b_BothChannelsExhausted429 — the fallback key is
// exhausted too → 429 "upstream quota exhausted".

// The tests live in the external package (proxy_test) so they may import
// internal/server (which itself imports proxy) without an import cycle.
package proxy_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"zcode2api/internal/admin"
	"zcode2api/internal/config"
	"zcode2api/internal/pool"
	"zcode2api/internal/server"
)

// --- test constants -----------------------------------------------------------
const (
	e2eClientKey = "e2e-client-key-0123456789abcdef"
	e2eAdminPass = "e2e-admin-password-0123456789abcdef"

	// Fake-but-well-formed JWTs (three base64url segments); upstream validity
	// is only judged by the fake upstream's behavior.
	e2eJWTA = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhY2NvdW50LWEifQ.c2lnLWFhYWE"
	e2eJWTB = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhY2NvdW50LWIifQ.c2lnLWJiYmI"
	e2eJWTC = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhY2NvdW50LWMifQ.c2lnLWNjY2M"
	// Extra accounts for the admin-API tests.
	e2eJWTExtra  = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhY2NvdW50LWQuZnVsbCJ9.c2lnLWRkZGQ"
	e2eJWTNoCSRF = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhY2NvdW50LWUifQ.c2lnLWVlZWU"
	e2eJWTLeak   = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJsZWFrLXByb3ZlLXJ1bm5lcjRmM2UyZCJ9.c2lnLWxlYWs"

	e2eFallbackKey = "sk-e2e-fallback-0123456789abcdef"
)

// A complete Anthropic SSE stream used by the "good" upstream account.
const (
	sseMessageStart = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":120,\"output_tokens\":1}}}\n\n"
	sseBlocks       = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n" +
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n"
	sseMessageDelta = "event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":57}}\n\n"
	sseMessageStop  = "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	sseFullStream   = sseMessageStart + sseBlocks + sseMessageDelta + sseMessageStop
)

// --- fake upstream --------------------------------------------------------------

// fakeUpstream lets a test swap the upstream behavior between phases.
type fakeUpstream struct {
	mu   sync.RWMutex
	next http.Handler
}

func (f *fakeUpstream) set(h http.Handler) {
	f.mu.Lock()
	f.next = h
	f.mu.Unlock()
}

func (f *fakeUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.RLock()
	h := f.next
	f.mu.RUnlock()
	if h == nil {
		http.Error(w, "no upstream behavior configured", http.StatusInternalServerError)
		return
	}
	h.ServeHTTP(w, r)
}

// upstreamCall is one recorded upstream request. Credentials appear only as
// parsed-out header values held in test memory, never in logs.
type upstreamCall struct {
	JWT              string // Authorization Bearer token (channel A)
	DeviceMid        string // X-Device-Mid (channel A)
	Referer          string // HTTP-Referer (channel A)
	Title            string // X-Title (channel A)
	Platform         string // X-Platform (channel A)
	AnthropicVersion string // anthropic-version (both channels)
	APIKey           string // x-api-key (channel B)
	Captcha          string // X-Aliyun-Captcha-Verify-Param (claim retry only)
	Path             string
	UA               string
	Model            string
	Method           string
	AppVersion       string // app_version= query parameter (balance/claim)
}

type callRecorder struct {
	mu    sync.Mutex
	calls []upstreamCall
}

func (c *callRecorder) add(u upstreamCall) {
	c.mu.Lock()
	c.calls = append(c.calls, u)
	c.mu.Unlock()
}

func (c *callRecorder) all() []upstreamCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]upstreamCall(nil), c.calls...)
}

// --- fixture ---------------------------------------------------------------------
type e2eFixture struct {
	t        *testing.T
	upstream *fakeUpstream
	calls    *callRecorder
	gateway  *httptest.Server
	pool     *pool.Pool
	logs     *admin.Logs
}

// newE2EWithAccounts builds the full gateway (server.NewServer — no SPA fs, so
// web/dist is not needed) against a fake upstream, importing the named
// accounts ("Name | eyJ..." lines).
func newE2EWithAccounts(t *testing.T, mutate func(*config.Config), accounts ...string) *e2eFixture {
	t.Helper()

	fu := &fakeUpstream{}
	rec := &callRecorder{}
	upSrv := httptest.NewServer(http.HandlerFunc(fu.ServeHTTP))
	t.Cleanup(upSrv.Close)

	cfg := &config.Config{
		Host:               "127.0.0.1",
		Port:               0,
		ClientAPIKeys:      []string{e2eClientKey},
		AdminPassword:      e2eAdminPass,
		DBPath:             filepath.Join(t.TempDir(), "e2e.db"),
		UpstreamURL:        upSrv.URL, // channel A origin (fake zcode-plan)
		FallbackURL:        upSrv.URL, // channel B origin (fake api.z.ai)
		DefaultModelOpus:   "glm-opus-test",
		DefaultModelSonnet: "glm-sonnet-test",
		DefaultModelHaiku:  "glm-haiku-test",
		StrictModels:       false,
		ClaimPlanID:        "test-plan",
		InflightLimit:      3,
		MaxAttempts:        3,
		AttemptDeadline:    15 * time.Second,
		FirstEventTimeout:  5 * time.Second,
		FirstEventMaxBytes: 65536,
		MaxBodyBytes:       1 << 20,
		CoolingDuration:    60 * time.Second,
		ExhaustedFallback:  time.Hour,
		FailThreshold:      3,
		AffinityEnabled:    false, // deterministic: quota + round-robin only
		AffinityTTL:        30 * time.Minute,
		LogLevel:           "error",
	}
	if mutate != nil {
		mutate(cfg)
	}

	logs := admin.NewLogs()
	store, err := pool.Open(cfg.DBPath, "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	// Insert accounts with staggered timestamps so the pool loads them in a
	// deterministic order (ImportAccounts would stamp them with the same
	// second and ORDER BY created_at, id would tie-break on random ids).
	base := time.Now()
	for i, line := range accounts {
		name, jwt, _ := strings.Cut(line, "|")
		name, jwt = strings.TrimSpace(name), strings.TrimSpace(jwt)
		if _, err := store.AddAccount(name, []byte(jwt), "", base.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("add account %q: %v", name, err)
		}
	}
	pl, err := pool.NewPool(store, pool.Config{
		InflightLimit:   cfg.InflightLimit,
		FailThreshold:   cfg.FailThreshold,
		AffinityEnabled: cfg.AffinityEnabled,
		AffinityTTL:     cfg.AffinityTTL,
		UpstreamURL:     cfg.UpstreamURL,
	})
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}

	claimer, err := pool.NewClaimer(pl, pool.ClaimConfig{
		Enabled:          cfg.DailyClaim, // false by default: on-demand claims only
		ClaimTime:        cfg.ClaimTime,
		SolverURL:        cfg.ClaimSolverURL,
		SolverKey:        cfg.ClaimSolverKey,
		IncludeExhausted: cfg.ClaimExhausted,
		UpstreamURL:      cfg.UpstreamURL,
		PlanID:           cfg.ClaimPlanID, // "" = discovery-driven; the e2e fixtures pin a plan
		PauseBetween:     time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new claimer: %v", err)
	}

	mp, err := pool.NewMapping(store)
	if err != nil {
		t.Fatalf("new mapping: %v", err)
	}
	if _, err := mp.Add("claude-sonnet-*", "glm-sonnet-test", pool.ModeMapped, time.Now()); err != nil {
		t.Fatalf("add mapping: %v", err)
	}

	srv := server.NewServer(cfg, server.Deps{Pool: pl, Mapping: mp, Store: store, Logs: logs, Claims: claimer}, nil)
	gw := httptest.NewServer(srv.Handler)
	t.Cleanup(func() {
		// Stop the claimer and pool first, then close the log hub so any open
		// SSE subscriber handler returns — otherwise gw.Close would wait on
		// the still-running /api/logs/stream request.
		claimer.Close()
		pl.Stop()
		logs.Close()
		store.Close()
		gw.Close()
	})

	return &e2eFixture{t: t, upstream: fu, calls: rec, gateway: gw, pool: pl, logs: logs}
}

func newE2E3Accounts(t *testing.T) *e2eFixture {
	return newE2EWithAccounts(t, nil,
		"Account A | "+e2eJWTA, "Account B | "+e2eJWTB, "Account C | "+e2eJWTC)
}

func newE2E2Accounts(t *testing.T) *e2eFixture {
	return newE2EWithAccounts(t, nil, "Account A | "+e2eJWTA, "Account B | "+e2eJWTB)
}

// serveUpstream installs the upstream behavior; every call is recorded first.
func (f *e2eFixture) serveUpstream(handler func(call upstreamCall, w http.ResponseWriter, r *http.Request)) {
	f.upstream.set(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var m struct {
			Model string `json:"model"`
		}
		_ = json.Unmarshal(body, &m)
		call := upstreamCall{
			JWT:              strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "),
			DeviceMid:        r.Header.Get("X-Device-Mid"),
			Referer:          r.Header.Get("HTTP-Referer"),
			Title:            r.Header.Get("X-Title"),
			Platform:         r.Header.Get("X-Platform"),
			AnthropicVersion: r.Header.Get("Anthropic-Version"),
			APIKey:           r.Header.Get("X-Api-Key"),
			Captcha:          r.Header.Get("X-Aliyun-Captcha-Verify-Param"),
			Path:             r.URL.Path,
			UA:               r.Header.Get("User-Agent"),
			Model:            m.Model,
			Method:           r.Method,
			AppVersion:       r.URL.Query().Get("app_version"),
		}
		f.calls.add(call)
		handler(call, w, r)
	}))
}

func (f *e2eFixture) accountByName(name string) pool.Account {
	f.t.Helper()
	for _, a := range f.pool.Accounts() {
		if a.Name == name {
			return a
		}
	}
	f.t.Fatalf("account %q not found", name)
	return pool.Account{}
}

func (f *e2eFixture) accountID(name string) string {
	return f.accountByName(name).ID
}

// post sends a client request to the gateway with the client key and reads
// the whole response body.
func (f *e2eFixture) post(path, body string, headers map[string]string) (*http.Response, []byte) {
	f.t.Helper()
	req, err := http.NewRequest(http.MethodPost, f.gateway.URL+path, strings.NewReader(body))
	if err != nil {
		f.t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", e2eClientKey)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := f.gateway.Client().Do(req)
	if err != nil {
		f.t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		f.t.Fatalf("read response: %v", err)
	}
	return resp, raw
}

func (f *e2eFixture) postMessages(body string) (*http.Response, []byte) {
	return f.post("/v1/messages", body, nil)
}

func streamRequest(model string) string {
	return fmt.Sprintf(`{"model":%q,"stream":true,"max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`, model)
}

func plainRequest(model string) string {
	return fmt.Sprintf(`{"model":%q,"stream":false,"max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`, model)
}

// admin performs an admin API call with optional session cookie and CSRF
// header.
func (f *e2eFixture) admin(method, path, body string, cookie *http.Cookie, csrf bool) (*http.Response, []byte) {
	f.t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, f.gateway.URL+path, rd)
	if err != nil {
		f.t.Fatalf("new request %s %s: %v", method, path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if cookie != nil {
		req.AddCookie(cookie)
	}
	if csrf {
		req.Header.Set("X-Requested-With", "zcode2api")
	}
	resp, err := f.gateway.Client().Do(req)
	if err != nil {
		f.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		f.t.Fatalf("read response: %v", err)
	}
	return resp, raw
}

// adminLogin performs POST /api/login and returns the session cookie.
func (f *e2eFixture) adminLogin() *http.Cookie {
	f.t.Helper()
	resp, raw := f.admin(http.MethodPost, "/api/login",
		fmt.Sprintf(`{"password":%q}`, e2eAdminPass), nil, false)
	if resp.StatusCode != http.StatusNoContent {
		f.t.Fatalf("login status = %d, body %s", resp.StatusCode, raw)
	}
	for _, c := range resp.Cookies() {
		if c.Name == "z2a_session" {
			return c
		}
	}
	f.t.Fatal("login did not set a session cookie")
	return nil
}

// --- SSE helpers -------------------------------------------------------------------
func writeSSE(w http.ResponseWriter, payload string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, payload)
	if fl, ok := w.(http.Flusher); ok {
		fl.Flush()
	}
}

type sseEvent struct {
	Name string
	Data string
}

type sseCollector struct {
	mu     sync.Mutex
	events []sseEvent
}

func (c *sseCollector) add(e sseEvent) {
	c.mu.Lock()
	c.events = append(c.events, e)
	c.mu.Unlock()
}

func (c *sseCollector) snapshot() []sseEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]sseEvent(nil), c.events...)
}

// collectSSE reads SSE blocks from body until it closes (runs in a goroutine).
func collectSSE(body io.ReadCloser, out *sseCollector) {
	defer body.Close()
	br := bufio.NewReader(body)
	var block strings.Builder
	for {
		line, err := br.ReadString('\n')
		block.WriteString(line)
		if err != nil {
			return
		}
		if line == "\n" || line == "\r\n" {
			name, data := parseSSEBlock(block.String())
			if name != "" || data != "" {
				out.add(sseEvent{Name: name, Data: data})
			}
			block.Reset()
		}
	}
}

func parseSSEBlock(block string) (name, data string) {
	var dataLines []string
	for _, ln := range strings.Split(strings.TrimSuffix(block, "\n"), "\n") {
		ln = strings.TrimSuffix(ln, "\r")
		switch {
		case strings.HasPrefix(ln, "event:"):
			name = strings.TrimSpace(strings.TrimPrefix(ln, "event:"))
		case strings.HasPrefix(ln, "data:"):
			dataLines = append(dataLines, strings.TrimPrefix(strings.TrimPrefix(ln, "data:"), " "))
		}
	}
	return name, strings.Join(dataLines, "\n")
}

func hasEvent(evs []sseEvent, name string) bool {
	for _, e := range evs {
		if e.Name == name {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, timeout time.Duration, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// =====================================================================================
// Scenario 1 — Account A returns 429 {"error":{"code":"1302"}}, account B
// streams OK: the client gets 200 with the full stream, A enters COOLING, and
// the x-zcode2api-attempts header is 2. Channel A carries the ZCode source
// headers and never a captcha header.
// =====================================================================================
func TestE2E1_FailoverOn429(t *testing.T) {
	f := newE2E2Accounts(t)
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		switch call.JWT {
		case e2eJWTA:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"error":{"code":"1302","message":"rate limit exceeded"}}`)
		default:
			writeSSE(w, sseFullStream)
		}
	})

	resp, body := f.postMessages(streamRequest("claude-sonnet-4-5-20250929"))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("x-zcode2api-attempts"); got != "2" {
		t.Errorf("x-zcode2api-attempts = %q, want 2", got)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	if got := resp.Header.Get("x-zcode2api-channel"); got != "A" {
		t.Errorf("x-zcode2api-channel = %q, want A (the commit happened on a pool account)", got)
	}
	for _, want := range []string{"message_start", "message_stop"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("client stream missing %q", want)
		}
	}
	if strings.Contains(string(body), "1302") {
		t.Errorf("client saw the failed account's upstream body: %s", body)
	}

	calls := f.calls.all()
	if len(calls) != 2 {
		t.Fatalf("upstream calls = %d, want 2", len(calls))
	}
	if calls[0].JWT != e2eJWTA || calls[1].JWT != e2eJWTB {
		t.Errorf("attempt order = [%s, %s], want A then B", calls[0].JWT, calls[1].JWT)
	}
	for i, c := range calls {
		// Channel A identity: the ZCode
		// client headers and never a captcha header. Model requests carry the
		// ai-sdk UA and NO X-Device-Mid — the device fingerprint travels in
		// body.metadata.user_id instead.
		if !strings.HasPrefix(c.UA, "ZCode/") {
			t.Errorf("call %d User-Agent = %q, want ZCode/<version>", i, c.UA)
		}
		if !strings.Contains(c.UA, "ai-sdk/provider-utils/") {
			t.Errorf("call %d User-Agent = %q, want the ai-sdk composite (model request)", i, c.UA)
		}
		if c.DeviceMid != "" {
			t.Errorf("call %d carried X-Device-Mid, want it absent on model requests", i)
		}
		if c.Referer != "https://zcode.z.ai" {
			t.Errorf("call %d HTTP-Referer = %q, want the zcode origin", i, c.Referer)
		}
		if c.Captcha != "" {
			t.Errorf("call %d carried %q — the captcha header never rides chat", i, c.Captcha)
		}
		if c.Path != "/api/v1/zcode-plan/anthropic/v1/messages" {
			t.Errorf("call %d path = %q, want the zcode-plan messages endpoint", i, c.Path)
		}
	}
	if calls[1].Model != "glm-sonnet-test" {
		t.Errorf("outbound model = %q, want the mapped glm-sonnet-test", calls[1].Model)
	}

	a := f.accountByName("Account A")
	if a.Status != pool.StatusCooling || a.StatusReason != "rate_limit" {
		t.Errorf("Account A = %s (%s), want COOLING (rate_limit)", a.Status, a.StatusReason)
	}
	if b := f.accountByName("Account B").Status; b != pool.StatusActive {
		t.Errorf("Account B = %s, want ACTIVE", b)
	}
}

// =====================================================================================
// Scenario 2 — Account A returns HTTP 200 whose first SSE event is
// `event: error` with code 1005 (the channel A quota code): failover to B, A
// becomes EXHAUSTED until the parsed next_flush_time.
// =====================================================================================
func TestE2E2_FailoverOn1005FirstEvent(t *testing.T) {
	f := newE2E2Accounts(t)
	reset := time.Now().Add(time.Hour).Truncate(time.Second).Unix()
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		switch call.JWT {
		case e2eJWTA:
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "event: error\ndata: {\"error\":{\"code\":\"1005\",\"message\":\"quota exhausted, next_flush_time="+strconv.FormatInt(reset, 10)+"\"}}\n\n")
			if fl, ok := w.(http.Flusher); ok {
				fl.Flush()
			}
		default:
			writeSSE(w, sseFullStream)
		}
	})

	resp, body := f.postMessages(streamRequest("claude-sonnet-4-5-20250929"))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("x-zcode2api-attempts"); got != "2" {
		t.Errorf("x-zcode2api-attempts = %q, want 2", got)
	}
	if !strings.Contains(string(body), "message_start") {
		t.Errorf("client stream missing message_start: %s", body)
	}
	if strings.Contains(string(body), "1005") {
		t.Errorf("client saw the pre-commit error event from A: %s", body)
	}

	a := f.accountByName("Account A")
	if a.Status != pool.StatusActive {
		t.Errorf("Account A = %s (%s), want ACTIVE (a chat 1005 benches only the model pair)", a.Status, a.StatusReason)
	}
	if until, ok := a.ExhaustedModels["glm-sonnet-test"]; !ok || until.Unix() != reset {
		t.Errorf("Account A exhausted models = %v, want glm-sonnet-test until %d", a.ExhaustedModels, reset)
	}
	if b := f.accountByName("Account B"); b.Status != pool.StatusActive {
		t.Errorf("Account B = %s, want ACTIVE", b.Status)
	}
}

// =====================================================================================
// Scenario — the upstream cuts the SSE stream after COMMIT (EOF before
// message_stop): the client still receives a terminal [1312] error frame and
// the account takes a failure-streak hit instead of a success.
// =====================================================================================
func TestE2E_TruncatedStreamGetsTerminalFrame(t *testing.T) {
	f := newE2E2Accounts(t)
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		writeSSE(w, sseMessageStart+sseBlocks) // truncated: no message_delta, no message_stop
	})

	resp, body := f.postMessages(streamRequest("claude-sonnet-4-5-20250929"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "message_start") {
		t.Fatalf("client stream missing message_start: %s", body)
	}
	if strings.Contains(string(body), "event: message_stop") {
		t.Fatal("a truncated upstream stream must not carry message_stop to the client")
	}
	if !strings.Contains(string(body), "event: error") || !strings.Contains(string(body), "[1312][") {
		t.Errorf("client stream missing the terminal [1312] error frame: %s", body)
	}
	if !strings.Contains(string(body), "overloaded_error") {
		t.Errorf("terminal frame is not overloaded_error: %s", body)
	}

	a := f.accountByName("Account A")
	if a.FailStreak != 1 {
		t.Errorf("Account A fail streak = %d, want 1 (EOF without message_stop is a failure)", a.FailStreak)
	}
}

// =====================================================================================
// Scenario — the first upstream answer is model-busy (3010): the SAME account
// is replayed ~1s later (no failover, no cooling), the replay streams OK.
// =====================================================================================
func TestE2E_ModelBusyReplaysSameAccount(t *testing.T) {
	f := newE2E2Accounts(t)
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		if len(f.calls.all()) == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"error":{"code":"3010","message":"model busy"}}`)
			return
		}
		writeSSE(w, sseFullStream)
	})

	resp, body := f.postMessages(streamRequest("claude-sonnet-4-5-20250929"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("x-zcode2api-attempts"); got != "1" {
		t.Errorf("x-zcode2api-attempts = %q, want 1 (replayed on the same account)", got)
	}
	if !strings.Contains(string(body), "message_stop") {
		t.Errorf("client stream missing message_stop: %s", body)
	}
	calls := f.calls.all()
	if len(calls) != 2 {
		t.Fatalf("upstream calls = %d, want 2 (one same-account replay)", len(calls))
	}
	if calls[0].JWT != e2eJWTA || calls[1].JWT != e2eJWTA {
		t.Errorf("both calls must hit the same account, got [%s, %s]", calls[0].JWT, calls[1].JWT)
	}
	if a := f.accountByName("Account A"); a.Status != pool.StatusActive {
		t.Errorf("Account A = %s, want ACTIVE (model-busy never cools the account)", a.Status)
	}
}

// =====================================================================================
// Scenario — the first upstream answer is a platform-wide overload (529): the
// SAME account is replayed ~1s later, the replay streams OK, and the account
// takes no fail-streak (the overload is the platform's, not the account's).
// =====================================================================================
func TestE2E_PlatformOverloadReplaysSameAccount(t *testing.T) {
	f := newE2E2Accounts(t)
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		if len(f.calls.all()) == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(529)
			_, _ = io.WriteString(w, `{"error":{"type":"overloaded_error","message":"platform overloaded"}}`)
			return
		}
		writeSSE(w, sseFullStream)
	})

	resp, body := f.postMessages(streamRequest("claude-sonnet-4-5-20250929"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("x-zcode2api-attempts"); got != "1" {
		t.Errorf("x-zcode2api-attempts = %q, want 1 (replayed on the same account)", got)
	}
	if !strings.Contains(string(body), "message_stop") {
		t.Errorf("client stream missing message_stop: %s", body)
	}
	calls := f.calls.all()
	if len(calls) != 2 {
		t.Fatalf("upstream calls = %d, want 2 (one same-account replay)", len(calls))
	}
	if calls[0].JWT != e2eJWTA || calls[1].JWT != e2eJWTA {
		t.Errorf("both calls must hit the same account, got [%s, %s]", calls[0].JWT, calls[1].JWT)
	}
	a := f.accountByName("Account A")
	if a.Status != pool.StatusActive || a.FailStreak != 0 {
		t.Errorf("Account A = %s streak=%d, want ACTIVE with no streak (platform overload marks nothing)", a.Status, a.FailStreak)
	}
}

// =====================================================================================
// Scenario 3 — An error event AFTER message_start (i.e. after COMMIT): the
// client receives the `event: error` verbatim, there is no retry.
// =====================================================================================
func TestE2E3_MidStreamErrorForwardedNoRetry(t *testing.T) {
	f := newE2E2Accounts(t)
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, sseMessageStart)
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		_, _ = io.WriteString(w, "event: error\ndata: {\"error\":{\"code\":\"1302\",\"message\":\"rate limited mid-stream\"}}\n\n")
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
	})

	resp, body := f.postMessages(streamRequest("claude-sonnet-4-5-20250929"))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d (a committed stream stays 200), body %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("x-zcode2api-attempts"); got != "1" {
		t.Errorf("x-zcode2api-attempts = %q, want 1 (no retry after COMMIT)", got)
	}
	if !strings.Contains(string(body), "message_start") {
		t.Errorf("client stream missing message_start: %s", body)
	}
	if !strings.Contains(string(body), "1302") {
		t.Errorf("client must receive the upstream error event verbatim: %s", body)
	}
	if n := len(f.calls.all()); n != 1 {
		t.Errorf("upstream calls = %d, want 1 (no failover after COMMIT)", n)
	}
	a := f.accountByName("Account A")
	if a.Status != pool.StatusCooling || a.StatusReason != "rate_limit" {
		t.Errorf("Account A = %s (%s), want COOLING (the mid-stream error still classifies the account)", a.Status, a.StatusReason)
	}
}

// =====================================================================================
// Scenario 4 — All three accounts COOLING: the client gets 429
// rate_limit_error with a correct retry-after (seconds until the earliest
// account recovers).
// =====================================================================================
func TestE2E4_AllAccountsCooling429WithRetryAfter(t *testing.T) {
	f := newE2E3Accounts(t)
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"code":"1302","message":"rate limit exceeded"}}`)
	})

	// Step 1: burn MAX_ATTEMPTS on 429s — every account ends up COOLING.
	resp1, body1 := f.postMessages(plainRequest("claude-sonnet-4-5-20250929"))
	if resp1.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("step 1 status = %d, body %s", resp1.StatusCode, body1)
	}
	if got := resp1.Header.Get("x-zcode2api-attempts"); got != "3" {
		t.Errorf("step 1 attempts = %q, want 3", got)
	}
	var errBody struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body1, &errBody); err != nil || errBody.Error.Type != "rate_limit_error" {
		t.Errorf("step 1 error type = %q (err %v), want rate_limit_error", errBody.Error.Type, err)
	}
	for _, name := range []string{"Account A", "Account B", "Account C"} {
		if st := f.accountByName(name).Status; st != pool.StatusCooling {
			t.Errorf("%s = %s, want COOLING", name, st)
		}
	}

	// Step 2: with every account COOLING, Acquire fails with the earliest
	// recovery time and the gateway answers 429 + Retry-After.
	now := time.Now()
	var earliest time.Time
	for _, a := range f.pool.Accounts() {
		if a.CooldownUntil.After(now) && (earliest.IsZero() || a.CooldownUntil.Before(earliest)) {
			earliest = a.CooldownUntil
		}
	}
	resp2, body2 := f.postMessages(streamRequest("claude-sonnet-4-5-20250929"))
	if resp2.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("step 2 status = %d, body %s", resp2.StatusCode, body2)
	}
	if err := json.Unmarshal(body2, &errBody); err != nil || errBody.Error.Type != "rate_limit_error" {
		t.Errorf("step 2 error type = %q, want rate_limit_error", errBody.Error.Type)
	}
	want := int(math.Ceil(time.Until(earliest).Seconds()))
	got, perr := strconv.Atoi(resp2.Header.Get("Retry-After"))
	if perr != nil {
		t.Fatalf("missing/invalid Retry-After: %q", resp2.Header.Get("Retry-After"))
	}
	if got < want-2 || got > want {
		t.Errorf("Retry-After = %d, want ~%d (earliest recovery)", got, want)
	}
	if got := resp2.Header.Get("x-zcode2api-attempts"); got != "0" {
		t.Errorf("step 2 attempts = %q, want 0 (no upstream call was possible)", got)
	}
	if n := len(f.calls.all()); n != 3 {
		t.Errorf("upstream calls = %d, want 3 (step 2 must not touch upstream)", n)
	}
}

// =====================================================================================
// Scenario 5 — Upstream 400: forwarded verbatim, no retry, no account state
// change.
// =====================================================================================
func TestE2E5_ClientError400Passthrough(t *testing.T) {
	f := newE2E2Accounts(t)
	const upstream400 = `{"type":"error","error":{"type":"invalid_request_error","message":"max_tokens: Field required"}}`
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, upstream400)
	})

	resp, body := f.postMessages(plainRequest("claude-sonnet-4-5-20250929"))

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want the upstream 400 kept", resp.StatusCode)
	}
	if strings.TrimSpace(string(body)) != upstream400 {
		t.Errorf("body = %s, want the upstream body verbatim", body)
	}
	if got := resp.Header.Get("x-zcode2api-attempts"); got != "1" {
		t.Errorf("attempts = %q, want 1 (client errors never retry)", got)
	}
	if n := len(f.calls.all()); n != 1 {
		t.Errorf("upstream calls = %d, want 1", n)
	}
	for _, name := range []string{"Account A", "Account B"} {
		a := f.accountByName(name)
		if a.Status != pool.StatusActive || a.StatusReason != "" {
			t.Errorf("%s = %s (%q), want ACTIVE with no reason — client errors never change account state", name, a.Status, a.StatusReason)
		}
	}
}

// =====================================================================================
// Scenario 6 — First chunk is only 10 bytes, the rest arrives after 2s: no
// hang; the client receives data as soon as the first event is complete
// (event-boundary reading, NOT Peek(512)).
// =====================================================================================
func TestE2E6_SmallFirstChunkNoHang(t *testing.T) {
	f := newE2E2Accounts(t)
	var tBoundary, tFinal atomic.Int64
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// First write: exactly 10 bytes, mid-line — an event boundary is only
		// complete 2s later, together with the rest of the line.
		_, _ = io.WriteString(w, "event: mes")
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		time.Sleep(2 * time.Second)
		_, _ = io.WriteString(w, "sage_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10}}}\n\n")
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		tBoundary.Store(time.Now().UnixNano())
		time.Sleep(1500 * time.Millisecond)
		_, _ = io.WriteString(w, sseMessageStop)
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		tFinal.Store(time.Now().UnixNano())
	})

	req, err := http.NewRequest(http.MethodPost, f.gateway.URL+"/v1/messages",
		strings.NewReader(streamRequest("claude-sonnet-4-5-20250929")))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", e2eClientKey)
	start := time.Now()
	resp, err := f.gateway.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	gotHeaders := time.Now()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body %s", resp.StatusCode, raw)
	}
	if got := resp.Header.Get("x-zcode2api-attempts"); got != "1" {
		t.Errorf("attempts = %q, want 1 (the boundary arrived in time)", got)
	}
	if elapsed := gotHeaders.Sub(start); elapsed < 1900*time.Millisecond {
		t.Errorf("headers after %v — the gateway must wait for the event boundary (~2s)", elapsed)
	}
	if elapsed := gotHeaders.Sub(start); elapsed > 4*time.Second {
		t.Errorf("headers after %v — the gateway hung instead of committing at the boundary (FIRST_EVENT_TIMEOUT=5s)", elapsed)
	}
	if fin := tFinal.Load(); fin == 0 || gotHeaders.UnixNano() >= fin {
		t.Errorf("client got headers at %d but the final upstream write was at %d — data must flow right after the first complete event, not at stream end",
			gotHeaders.UnixNano(), fin)
	}
	if !strings.Contains(string(raw), "message_start") || !strings.Contains(string(raw), "message_stop") {
		t.Errorf("client stream incomplete: %s", raw)
	}
}

// =====================================================================================
// Scenario 7 — The client closes the connection mid-stream: the upstream
// request is canceled and inflight drops back to 0.
// =====================================================================================
func TestE2E7_ClientCancelCancelsUpstream(t *testing.T) {
	f := newE2E2Accounts(t)
	var canceled atomic.Bool
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		writeSSE(w, sseMessageStart)
		<-r.Context().Done()
		canceled.Store(true)
	})

	req, err := http.NewRequest(http.MethodPost, f.gateway.URL+"/v1/messages",
		strings.NewReader(streamRequest("claude-sonnet-4-5-20250929")))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", e2eClientKey)
	resp, err := f.gateway.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	// Read the committed first event, then hang up mid-stream.
	buf := make([]byte, 32)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		t.Fatalf("read first bytes: %v", err)
	}
	resp.Body.Close()

	waitFor(t, 10*time.Second, "upstream cancellation and inflight==0", func() bool {
		return canceled.Load() && f.pool.InflightTotal() == 0
	})
	if !canceled.Load() {
		t.Error("upstream request was not canceled after the client disconnected")
	}
	if n := f.pool.InflightTotal(); n != 0 {
		t.Errorf("inflight = %d, want 0", n)
	}
}

// =====================================================================================
// Scenario 8 — GET /api/logs/stream receives request, failover and state
// events for the failover case above (`acc` field,).
// =====================================================================================
func TestE2E8_AdminLogStreamEvents(t *testing.T) {
	f := newE2E2Accounts(t)
	cookie := f.adminLogin()

	// Subscribe before the request so every event is observed.
	req, err := http.NewRequest(http.MethodGet, f.gateway.URL+"/api/logs/stream", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.AddCookie(cookie)
	sseResp, err := f.gateway.Client().Do(req)
	if err != nil {
		t.Fatalf("logs/stream: %v", err)
	}
	// Registered after the fixture cleanup, so it runs first (LIFO): the SSE
	// request must end before httptest.Server.Close waits on it.
	t.Cleanup(func() { sseResp.Body.Close() })
	if ct := sseResp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("logs/stream Content-Type = %q", ct)
	}
	col := &sseCollector{}
	go collectSSE(sseResp.Body, col)

	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		switch call.JWT {
		case e2eJWTA:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"error":{"code":"1302","message":"rate limit exceeded"}}`)
		default:
			writeSSE(w, sseFullStream)
		}
	})
	resp, body := f.postMessages(streamRequest("claude-sonnet-4-5-20250929"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("request status = %d, body %s", resp.StatusCode, body)
	}

	waitFor(t, 10*time.Second, "request, failover and state events", func() bool {
		evs := col.snapshot()
		return hasEvent(evs, "request") && hasEvent(evs, "failover") && hasEvent(evs, "state")
	})
	evs := col.snapshot()

	var requestEv struct {
		RID      string `json:"rid"`
		Acc      string `json:"acc"`
		Channel  string `json:"channel"`
		Status   int    `json:"status"`
		Attempts int    `json:"attempts"`
	}
	for _, e := range evs {
		if e.Name == "request" {
			_ = json.Unmarshal([]byte(e.Data), &requestEv)
		}
	}
	if requestEv.RID == "" {
		t.Error("request event missing rid")
	}
	if requestEv.Acc != f.accountID("Account B") || requestEv.Channel != "A" {
		t.Errorf("request event acc/channel = %q/%q, want the serving account on channel A", requestEv.Acc, requestEv.Channel)
	}
	if requestEv.Attempts != 2 || requestEv.Status != 200 {
		t.Errorf("request event = %+v, want status 200 attempts 2", requestEv)
	}

	var failoverEv struct {
		RID  string `json:"rid"`
		From string `json:"from"`
		To   string `json:"to"`
		Kind string `json:"kind"`
	}
	for _, e := range evs {
		if e.Name == "failover" {
			_ = json.Unmarshal([]byte(e.Data), &failoverEv)
		}
	}
	if failoverEv.From != f.accountID("Account A") || failoverEv.To != f.accountID("Account B") {
		t.Errorf("failover event = %+v, want from Account A to Account B", failoverEv)
	}
	if failoverEv.Kind != "rate_limit" {
		t.Errorf("failover kind = %q, want rate_limit", failoverEv.Kind)
	}
	if failoverEv.RID != requestEv.RID {
		t.Errorf("failover rid %q does not match request rid %q", failoverEv.RID, requestEv.RID)
	}

	var stateEv struct {
		Acc string `json:"acc"`
		To  string `json:"to"`
	}
	for _, e := range evs {
		if e.Name == "state" {
			_ = json.Unmarshal([]byte(e.Data), &stateEv)
		}
	}
	if stateEv.Acc != f.accountID("Account A") || stateEv.To != "COOLING" {
		t.Errorf("state event = %+v, want Account A -> COOLING", stateEv)
	}
}

// =====================================================================================
// Scenario 9 — Without a cookie /api/* returns 401; a write without
// X-Requested-With returns 403.
// =====================================================================================
func TestE2E9_AdminAuthGuards(t *testing.T) {
	f := newE2E3Accounts(t)

	resp, body := f.admin(http.MethodGet, "/api/stats", "", nil, false)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET /api/stats without cookie = %d, want 401", resp.StatusCode)
	}
	if !strings.Contains(string(body), "\"error\"") {
		t.Errorf("401 body %s is not {\"error\":...}", body)
	}

	resp, _ = f.admin(http.MethodPost, "/api/accounts",
		`{"lines":"NoCSRF | `+e2eJWTNoCSRF+`"}`, nil, false)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("POST /api/accounts without cookie = %d, want 401", resp.StatusCode)
	}

	cookie := f.adminLogin()

	resp, _ = f.admin(http.MethodPost, "/api/accounts",
		`{"lines":"NoCSRF | `+e2eJWTNoCSRF+`"}`, cookie, false)
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("POST /api/accounts without X-Requested-With = %d, want 403", resp.StatusCode)
	}

	resp, _ = f.admin(http.MethodGet, "/api/stats", "", cookie, false)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /api/stats with cookie = %d, want 200 (GET needs no CSRF header)", resp.StatusCode)
	}

	resp, _ = f.admin(http.MethodPost, "/api/accounts",
		`{"lines":"CSRF OK | `+e2eJWTExtra+`"}`, cookie, true)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("POST /api/accounts with cookie+CSRF = %d, want 200", resp.StatusCode)
	}

	// Wrong password stays 401; the login route itself needs no cookie/CSRF.
	resp, _ = f.admin(http.MethodPost, "/api/login", `{"password":"wrong-password-123456"}`, nil, false)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("login with wrong password = %d, want 401", resp.StatusCode)
	}
}

// =====================================================================================
// Scenario 10 — No admin response contains the full JWT (grep the fake token
// across every response body of the exercised endpoints).
// =====================================================================================
func TestE2E10_NoAdminResponseLeaksToken(t *testing.T) {
	f := newE2E3Accounts(t)
	cookie := f.adminLogin()

	token := e2eJWTLeak
	hint := token[len(token)-4:] // last 4 characters

	resp, body := f.admin(http.MethodPost, "/api/accounts",
		fmt.Sprintf(`{"lines":"Leak Probe | %s"}`, token), cookie, true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("import status = %d, body %s", resp.StatusCode, body)
	}

	var all strings.Builder
	all.Write(body)
	endpoints := []struct{ method, path string }{
		{http.MethodGet, "/api/accounts"},
		{http.MethodGet, "/api/accounts?status=ACTIVE"},
		{http.MethodGet, "/api/accounts?q=leak"},
		{http.MethodGet, "/api/stats"},
		{http.MethodGet, "/api/settings"},
		{http.MethodGet, "/api/settings/export"},
		{http.MethodGet, "/api/mappings"},
		{http.MethodGet, "/api/mappings/resolve?model=claude-sonnet-4-5"},
		{http.MethodGet, "/api/proxies"},
		{http.MethodGet, "/api/logs?limit=200"},
		{http.MethodGet, "/api/session"},
	}
	for _, ep := range endpoints {
		r, b := f.admin(ep.method, ep.path, "", cookie, false)
		if r.StatusCode != http.StatusOK {
			t.Errorf("%s %s = %d, want 200", ep.method, ep.path, r.StatusCode)
			continue
		}
		all.Write(b)
	}

	if strings.Contains(all.String(), token) {
		t.Errorf("the full JWT leaked into admin responses")
	}
	if !strings.Contains(all.String(), "…"+hint) {
		t.Errorf("masked token form %q missing from admin responses", "…"+hint)
	}

	// The export carries the hint only, and explicitly no token field with
	// JWT material.
	var export struct {
		Accounts []struct {
			TokenHint string `json:"token_hint"`
			Name      string `json:"name"`
		} `json:"accounts"`
	}
	r, b := f.admin(http.MethodGet, "/api/settings/export", "", cookie, false)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("export status = %d", r.StatusCode)
	}
	if err := json.Unmarshal(b, &export); err != nil {
		t.Fatalf("export is not JSON: %v", err)
	}
	found := false
	for _, a := range export.Accounts {
		if a.Name == "Leak Probe" {
			found = true
			if a.TokenHint != hint {
				t.Errorf("export token_hint = %q, want %q", a.TokenHint, hint)
			}
		}
	}
	if !found {
		t.Error("export is missing the imported account")
	}
}

// =====================================================================================
// Scenario 11 — Pool exhausted + FALLBACK_API_KEY configured: one channel
// B attempt (x-api-key, gateway User-Agent) serves the request.
// =====================================================================================
func TestE2E11_FallbackChannelBWhenPoolExhausted(t *testing.T) {
	// No accounts at all: Acquire fails with ErrPoolExhausted immediately.
	f := newE2EWithAccounts(t, func(cfg *config.Config) { cfg.FallbackAPIKey = e2eFallbackKey })
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		if call.Path != "/api/anthropic/v1/messages" {
			t.Errorf("fallback path = %q, want /api/anthropic/v1/messages", call.Path)
		}
		if call.APIKey != e2eFallbackKey {
			t.Errorf("fallback x-api-key = %q, want the FALLBACK_API_KEY", call.APIKey)
		}
		if call.JWT != "" {
			t.Error("a fallback attempt must not carry a pool JWT")
		}
		if !strings.HasPrefix(call.UA, "Zcode2Api/") {
			t.Errorf("fallback User-Agent = %q, want Zcode2Api/<version>", call.UA)
		}
		writeSSE(w, sseFullStream)
	})

	resp, body := f.postMessages(streamRequest("claude-sonnet-4-5-20250929"))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("x-zcode2api-attempts"); got != "1" {
		t.Errorf("attempts = %q, want 1 (the single channel B attempt)", got)
	}
	if got := resp.Header.Get("x-zcode2api-channel"); got != "B" {
		t.Errorf("x-zcode2api-channel = %q, want B (the fallback channel served)", got)
	}
	if !strings.Contains(string(body), "message_stop") {
		t.Errorf("client stream incomplete: %s", body)
	}
	if calls := f.calls.all(); len(calls) != 1 {
		t.Errorf("upstream calls = %d, want 1 (channel B only)", len(calls))
	}
}

// =====================================================================================
// Scenario 12 — The claim endpoint: upstream 3007 without a solver →
// {"captcha_required":true} and no crash.
// =====================================================================================
func TestE2E12_ClaimEndpoint3007WithoutSolver(t *testing.T) {
	f := newE2E3Accounts(t)
	cookie := f.adminLogin()

	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		if call.Path != "/api/v1/zcode-plan/billing/claim" {
			t.Errorf("claim path = %q", call.Path)
		}
		if call.JWT != e2eJWTA {
			t.Errorf("claim JWT = %q, want the target account's JWT", call.JWT)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"error":{"code":"3007","message":"captcha required"}}`)
	})

	id := f.accountID("Account A")
	resp, body := f.admin(http.MethodPost, "/api/accounts/"+id+"/claim", "", cookie, true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("claim status = %d, body %s", resp.StatusCode, body)
	}
	var report struct {
		OK              bool   `json:"ok"`
		Kind            string `json:"kind"`
		CaptchaRequired bool   `json:"captcha_required"`
	}
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("claim body not JSON: %v (%s)", err, body)
	}
	if report.OK || !report.CaptchaRequired || report.Kind != "captcha_required" {
		t.Errorf("claim report = %+v, want {ok:false, captcha_required:true, kind:captcha_required}", report)
	}

	// The claim was recorded on the account.
	waitFor(t, 5*time.Second, "last_claim_kind recorded", func() bool {
		a := f.accountByName("Account A")
		return a.LastClaimKind == "captcha_required"
	})
	if evs := f.logs.Snapshot(10); len(evs) == 0 {
		t.Error("no log entry recorded for the claim")
	}
}

// =====================================================================================
// Scenario 13 — The claim endpoint with a fake solver: 3007 → solver
// POST → verify_param → the retried claim succeeds with the captcha header.
// =====================================================================================
func TestE2E13_ClaimEndpointWithFakeSolver(t *testing.T) {
	solverCalls := 0
	var solverToken, captchaHeader string
	solverSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		solverCalls++
		body, _ := io.ReadAll(r.Body)
		var req map[string]string
		_ = json.Unmarshal(body, &req)
		solverToken = req["token"]
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"verify_param":"solver-verify-param"}`)
	}))
	defer solverSrv.Close()

	f := newE2EWithAccounts(t, func(cfg *config.Config) { cfg.ClaimSolverURL = solverSrv.URL },
		"Account A | "+e2eJWTA)
	cookie := f.adminLogin()

	var claimCalls int
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if call.Path == "/api/v1/zcode-plan/billing/balance" {
			// The post-claim balance refresh rides the same fake origin.
			_, _ = io.WriteString(w, `{"code":0,"data":{"balances":[{"total_units":10,"used_units":0,"remaining_units":10,"expires_at":1795046400}]}}`)
			return
		}
		if call.Path != "/api/v1/zcode-plan/billing/claim" {
			t.Errorf("claim path = %q", call.Path)
			_, _ = io.WriteString(w, `{"code":0}`)
			return
		}
		claimCalls++
		if claimCalls == 1 {
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"error":{"code":"3007","message":"captcha required"}}`)
			return
		}
		captchaHeader = call.Captcha
		_, _ = io.WriteString(w, `{"code":0,"message":"ok"}`)
	})

	id := f.accountID("Account A")
	resp, body := f.admin(http.MethodPost, "/api/accounts/"+id+"/claim", "", cookie, true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("claim status = %d, body %s", resp.StatusCode, body)
	}
	var report struct {
		OK              bool   `json:"ok"`
		Kind            string `json:"kind"`
		Claimed         bool   `json:"claimed"`
		CaptchaRequired bool   `json:"captcha_required"`
	}
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("claim body not JSON: %v (%s)", err, body)
	}
	if !report.OK || report.Kind != "ok" || report.CaptchaRequired {
		t.Errorf("claim report = %+v, want a successful claim via the solver", report)
	}
	if solverCalls != 1 {
		t.Errorf("solver calls = %d, want 1", solverCalls)
	}
	if solverToken != e2eJWTA {
		t.Errorf("solver token = %q, want the account JWT", solverToken)
	}
	if captchaHeader != "solver-verify-param" {
		t.Errorf("retried claim captcha header = %q, want the solver verify_param", captchaHeader)
	}
	if claimCalls != 2 {
		t.Errorf("claim calls = %d, want 2 (initial + one retry)", claimCalls)
	}
	waitFor(t, 5*time.Second, "last_claim_kind ok", func() bool {
		a := f.accountByName("Account A")
		return a.LastClaimKind == "ok"
	})
}

// =====================================================================================
// Scenario 1 — Chat non-stream success via channel A: the pooled JWT, the
// full ZCode source header set, the mapped outbound model and the gateway
// debug headers.
// =====================================================================================
func TestE2EScenario1_NonStreamSuccessChannelA(t *testing.T) {
	f := newE2E2Accounts(t)
	const reply = `{"id":"msg_01","type":"message","role":"assistant","model":"glm-sonnet-test",` +
		`"content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn",` +
		`"usage":{"input_tokens":12,"output_tokens":34}}`
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		if call.JWT != e2eJWTA {
			t.Errorf("Authorization JWT = %q, want the pooled account A JWT", call.JWT)
		}
		if call.Path != "/api/v1/zcode-plan/anthropic/v1/messages" {
			t.Errorf("path = %q, want the zcode-plan messages endpoint", call.Path)
		}
		if !strings.HasPrefix(call.UA, "ZCode/") {
			t.Errorf("User-Agent = %q, want ZCode/<ZCODE_APP_VERSION>", call.UA)
		}
		if call.Referer != "https://zcode.z.ai" {
			t.Errorf("HTTP-Referer = %q, want the zcode origin", call.Referer)
		}
		if call.Title != "Z Code@electron" {
			t.Errorf("X-Title = %q, want %q", call.Title, "Z Code@electron")
		}
		if call.Platform == "" {
			t.Error("missing X-Platform")
		}
		if call.DeviceMid != "" {
			t.Error("model request carried X-Device-Mid, want it absent ( golden parity)")
		}
		if call.AnthropicVersion != "2023-06-01" {
			t.Errorf("anthropic-version = %q, want the default 2023-06-01 (client sent none)", call.AnthropicVersion)
		}
		if call.Captcha != "" {
			t.Errorf("call carried %q — the captcha header never rides chat", call.Captcha)
		}
		if call.Model != "glm-sonnet-test" {
			t.Errorf("outbound model = %q, want the mapped glm-sonnet-test", call.Model)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, reply)
	})

	resp, body := f.postMessages(plainRequest("claude-sonnet-4-5-20250929"))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body %s", resp.StatusCode, body)
	}
	if strings.TrimSpace(string(body)) != reply {
		t.Errorf("body = %s, want the upstream reply verbatim", body)
	}
	if got := resp.Header.Get("x-zcode2api-attempts"); got != "1" {
		t.Errorf("x-zcode2api-attempts = %q, want 1", got)
	}
	if got := resp.Header.Get("x-zcode2api-channel"); got != "A" {
		t.Errorf("x-zcode2api-channel = %q, want A", got)
	}
}

// =====================================================================================
// Scenario 2 — Chat stream SSE passthrough with a flush
// per chunk: chunk 1 (message_start) must reach the client while the upstream
// is still between chunk 2 and chunk 3 — the gateway may not buffer the whole
// stream before writing.
// =====================================================================================
func TestE2EScenario2_StreamFlushPerChunk(t *testing.T) {
	f := newE2E2Accounts(t)
	var thirdChunkAt atomic.Int64
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, sseMessageStart) // chunk 1
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		time.Sleep(500 * time.Millisecond)
		_, _ = io.WriteString(w, sseBlocks) // chunk 2
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		time.Sleep(500 * time.Millisecond)
		thirdChunkAt.Store(time.Now().UnixNano())
		_, _ = io.WriteString(w, sseMessageDelta+sseMessageStop) // chunk 3
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
	})

	req, err := http.NewRequest(http.MethodPost, f.gateway.URL+"/v1/messages",
		strings.NewReader(streamRequest("claude-sonnet-4-5-20250929")))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", e2eClientKey)
	resp, err := f.gateway.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	// Read incrementally, timestamping the arrival of the first and last events.
	var startArrived, stopArrived atomic.Int64
	br := bufio.NewReader(resp.Body)
	var block strings.Builder
	for stopArrived.Load() == 0 {
		line, rerr := br.ReadString('\n')
		block.WriteString(line)
		if rerr != nil {
			t.Fatalf("stream ended before message_stop: %v (block %q)", rerr, block.String())
		}
		if line == "\n" || line == "\r\n" {
			name, _ := parseSSEBlock(block.String())
			switch name {
			case "message_start":
				startArrived.Store(time.Now().UnixNano())
			case "message_stop":
				stopArrived.Store(time.Now().UnixNano())
			}
			block.Reset()
		}
	}
	third := thirdChunkAt.Load()
	if third == 0 {
		t.Fatal("upstream never wrote chunk 3")
	}
	if startArrived.Load() == 0 {
		t.Fatal("client never received message_start")
	}
	if startArrived.Load() >= third {
		t.Errorf("message_start arrived at %d, at/after chunk 3 written at %d — the gateway buffered instead of flushing per chunk",
			startArrived.Load(), third)
	}
	if stopArrived.Load() < third {
		t.Errorf("message_stop arrived at %d, before chunk 3 was written at %d — impossible unless fabricated",
			stopArrived.Load(), third)
	}
}

// =====================================================================================
// Scenario 3 — Upstream HTTP 200 with a PLAIN JSON body
// {"code":1005} on a stream request (the channel A pure-JSON branch):
// classify without SSE framing, failover to B, client
// receives the full stream, and the exhaustion lands on the (account, model)
// pair — the account stays ACTIVE for its other models. (The SSE-framed
// variant of the same scenario is TestE2E2_FailoverOn1005FirstEvent.)
// =====================================================================================
func TestE2EScenario3_PureJSON1005Failover(t *testing.T) {
	f := newE2E2Accounts(t)
	reset := time.Now().Add(time.Hour).Truncate(time.Second).Unix()
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		switch call.JWT {
		case e2eJWTA:
			// application/json (NOT text/event-stream) at HTTP 200 — the whole
			// body is the "first event".
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"error":{"code":"1005","message":"quota exhausted, next_flush_time=`+
				strconv.FormatInt(reset, 10)+`"}}`)
		default:
			writeSSE(w, sseFullStream)
		}
	})

	resp, body := f.postMessages(streamRequest("claude-sonnet-4-5-20250929"))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("x-zcode2api-attempts"); got != "2" {
		t.Errorf("x-zcode2api-attempts = %q, want 2 (failover to the second account)", got)
	}
	if !strings.Contains(string(body), "message_start") || !strings.Contains(string(body), "message_stop") {
		t.Errorf("client stream incomplete: %s", body)
	}
	if strings.Contains(string(body), "1005") {
		t.Errorf("client saw the pre-commit JSON error from A: %s", body)
	}

	a := f.accountByName("Account A")
	if a.Status != pool.StatusActive {
		t.Errorf("Account A = %s (%s), want ACTIVE (a chat 1005 benches only the model pair)", a.Status, a.StatusReason)
	}
	if until, ok := a.ExhaustedModels["glm-sonnet-test"]; !ok || until.Unix() != reset {
		t.Errorf("Account A exhausted models = %v, want glm-sonnet-test until %d", a.ExhaustedModels, reset)
	}
	if !a.CooldownUntil.IsZero() {
		t.Errorf("Account A cooldown until = %s, want zero (no account-wide bench)", a.CooldownUntil)
	}
}

// =====================================================================================
// Scenario 4 — Upstream code 3012 (WAF unusual activity,
// HTTP 200 JSON): the account enters RISK_COOLDOWN (pool COOLING with the
// RISK_COOLDOWN duration), the request retries with another
// account and succeeds. The egress proxy is never rotated on a 3012
// — only the account is isolated.
// =====================================================================================
func TestE2EScenario4_RiskBlock3012Retry(t *testing.T) {
	f := newE2EWithAccounts(t, func(cfg *config.Config) { cfg.RiskCooldown = 2 * time.Minute },
		"Account A | "+e2eJWTA, "Account B | "+e2eJWTB)
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		switch call.JWT {
		case e2eJWTA:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"error":{"code":"3012","message":"unusual activity, please retry later"}}`)
		default:
			writeSSE(w, sseFullStream)
		}
	})

	resp, body := f.postMessages(streamRequest("claude-sonnet-4-5-20250929"))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("x-zcode2api-attempts"); got != "2" {
		t.Errorf("x-zcode2api-attempts = %q, want 2", got)
	}
	if !strings.Contains(string(body), "message_stop") {
		t.Errorf("client stream incomplete: %s", body)
	}
	if strings.Contains(string(body), "3012") {
		t.Errorf("client saw the pre-commit risk block from A: %s", body)
	}
	calls := f.calls.all()
	if len(calls) != 2 || calls[0].JWT != e2eJWTA || calls[1].JWT != e2eJWTB {
		t.Errorf("attempt order = %d calls [%s, %s], want A then B",
			len(calls), calls[0].JWT, calls[1].JWT)
	}

	a := f.accountByName("Account A")
	if a.Status != pool.StatusCooling || a.StatusReason != "risk_block" {
		t.Errorf("Account A = %s (%s), want COOLING (risk_block)", a.Status, a.StatusReason)
	}
	// RISK_COOLDOWN = 2m in this fixture (applied through PUT /api/settings
	// semantics by server.NewServer); the cooldown must reflect it, not the
	// 60s rate-limit COOLING_DURATION.
	lo := time.Now().Add(90 * time.Second).Unix()
	hi := time.Now().Add(150 * time.Second).Unix()
	if got := a.CooldownUntil.Unix(); got < lo || got > hi {
		t.Errorf("Account A cooldown until = %d, want within [%d, %d] (RISK_COOLDOWN=2m)", got, lo, hi)
	}
	if b := f.accountByName("Account B").Status; b != pool.StatusActive {
		t.Errorf("Account B = %s, want ACTIVE", b)
	}
}

// =====================================================================================
// Scenario 5 — Upstream code 1006 (JWT dead, HTTP 200):
// the account is DISABLED, the request retries
// with another account and succeeds.
// =====================================================================================
func TestE2EScenario5_TokenDead1006Retry(t *testing.T) {
	f := newE2E2Accounts(t)
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		switch call.JWT {
		case e2eJWTA:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"error":{"code":"1006","message":"token expired"}}`)
		default:
			writeSSE(w, sseFullStream)
		}
	})

	resp, body := f.postMessages(streamRequest("claude-sonnet-4-5-20250929"))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("x-zcode2api-attempts"); got != "2" {
		t.Errorf("x-zcode2api-attempts = %q, want 2", got)
	}
	if !strings.Contains(string(body), "message_stop") {
		t.Errorf("client stream incomplete: %s", body)
	}
	if strings.Contains(string(body), "1006") {
		t.Errorf("client saw the pre-commit auth error from A: %s", body)
	}

	a := f.accountByName("Account A")
	if a.Status != pool.StatusDisabled || a.StatusReason != "auth" {
		t.Errorf("Account A = %s (%s), want DISABLED (auth)", a.Status, a.StatusReason)
	}
	if b := f.accountByName("Account B").Status; b != pool.StatusActive {
		t.Errorf("Account B = %s, want ACTIVE", b)
	}
}

// =====================================================================================
// Scenario 9, second half — an unknown model with
// STRICT_MODELS=true is rejected with 404 not_found_error before any upstream
// call; a mapped model still passes.
// =====================================================================================
func TestE2EScenario9_StrictModels404(t *testing.T) {
	f := newE2EWithAccounts(t, func(cfg *config.Config) { cfg.StrictModels = true },
		"Account A | "+e2eJWTA)

	resp, body := f.postMessages(plainRequest("totally-unknown-model"))

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, body %s", resp.StatusCode, body)
	}
	var e struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil || e.Error.Type != "not_found_error" {
		t.Errorf("error body = %s (err %v), want not_found_error", body, err)
	}
	if n := len(f.calls.all()); n != 0 {
		t.Errorf("upstream calls = %d, want 0 (the model is rejected before acquire)", n)
	}

	// A mapped model passes the strict gate and reaches channel A.
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		if call.JWT != e2eJWTA {
			t.Errorf("JWT = %q, want account A", call.JWT)
		}
		writeSSE(w, sseFullStream)
	})
	resp2, body2 := f.postMessages(streamRequest("claude-sonnet-4-5"))
	if resp2.StatusCode != http.StatusOK || !strings.Contains(string(body2), "message_stop") {
		t.Errorf("mapped model under STRICT_MODELS = %d %s, want a 200 stream", resp2.StatusCode, body2)
	}
}

// =====================================================================================
// Acceptance extra — 429 when both channels are exhausted: the
// pool is empty AND the fallback key answers 1308 (usage limit): the client
// gets 429 rate_limit_error with the documented "upstream quota exhausted"
// message, x-zcode2api-channel: B and a Retry-After taken from
// EXHAUSTED_FALLBACK (no next_flush_time in the body).
// =====================================================================================
func TestE2EScenario7b_BothChannelsExhausted429(t *testing.T) {
	f := newE2EWithAccounts(t, func(cfg *config.Config) { cfg.FallbackAPIKey = e2eFallbackKey })
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		if call.Path != "/api/anthropic/v1/messages" {
			t.Errorf("fallback path = %q, want /api/anthropic/v1/messages", call.Path)
		}
		if call.APIKey != e2eFallbackKey {
			t.Errorf("fallback x-api-key = %q, want the FALLBACK_API_KEY", call.APIKey)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"error":{"code":"1308","message":"usage limit reached"}}`)
	})

	resp, body := f.postMessages(streamRequest("claude-sonnet-4-5-20250929"))

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, body %s", resp.StatusCode, body)
	}
	var e struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("body not JSON: %v (%s)", err, body)
	}
	if e.Error.Type != "rate_limit_error" {
		t.Errorf("error.type = %q, want rate_limit_error", e.Error.Type)
	}
	if e.Error.Message != "upstream quota exhausted" {
		t.Errorf("error.message = %q, want %q", e.Error.Message, "upstream quota exhausted")
	}
	if got := resp.Header.Get("x-zcode2api-attempts"); got != "1" {
		t.Errorf("x-zcode2api-attempts = %q, want 1 (the single channel B attempt)", got)
	}
	if got := resp.Header.Get("x-zcode2api-channel"); got != "B" {
		t.Errorf("x-zcode2api-channel = %q, want B", got)
	}
	// No next_flush_time in the body → EXHAUSTED_FALLBACK (1h) bounds the wait.
	ra, perr := strconv.Atoi(resp.Header.Get("Retry-After"))
	if perr != nil || ra <= 0 || ra > 3600+5 {
		t.Errorf("Retry-After = %q (err %v), want ~3600 (EXHAUSTED_FALLBACK)", resp.Header.Get("Retry-After"), perr)
	}
	if n := len(f.calls.all()); n != 1 {
		t.Errorf("upstream calls = %d, want 1 (channel B only)", n)
	}
}

// =====================================================================================
// Extra coverage for the endpoints of that the scenarios
// do not exercise directly: /healthz, /v1/models, /v1/messages/count_tokens,
// client authentication, MAX_BODY_BYTES and the login rate limit.
// =====================================================================================
func TestE2EHealthzAndModels(t *testing.T) {
	f := newE2E3Accounts(t)

	resp, body := f.admin(http.MethodGet, "/healthz", "", nil, false)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"status":"ok"`) {
		t.Errorf("/healthz = %d %s, want 200 {\"status\":\"ok\"}", resp.StatusCode, body)
	}

	req, _ := http.NewRequest(http.MethodGet, f.gateway.URL+"/v1/models", nil)
	req.Header.Set("X-Api-Key", e2eClientKey)
	r, err := f.gateway.Client().Do(req)
	if err != nil {
		t.Fatalf("/v1/models: %v", err)
	}
	raw, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != http.StatusOK {
		t.Fatalf("/v1/models = %d, body %s", r.StatusCode, raw)
	}
	var list struct {
		Data []struct {
			Type        string `json:"type"`
			ID          string `json:"id"`
			DisplayName string `json:"display_name"`
		} `json:"data"`
		HasMore bool `json:"has_more"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("/v1/models not JSON: %v", err)
	}
	// The outbound models of the rules (prefix patterns are
	// not concrete ids).
	ids := map[string]bool{}
	for _, m := range list.Data {
		ids[m.ID] = true
		if m.Type != "model" {
			t.Errorf("model entry type = %q, want \"model\"", m.Type)
		}
	}
	if !ids["glm-sonnet-test"] {
		t.Errorf("/v1/models missing the outbound model glm-sonnet-test: %s", raw)
	}
	if ids["claude-sonnet-*"] {
		t.Error("/v1/models must not list prefix patterns")
	}
	if list.HasMore {
		t.Error("has_more = true, want false")
	}
}

func TestE2ECountTokensPassthrough(t *testing.T) {
	f := newE2E2Accounts(t)
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		if call.Path != "/api/v1/zcode-plan/anthropic/v1/messages/count_tokens" {
			t.Errorf("upstream path = %q, want the channel A count_tokens endpoint", call.Path)
		}
		if call.JWT != e2eJWTA {
			t.Errorf("count_tokens JWT = %q, want the pooled account JWT", call.JWT)
		}
		if call.Model != "glm-sonnet-test" {
			t.Errorf("count_tokens model = %q, want the mapped glm-sonnet-test", call.Model)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"input_tokens":42}`)
	})

	resp, body := f.post("/v1/messages/count_tokens",
		`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}`, nil)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"input_tokens":42`) {
		t.Errorf("count_tokens = %d %s", resp.StatusCode, body)
	}

	// A 404 from upstream is passed through verbatim so the client estimates
	// the count itself.
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"type":"error","error":{"type":"not_found_error","message":"not supported"}}`)
	})
	resp, body = f.post("/v1/messages/count_tokens",
		`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}`, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("count_tokens 404 = %d, want 404 verbatim", resp.StatusCode)
	}
	if !strings.Contains(string(body), "not_found_error") {
		t.Errorf("count_tokens 404 body = %s, want the upstream body", body)
	}

	// A rate limit on the single-shot path is not failed over; the client
	// still gets an Anthropic-shaped 429.
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"code":"1302","message":"rate limit exceeded"}}`)
	})
	resp, body = f.post("/v1/messages/count_tokens",
		`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}`, nil)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("count_tokens 429 = %d, want 429", resp.StatusCode)
	}
	var errBody struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &errBody); err != nil || errBody.Error.Type != "rate_limit_error" {
		t.Errorf("count_tokens 429 shape = %s, want rate_limit_error", body)
	}
}

func TestE2EClientAuth401(t *testing.T) {
	f := newE2E2Accounts(t)

	req, _ := http.NewRequest(http.MethodPost, f.gateway.URL+"/v1/messages",
		strings.NewReader(plainRequest("claude-sonnet-4-5")))
	resp, err := f.gateway.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("missing key = %d, want 401", resp.StatusCode)
	}
	var e struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &e); err != nil || e.Error.Type != "authentication_error" {
		t.Errorf("missing key body = %s, want authentication_error", raw)
	}

	// Bearer form works (Claude Code with ANTHROPIC_AUTH_TOKEN).
	req, _ = http.NewRequest(http.MethodGet, f.gateway.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+e2eClientKey)
	resp, err = f.gateway.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Bearer auth = %d, want 200", resp.StatusCode)
	}
}

func TestE2EBodyTooLarge413(t *testing.T) {
	f := newE2EWithAccounts(t, func(cfg *config.Config) { cfg.MaxBodyBytes = 64 },
		"Account A | "+e2eJWTA)
	big := `{"model":"claude-sonnet-4-5","stream":false,"messages":[{"role":"user","content":"` +
		strings.Repeat("x", 200) + `"}]}`
	resp, body := f.postMessages(big)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized body = %d, want 413", resp.StatusCode)
	}
	var e struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil || e.Error.Type != "request_too_large" {
		t.Errorf("413 body = %s, want request_too_large", body)
	}
	if n := len(f.calls.all()); n != 0 {
		t.Errorf("oversized body reached upstream %d times, want 0", n)
	}
}

func TestE2ELoginRateLimit(t *testing.T) {
	f := newE2E3Accounts(t)
	for i := 0; i < 5; i++ {
		resp, _ := f.admin(http.MethodPost, "/api/login", `{"password":"wrong-password-123456"}`, nil, false)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d, want 401", i+1, resp.StatusCode)
		}
	}
	resp, body := f.admin(http.MethodPost, "/api/login", `{"password":"wrong-password-123456"}`, nil, false)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("6th attempt = %d, want 429 (5 per minute per IP)", resp.StatusCode)
	}
	if !strings.Contains(string(body), "\"error\"") {
		t.Errorf("429 body = %s", body)
	}
}

// =====================================================================================
// Graceful shutdown — the real http.Server from server.NewServer serves a
// stream; server.Shutdown must wait for the in-flight stream to finish, up
// to 30s, then stop accepting new requests.
// =====================================================================================
func TestE2EGracefulShutdownWaitsForStreams(t *testing.T) {
	fu := &fakeUpstream{}
	upSrv := httptest.NewServer(http.HandlerFunc(fu.ServeHTTP))
	t.Cleanup(upSrv.Close)

	cfg := &config.Config{
		ClientAPIKeys:      []string{e2eClientKey},
		AdminPassword:      e2eAdminPass,
		DBPath:             filepath.Join(t.TempDir(), "shutdown.db"),
		UpstreamURL:        upSrv.URL, // channel A origin (fake zcode-plan)
		FallbackURL:        upSrv.URL,
		DefaultModelHaiku:  "glm-haiku-test",
		InflightLimit:      3,
		MaxAttempts:        3,
		AttemptDeadline:    15 * time.Second,
		FirstEventTimeout:  5 * time.Second,
		FirstEventMaxBytes: 65536,
		MaxBodyBytes:       1 << 20,
		CoolingDuration:    60 * time.Second,
		ExhaustedFallback:  time.Hour,
		FailThreshold:      3,
		AffinityEnabled:    false,
		LogLevel:           "error",
	}
	logs := admin.NewLogs()
	store, err := pool.Open(cfg.DBPath, "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	now := time.Now()
	if _, err := store.AddAccount("Account A", []byte(e2eJWTA), "", now); err != nil {
		t.Fatalf("add account: %v", err)
	}
	if _, err := store.AddAccount("Account B", []byte(e2eJWTB), "", now.Add(time.Minute)); err != nil {
		t.Fatalf("add account: %v", err)
	}
	pl, err := pool.NewPool(store, pool.Config{InflightLimit: 3, FailThreshold: 3, AffinityEnabled: false, UpstreamURL: cfg.UpstreamURL})
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	mp, err := pool.NewMapping(store)
	if err != nil {
		t.Fatalf("new mapping: %v", err)
	}
	if _, err := mp.Add("claude-sonnet-*", "glm-sonnet-test", pool.ModeMapped, now); err != nil {
		t.Fatalf("add mapping: %v", err)
	}
	srv := server.NewServer(cfg, server.Deps{Pool: pl, Mapping: mp, Store: store, Logs: logs}, nil)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv.Addr = ln.Addr().String()
	go srv.Serve(ln) // ErrServerClosed after Shutdown is expected
	baseURL := "http://" + ln.Addr().String()
	t.Cleanup(func() {
		pl.Stop()
		logs.Close()
		store.Close()
		upSrv.Close()
	})

	// Upstream: the stream tail arrives ~1.2s after the first event.
	var tailAt atomic.Int64
	fu.set(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, sseMessageStart)
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		time.Sleep(1200 * time.Millisecond)
		_, _ = io.WriteString(w, sseMessageStop)
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		tailAt.Store(time.Now().UnixNano())
	}))

	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/messages",
		strings.NewReader(streamRequest("claude-sonnet-4-5-20250929")))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", e2eClientKey)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	// Give the stream time to commit, then shut down while it is running.
	time.Sleep(300 * time.Millisecond)
	shutdownStarted := time.Now()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- server.Shutdown(srv) }()

	// The client keeps reading while the server is shutting down: the stream
	// must run to completion.
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read stream during shutdown: %v", err)
	}
	if !strings.Contains(string(raw), "message_stop") {
		t.Errorf("stream truncated during shutdown: %s", raw)
	}

	if err := <-shutdownDone; err != nil {
		t.Errorf("Shutdown = %v, want nil (streams finished well within 30s)", err)
	}
	if elapsed := time.Since(shutdownStarted); elapsed < 800*time.Millisecond {
		t.Errorf("Shutdown returned after %v — it did not wait for the in-flight stream", elapsed)
	}
	if tail := tailAt.Load(); tail != 0 && shutdownStarted.UnixNano() > tail {
		t.Errorf("shutdown began after the stream ended — test is not exercising the wait")
	}

	// After Shutdown, new connections are refused.
	if _, err := (&http.Client{}).Post(baseURL+"/v1/messages", "application/json",
		strings.NewReader(plainRequest("claude-sonnet-4-5"))); err == nil {
		t.Error("request succeeded after Shutdown, want connection refused")
	}
}
