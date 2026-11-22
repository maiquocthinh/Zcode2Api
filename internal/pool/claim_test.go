package pool

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- test infrastructure -----------------------------------------------------

// claimCollector records claim events.
type claimCollector struct {
	mu  sync.Mutex
	evs []ClaimEvent
}

func (c *claimCollector) handle(ev ClaimEvent) {
	c.mu.Lock()
	c.evs = append(c.evs, ev)
	c.mu.Unlock()
}

func (c *claimCollector) snapshot() []ClaimEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ClaimEvent(nil), c.evs...)
}

// newTestClaimer builds a claimer against a fake upstream origin, with an
// injectable clock and a zero pause between accounts (the pause is
// covered separately).
func newTestClaimer(t *testing.T, p *Pool, cfg ClaimConfig) (*Claimer, *claimCollector) {
	t.Helper()
	if cfg.Now == nil {
		clk := newFakeClock()
		cfg.Now = clk.Now
	}
	if cfg.Wait == nil {
		cfg.Wait = func(ctx context.Context, d time.Duration) error { return ctx.Err() }
	}
	if cfg.PauseBetween == 0 {
		cfg.PauseBetween = time.Millisecond // tests never sleep 2s
	}
	c, err := NewClaimer(p, cfg)
	if err != nil {
		t.Fatalf("NewClaimer: %v", err)
	}
	col := &claimCollector{}
	c.SetClaimHandler(col.handle)
	t.Cleanup(c.Close)
	return c, col
}

// --- schedule math --------
func TestNextRunAfterGMTPlus8(t *testing.T) {
	// 2026-11-21 16:30:00 UTC == 2026-11-22 00:30 GMT+8: today's (GMT+8)
	// 00:05 run is in the past, so the next run is tomorrow 00:05 GMT+8
	// (= 2026-11-22 16:05 UTC).
	now := time.Date(2026, 11, 21, 16, 30, 0, 0, time.UTC)
	got := nextRunAfter(now, 0, 5, 0, nil)
	want := time.Date(2026, 11, 23, 0, 5, 0, 0, claimZone)
	if !got.Equal(want) {
		t.Fatalf("nextRunAfter = %v, want %v (tomorrow 00:05 GMT+8)", got, want)
	}

	// 16:00 UTC == 00:00 GMT+8 on the same GMT+8 day: 00:05 is still ahead
	// today.
	now = time.Date(2026, 11, 21, 16, 0, 0, 0, time.UTC)
	got = nextRunAfter(now, 0, 5, 0, nil)
	want = time.Date(2026, 11, 21, 16, 5, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("nextRunAfter = %v, want %v (today 00:05 GMT+8 = 16:05 UTC)", got, want)
	}

	// Exactly at the run time: the instant is already spent, so the next run
	// is tomorrow.
	now = time.Date(2026, 11, 21, 16, 5, 0, 0, time.UTC)
	got = nextRunAfter(now, 0, 5, 0, nil)
	want = time.Date(2026, 11, 22, 16, 5, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("nextRunAfter at the exact instant = %v, want tomorrow", got)
	}

	// Jitter lands in [run, run+jitter).
	now = time.Date(2026, 11, 21, 16, 0, 0, 0, time.UTC)
	base := nextRunAfter(now, 0, 5, 10*time.Minute, func() float64 { return 0.42 })
	if want := time.Date(2026, 11, 21, 16, 5, 0, 0, time.UTC).Add(252 * time.Second); !base.Equal(want) {
		t.Fatalf("jittered run = %v, want %v (0.42 × 10m)", base, want)
	}
	low := nextRunAfter(now, 0, 5, 10*time.Minute, func() float64 { return 0 })
	if !low.Equal(time.Date(2026, 11, 21, 16, 5, 0, 0, time.UTC)) {
		t.Fatalf("zero jitter = %v, want the plain run time", low)
	}
}

func TestNewClaimerValidatesClaimTime(t *testing.T) {
	_, p := newSeededPool(t, 0, newFakeClock(), nil, nil)
	if _, err := NewClaimer(p, ClaimConfig{PlanID: "test-plan", ClaimTime: "not-a-time"}); err == nil {
		t.Fatal("NewClaimer accepted an invalid CLAIM_TIME")
	}
	if _, err := NewClaimer(p, ClaimConfig{PlanID: "test-plan", ClaimTime: "0:5"}); err == nil {
		t.Fatal("NewClaimer accepted a single-digit CLAIM_TIME (strict HH:MM)")
	}
	if _, err := NewClaimer(p, ClaimConfig{PlanID: "test-plan"}); err != nil {
		t.Fatalf("NewClaimer with defaults: %v", err)
	}
}

// --- claim flow without a solver ---------------------------------------------

// TestClaimWithoutSolverSkips3007: with 3007
// (captcha_required) and no CLAIM_SOLVER_URL, the account's claim is skipped,
// counted into claims_captcha_skipped, recorded as last_claim_kind and
// emitted as a `claim` event — and the account state does NOT change.
func TestClaimWithoutSolverSkips3007(t *testing.T) {
	clk := newFakeClock()
	col := &eventCollector{}
	_, p := newSeededPool(t, 2, clk, col, nil)
	ids := accountIDs(t, p)

	var claimCalls int
	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claimCalls++
		if r.URL.Path != "/api/v1/zcode-plan/billing/claim" {
			t.Errorf("claim path = %q", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("claim method = %s", r.Method)
		}
		if r.Header.Get("X-Aliyun-Captcha-Verify-Param") != "" {
			t.Error("a claim without a solver must not carry a captcha header")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"error":{"code":"3007","message":"captcha required"}}`)
	}))
	defer upstreamSrv.Close()

	c, claims := newTestClaimer(t, p, ClaimConfig{PlanID: "test-plan", UpstreamURL: upstreamSrv.URL})
	report := c.ClaimOne(context.Background(), ids[0])

	if report.OK || report.Kind != "captcha_required" || !report.CaptchaRequired {
		t.Fatalf("report = %+v, want captcha_required skip", report)
	}
	if claimCalls != 1 {
		t.Errorf("claim calls = %d, want 1 (no solver retry)", claimCalls)
	}
	if c.CaptchaSkipped() != 1 {
		t.Errorf("claims_captcha_skipped = %d, want 1", c.CaptchaSkipped())
	}
	if c.ClaimsTotal() != 0 {
		t.Errorf("claims_total = %d, want 0", c.ClaimsTotal())
	}

	// last_claim_at/last_claim_kind persisted.
	a, _ := p.AccountByID(ids[0])
	if a.LastClaimKind != "captcha_required" || !a.LastClaimAt.Equal(clk.Now()) {
		t.Errorf("last claim = %q @ %v, want captcha_required @ %v", a.LastClaimKind, a.LastClaimAt, clk.Now())
	}

	// The account state is untouched by a captcha skip (3007 leaves the
	// status unchanged).
	if a.Status != StatusActive {
		t.Errorf("status after captcha skip = %s, want ACTIVE", a.Status)
	}

	evs := claims.snapshot()
	if len(evs) != 1 || evs[0].OK || evs[0].Kind != "captcha_required" || !evs[0].CaptchaRequired || evs[0].AccountID != ids[0] {
		t.Fatalf("claim events = %+v, want one captcha_required skip for %s", evs, ids[0])
	}
}

// --- claim flow with a solver ------------------------------------------------

// TestClaimWithSolverRetriesAfter3007: with
// CLAIM_SOLVER_URL configured, a 3007 answer triggers the solver POST
// {"token":...,"endpoint":"claim"}, the verify_param is taken from the reply
// and the claim is retried exactly once — with the captcha header this time.
func TestClaimWithSolverRetriesAfter3007(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 1, clk, nil, nil)
	id := accountIDs(t, p)[0]

	var claimCalls int
	var solverCalls int
	var solverBody map[string]string
	var captchaHeader string
	var solverAuth string

	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claimCalls++
		if v := r.Header.Get("X-Aliyun-Captcha-Verify-Param"); v != "" {
			captchaHeader = v
		}
		w.Header().Set("Content-Type", "application/json")
		if claimCalls == 1 {
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"error":{"code":"3007","message":"captcha required"}}`)
			return
		}
		// The retried claim (verify_param present) succeeds.
		_, _ = io.WriteString(w, `{"code":0,"message":"ok","data":{"claimed":true}}`)
	}))
	defer upstreamSrv.Close()

	solverSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		solverCalls++
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &solverBody)
		solverAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"verify_param":"captcha-token-xyz"}`)
	}))
	defer solverSrv.Close()

	c, _ := newTestClaimer(t, p, ClaimConfig{PlanID: "test-plan",
		UpstreamURL: upstreamSrv.URL,
		SolverURL:   solverSrv.URL,
		SolverKey:   "solver-secret-key",
	})
	report := c.ClaimOne(context.Background(), id)

	if !report.OK || report.Kind != "ok" || !report.Claimed || report.CaptchaRequired {
		t.Fatalf("report = %+v, want a successful claim after the solver retry", report)
	}
	if solverCalls != 1 {
		t.Errorf("solver calls = %d, want 1", solverCalls)
	}
	if solverBody["token"] != testJWT(0) || solverBody["endpoint"] != "claim" {
		t.Errorf("solver body = %v, want {token: <JWT>, endpoint: claim}", solverBody)
	}
	if solverAuth != "Bearer solver-secret-key" {
		t.Errorf("solver Authorization = %q", solverAuth)
	}
	if captchaHeader != "captcha-token-xyz" {
		t.Errorf("retried claim captcha header = %q, want the verify_param", captchaHeader)
	}
	if claimCalls != 2 {
		t.Errorf("claim calls = %d, want 2 (initial + one retry)", claimCalls)
	}
	if c.ClaimsTotal() != 1 || c.CaptchaSkipped() != 0 {
		t.Errorf("counters = total %d skipped %d, want 1/0", c.ClaimsTotal(), c.CaptchaSkipped())
	}

	a, _ := p.AccountByID(id)
	if a.LastClaimKind != "ok" {
		t.Errorf("last_claim_kind = %q, want ok", a.LastClaimKind)
	}

	// example shape: {"ts","acc","ok":true,"kind":"ok","captcha_required":false}
	raw, _ := json.Marshal(ClaimEvent{Ts: 1, AccountID: "acc_x", OK: true, Kind: "ok"})
	var decoded map[string]any
	_ = json.Unmarshal(raw, &decoded)
	if _, ok := decoded["acc"]; !ok {
		t.Error("claim event JSON must use the acc field")
	}
}

// TestClaimWithFailingSolverSkips: a solver that errors must not crash the
// flow — the account is skipped and counted like the no-solver case.
func TestClaimWithFailingSolverSkips(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 1, clk, nil, nil)
	id := accountIDs(t, p)[0]

	var claimCalls int
	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claimCalls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"error":{"code":"3007","message":"captcha required"}}`)
	}))
	defer upstreamSrv.Close()

	solverSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer solverSrv.Close()

	c, _ := newTestClaimer(t, p, ClaimConfig{PlanID: "test-plan", UpstreamURL: upstreamSrv.URL, SolverURL: solverSrv.URL})
	report := c.ClaimOne(context.Background(), id)

	if report.OK || !report.CaptchaRequired {
		t.Errorf("report = %+v, want a captcha skip", report)
	}
	if claimCalls != 1 {
		t.Errorf("claim calls = %d, want 1 (a failed solver must not retry)", claimCalls)
	}
	if c.CaptchaSkipped() != 1 {
		t.Errorf("captcha skipped = %d, want 1", c.CaptchaSkipped())
	}
}

// --- plain success and failure mapping --------------------------------------
func TestClaimSuccessRecordsAndCounts(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 2, clk, nil, nil)
	ids := accountIDs(t, p)

	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":0,"message":"ok"}`)
	}))
	defer upstreamSrv.Close()

	c, claims := newTestClaimer(t, p, ClaimConfig{PlanID: "test-plan", UpstreamURL: upstreamSrv.URL, PauseBetween: 0})
	n := c.ClaimNow(context.Background())

	if n != 2 {
		t.Errorf("ClaimNow claimed %d, want 2", n)
	}
	if c.ClaimsTotal() != 2 {
		t.Errorf("claims_total = %d, want 2", c.ClaimsTotal())
	}
	for _, id := range ids {
		a, _ := p.AccountByID(id)
		if a.LastClaimKind != "ok" {
			t.Errorf("%s last_claim_kind = %q, want ok", id, a.LastClaimKind)
		}
		if !a.LastClaimAt.Equal(clk.Now()) {
			t.Errorf("%s last_claim_at = %v, want the pool clock", id, a.LastClaimAt)
		}
	}
	if evs := claims.snapshot(); len(evs) != 2 {
		t.Errorf("claim events = %d, want 2", len(evs))
	}
}

// TestClaimApplies1006Disable: a dead JWT discovered during the claim
// disables the account through the same state machine.
func TestClaimApplies1006Disable(t *testing.T) {
	clk := newFakeClock()
	col := &eventCollector{}
	_, p := newSeededPool(t, 1, clk, col, nil)
	id := accountIDs(t, p)[0]

	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"error":{"code":"1006","message":"token expired"}}`)
	}))
	defer upstreamSrv.Close()

	c, _ := newTestClaimer(t, p, ClaimConfig{PlanID: "test-plan", UpstreamURL: upstreamSrv.URL})
	report := c.ClaimOne(context.Background(), id)

	if report.OK || report.Kind != "auth" {
		t.Errorf("report = %+v, want a failed auth claim", report)
	}
	a, _ := p.AccountByID(id)
	if a.Status != StatusDisabled || a.StatusReason != "auth" {
		t.Errorf("status after a 1006 claim = %s (%s), want DISABLED (auth)", a.Status, a.StatusReason)
	}
}

// TestClaimOnlyActiveAccounts pins: DISABLED/EXHAUSTED accounts are not
// claimed on their behalf unless CLAIM_EXHAUSTED is set.
func TestClaimOnlyActiveAccounts(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 3, clk, nil, nil)
	ids := accountIDs(t, p)

	accounts := p.Accounts()
	p.Feedback(&accounts[0], exhaustedOutcome(clk.Now().Add(time.Hour))) // EXHAUSTED
	if err := p.SetAccountEnabled(ids[1], false); err != nil {           // DISABLED
		t.Fatalf("disable: %v", err)
	}

	var claimed []string
	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The JWT payload segment identifies the account without logging the
		// token itself: only the base64 payload is recorded here.
		parts := strings.Split(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), ".")
		if len(parts) == 3 {
			claimed = append(claimed, parts[1])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":0,"message":"ok"}`)
	}))
	defer upstreamSrv.Close()

	c, _ := newTestClaimer(t, p, ClaimConfig{PlanID: "test-plan", UpstreamURL: upstreamSrv.URL})
	n := c.ClaimNow(context.Background())
	if n != 1 {
		t.Errorf("claimed %d accounts, want 1 (only the ACTIVE one)", n)
	}
	if len(claimed) != 1 {
		t.Fatalf("upstream saw %d claim calls, want 1", len(claimed))
	}

	// CLAIM_EXHAUSTED=true adds the EXHAUSTED account, still not DISABLED.
	c2, _ := newTestClaimer(t, p, ClaimConfig{PlanID: "test-plan", UpstreamURL: upstreamSrv.URL, IncludeExhausted: true})
	n = c2.ClaimNow(context.Background())
	if n != 2 {
		t.Errorf("claimed %d accounts with CLAIM_EXHAUSTED, want 2 (ACTIVE + EXHAUSTED)", n)
	}
}

// --- scheduler ---------------------------------------------------------------

// TestSchedulerDisabledByDefault: DAILY_CLAIM=false must never schedule
// — on-demand claims still work.
func TestSchedulerDisabledByDefault(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 1, clk, nil, nil)

	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":0,"message":"ok"}`)
	}))
	defer upstreamSrv.Close()

	c, _ := newTestClaimer(t, p, ClaimConfig{PlanID: "test-plan",
		Enabled:     false, // DAILY_CLAIM=false
		UpstreamURL: upstreamSrv.URL,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	c.Start(ctx) // must return immediately without claiming
	if n := c.ClaimsTotal(); n != 0 {
		t.Errorf("a disabled claimer claimed %d accounts", n)
	}
	// On-demand claim still works.
	if n := c.ClaimNow(context.Background()); n != 1 {
		t.Errorf("on-demand claim = %d, want 1", n)
	}
}

// TestSchedulerFiresAtClaimTime drives the scheduler loop with an injected
// clock and a Wait that returns once the computed delay matches the fake
// clock's advance — proving the run happens at CLAIM_TIME (GMT+8) + jitter.
func TestSchedulerFiresAtClaimTime(t *testing.T) {
	clk := newFakeClock()
	// 2026-11-20 16:00 UTC = 00:00 GMT+8 → 00:05 GMT+8 is 5 minutes ahead.
	// Written before Start: no goroutine shares the clock yet.
	clk.cur = time.Date(2026, 11, 20, 16, 0, 0, 0, time.UTC)

	_, p := newSeededPool(t, 1, clk, nil, nil)
	// Point the pool's billing origin at the fake upstream (ClaimAll →
	// ClaimOne → upstream.ClaimQuota(WithOrigin)).
	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":0,"message":"ok"}`)
	}))
	defer upstreamSrv.Close()
	p.mu.Lock()
	p.cfg.UpstreamURL = upstreamSrv.URL
	p.mu.Unlock()

	fired := make(chan struct{}, 4)
	waited := 0
	cfg := ClaimConfig{PlanID: "test-plan",
		Enabled:     true,
		ClaimTime:   "00:05",
		Jitter:      10 * time.Minute,
		UpstreamURL: upstreamSrv.URL,
		Now:         clk.Now,
		Rand01:      func() float64 { return 0 }, // no jitter: run at exactly 00:05
		Wait: func(ctx context.Context, d time.Duration) error {
			waited++
			if waited == 1 {
				// The first wait must be the 5-minute gap to 00:05 GMT+8.
				if d != 5*time.Minute {
					t.Errorf("scheduler wait = %v, want %v (00:00→00:05 GMT+8)", d, 5*time.Minute)
				}
				clk.Advance(d)
				select {
				case fired <- struct{}{}:
				default:
				}
				return nil // let the claim pass run
			}
			return errors.New("scheduler test stop") // end the loop after one pass
		},
	}
	c, claims := newTestClaimer(t, p, cfg)
	c.Start(context.Background())

	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler never fired")
	}
	// Wait for the claim pass to record its result.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.ClaimsTotal() == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if c.ClaimsTotal() != 1 {
		t.Fatalf("claims_total = %d, want 1 after the scheduled run", c.ClaimsTotal())
	}
	if evs := claims.snapshot(); len(evs) != 1 || !evs[0].OK {
		t.Fatalf("claim events = %+v, want one ok", evs)
	}
}

// TestClaimAllPausesBetweenAccounts pins the claim-all pacing: exactly
// one pause of PauseBetween between consecutive accounts.
func TestClaimAllPausesBetweenAccounts(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 3, clk, nil, nil)

	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":0,"message":"ok"}`)
	}))
	defer upstreamSrv.Close()

	var waits []time.Duration
	c, _ := newTestClaimer(t, p, ClaimConfig{PlanID: "test-plan",
		UpstreamURL:  upstreamSrv.URL,
		PauseBetween: 2 * time.Second,
		Wait: func(ctx context.Context, d time.Duration) error {
			waits = append(waits, d)
			return ctx.Err()
		},
	})
	if n := c.ClaimNow(context.Background()); n != 3 {
		t.Fatalf("claimed %d, want 3", n)
	}
	if len(waits) != 2 {
		t.Fatalf("pauses = %v, want exactly 2 (between 3 accounts)", waits)
	}
	for i, d := range waits {
		if d != 2*time.Second {
			t.Errorf("pause %d = %v, want 2s (pause between accounts)", i+1, d)
		}
	}
}
