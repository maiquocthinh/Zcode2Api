package pool

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"zcode2api/internal/config"
	"zcode2api/internal/upstream"
)

// --- test infrastructure -----------------------------------------------------

// fakeClock is the injectable clock used to drive the pool's schedules. Its
// methods are safe for concurrent use.
type fakeClock struct {
	mu  sync.Mutex
	cur time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{cur: time.Unix(1_700_000_000, 0).UTC()}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.cur = c.cur.Add(d)
	c.mu.Unlock()
}

// eventCollector records state events emitted by the pool.
type eventCollector struct {
	mu  sync.Mutex
	evs []StateEvent
}

func (c *eventCollector) handle(ev StateEvent) {
	c.mu.Lock()
	c.evs = append(c.evs, ev)
	c.mu.Unlock()
}

func (c *eventCollector) snapshot() []StateEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]StateEvent, len(c.evs))
	copy(out, c.evs)
	return out
}

func (c *eventCollector) last() (StateEvent, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.evs) == 0 {
		return StateEvent{}, false
	}
	return c.evs[len(c.evs)-1], true
}

// testJWT builds a fake-but-well-formed JWT for seeding (upstream validity is
// never checked locally — a dead JWT only surfaces as 1006 upstream). The
// payload encodes the index so every seeded account has a distinct token.
func testJWT(i int) string {
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"sub":"account-%03d"}`, i)))
	return "eyJhbGciOiJIUzI1NiJ9." + payload + ".c2lnLTAwMA"
}

// newSeededPool opens a plain store, adds n accounts and builds a pool on top
// of it, wired to the given clock and state-event collector.
func newSeededPool(t *testing.T, n int, clk *fakeClock, col *eventCollector, mutate func(*Config)) (*Store, *Pool) {
	t.Helper()
	s := openPlain(t, filepath.Join(t.TempDir(), "pool.db"))
	now := clk.Now()
	for i := 0; i < n; i++ {
		id, err := s.AddAccount(fmt.Sprintf("Account %c", 'A'+i), []byte(testJWT(i)), "", now.Add(time.Duration(i)*time.Minute))
		if err != nil {
			t.Fatalf("AddAccount %d: %v", i, err)
		}
		// Deterministic creation order in the store (ORDER BY created_at, id
		// would tie-break on random ids).
		if _, err := s.db.Exec(`UPDATE accounts SET created_at = ? WHERE id = ?`, now.Add(time.Duration(i)*time.Minute).Unix(), id); err != nil {
			t.Fatalf("stagger created_at: %v", err)
		}
	}
	cfg := Config{
		Now:             clk.Now,
		AffinityEnabled: true, // config default
	}
	if col != nil {
		cfg.OnState = col.handle
	}
	if mutate != nil {
		mutate(&cfg)
	}
	p, err := NewPool(s, cfg)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(func() { p.Stop() })
	return s, p
}

// accountIDs returns the seeded account ids in creation order.
func accountIDs(t *testing.T, p *Pool) []string {
	t.Helper()
	accounts := p.Accounts()
	ids := make([]string, 0, len(accounts))
	for _, a := range accounts {
		ids = append(ids, a.ID)
	}
	return ids
}

func okOutcome() upstream.Outcome {
	return upstream.Outcome{Kind: upstream.KindOK, AccountAction: upstream.AccountResetStreak}
}

func coolOutcome(until time.Time) upstream.Outcome {
	return upstream.Outcome{Kind: upstream.KindRateLimit, AccountAction: upstream.AccountCooling, Retryable: true, Until: until}
}

func exhaustedOutcome(until time.Time) upstream.Outcome {
	return upstream.Outcome{Kind: upstream.KindQuotaExhausted, AccountAction: upstream.AccountExhausted, Retryable: true, Until: until}
}

func disabledOutcome(kind upstream.Kind) upstream.Outcome {
	return upstream.Outcome{Kind: kind, AccountAction: upstream.AccountDisabled, Retryable: true}
}

func unavailableOutcome() upstream.Outcome {
	return upstream.Outcome{Kind: upstream.KindUpstreamUnavailable, AccountAction: upstream.AccountFailStreak, Retryable: true}
}

// riskCooldownOutcome is the outcome of a 3012 WAF block: RISK_COOLDOWN
// with an Until several hours out (mapped onto COOLING by the pool).
func riskCooldownOutcome(until time.Time) upstream.Outcome {
	return upstream.Outcome{Kind: upstream.KindRiskBlock, AccountAction: upstream.AccountRiskCooldown, Retryable: true, Until: until}
}

func acquireFailure(t *testing.T, err error) *AcquireError {
	t.Helper()
	var ae *AcquireError
	if !errors.As(err, &ae) {
		t.Fatalf("error %v (%T) is not an *AcquireError", err, err)
	}
	return ae
}

// --- basics ------------------------------------------------------------------
func TestAcquireReleaseBasics(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 2, clk, nil, nil)

	lease, err := p.Acquire("", nil, "")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease.JWT() == "" || lease.DeviceMid() == "" {
		t.Error("lease must expose the JWT and device_mid for the channel A request")
	}
	if !isUUIDv4(lease.DeviceMid()) {
		t.Errorf("device_mid = %q, want a UUID v4", lease.DeviceMid())
	}
	if p.InflightTotal() != 1 {
		t.Errorf("inflight = %d, want 1", p.InflightTotal())
	}
	// The lease must carry the channel A credential of the account, and
	// Accounts must never leak it.
	for _, a := range p.Accounts() {
		if a.JWT != "" {
			t.Error("Accounts() leaked the JWT")
		}
	}
	lease.Release(okOutcome())
	if p.InflightTotal() != 0 {
		t.Errorf("inflight after release = %d, want 0", p.InflightTotal())
	}
	lease.Release(okOutcome()) // idempotent
	if p.InflightTotal() != 0 {
		t.Errorf("inflight after double release = %d, want 0", p.InflightTotal())
	}
}

func TestAcquireRespectsInflightLimitPerAccount(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 1, clk, nil, func(c *Config) { c.InflightLimit = 2 })

	l1, err := p.Acquire("", nil, "")
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	l2, err := p.Acquire("", nil, "")
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	_, err = p.Acquire("", nil, "")
	ae := acquireFailure(t, err)
	if ae.Failure != AcquireBusy {
		t.Errorf("failure = %q, want busy", ae.Failure)
	}
	if !errors.Is(err, ErrPoolExhausted) {
		t.Error("a busy pool must match ErrPoolExhausted")
	}
	l1.Release(okOutcome())
	l2.Release(okOutcome())
}

// TestConcurrentAcquireRelease: concurrent
// acquire/release never over- or under-counts inflight, never hands out the
// same lease twice beyond the limit, and runs clean under -race. With fewer
// slots than workers, every round either acquires (success) or fails with the
// busy class — the two together must account for every round, proving no
// lease was lost or double-counted.
func TestConcurrentAcquireRelease(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 4, clk, nil, func(c *Config) { c.InflightLimit = 2 })

	const (
		workers = 16
		rounds  = 50
	)
	var wg sync.WaitGroup
	var mu sync.Mutex
	acquired := map[string]int{}
	var busyFailures, otherFailures int
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				lease, err := p.Acquire("", nil, "")
				if err != nil {
					ae := acquireFailure(t, err)
					mu.Lock()
					if ae.Failure == AcquireBusy {
						busyFailures++
					} else {
						otherFailures++
					}
					mu.Unlock()
					continue
				}
				mu.Lock()
				acquired[lease.AccountID()]++
				mu.Unlock()
				lease.Release(okOutcome())
			}
		}()
	}
	wg.Wait()

	total := 0
	for _, n := range acquired {
		total += n
	}
	if total+busyFailures+otherFailures != workers*rounds {
		t.Errorf("outcomes = %d acquires + %d busy + %d other, want %d rounds (no lost or double leases)",
			total, busyFailures, otherFailures, workers*rounds)
	}
	if otherFailures != 0 {
		t.Errorf("%d acquire failures outside the busy class: no account state was touched", otherFailures)
	}
	if total == 0 {
		t.Error("no acquire ever succeeded")
	}
	if p.InflightTotal() != 0 {
		t.Errorf("inflight after the storm = %d, want 0", p.InflightTotal())
	}
}

// TestAcquireNeverReturnsTriedAccount: Acquire
// with a tried set never returns an account that already failed this request.
func TestAcquireNeverReturnsTriedAccount(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 3, clk, nil, nil)
	ids := accountIDs(t, p)

	tried := map[string]bool{ids[0]: true, ids[1]: true}
	lease, err := p.Acquire("", tried, "")
	if err != nil {
		t.Fatalf("Acquire with 2 of 3 tried: %v", err)
	}
	if lease.AccountID() != ids[2] {
		t.Errorf("got %s, want the only untried account %s", lease.AccountID(), ids[2])
	}
	lease.Release(okOutcome())

	// Every account tried: nothing may be returned.
	tried[ids[2]] = true
	_, err = p.Acquire("", tried, "")
	ae := acquireFailure(t, err)
	if ae.Failure != AcquireTriedAll {
		t.Errorf("failure = %q, want tried", ae.Failure)
	}
	if !errors.Is(err, ErrPoolExhausted) {
		t.Error("a tried-all pool must match ErrPoolExhausted")
	}
}

// TestErrPoolExhaustedSentinel pins the contract: every "no account"
// outcome matches ErrPoolExhausted so the attempt loop can spend its one
// channel B attempt.
func TestErrPoolExhaustedSentinel(t *testing.T) {
	clk := newFakeClock()

	// Empty pool.
	_, p := newSeededPool(t, 0, clk, nil, nil)
	_, err := p.Acquire("", nil, "")
	ae := acquireFailure(t, err)
	if ae.Failure != AcquireEmpty || !errors.Is(err, ErrPoolExhausted) {
		t.Errorf("empty pool err = %v, want AcquireError{empty} matching ErrPoolExhausted", err)
	}
	p.Stop()

	// All disabled.
	_, p = newSeededPool(t, 2, clk, nil, nil)
	for _, id := range accountIDs(t, p) {
		if err := p.SetAccountEnabled(id, false); err != nil {
			t.Fatalf("disable: %v", err)
		}
	}
	_, err = p.Acquire("", nil, "")
	ae = acquireFailure(t, err)
	if ae.Failure != AcquireAllDisabled || !errors.Is(err, ErrPoolExhausted) {
		t.Errorf("all-disabled err = %v, want all_disabled matching ErrPoolExhausted", err)
	}
	p.Stop()

	// All recovering (COOLING with RetryAfter): both accounts rest.
	_, p = newSeededPool(t, 2, clk, nil, nil)
	l1, _ := p.Acquire("", nil, "")
	l2, _ := p.Acquire("", nil, "")
	l1.Release(coolOutcome(clk.Now().Add(time.Minute)))
	l2.Release(coolOutcome(clk.Now().Add(2 * time.Minute)))
	_, err = p.Acquire("", nil, "")
	ae = acquireFailure(t, err)
	if ae.Failure != AcquireRecovering || !errors.Is(err, ErrPoolExhausted) || ae.RetryAfter <= 0 {
		t.Errorf("recovering err = %v, want recovering with RetryAfter matching ErrPoolExhausted", err)
	}
	p.Stop()
}

// --- outcomes and the state machine -----------------------------------------

// TestTransition1005WithParsedUntil: a 1005
// answer classified by upstream.Classify puts the account into EXHAUSTED
// until the next_flush_time parsed from the body.
func TestTransition1005WithParsedUntil(t *testing.T) {
	clk := newFakeClock()
	col := &eventCollector{}
	_, p := newSeededPool(t, 2, clk, col, nil)
	ids := accountIDs(t, p)

	reset := clk.Now().Add(3 * time.Hour).Truncate(time.Second)
	body := fmt.Sprintf(`{"error":{"code":"1005","message":"quota exhausted, next_flush_time=%d"}}`, reset.Unix())
	out := upstream.Classify(http.StatusOK, []byte(body), nil)
	if out.Kind != upstream.KindQuotaExhausted || out.AccountAction != upstream.AccountExhausted {
		t.Fatalf("Classify(1005) = %+v, want quota_exhausted/EXHAUSTED", out)
	}
	if !out.Until.Equal(reset) {
		t.Fatalf("Until = %v, want the parsed next_flush_time %v", out.Until, reset)
	}

	lease, err := p.Acquire("", nil, "")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	lease.Release(out)

	a, ok := p.AccountByID(ids[0])
	if !ok || a.Status != StatusExhausted || a.StatusReason != "quota_exhausted" {
		t.Fatalf("account = %+v, want EXHAUSTED (quota_exhausted)", a)
	}
	if a.CooldownUntil.Unix() != reset.Unix() {
		t.Errorf("cooldown_until = %v, want %v", a.CooldownUntil, reset)
	}

	ev, ok := col.last()
	if !ok || ev.To != StatusExhausted || ev.From != StatusActive || ev.AccountID != ids[0] || ev.Kind != "quota_exhausted" {
		t.Fatalf("state event = %+v, want ACTIVE→EXHAUSTED (quota_exhausted) for %s", ev, ids[0])
	}
	if ev.Until.Unix() != reset.Unix() {
		t.Errorf("event until = %v, want %v", ev.Until, reset)
	}
}

// TestTransition3012RiskCooldown: a 3012 WAF answer isolates the account as
// COOLING with the RISK_COOLDOWN duration (; has exactly
// four states). No egress proxy rotation ever happens. The pool stores the
// Until computed by Classify verbatim.
func TestTransition3012RiskCooldown(t *testing.T) {
	clk := newFakeClock()
	col := &eventCollector{}
	_, p := newSeededPool(t, 1, clk, col, nil)
	ids := accountIDs(t, p)

	out := upstream.Classify(http.StatusOK, []byte(`{"error":{"code":"3012","message":"unusual activity"}}`), nil)
	if out.Kind != upstream.KindRiskBlock || out.AccountAction != upstream.AccountRiskCooldown {
		t.Fatalf("Classify(3012) = %+v, want risk_block/RISK_COOLDOWN", out)
	}
	lease, _ := p.Acquire("", nil, "")
	lease.Release(out)

	a, _ := p.AccountByID(ids[0])
	if a.Status != StatusCooling || a.StatusReason != "risk_block" {
		t.Fatalf("account = %s (%s), want COOLING (risk_block)", a.Status, a.StatusReason)
	}
	if !a.CooldownUntil.Equal(out.Until) {
		t.Errorf("cooldown_until = %v, want the outcome Until %v (RISK_COOLDOWN)", a.CooldownUntil, out.Until)
	}
	if d := out.Until.Sub(time.Now()); d < upstream.DefaultRiskCooldown-time.Minute {
		t.Errorf("Classify's Until is only %v out, want ~RISK_COOLDOWN (%v)", d, upstream.DefaultRiskCooldown)
	}
	// The sweeper must not release the account before the risk cooldown ends.
	if n := p.Sweep(out.Until.Add(-time.Minute)); n != 0 {
		t.Errorf("sweep released %d accounts before the risk cooldown ended", n)
	}
	if n := p.Sweep(out.Until); n != 1 {
		t.Errorf("sweep at Until released %d accounts, want 1", n)
	}
}

// TestTransition429And1006 covers the rate-limit and dead-JWT mappings: 429 →
// COOLING until Retry-After/COOLING_DURATION, 1006 → DISABLED. The pool
// stores the Until computed by Classify verbatim (that clock is upstream's).
func TestTransition429And1006(t *testing.T) {
	clk := newFakeClock()
	col := &eventCollector{}
	_, p := newSeededPool(t, 2, clk, col, nil)
	ids := accountIDs(t, p)

	// 429 without a code and without Retry-After: COOLING for COOLING_DURATION.
	lease, _ := p.Acquire("", nil, "")
	out := upstream.Classify(http.StatusTooManyRequests, []byte(`{"message":"slow down"}`), nil)
	lease.Release(out)
	a, _ := p.AccountByID(ids[0])
	if a.Status != StatusCooling || a.StatusReason != "rate_limit" {
		t.Fatalf("after 429: %s (%s), want COOLING (rate_limit)", a.Status, a.StatusReason)
	}
	if !a.CooldownUntil.Equal(out.Until) {
		t.Errorf("cooldown_until = %v, want the outcome Until %v", a.CooldownUntil, out.Until)
	}

	// Retry-After extends the pause on the second account — capped at 120s so
	// an anomalous value cannot freeze an account for hours.
	lease, _ = p.Acquire("", nil, "")
	h := http.Header{}
	h.Set("Retry-After", "300")
	outRetry := upstream.Classify(http.StatusTooManyRequests, []byte(`{"message":"slow down"}`), h)
	lease.Release(outRetry)
	a, _ = p.AccountByID(ids[1])
	if a.Status != StatusCooling {
		t.Fatalf("after the 429 with Retry-After: %s, want COOLING", a.Status)
	}
	if !a.CooldownUntil.Equal(outRetry.Until) {
		t.Errorf("cooldown_until after Retry-After = %v, want %v", a.CooldownUntil, outRetry.Until)
	}
	if d := outRetry.Until.Sub(time.Now()); d < 119*time.Second || d > 126*time.Second {
		t.Errorf("Retry-After=300s produced %v, want ~120s (capped)", d)
	}

	// 1006 kills an account (through Feedback — the pool is now exhausted).
	accounts := p.Accounts()
	outDead := upstream.Classify(http.StatusOK, []byte(`{"error":{"code":"1006","message":"token expired"}}`), nil)
	p.Feedback(&accounts[0], outDead)
	a, _ = p.AccountByID(ids[0])
	if a.Status != StatusDisabled || a.StatusReason != "auth" {
		t.Fatalf("after 1006: %s (%s), want DISABLED (auth)", a.Status, a.StatusReason)
	}
}

// TestFeedbackIsTheSingleMapping pins Feedback: the same outcome mapping as
// Lease.Release, applied through the same transitionLocked path (it must not
// touch inflight or counters — that is Release's job).
func TestFeedbackIsTheSingleMapping(t *testing.T) {
	clk := newFakeClock()
	col := &eventCollector{}
	_, p := newSeededPool(t, 1, clk, col, nil)
	ids := accountIDs(t, p)

	accounts := p.Accounts() // JWT-less copies
	reset := clk.Now().Add(2 * time.Hour).Truncate(time.Second)
	out := exhaustedOutcome(reset)
	p.Feedback(&accounts[0], out)

	a, _ := p.AccountByID(ids[0])
	if a.Status != StatusExhausted || !a.CooldownUntil.Equal(reset) {
		t.Fatalf("after Feedback: %s until %v, want EXHAUSTED until %v", a.Status, a.CooldownUntil, reset)
	}
	if a.RequestsTotal != 0 || a.FailuresTotal != 0 {
		t.Errorf("Feedback bumped counters: %+v (only Release does)", a)
	}
	if ev, ok := col.last(); !ok || ev.To != StatusExhausted {
		t.Errorf("Feedback emitted %+v, want the EXHAUSTED state event", ev)
	}

	// A stale (deleted) account is a no-op.
	acc := p.Accounts()[0]
	if err := p.DeleteAccount(ids[0]); err != nil {
		t.Fatalf("DeleteAccount: %v", err)
	}
	p.Feedback(&acc, coolOutcome(clk.Now().Add(time.Minute))) // must not panic
}

func TestFailStreakCooldown(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 1, clk, nil, func(c *Config) { c.FailThreshold = 3 })
	ids := accountIDs(t, p)

	for i := 0; i < 2; i++ {
		lease, _ := p.Acquire("", nil, "")
		lease.Release(unavailableOutcome())
	}
	a, _ := p.AccountByID(ids[0])
	if a.Status != StatusActive {
		t.Fatalf("after %d failures: %s, want ACTIVE (below FAIL_THRESHOLD)", 2, a.Status)
	}
	lease, _ := p.Acquire("", nil, "")
	lease.Release(unavailableOutcome())
	a, _ = p.AccountByID(ids[0])
	if a.Status != StatusCooling {
		t.Fatalf("after %d failures: %s, want COOLING (FAIL_THRESHOLD reached)", 3, a.Status)
	}
	if want := clk.Now().Add(failStreakCooling); !a.CooldownUntil.Equal(want) {
		t.Errorf("cooldown_until = %v, want %v (30s)", a.CooldownUntil, want)
	}
}

// TestSameStatusCooldownExtension: a later outcome of the same kind extends a
// cooldown but never shortens it.
func TestSameStatusCooldownExtension(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 1, clk, nil, nil)

	long := clk.Now().Add(2 * time.Hour)
	lease, _ := p.Acquire("", nil, "")
	lease.Release(exhaustedOutcome(long))

	// A later outcome of the same kind must extend — never shorten — the
	// cooldown (feedback arrives outside a lease, via Feedback).
	accounts := p.Accounts()
	short := clk.Now().Add(time.Minute)
	p.Feedback(&accounts[0], exhaustedOutcome(short))
	a, _ := p.AccountByID(accountIDs(t, p)[0])
	if !a.CooldownUntil.Equal(long) {
		t.Errorf("cooldown_until = %v, want %v (never shortened by a later outcome)", a.CooldownUntil, long)
	}
	longer := clk.Now().Add(3 * time.Hour)
	p.Feedback(&accounts[0], exhaustedOutcome(longer))
	a, _ = p.AccountByID(accountIDs(t, p)[0])
	if !a.CooldownUntil.Equal(longer) {
		t.Errorf("cooldown_until = %v, want %v (extended in place)", a.CooldownUntil, longer)
	}
}

// --- sweeper -----------------------------------------------------------------

// TestSweeperReturnsExpiredAccountsToActive: a COOLING account returns to
// ACTIVE exactly when its cooldown passes, driven by the injected fake clock.
// DISABLED accounts never recover on their own.
func TestSweeperReturnsExpiredAccountsToActive(t *testing.T) {
	clk := newFakeClock()
	col := &eventCollector{}
	_, p := newSeededPool(t, 3, clk, col, nil)
	ids := accountIDs(t, p)

	// Account 0: COOLING for 60s. Account 1: EXHAUSTED until the 1005 reset.
	// Account 2: DISABLED (admin).
	lease, _ := p.Acquire("", nil, "")
	lease.Release(coolOutcome(clk.Now().Add(60 * time.Second)))
	accounts := p.Accounts()
	p.Feedback(&accounts[1], exhaustedOutcome(clk.Now().Add(90*time.Second)))
	if err := p.SetAccountEnabled(ids[2], false); err != nil {
		t.Fatalf("disable: %v", err)
	}

	// Before the cooldowns nothing recovers.
	if n := p.Sweep(clk.Now().Add(59 * time.Second)); n != 0 {
		t.Fatalf("sweep at +59s recovered %d accounts, want 0", n)
	}

	// +60s: the COOLING account recovers.
	if n := p.Sweep(clk.Now().Add(60 * time.Second)); n != 1 {
		t.Fatalf("sweep at +60s recovered %d accounts, want 1", n)
	}
	a0, _ := p.AccountByID(ids[0])
	if a0.Status != StatusActive || a0.StatusReason != "" || !a0.CooldownUntil.IsZero() {
		t.Errorf("account 0 = %s (%q, until %v), want ACTIVE with a cleared reason/cooldown",
			a0.Status, a0.StatusReason, a0.CooldownUntil)
	}

	// +90s: the EXHAUSTED account recovers (its next_flush_time passed).
	if n := p.Sweep(clk.Now().Add(90 * time.Second)); n != 1 {
		t.Fatalf("sweep at +90s recovered %d accounts, want 1", n)
	}
	a1, _ := p.AccountByID(ids[1])
	if a1.Status != StatusActive {
		t.Errorf("account 1 = %s, want ACTIVE after the quota reset time", a1.Status)
	}

	// DISABLED stays DISABLED no matter how much time passes.
	clk.Advance(24 * time.Hour)
	if n := p.Sweep(clk.Now()); n != 0 {
		t.Fatalf("sweep recovered %d DISABLED accounts, want 0", n)
	}
	if a2, _ := p.AccountByID(ids[2]); a2.Status != StatusDisabled {
		t.Errorf("account 2 = %s, want DISABLED (admin re-enable only)", a2.Status)
	}

	// The state events of the two recoveries were emitted.
	recovered := 0
	for _, ev := range col.snapshot() {
		if ev.To == StatusActive && ev.Kind == "" {
			recovered++
		}
	}
	if recovered != 2 {
		t.Errorf("%d recovery events emitted, want 2", recovered)
	}
}

// TestBackgroundSweeperAndFlusher runs the real 15s/60s background loops with
// shortened intervals and verifies both fire. The wall clock here is the real
// one (the background ticker is wall-clock; the sweep *math* with a fake
// clock is covered by TestSweeperReturnsExpiredAccountsToActive).
func TestBackgroundSweeperAndFlusher(t *testing.T) {
	s, p := newSeededPool(t, 1, newFakeClock(), nil, func(c *Config) {
		c.Now = time.Now // real time for the wall-clock ticker
		c.SweepInterval = 20 * time.Millisecond
		c.FlushInterval = 20 * time.Millisecond
	})
	ids := accountIDs(t, p)

	lease, _ := p.Acquire("", nil, "")
	lease.Release(coolOutcome(time.Now().Add(40 * time.Millisecond)))
	p.Start()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if a, _ := p.AccountByID(ids[0]); a.Status == StatusActive {
			break // recovered by the background sweeper
		}
		time.Sleep(5 * time.Millisecond)
	}
	if a, _ := p.AccountByID(ids[0]); a.Status != StatusActive {
		t.Fatalf("background sweeper did not recover the account: %s", a.Status)
	}
	lease, err := p.Acquire("", nil, "")
	if err != nil {
		t.Fatalf("acquire after recovery: %v", err)
	}
	lease.Release(okOutcome())

	// The flusher persists the RAM counters to SQLite.
	deadline = time.Now().Add(5 * time.Second)
	persisted := false
	for time.Now().Before(deadline) {
		recs, lerr := s.ListAccounts()
		if lerr == nil && len(recs) == 1 && recs[0].RequestsTotal >= 2 {
			persisted = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !persisted {
		t.Error("background flusher did not persist the counters to SQLite")
	}
}

// TestRestartPreservesStateAndCooldown verifies the DB round-trip of the state
// machine: a fresh Pool over the same store restores status, reason, cooldown,
// quota and claim fields.
func TestRestartPreservesStateAndCooldown(t *testing.T) {
	clk := newFakeClock()
	s := openPlain(t, filepath.Join(t.TempDir(), "restart.db"))
	now := clk.Now()
	id, err := s.AddAccount("Persisted", []byte(testJWT(1)), "", now)
	if err != nil {
		t.Fatalf("AddAccount: %v", err)
	}

	reset := now.Add(2 * time.Hour).Truncate(time.Second)
	p1, err := NewPool(s, Config{Now: clk.Now})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	accounts := p1.Accounts()
	p1.Feedback(&accounts[0], exhaustedOutcome(reset))
	remaining, total := int64(120), int64(500)
	if err := s.UpdateAccountQuota(id, &remaining, &total, now.Add(72*time.Hour), now); err != nil {
		t.Fatalf("UpdateAccountQuota: %v", err)
	}
	if err := s.UpdateAccountClaim(id, now, "ok"); err != nil {
		t.Fatalf("UpdateAccountClaim: %v", err)
	}
	p1.Stop()

	p2, err := NewPool(s, Config{Now: clk.Now})
	if err != nil {
		t.Fatalf("NewPool (2nd): %v", err)
	}
	defer p2.Stop()
	a, ok := p2.AccountByID(id)
	if !ok {
		t.Fatal("account lost after restart")
	}
	if a.Status != StatusExhausted || a.StatusReason != "quota_exhausted" {
		t.Errorf("status after restart = %s (%s), want EXHAUSTED (quota_exhausted)", a.Status, a.StatusReason)
	}
	if !a.CooldownUntil.Equal(reset) {
		t.Errorf("cooldown after restart = %v, want %v", a.CooldownUntil, reset)
	}
	if a.RemainingQuota == nil || *a.RemainingQuota != 120 || a.TotalQuota == nil || *a.TotalQuota != 500 {
		t.Errorf("quota after restart = %v/%v", a.RemainingQuota, a.TotalQuota)
	}
	if a.LastClaimKind != "ok" {
		t.Errorf("last_claim_kind after restart = %q", a.LastClaimKind)
	}
	if !a.PlanExpiresAt.Equal(now.Add(72 * time.Hour)) {
		t.Errorf("plan_expires_at after restart = %v", a.PlanExpiresAt)
	}

	// The sweeper of the restarted pool releases the account when the reset
	// time passes (fake clock).
	p2.Start()
	clk.Advance(2*time.Hour + time.Minute)
	if n := p2.Sweep(clk.Now()); n != 1 {
		t.Fatalf("sweep after restart recovered %d, want 1", n)
	}
}

// --- counters, errors, affinity, admin ops ----------------------------------
func TestFlushPersistsCounters(t *testing.T) {
	clk := newFakeClock()
	s, p := newSeededPool(t, 1, clk, nil, nil)

	lease, _ := p.Acquire("", nil, "")
	lease.AddUsage(120, 45)
	lease.Release(okOutcome())
	if err := p.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	recs, err := s.ListAccounts()
	if err != nil || len(recs) != 1 {
		t.Fatalf("ListAccounts = %v (%d)", err, len(recs))
	}
	r := recs[0]
	if r.RequestsTotal != 1 || r.InputTokens != 120 || r.OutputTokens != 45 {
		t.Errorf("flushed counters = %+v", r)
	}
	if r.LastUsedAt.IsZero() {
		t.Error("last_used_at not flushed")
	}
}

func TestAcquireErrorClassification(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 2, clk, nil, func(c *Config) { c.InflightLimit = 1 })
	ids := accountIDs(t, p)

	// One busy + one cooling → recovering wins (the 429 mapping needs the
	// earliest recovery time).
	l1, _ := p.Acquire("", nil, "")
	l2, _ := p.Acquire("", nil, "")
	l2.Release(coolOutcome(clk.Now().Add(90 * time.Second)))
	_, err := p.Acquire("", nil, "")
	ae := acquireFailure(t, err)
	if ae.Failure != AcquireRecovering {
		t.Errorf("failure = %q, want recovering", ae.Failure)
	}
	want := clk.Now().Add(90 * time.Second).Sub(clk.Now())
	if ae.RetryAfter < want-time.Second || ae.RetryAfter > want {
		t.Errorf("RetryAfter = %v, want ~%v", ae.RetryAfter, want)
	}
	l1.Release(okOutcome())

	// The only ACTIVE account is busy (inflight at the limit) and the other
	// one was disabled out of its COOLING → busy.
	l3, _ := p.Acquire("", nil, "")
	if err := p.SetAccountEnabled(ids[1], false); err != nil {
		t.Fatalf("disable: %v", err)
	}
	_, err = p.Acquire("", nil, "")
	ae = acquireFailure(t, err)
	if ae.Failure != AcquireBusy {
		t.Errorf("failure = %q, want busy (one active-at-limit, one disabled)", ae.Failure)
	}
	// The held account joins COOLING too → recovering.
	l3.Release(coolOutcome(clk.Now().Add(2 * time.Hour)))
	_, err = p.Acquire("", nil, "")
	ae = acquireFailure(t, err)
	if ae.Failure != AcquireRecovering {
		t.Errorf("failure = %q, want recovering (the first account is cooling)", ae.Failure)
	}

	// Disable the cooling account too: every account is now DISABLED.
	if err := p.SetAccountEnabled(ids[0], false); err != nil {
		t.Fatalf("disable: %v", err)
	}
	_, err = p.Acquire("", nil, "")
	ae = acquireFailure(t, err)
	if ae.Failure != AcquireAllDisabled {
		t.Errorf("failure = %q, want all_disabled", ae.Failure)
	}
}

func TestAffinityPinnedAcrossRequests(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 3, clk, nil, nil)

	session := "user-42"
	l1, err := p.Acquire(session, nil, "")
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	pinned := l1.AccountID()
	l1.Release(okOutcome())

	for i := 0; i < 3; i++ {
		l, err := p.Acquire(session, nil, "")
		if err != nil {
			t.Fatalf("acquire %d: %v", i+2, err)
		}
		if l.AccountID() != pinned {
			t.Fatalf("acquire %d = %s, want the pinned %s (session affinity)", i+2, l.AccountID(), pinned)
		}
		l.Release(okOutcome())
	}

	// A different session may get another account.
	l2, err := p.Acquire("user-43", nil, "")
	if err != nil {
		t.Fatalf("acquire other session: %v", err)
	}
	l2.Release(okOutcome())

	// TTL expiry (fake clock): after the TTL the binding is gone.
	clk.Advance(config.DefaultAffinityTTL + time.Minute)
	l3, err := p.Acquire(session, nil, "")
	if err != nil {
		t.Fatalf("acquire after TTL: %v", err)
	}
	if l3.AccountID() != pinned {
		t.Logf("note: after TTL expiry the session moved to %s (expected)", l3.AccountID())
	}
	l3.Release(okOutcome())
}

func TestImportAccounts(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 0, clk, nil, nil)

	jwt := testJWT(99)
	raw := "Acc One | " + jwt + "\n" + testJWT(98) + "\n" + jwt + "\nnot-a-jwt\n  \nBad | short"
	added, duplicates, invalid, err := p.ImportAccounts(raw)
	if err != nil {
		t.Fatalf("ImportAccounts: %v", err)
	}
	if added != 2 {
		t.Errorf("added = %d, want 2", added)
	}
	if duplicates != 1 {
		t.Errorf("duplicates = %d, want 1", duplicates)
	}
	if len(invalid) != 2 || invalid[0] != 4 || invalid[1] != 6 {
		t.Errorf("invalid = %v, want [4 6]", invalid)
	}
	if p.NumAccounts() != 2 {
		t.Errorf("NumAccounts = %d, want 2", p.NumAccounts())
	}
	// device_mid was generated at import.
	for _, a := range p.Accounts() {
		if !isUUIDv4(a.DeviceMid) {
			t.Errorf("imported account %s has device_mid %q", a.ID, a.DeviceMid)
		}
	}
}

func TestSetAccountEnabled(t *testing.T) {
	clk := newFakeClock()
	col := &eventCollector{}
	_, p := newSeededPool(t, 1, clk, col, nil)
	id := accountIDs(t, p)[0]

	if err := p.SetAccountEnabled(id, false); err != nil {
		t.Fatalf("disable: %v", err)
	}
	a, _ := p.AccountByID(id)
	if a.Status != StatusDisabled || a.StatusReason != "disabled_by_admin" {
		t.Errorf("disabled = %s (%s)", a.Status, a.StatusReason)
	}

	// A DISABLED account never recovers through the sweeper.
	clk.Advance(time.Hour)
	if n := p.Sweep(clk.Now()); n != 0 {
		t.Errorf("sweep recovered a DISABLED account (%d)", n)
	}

	if err := p.SetAccountEnabled(id, true); err != nil {
		t.Fatalf("enable: %v", err)
	}
	a, _ = p.AccountByID(id)
	if a.Status != StatusActive || a.StatusReason != "" || !a.CooldownUntil.IsZero() {
		t.Errorf("enabled = %s (%s, until %v), want ACTIVE with cleared fields", a.Status, a.StatusReason, a.CooldownUntil)
	}

	if err := p.SetAccountEnabled("acc_missing", true); err == nil {
		t.Error("enabling a missing account must fail")
	}
}

func TestRenameAndProxyAssignment(t *testing.T) {
	clk := newFakeClock()
	s, p := newSeededPool(t, 1, clk, nil, nil)
	id := accountIDs(t, p)[0]

	pxID, err := s.AddProxy("EU", "socks5://10.0.0.1:1080", clk.Now())
	if err != nil {
		t.Fatalf("AddProxy: %v", err)
	}
	if err := p.RenameAccount(id, "Renamed Acc"); err != nil {
		t.Fatalf("RenameAccount: %v", err)
	}
	if err := p.SetAccountProxy(id, pxID); err != nil {
		t.Fatalf("SetAccountProxy: %v", err)
	}
	a, _ := p.AccountByID(id)
	if a.Name != "Renamed Acc" || a.ProxyID != pxID {
		t.Errorf("account = %+v", a)
	}

	// The lease resolves the dedicated proxy URL.
	lease, err := p.Acquire("", nil, "")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease.ProxyURL() != "socks5://10.0.0.1:1080" {
		t.Errorf("lease proxy = %q, want the dedicated proxy", lease.ProxyURL())
	}
	lease.Release(okOutcome())

	// Unknown ids fail with ErrAccountNotFound.
	if err := p.RenameAccount("acc_missing", "x"); !errors.Is(err, ErrAccountNotFound) {
		t.Errorf("rename missing err = %v", err)
	}
	if err := p.SetAccountProxy("acc_missing", pxID); !errors.Is(err, ErrAccountNotFound) {
		t.Errorf("proxy missing err = %v", err)
	}
}

func TestLeaseProxyURLFallback(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 1, clk, nil, func(c *Config) { c.DefaultProxyURL = "http://default-proxy:8080" })

	lease, _ := p.Acquire("", nil, "")
	if lease.ProxyURL() != "http://default-proxy:8080" {
		t.Errorf("lease proxy = %q, want the global default", lease.ProxyURL())
	}
	lease.Release(okOutcome())
}

func TestDeleteAccount(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 2, clk, nil, nil)
	ids := accountIDs(t, p)

	if err := p.DeleteAccount(ids[0]); err != nil {
		t.Fatalf("DeleteAccount: %v", err)
	}
	if p.NumAccounts() != 1 {
		t.Errorf("NumAccounts = %d, want 1", p.NumAccounts())
	}
	if _, ok := p.AccountByID(ids[0]); ok {
		t.Error("deleted account still visible")
	}
	if err := p.DeleteAccount(ids[0]); !errors.Is(err, ErrAccountNotFound) {
		t.Errorf("double delete err = %v", err)
	}
}

func TestStopIsIdempotent(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 1, clk, nil, func(c *Config) {
		c.SweepInterval = 10 * time.Millisecond
		c.FlushInterval = 10 * time.Millisecond
	})
	p.Start()
	p.Stop()
	p.Stop() // must not panic or deadlock
	if _, err := p.Acquire("", nil, ""); !errors.Is(err, ErrPoolClosed) {
		t.Errorf("Acquire after Stop = %v, want ErrPoolClosed", err)
	}
	p.Start() // restarting a closed pool is a no-op
}

func TestApplyRuntime(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 1, clk, nil, func(c *Config) { c.InflightLimit = 1 })

	lease, _ := p.Acquire("", nil, "")
	if _, err := p.Acquire("", nil, ""); err == nil {
		t.Fatal("second acquire must hit the limit of 1")
	}
	p.ApplyRuntime(2, 0, true, 0, "")
	l2, err := p.Acquire("", nil, "")
	if err != nil {
		t.Fatalf("acquire after ApplyRuntime(2): %v", err)
	}
	l2.Release(okOutcome())
	lease.Release(okOutcome())
}

func TestStatusCounts(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 3, clk, nil, nil)
	ids := accountIDs(t, p)

	accounts := p.Accounts()
	p.Feedback(&accounts[0], coolOutcome(clk.Now().Add(time.Minute)))
	p.Feedback(&accounts[1], exhaustedOutcome(clk.Now().Add(time.Minute)))
	if err := p.SetAccountEnabled(ids[2], false); err != nil {
		t.Fatalf("disable: %v", err)
	}

	counts := p.StatusCounts()
	if counts[StatusActive] != 0 || counts[StatusCooling] != 1 ||
		counts[StatusExhausted] != 1 || counts[StatusDisabled] != 1 {
		t.Errorf("counts = %v, want 0/1/1/1", counts)
	}
}

// --- balance (POST /api/accounts/{id}/balance) ------------------------------

// TestRefreshBalanceUpdatesQuota drives RefreshBalance against an httptest
// server acting as the zcode-plan billing endpoint (no real network; the
// The project forbids real upstream calls in tests).
func TestRefreshBalanceUpdatesQuota(t *testing.T) {
	clk := newFakeClock()
	col := &eventCollector{}
	s, p := newSeededPool(t, 1, clk, col, nil)
	id := accountIDs(t, p)[0]

	planExpires := clk.Now().Add(72 * time.Hour).Truncate(time.Second).Unix()
	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/zcode-plan/billing/balance" {
			http.NotFound(w, r)
			return
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer "+testJWT(0) {
			t.Errorf("balance Authorization = %q, want the account JWT", auth)
		}
		if r.Header.Get("X-Device-Mid") == "" {
			t.Error("balance request missing X-Device-Mid")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"remaining_quota": 500,
			"total_quota":     1000,
			"plan_expires_at": planExpires,
		})
	}))
	defer upstreamSrv.Close()

	p.ApplyRuntime(0, 0, true, 0, "")
	// Point the pool's billing origin at the fake upstream.
	p.mu.Lock()
	p.cfg.UpstreamURL = upstreamSrv.URL
	p.mu.Unlock()

	balance, out, err := p.RefreshBalance(t.Context(), id)
	if err != nil {
		t.Fatalf("RefreshBalance: %v", err)
	}
	if out.Kind != upstream.KindOK {
		t.Errorf("outcome = %s, want ok", out.Kind)
	}
	if balance.RemainingQuota == nil || *balance.RemainingQuota != 500 {
		t.Errorf("remaining = %v, want 500", balance.RemainingQuota)
	}
	if balance.PlanExpiresAt.Unix() != planExpires {
		t.Errorf("plan_expires_at = %v, want %d", balance.PlanExpiresAt, planExpires)
	}

	a, _ := p.AccountByID(id)
	if a.RemainingQuota == nil || *a.RemainingQuota != 500 || a.TotalQuota == nil || *a.TotalQuota != 1000 {
		t.Errorf("account quota after refresh = %v/%v", a.RemainingQuota, a.TotalQuota)
	}
	if !a.QuotaCheckedAt.Equal(clk.Now()) {
		t.Errorf("quota_checked_at = %v, want the pool clock now", a.QuotaCheckedAt)
	}

	recs, _ := s.ListAccounts()
	if recs[0].RemainingQuota == nil || *recs[0].RemainingQuota != 500 {
		t.Errorf("persisted remaining_quota = %v, want 500", recs[0].RemainingQuota)
	}

	if _, _, err := p.RefreshBalance(t.Context(), "acc_missing"); !errors.Is(err, ErrAccountNotFound) {
		t.Errorf("balance of a missing account err = %v", err)
	}
}

// TestRefreshBalanceDisablesDeadJWT: a 1006 from the balance endpoint flows
// through the same state machine.
func TestRefreshBalanceDisablesDeadJWT(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 1, clk, nil, nil)
	id := accountIDs(t, p)[0]

	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":{"code":"1006","message":"token expired"}}`))
	}))
	defer upstreamSrv.Close()

	p.mu.Lock()
	p.cfg.UpstreamURL = upstreamSrv.URL
	p.mu.Unlock()

	_, out, err := p.RefreshBalance(t.Context(), id)
	if err != nil {
		t.Fatalf("RefreshBalance: %v", err)
	}
	if out.Kind != upstream.KindAuth {
		t.Errorf("outcome = %s, want auth", out.Kind)
	}
	a, _ := p.AccountByID(id)
	if a.Status != StatusDisabled {
		t.Errorf("status after a 1006 balance = %s, want DISABLED", a.Status)
	}
}
