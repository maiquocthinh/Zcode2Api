package pool

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func ptrInt64(v int64) *int64 { return &v }

// waitForStatus polls AccountByID until the account reaches the status (or
// the test times out): it bridges the recheck goroutine, which restores the
// account asynchronously.
func waitForStatus(t *testing.T, p *Pool, id string, want Status) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if a, ok := p.AccountByID(id); ok && a.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	a, _ := p.AccountByID(id)
	t.Fatalf("account status = %s, want %s (timeout)", a.Status, want)
}

// TestLeaseQuotaStrikeAndPerModelBench pins the two chat-quota paths of
// Lease.Release: on a snapshot that still shows quota, the first 1005 keeps
// the account serving (strike 1) and the second benches it for two minutes
// and verifies by a free billing call; on a snapshot that shows no quota the
// exhaustion lands on the (account, model) pair instead of the whole account.
func TestLeaseQuotaStrikeAndPerModelBench(t *testing.T) {
	clk := newFakeClock()
	// The recheck's billing answer always shows quota: after the strike bench
	// the account must come straight back to ACTIVE.
	billing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":0,"data":{"balances":[{"total_units":500000000,"used_units":0,"remaining_units":500000000,"expires_at":1795046400}]}}`)
	}))
	defer billing.Close()

	_, p := newSeededPool(t, 1, clk, nil, nil)
	id := accountIDs(t, p)[0]

	p.mu.Lock()
	p.cfg.UpstreamURL = billing.URL
	p.byID[id].RemainingQuota = ptrInt64(500_000_000)
	p.mu.Unlock()

	// Strike 1: no bench, the account keeps serving.
	lease, err := p.Acquire("", nil, "glm-5.3")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	lease.Release(exhaustedOutcome(clk.Now().Add(time.Hour)))
	a, _ := p.AccountByID(id)
	if a.Status != StatusActive || a.QuotaStrikes != 1 || len(a.ExhaustedModels) != 0 {
		t.Fatalf("after strike 1: status=%s strikes=%d models=%v, want ACTIVE/1/none", a.Status, a.QuotaStrikes, a.ExhaustedModels)
	}

	// Strike 2: a short account-wide bench plus the billing verification.
	lease, err = p.Acquire("", nil, "glm-5.3")
	if err != nil {
		t.Fatalf("Acquire (strike 2): %v", err)
	}
	lease.Release(exhaustedOutcome(clk.Now().Add(time.Hour)))
	a, _ = p.AccountByID(id)
	if a.Status != StatusExhausted {
		t.Fatalf("after strike 2: status=%s, want EXHAUSTED", a.Status)
	}
	if bench := a.CooldownUntil.Sub(clk.Now()); bench < quotaRecheckBench-time.Second || bench > quotaRecheckBench+time.Second {
		t.Errorf("bench until = %s after now, want ~%s", bench, quotaRecheckBench)
	}

	// The recheck's billing call shows quota, but the balance cannot see the
	// minute-level limit: the account stays benched until the bench ends,
	// then the sweeper restores it.
	for i := 0; i < 100; i++ {
		if a, _ = p.AccountByID(id); !a.QuotaCheckedAt.IsZero() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if a.QuotaCheckedAt.IsZero() || a.Status != StatusExhausted {
		t.Fatalf("after recheck: checked=%v status=%s, want a refreshed snapshot and still EXHAUSTED", !a.QuotaCheckedAt.IsZero(), a.Status)
	}
	clk.Advance(quotaRecheckBench + time.Second)
	p.Sweep(clk.Now())
	waitForStatus(t, p, id, StatusActive)
	a, _ = p.AccountByID(id)
	if a.RemainingQuota == nil || *a.RemainingQuota != 500_000_000 {
		t.Errorf("RemainingQuota = %v, want the refreshed 500000000", a.RemainingQuota)
	}

	// An empty snapshot: the next 1005 benches only the (account, model)
	// pair — the account stays ACTIVE and still serves other models.
	p.mu.Lock()
	p.byID[id].RemainingQuota = ptrInt64(0)
	p.mu.Unlock()
	lease, err = p.Acquire("", nil, "glm-5.3")
	if err != nil {
		t.Fatalf("Acquire (per-model): %v", err)
	}
	lease.Release(exhaustedOutcome(clk.Now().Add(time.Hour)))
	a, _ = p.AccountByID(id)
	if a.Status != StatusActive {
		t.Fatalf("after per-model 1005: status=%s, want ACTIVE", a.Status)
	}
	if until, ok := a.ExhaustedModels["glm-5.3"]; !ok || until.Unix() != clk.Now().Add(time.Hour).Unix() {
		t.Errorf("exhausted models = %v, want glm-5.3 until +1h", a.ExhaustedModels)
	}
	if _, err := p.Acquire("", nil, "glm-5.3"); err == nil {
		t.Error("Acquire for the exhausted model succeeded, want a skip")
	}
	l45, err := p.Acquire("", nil, "glm-4.5")
	if err != nil {
		t.Fatalf("Acquire for another model failed: %v", err)
	}
	l45.Release(okOutcome())

	// A cooldown ending (sweeper COOLING -> ACTIVE) proves nothing about the
	// model quota: the pair survives.
	p.mu.Lock()
	var evs []StateEvent
	_ = p.transitionLocked(p.byID[id], StatusCooling, clk.Now().Add(time.Minute), "rate_limit", clk.Now(), &evs)
	p.mu.Unlock()
	clk.Advance(2 * time.Minute)
	p.Sweep(clk.Now())
	if a, _ = p.AccountByID(id); a.Status != StatusActive || len(a.ExhaustedModels) == 0 {
		t.Fatalf("after sweep: status=%s models=%v, want ACTIVE with the glm-5.3 mark kept", a.Status, a.ExhaustedModels)
	}

	// Clearing: the operator enable path drops the pair, so the model is
	// servable again.
	if err := p.SetAccountEnabled(id, true); err != nil {
		t.Fatalf("SetAccountEnabled: %v", err)
	}
	if _, err := p.Acquire("", nil, "glm-5.3"); err != nil {
		t.Errorf("Acquire for the previously exhausted model failed after ACTIVE: %v", err)
	}
}
