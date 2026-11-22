package pool

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestClaimAllSingleFlight pins the gate: while a pass is running,
// TryStartClaimAll reports started=false and starts nothing, and the
// scheduler's ClaimAll gives up after the bounded wait instead of stacking a
// second pass over the same accounts.
func TestClaimAllSingleFlight(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 2, clk, nil, nil)

	release := make(chan struct{})
	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Park the claim call until the test releases it: the pass is
		// demonstrably running while the re-entry attempts happen.
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":0,"message":"ok"}`)
	}))
	defer upstreamSrv.Close()

	// The bounded wait drives the SAME fake clock Now reads, so the
	// scheduler's 30s wait elapses without real sleeping.
	c, _ := newTestClaimer(t, p, ClaimConfig{
		PlanID:      "test-plan",
		UpstreamURL: upstreamSrv.URL,
		Now:         clk.Now,
		Wait: func(ctx context.Context, d time.Duration) error {
			clk.Advance(d)
			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started, done := c.TryStartClaimAll(ctx)
	if !started {
		t.Fatal("TryStartClaimAll = false, want the gate to be free initially")
	}

	// While the pass is parked on the held claim call, every re-entry is
	// rejected and starts nothing.
	if started2, done2 := c.TryStartClaimAll(ctx); started2 || done2 != nil {
		t.Fatal("re-entry while a pass runs must report started=false")
	}
	if got := c.ClaimAll(ctx); got != 0 {
		t.Errorf("scheduler ClaimAll during a running pass = %d, want 0 (bounded wait gave up)", got)
	}

	close(release)
	if got := <-done; got != 2 {
		t.Errorf("claimed = %d, want 2 (each account claimed exactly once)", got)
	}
	if claims := c.ClaimsTotal(); claims != 2 {
		t.Errorf("claims_total = %d, want 2 — a rejected re-entry must not double-claim", claims)
	}
}
