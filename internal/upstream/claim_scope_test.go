package upstream

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// TestClaimScopedOutcome pins the claim endpoint's business-code rewrites:
// 1003 is an idempotent success, 1005 (daily claim limit) never benches chat,
// and 1001/1002/1004 are plan-side failures — none of them reaches the pool
// state machine with an account action.
func TestClaimScopedOutcome(t *testing.T) {
	// The shared table disables the account on the 1000-1004 family and
	// exhausts it on 1005 — the claim scope must rewrite both.
	out := Classify(http.StatusOK, []byte(`{"code":1003,"message":"already claimed"}`), nil)
	if out.Kind != KindAuth || out.AccountAction != AccountDisabled {
		t.Fatalf("chat Classify(1003) = %+v, want auth/DISABLED", out)
	}
	scoped := claimScopedOutcome(out, []byte(`{"code":1003,"message":"already claimed"}`))
	if scoped.Kind != KindClaimAlready || scoped.AccountAction != AccountResetStreak {
		t.Fatalf("claim 1003 = %+v, want claim_already/RESET_STREAK", scoped)
	}

	body1005 := []byte(`{"code":1005,"message":"daily claim limit","data":{"plan":{"ends_at":1763638800}}}`)
	out = Classify(http.StatusOK, body1005, nil)
	if out.Kind != KindQuotaExhausted || out.AccountAction != AccountExhausted {
		t.Fatalf("chat Classify(1005) = %+v, want quota_exhausted/EXHAUSTED", out)
	}
	scoped = claimScopedOutcome(out, body1005)
	if scoped.Kind != KindClaimLimit || scoped.AccountAction != AccountNone || scoped.Retryable {
		t.Fatalf("claim 1005 = %+v, want claim_limit/NONE/not retryable", scoped)
	}

	for _, code := range []string{"1001", "1002", "1004"} {
		body := []byte(`{"code":` + code + `,"message":"plan not claimable"}`)
		scoped := claimScopedOutcome(Classify(http.StatusOK, body, nil), body)
		if scoped.Kind != KindClaimBusiness || scoped.AccountAction != AccountNone {
			t.Fatalf("claim %s = %+v, want claim_business/NONE", code, scoped)
		}
	}

	// Non-business codes pass through untouched: a dead JWT during claim
	// still disables the account.
	kept := claimScopedOutcome(Outcome{Kind: KindAuth, AccountAction: AccountDisabled, Retryable: true},
		[]byte(`{"code":1006,"message":"token expired"}`))
	if kept.Kind != KindAuth || kept.AccountAction != AccountDisabled {
		t.Fatalf("claim 1006 = %+v, want the shared-table outcome untouched", kept)
	}
}

// TestClaimQuotaBusinessCodes drives ClaimQuota end to end over the business
// answers: 1003 reports success with the next-claim instant, 1005 reports
// claim_limit without touching the account, and NextClaimAt carries
// data.plan.ends_at.
func TestClaimQuotaBusinessCodes(t *testing.T) {
	applyBillingIdentity(t)

	srv := newBillingServer(t, &billingRecorder{}, http.StatusOK,
		`{"code":1003,"message":"already claimed","data":{"plan":{"ends_at":1763638800}}}`)
	res, err := ClaimQuota(context.Background(), testAccount(), "", "", WithOrigin(srv.URL))
	if err != nil {
		t.Fatalf("ClaimQuota(1003): %v", err)
	}
	if res.Outcome.Kind != KindClaimAlready || res.Outcome.AccountAction != AccountResetStreak {
		t.Errorf("1003 Outcome = %+v, want claim_already/RESET_STREAK", res.Outcome)
	}
	if !res.NextClaimAt.Equal(time.Unix(1763638800, 0)) {
		t.Errorf("1003 NextClaimAt = %s, want %s", res.NextClaimAt, time.Unix(1763638800, 0))
	}

	srv = newBillingServer(t, &billingRecorder{}, http.StatusOK,
		`{"code":1005,"message":"daily claim limit","data":{"plan":{"ends_at":1763638800}}}`)
	res, err = ClaimQuota(context.Background(), testAccount(), "", "", WithOrigin(srv.URL))
	if err != nil {
		t.Fatalf("ClaimQuota(1005): %v", err)
	}
	if res.Outcome.Kind != KindClaimLimit || res.Outcome.AccountAction != AccountNone {
		t.Errorf("1005 Outcome = %+v, want claim_limit/NONE", res.Outcome)
	}
	if !res.NextClaimAt.Equal(time.Unix(1763638800, 0)) {
		t.Errorf("1005 NextClaimAt = %s, want %s", res.NextClaimAt, time.Unix(1763638800, 0))
	}
}
