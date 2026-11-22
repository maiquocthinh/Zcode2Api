package pool

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestClaimDiscoveredPlans pins the CLAIM_PLAN_ID-empty flow: the activation
// events are posted, the marketing delivery's plan is claimed with its
// campaign id, the success confirms against the campaign, and the post-claim
// balance refresh lands on the account.
func TestClaimDiscoveredPlans(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 1, clk, nil, nil)
	id := accountIDs(t, p)[0]

	var (
		events    int
		claimBody map[string]string
		confirms  []map[string]string
	)
	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/event/report"):
			events++
			_, _ = io.WriteString(w, `{"code":0}`)
		case strings.HasSuffix(r.URL.Path, "/marketing/touch") && !strings.HasSuffix(r.URL.Path, "/action"):
			_, _ = io.WriteString(w, `{"code":0,"data":{"deliveries":[{"campaign_id":"camp-1","banner":{`+
				`"buttons":[{"action":{"type":"claim_zcode_plan","args":{"plan_id":"plan-weekly"}}}],`+
				`"background":{"args":{"zcode_plan":{"name":"Weekly","priority":7}}}}}]}}`)
		case strings.HasSuffix(r.URL.Path, "/billing/claim"):
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &claimBody)
			_, _ = io.WriteString(w, `{"code":0,"data":{"plan":{"ends_at":1795046400}}}`)
		case strings.HasSuffix(r.URL.Path, "/marketing/touch/action"):
			body, _ := io.ReadAll(r.Body)
			var m map[string]string
			_ = json.Unmarshal(body, &m)
			confirms = append(confirms, m)
			_, _ = io.WriteString(w, `{"code":0}`)
		case strings.HasSuffix(r.URL.Path, "/billing/balance"):
			_, _ = io.WriteString(w, `{"code":0,"data":{"balances":[{"total_units":999999,"used_units":0,"remaining_units":123456,"expires_at":1795046400}]}}`)
		default:
			t.Errorf("unexpected upstream path %q", r.URL.Path)
			_, _ = io.WriteString(w, `{"code":0}`)
		}
	}))
	defer upstreamSrv.Close()

	// The post-claim refresh uses the pool's billing origin: wire it to the
	// same fake upstream.
	p.mu.Lock()
	p.cfg.UpstreamURL = upstreamSrv.URL
	p.mu.Unlock()

	c, _ := newTestClaimer(t, p, ClaimConfig{UpstreamURL: upstreamSrv.URL})
	report := c.ClaimOne(context.Background(), id)

	if !report.OK || !report.Claimed || report.Kind != "ok" {
		t.Fatalf("report = %+v, want a claimed discovery claim", report)
	}
	if report.NextClaimAt.IsZero() || report.NextClaimAt.Unix() != 1795046400 {
		t.Errorf("NextClaimAt = %v, want data.plan.ends_at", report.NextClaimAt)
	}
	if events != 3 {
		t.Errorf("activation events = %d, want 3 (launch/daily/login)", events)
	}
	if claimBody["plan_id"] != "plan-weekly" {
		t.Errorf("claim body plan_id = %v, want plan-weekly", claimBody["plan_id"])
	}
	if len(confirms) != 1 || confirms[0]["campaign_id"] != "camp-1" || confirms[0]["action_type"] != "confirm" {
		t.Errorf("confirms = %v, want one confirm receipt for camp-1", confirms)
	}
	if claims := c.ClaimsTotal(); claims != 1 {
		t.Errorf("claims_total = %d, want 1", claims)
	}

	// The post-claim balance refresh refreshed the snapshot (same origin).
	a, _ := p.AccountByID(id)
	if a.RemainingQuota == nil || *a.RemainingQuota != 123456 {
		t.Errorf("RemainingQuota after the post-claim refresh = %v, want 123456", a.RemainingQuota)
	}
}

// TestClaimNoOfferStopsWithoutClaiming pins the 0-delivery answer: the flow
// stops with a no_offer report and the claim endpoint is never called (no
// campaign right now is transient, never a cooldown-worthy failure).
func TestClaimNoOfferStopsWithoutClaiming(t *testing.T) {
	clk := newFakeClock()
	_, p := newSeededPool(t, 1, clk, nil, nil)
	id := accountIDs(t, p)[0]

	var claimCalls int
	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/billing/claim") {
			claimCalls++
		}
		_, _ = io.WriteString(w, `{"code":0,"data":{"deliveries":[]}}`)
	}))
	defer upstreamSrv.Close()

	c, _ := newTestClaimer(t, p, ClaimConfig{UpstreamURL: upstreamSrv.URL})
	report := c.ClaimOne(context.Background(), id)

	if report.OK || report.Kind != "no_offer" {
		t.Fatalf("report = %+v, want a no_offer stop", report)
	}
	if claimCalls != 0 {
		t.Errorf("claim calls = %d, want 0 (nothing offered, nothing claimed)", claimCalls)
	}
}
