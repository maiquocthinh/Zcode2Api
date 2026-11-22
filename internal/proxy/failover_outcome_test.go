package proxy_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"zcode2api/internal/pool"
)

func errorType(t *testing.T, body []byte) string {
	t.Helper()
	var env struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("error body is not JSON: %v (%s)", err, body)
	}
	return env.Error.Type
}

func TestE2E_PlatformOverloadStopsWithoutFailover(t *testing.T) {
	f := newE2E2Accounts(t)
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(529)
		_, _ = io.WriteString(w, `{"error":{"type":"overloaded_error","message":"platform overloaded"}}`)
	})

	resp, body := f.postMessages(plainRequest("claude-sonnet-4-5-20250929"))
	if resp.StatusCode != 529 {
		t.Fatalf("status = %d, want 529; body %s", resp.StatusCode, body)
	}
	if got := errorType(t, body); got != "overloaded_error" {
		t.Errorf("error.type = %q, want overloaded_error", got)
	}
	if got := resp.Header.Get("x-zcode2api-attempts"); got != "1" {
		t.Errorf("x-zcode2api-attempts = %q, want 1 (no failover after the overload budget)", got)
	}
	calls := f.calls.all()
	if len(calls) != 3 {
		t.Fatalf("upstream calls = %d, want 3 (first call + two same-account replays)", len(calls))
	}
	for i, c := range calls {
		if c.JWT != e2eJWTA {
			t.Errorf("call %d hit %s, want Account A only", i, c.JWT)
		}
	}
	for _, name := range []string{"Account A", "Account B"} {
		if a := f.accountByName(name); a.Status != pool.StatusActive || a.FailStreak != 0 {
			t.Errorf("%s = %s streak=%d, want ACTIVE with no streak", name, a.Status, a.FailStreak)
		}
	}
}

func TestE2E_TriedAllAnswersQuotaFromLastOutcome(t *testing.T) {
	f := newE2E2Accounts(t) // MAX_ATTEMPTS=3 > 2 accounts: attempt 3 finds no untried account
	reset := time.Now().Add(time.Hour).Unix()
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, fmt.Sprintf(
			`{"error":{"code":"1311","message":"weekly limit","next_flush_time":%d}}`, reset))
	})

	resp, body := f.postMessages(plainRequest("claude-sonnet-4-5-20250929"))
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429 (not 529 for an all-tried pool); body %s", resp.StatusCode, body)
	}
	if got := errorType(t, body); got != "rate_limit_error" {
		t.Errorf("error.type = %q, want rate_limit_error", got)
	}
	ra, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	if err != nil || ra < 3500 || ra > 3600 {
		t.Errorf("Retry-After = %q, want ~3600s (the reported model reset)", resp.Header.Get("Retry-After"))
	}
	if n := len(f.calls.all()); n != 2 {
		t.Errorf("upstream calls = %d, want 2 (one per account)", n)
	}
	for _, name := range []string{"Account A", "Account B"} {
		if a := f.accountByName(name); a.Status != pool.StatusActive {
			t.Errorf("%s = %s, want ACTIVE (the exhaustion is per-model)", name, a.Status)
		}
	}
}

func TestE2E_TriedAllAnswersModelBusyFromLastOutcome(t *testing.T) {
	f := newE2E2Accounts(t)
	f.serveUpstream(func(call upstreamCall, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"code":"3010","message":"model busy"}}`)
	})

	resp, body := f.postMessages(plainRequest("claude-sonnet-4-5-20250929"))
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429 (not 529 for an all-tried pool); body %s", resp.StatusCode, body)
	}
	if got := errorType(t, body); got != "rate_limit_error" {
		t.Errorf("error.type = %q, want rate_limit_error", got)
	}
	if got := resp.Header.Get("x-zcode2api-attempts"); got != "2" {
		t.Errorf("x-zcode2api-attempts = %q, want 2 (model-busy still fails over)", got)
	}
	if n := len(f.calls.all()); n != 6 {
		t.Errorf("upstream calls = %d, want 6 (three per account)", n)
	}
}
