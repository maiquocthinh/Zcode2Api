package upstream

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// billingRecorder captures one billing call (headers, URL, body) — test
// memory only, never logged.
type billingRecorder struct {
	mu     sync.Mutex
	Method string
	Path   string
	Query  string
	Header http.Header
	Body   []byte
}

func (r *billingRecorder) record(req *http.Request, body []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Method = req.Method
	r.Path = req.URL.Path
	r.Query = req.URL.RawQuery
	r.Header = req.Header.Clone()
	r.Body = body
}

// newBillingServer runs an httptest upstream that records the request and
// answers with the given status and body. No real network is contacted.
func newBillingServer(t *testing.T, rec *billingRecorder, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		rec.record(r, raw)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func testAccount() ChannelAAccount {
	return ChannelAAccount{JWT: testJWT, DeviceMid: testDeviceMid}
}

// assertChannelAHeaders checks the channel A source headers on a billing
// call and that no captcha header is present unless it was asked for.
func assertChannelAHeaders(t *testing.T, h http.Header, wantCaptcha string) {
	t.Helper()
	if got := h.Get("Authorization"); got != "Bearer "+testJWT {
		t.Errorf("Authorization = %q, want Bearer <JWT>", got)
	}
	if got := h.Get("User-Agent"); got != "ZCode/3.16.3" {
		t.Errorf("User-Agent = %q, want ZCode/3.16.3", got)
	}
	if got := h.Get("HTTP-Referer"); got != "https://zcode.z.ai" {
		t.Errorf("HTTP-Referer = %q, want https://zcode.z.ai", got)
	}
	if got := h.Get("X-Title"); got != "Z Code@electron" {
		t.Errorf("X-Title = %q, want Z Code@electron", got)
	}
	if got := h.Get("X-Platform"); got != "darwin-x64" {
		t.Errorf("X-Platform = %q, want darwin-x64", got)
	}
	if got := h.Get("X-Device-Mid"); got != testDeviceMid {
		t.Errorf("X-Device-Mid = %q, want the account device_mid", got)
	}
	if got := h.Get(captchaHeader); got != wantCaptcha {
		t.Errorf("captcha header = %q, want %q", got, wantCaptcha)
	}
}

func applyBillingIdentity(t *testing.T) {
	t.Helper()
	SetChannelAIdentity("3.16.3", "darwin-x64")
	t.Cleanup(func() { SetChannelAIdentity(DefaultZcodeAppVersion, DefaultZcodePlatform) })
}

// TestQueryBalanceSuccessAndHeaders drives QueryBalance against a simulated
// upstream: GET with app_version=<ZCODE_APP_VERSION>, the channel A
// headers, no captcha header, and the parsed quota fields.
func TestQueryBalanceSuccessAndHeaders(t *testing.T) {
	applyBillingIdentity(t)
	rec := &billingRecorder{}
	body := `{"remaining_quota":1200,"total_quota":2000,"plan_expires_at":1790000000}`
	srv := newBillingServer(t, rec, http.StatusOK, body)

	res, err := QueryBalance(context.Background(), testAccount(), WithOrigin(srv.URL))
	if err != nil {
		t.Fatalf("QueryBalance: %v", err)
	}
	if rec.Method != http.MethodGet {
		t.Errorf("method = %s, want GET", rec.Method)
	}
	if rec.Path != PathChannelABalance {
		t.Errorf("path = %q, want %q", rec.Path, PathChannelABalance)
	}
	if got, want := rec.Query, "app_version=3.16.3"; got != want {
		t.Errorf("query = %q, want %q", got, want)
	}
	assertChannelAHeaders(t, rec.Header, "")

	if res.Outcome.Kind != KindOK || res.Outcome.AccountAction != AccountResetStreak {
		t.Errorf("Outcome = %+v, want ok/RESET_STREAK", res.Outcome)
	}
	if res.Balance.RemainingQuota == nil || *res.Balance.RemainingQuota != 1200 {
		t.Errorf("RemainingQuota = %v, want 1200", res.Balance.RemainingQuota)
	}
	if res.Balance.TotalQuota == nil || *res.Balance.TotalQuota != 2000 {
		t.Errorf("TotalQuota = %v, want 2000", res.Balance.TotalQuota)
	}
	if !res.Balance.PlanExpiresAt.Equal(time.Unix(1790000000, 0)) {
		t.Errorf("PlanExpiresAt = %s, want %s", res.Balance.PlanExpiresAt, time.Unix(1790000000, 0))
	}
}

// TestQueryBalanceDataWrapperAndRFC3339 pins the tolerant payload shapes:
// fields wrapped in "data" and a string plan_expires_at (RFC3339).
func TestQueryBalanceDataWrapperAndRFC3339(t *testing.T) {
	applyBillingIdentity(t)
	rec := &billingRecorder{}
	srv := newBillingServer(t, rec, http.StatusOK,
		`{"code":0,"data":{"remaining_quota":"350","total_quota":500,"plan_expires_at":"2026-09-01T08:00:00Z"}}`)

	res, err := QueryBalance(context.Background(), testAccount(), WithOrigin(srv.URL))
	if err != nil {
		t.Fatalf("QueryBalance: %v", err)
	}
	if res.Outcome.Kind != KindOK {
		t.Fatalf("Outcome = %+v, want ok", res.Outcome)
	}
	if res.Balance.RemainingQuota == nil || *res.Balance.RemainingQuota != 350 {
		t.Errorf("RemainingQuota = %v, want 350 (quoted number)", res.Balance.RemainingQuota)
	}
	if res.Balance.TotalQuota == nil || *res.Balance.TotalQuota != 500 {
		t.Errorf("TotalQuota = %v, want 500", res.Balance.TotalQuota)
	}
	want := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	if !res.Balance.PlanExpiresAt.Equal(want) {
		t.Errorf("PlanExpiresAt = %s, want %s", res.Balance.PlanExpiresAt, want)
	}
}

// TestQueryBalanceOfficialShape pins the official billing/balance contract
// (ZCode official client bigmodelUsageQuotaProvider): the quota lives in
// data.balances[] buckets (summed, scalars may be quoted strings), the
// subscription expiry is the ends_at of the active plans[] entry, and
// ResetAt carries the bucket expires_at.
func TestQueryBalanceOfficialShape(t *testing.T) {
	applyBillingIdentity(t)
	rec := &billingRecorder{}
	srv := newBillingServer(t, rec, http.StatusOK, `{"code":0,"msg":"ok","data":{
		"server_time":1795000000,
		"plans":[{"plan_id":"zcode-v3-start-plan","name":"Start Plan","status":"active","starts_at":1790000000,"ends_at":1792600000}],
		"balances":[
			{"bucket_id":"b1","total_units":100000,"used_units":30000,"remaining_units":70000,"expires_at":1795046400},
			{"bucket_id":"b2","total_units":100000,"used_units":10000,"remaining_units":"90000","expires_at":1795046400}
		]}}`)

	res, err := QueryBalance(context.Background(), testAccount(), WithOrigin(srv.URL))
	if err != nil {
		t.Fatalf("QueryBalance: %v", err)
	}
	if res.Outcome.Kind != KindOK {
		t.Fatalf("Outcome = %+v, want ok", res.Outcome)
	}
	if res.Balance.RemainingQuota == nil || *res.Balance.RemainingQuota != 160000 {
		t.Errorf("RemainingQuota = %v, want 160000 (sum of both buckets)", res.Balance.RemainingQuota)
	}
	if res.Balance.TotalQuota == nil || *res.Balance.TotalQuota != 200000 {
		t.Errorf("TotalQuota = %v, want 200000", res.Balance.TotalQuota)
	}
	if !res.Balance.PlanExpiresAt.Equal(time.Unix(1792600000, 0)) {
		t.Errorf("PlanExpiresAt = %s, want %s", res.Balance.PlanExpiresAt, time.Unix(1792600000, 0))
	}
	if !res.Balance.ResetAt.Equal(time.Unix(1795046400, 0)) {
		t.Errorf("ResetAt = %s, want %s", res.Balance.ResetAt, time.Unix(1795046400, 0))
	}
}

// TestQueryBalanceAuthFailure pins that a 401 classifies as auth/DISABLED and
// carries no balance.
func TestQueryBalanceAuthFailure(t *testing.T) {
	applyBillingIdentity(t)
	rec := &billingRecorder{}
	srv := newBillingServer(t, rec, http.StatusUnauthorized, `{"error":{"type":"authentication_error","message":"bad jwt"}}`)

	res, err := QueryBalance(context.Background(), testAccount(), WithOrigin(srv.URL))
	if err != nil {
		t.Fatalf("QueryBalance: %v", err)
	}
	if res.Outcome.Kind != KindAuth || res.Outcome.AccountAction != AccountDisabled || !res.Outcome.Retryable {
		t.Errorf("Outcome = %+v, want auth/DISABLED/retryable", res.Outcome)
	}
	if res.Balance.RemainingQuota != nil || res.Balance.TotalQuota != nil || !res.Balance.PlanExpiresAt.IsZero() {
		t.Errorf("Balance = %+v, want zero values for a failed call", res.Balance)
	}
}

// TestQueryBalanceTokenDeadOn200 pins the channel A dual-code case: HTTP 200
// with body code 1006 → auth/DISABLED.
func TestQueryBalanceTokenDeadOn200(t *testing.T) {
	applyBillingIdentity(t)
	srv := newBillingServer(t, &billingRecorder{}, http.StatusOK, `{"error":{"code":"1006","message":"token expired"}}`)

	res, err := QueryBalance(context.Background(), testAccount(), WithOrigin(srv.URL))
	if err != nil {
		t.Fatalf("QueryBalance: %v", err)
	}
	if res.Outcome.Kind != KindAuth || res.Outcome.AccountAction != AccountDisabled {
		t.Errorf("Outcome = %+v, want auth/DISABLED for 1006 over HTTP 200", res.Outcome)
	}
}

// TestQueryBalanceNextFlush pins that a next_flush_time in the balance
// response is surfaced (the upstream reports the reset time through
// claim/balance).
func TestQueryBalanceNextFlush(t *testing.T) {
	applyBillingIdentity(t)
	srv := newBillingServer(t, &billingRecorder{}, http.StatusOK,
		`{"error":{"code":"1005","message":"quota gone","next_flush_time":1763638800}}`)

	res, err := QueryBalance(context.Background(), testAccount(), WithOrigin(srv.URL))
	if err != nil {
		t.Fatalf("QueryBalance: %v", err)
	}
	if res.Outcome.Kind != KindQuotaExhausted || res.Outcome.AccountAction != AccountExhausted {
		t.Errorf("Outcome = %+v, want quota_exhausted/EXHAUSTED", res.Outcome)
	}
	if !res.NextFlush.Equal(time.Unix(1763638800, 0)) {
		t.Errorf("NextFlush = %s, want %s", res.NextFlush, time.Unix(1763638800, 0))
	}
}

// TestClaimQuotaSuccessAndHeaders drives ClaimQuota against a simulated
// upstream: POST to /api/v1/zcode-plan/billing/claim with the channel A
// headers and no captcha header when no verify parameter is given.
func TestClaimQuotaSuccessAndHeaders(t *testing.T) {
	applyBillingIdentity(t)
	rec := &billingRecorder{}
	srv := newBillingServer(t, rec, http.StatusOK, `{"code":0,"message":"claimed"}`)

	res, err := ClaimQuota(context.Background(), testAccount(), "", "", WithOrigin(srv.URL))
	if err != nil {
		t.Fatalf("ClaimQuota: %v", err)
	}
	if rec.Method != http.MethodPost {
		t.Errorf("method = %s, want POST", rec.Method)
	}
	if rec.Path != PathChannelAClaim {
		t.Errorf("path = %q, want %q", rec.Path, PathChannelAClaim)
	}
	assertChannelAHeaders(t, rec.Header, "")
	if res.Outcome.Kind != KindOK {
		t.Errorf("Outcome = %+v, want ok", res.Outcome)
	}
	if res.CaptchaRequired {
		t.Error("CaptchaRequired = true, want false for a clean claim")
	}
}

// TestClaimQuotaCaptchaRequired pins the branch: code 3007 →
// CaptchaRequired=true, the account state does not change
// (Outcome.AccountAction NONE), and no verify parameter is sent.
func TestClaimQuotaCaptchaRequired(t *testing.T) {
	applyBillingIdentity(t)
	rec := &billingRecorder{}
	srv := newBillingServer(t, rec, http.StatusForbidden, `{"error":{"code":"3007","message":"captcha required"}}`)

	res, err := ClaimQuota(context.Background(), testAccount(), "", "", WithOrigin(srv.URL))
	if err != nil {
		t.Fatalf("ClaimQuota: %v", err)
	}
	if !res.CaptchaRequired {
		t.Error("CaptchaRequired = false, want true for code 3007")
	}
	if res.Outcome.Kind != KindCaptchaRequired || res.Outcome.AccountAction != AccountNone || !res.Outcome.Retryable {
		t.Errorf("Outcome = %+v, want captcha_required/NONE/retryable", res.Outcome)
	}
}

// TestClaimQuotaWithVerifyParam pins the retry: the verify parameter from
// the external solver is sent as the captcha header — the only place in the
// gateway where that header is ever set.
func TestClaimQuotaWithVerifyParam(t *testing.T) {
	applyBillingIdentity(t)
	rec := &billingRecorder{}
	srv := newBillingServer(t, rec, http.StatusOK, `{"code":0,"message":"claimed"}`)

	res, err := ClaimQuota(context.Background(), testAccount(), "", "verify-token-abc", WithOrigin(srv.URL))
	if err != nil {
		t.Fatalf("ClaimQuota: %v", err)
	}
	if res.Outcome.Kind != KindOK {
		t.Errorf("Outcome = %+v, want ok", res.Outcome)
	}
	assertChannelAHeaders(t, rec.Header, "verify-token-abc")
}

// TestClaimQuotaNextFlush pins that the claim response may carry the quota
// reset time — parsed into NextFlush.
func TestClaimQuotaNextFlush(t *testing.T) {
	applyBillingIdentity(t)
	srv := newBillingServer(t, &billingRecorder{}, http.StatusOK,
		`{"code":0,"message":"claimed","next_flush_time":"2026-12-01T12:00:00Z"}`)

	res, err := ClaimQuota(context.Background(), testAccount(), "", "", WithOrigin(srv.URL))
	if err != nil {
		t.Fatalf("ClaimQuota: %v", err)
	}
	want := time.Date(2026, 12, 1, 12, 0, 0, 0, time.UTC)
	if !res.NextFlush.Equal(want) {
		t.Errorf("NextFlush = %s, want %s", res.NextFlush, want)
	}
}

// TestSetChannelAOriginWiresBilling pins that SetChannelAOrigin (applied by
// cmd at startup from UPSTREAM_URL) is the default origin of both helpers.
func TestSetChannelAOriginWiresBilling(t *testing.T) {
	applyBillingIdentity(t)
	rec := &billingRecorder{}
	srv := newBillingServer(t, rec, http.StatusOK, `{"remaining_quota":1,"total_quota":2}`)

	SetChannelAOrigin(srv.URL)
	defer SetChannelAOrigin(DefaultChannelAOrigin)

	res, err := QueryBalance(context.Background(), testAccount())
	if err != nil {
		t.Fatalf("QueryBalance: %v", err)
	}
	if rec.Path != PathChannelABalance || res.Balance.RemainingQuota == nil || *res.Balance.RemainingQuota != 1 {
		t.Errorf("default origin not used: recorded path %q, result %+v", rec.Path, res.Balance)
	}
}

// TestBillingCallTransportError pins that a transport failure surfaces as a
// Go error (the caller — pool/claim.go — maps it onto upstream_unavailable).
func TestBillingCallTransportError(t *testing.T) {
	// Port 1 on loopback: connection refused, still no real network.
	_, err := QueryBalance(context.Background(), testAccount(),
		WithOrigin("http://127.0.0.1:1"), WithHTTPClient(&http.Client{Timeout: 2 * time.Second}))
	if err == nil {
		t.Fatal("QueryBalance on a dead origin = nil error, want a transport error")
	}
	_, err = ClaimQuota(context.Background(), testAccount(), "", "",
		WithOrigin("http://127.0.0.1:1"), WithHTTPClient(&http.Client{Timeout: 2 * time.Second}))
	if err == nil {
		t.Fatal("ClaimQuota on a dead origin = nil error, want a transport error")
	}
}

// TestBalancePlausibleWindowGuards pins that implausible plan_expires_at
// values (year outside 2020â€“2200) count as absent instead of poisoning the
// account record.
func TestBalancePlausibleWindowGuards(t *testing.T) {
	applyBillingIdentity(t)
	rec := &billingRecorder{}
	srv := newBillingServer(t, rec, http.StatusOK,
		`{"remaining_quota":10,"total_quota":20,"plan_expires_at":200}`) // 1970-01-01

	res, err := QueryBalance(context.Background(), testAccount(), WithOrigin(srv.URL))
	if err != nil {
		t.Fatalf("QueryBalance: %v", err)
	}
	if !res.Balance.PlanExpiresAt.IsZero() {
		t.Errorf("PlanExpiresAt = %s, want zero for an implausible value", res.Balance.PlanExpiresAt)
	}
	if res.Balance.RemainingQuota == nil || *res.Balance.RemainingQuota != 10 {
		t.Errorf("RemainingQuota = %v, want 10", res.Balance.RemainingQuota)
	}
}

// TestBalanceRejectsUnparseableJSON documents the behavior for a garbage 2xx
// body: no balance fields are produced (the outcome stays ok — the HTTP layer
// succeeded; the caller sees empty quota fields).
func TestBalanceRejectsUnparseableJSON(t *testing.T) {
	applyBillingIdentity(t)
	rec := &billingRecorder{}
	srv := newBillingServer(t, rec, http.StatusOK, "<html>not json</html>")

	res, err := QueryBalance(context.Background(), testAccount(), WithOrigin(srv.URL))
	if err != nil {
		t.Fatalf("QueryBalance: %v", err)
	}
	if res.Outcome.Kind != KindOK {
		t.Errorf("Outcome = %+v, want ok (2xx, no error code)", res.Outcome)
	}
	if res.Balance.RemainingQuota != nil || res.Balance.TotalQuota != nil || !res.Balance.PlanExpiresAt.IsZero() {
		t.Errorf("Balance = %+v, want zero values for an unparseable body", res.Balance)
	}
}

// TestBalanceNeverLogsSecrets guards: the billing helpers
// must not log the JWT or device id. The test captures slog output while a
// call fails and asserts the secret material is absent.
func TestBalanceNeverLogsSecrets(t *testing.T) {
	applyBillingIdentity(t)
	var buf strings.Builder
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	_, _ = QueryBalance(context.Background(), testAccount(),
		WithOrigin("http://127.0.0.1:1"), WithHTTPClient(&http.Client{Timeout: 2 * time.Second}))
	_, _ = ClaimQuota(context.Background(), testAccount(), "", "verify-token-abc",
		WithOrigin("http://127.0.0.1:1"), WithHTTPClient(&http.Client{Timeout: 2 * time.Second}))

	logs := buf.String()
	if strings.Contains(logs, testJWT) {
		t.Error("log output contains the account JWT")
	}
	if strings.Contains(logs, testDeviceMid) {
		t.Error("log output contains the device_mid")
	}
	if strings.Contains(logs, "verify-token-abc") {
		t.Error("log output contains the captcha verify parameter")
	}
}

// TestBalanceJSONShapesCompile documents the accepted payload shapes as JSON
// so a regression in the tolerated forms is obvious (test-time sanity only).
func TestBalanceJSONShapesCompile(t *testing.T) {
	for _, shape := range []string{
		`{"remaining_quota":1,"total_quota":2,"plan_expires_at":1790000000}`,
		`{"code":0,"data":{"remaining_quota":1,"total_quota":2}}`,
		`{"remaining_quota":"15","plan_expires_at":"2026-09-01T08:00:00Z"}`,
	} {
		var v map[string]any
		if err := json.Unmarshal([]byte(shape), &v); err != nil {
			t.Fatalf("fixture %s is not JSON: %v", shape, err)
		}
	}
}
