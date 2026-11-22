package upstream

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestMain silences the default logger so WARN logs emitted by Classify do
// not pollute test output; the logging tests swap in a capturing handler and
// restore the discard handler afterwards.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

// flushEpoch is the next_flush_time value used across the table
// (2026-01-20 05:00:00 UTC, unix 1763638800).
var flushEpoch = time.Unix(1763638800, 0).UTC()

// TestClassifyTable walks the whole Classify table.
// Rows are evaluated top-down, so several cases exist purely to pin the
// priority order between rows and the extraction order error.code > code >
// error.type. The table must keep at least 20 cases and cover: every row
// split into channel A and channel B cases, plain-JSON and SSE bodies, and
// HTTP 200 carrying the channel A codes 1005/1006/3012/3007.
func TestClassifyTable(t *testing.T) {
	soon := time.Now().Add(90 * time.Second).UTC()
	past := time.Now().Add(-time.Hour).UTC()

	tests := []struct {
		name       string
		channel    string // "A", "B" or "A+B": the upstream channel the case belongs to
		status     int
		body       string
		headers    map[string]string // e.g. Retry-After
		nilHeader  bool              // pass a nil http.Header
		wantKind   Kind
		wantAction AccountAction
		wantRetry  bool
		fixedUntil time.Time     // exact match when non-zero
		nearUntil  time.Time     // match within 2s when non-zero
		untilMin   time.Duration // Until - now >= min when > 0
		untilMax   time.Duration // Until - now <= max when > 0
	}{
		// --- Row 1: code 1005 → quota_exhausted / EXHAUSTED until
		// next_flush_time (channel A, any HTTP status) ---
		{
			name:       "A/1005 json on http 200 with unix next_flush_time",
			channel:    "A",
			status:     http.StatusOK,
			body:       `{"error":{"code":"1005","message":"free quota used up, next_flush_time=1763638800"}}`,
			wantKind:   KindQuotaExhausted,
			wantAction: AccountExhausted,
			wantRetry:  true,
			fixedUntil: flushEpoch,
		},
		{
			name:       "A/1005 sse error event on http 200",
			channel:    "A",
			status:     http.StatusOK,
			body:       "event: error\ndata: {\"error\":{\"code\":\"1005\",\"message\":\"quota gone next_flush_time=1763638800\"}}\n\n",
			wantKind:   KindQuotaExhausted,
			wantAction: AccountExhausted,
			wantRetry:  true,
			fixedUntil: flushEpoch,
		},
		{
			name:       "A/1005 on http 429 still quota_exhausted (any status)",
			channel:    "A",
			status:     http.StatusTooManyRequests,
			body:       `{"error":{"code":"1005","next_flush_time":1763638800}}`,
			wantKind:   KindQuotaExhausted,
			wantAction: AccountExhausted,
			wantRetry:  true,
			fixedUntil: flushEpoch,
		},
		{
			name:       "A/1005 next_flush_time in unix milliseconds",
			channel:    "A",
			status:     http.StatusOK,
			body:       `{"error":{"code":"1005","message":"next_flush_time:1763638800000"}}`,
			wantKind:   KindQuotaExhausted,
			wantAction: AccountExhausted,
			wantRetry:  true,
			fixedUntil: flushEpoch,
		},
		{
			name:       "A/1005 iso next_flush_time in message",
			channel:    "A",
			status:     http.StatusOK,
			body:       `{"error":{"code":"1005","message":"resets at next_flush_time: 2026-12-01T12:00:00Z"}}`,
			wantKind:   KindQuotaExhausted,
			wantAction: AccountExhausted,
			wantRetry:  true,
			fixedUntil: time.Date(2026, 12, 1, 12, 0, 0, 0, time.UTC),
		},
		{
			name:       "A/1005 sse with crlf and split data lines",
			channel:    "A",
			status:     http.StatusOK,
			body:       "event: error\r\ndata: {\"error\":{\"code\":\"1005\",\r\ndata: \"message\":\"next_flush_time=1763638800\"}}\r\n\r\n",
			wantKind:   KindQuotaExhausted,
			wantAction: AccountExhausted,
			wantRetry:  true,
			fixedUntil: flushEpoch,
		},
		{
			name:       "A/1005 unparseable next_flush_time falls back to exhausted_fallback",
			channel:    "A",
			status:     http.StatusOK,
			body:       `{"error":{"code":"1005","message":"daily quota exhausted"}}`,
			wantKind:   KindQuotaExhausted,
			wantAction: AccountExhausted,
			wantRetry:  true,
			untilMin:   59 * time.Minute,
			untilMax:   63 * time.Minute,
		},

		// --- Row 2: code 1006 → auth / DISABLED (channel A, any status) ---
		{
			name:       "A/1006 json on http 200 disables the account",
			channel:    "A",
			status:     http.StatusOK,
			body:       `{"error":{"code":"1006","message":"token expired"}}`,
			wantKind:   KindAuth,
			wantAction: AccountDisabled,
			wantRetry:  true,
		},
		{
			name:       "A/1006 sse error event on http 200",
			channel:    "A",
			status:     http.StatusOK,
			body:       "event: error\ndata: {\"error\":{\"code\":\"1006\",\"message\":\"jwt dead\"}}\n\n",
			wantKind:   KindAuth,
			wantAction: AccountDisabled,
			wantRetry:  true,
		},

		// --- Row 3: code 3012 → risk_block / RISK_COOLDOWN (channel A) ---
		{
			name:       "A/3012 on http 200 starts the risk cooldown",
			channel:    "A",
			status:     http.StatusOK,
			body:       `{"error":{"code":"3012","message":"unusual activity"}}`,
			wantKind:   KindRiskBlock,
			wantAction: AccountRiskCooldown,
			wantRetry:  true,
			untilMin:   5*time.Hour + 59*time.Minute,
			untilMax:   6*time.Hour + 1*time.Minute,
		},
		{
			name:       "A/3012 on http 400 starts the risk cooldown",
			channel:    "A",
			status:     http.StatusBadRequest,
			body:       `{"error":{"code":"3012","message":"unusual activity"}}`,
			wantKind:   KindRiskBlock,
			wantAction: AccountRiskCooldown,
			wantRetry:  true,
			untilMin:   5*time.Hour + 59*time.Minute,
			untilMax:   6*time.Hour + 1*time.Minute,
		},
		{
			name:       "A/3012 on http 401 still risk_block (code beats the auth row)",
			channel:    "A",
			status:     http.StatusUnauthorized,
			body:       `{"error":{"code":"3012","message":"unusual activity"}}`,
			wantKind:   KindRiskBlock,
			wantAction: AccountRiskCooldown,
			wantRetry:  true,
			untilMin:   5*time.Hour + 59*time.Minute,
			untilMax:   6*time.Hour + 1*time.Minute,
		},

		// --- Row 4: code 3007 → captcha_required, account untouched ---
		{
			name:       "A/3007 on http 403 requires a captcha and keeps the state",
			channel:    "A",
			status:     http.StatusForbidden,
			body:       `{"error":{"code":"3007","message":"captcha required"}}`,
			wantKind:   KindCaptchaRequired,
			wantAction: AccountNone,
			wantRetry:  true,
		},
		{
			name:       "A/3007 sse on http 200 is captcha_required",
			channel:    "A",
			status:     http.StatusOK,
			body:       "event: error\ndata: {\"error\":{\"code\":\"3007\",\"message\":\"captcha required\"}}\n\n",
			wantKind:   KindCaptchaRequired,
			wantAction: AccountNone,
			wantRetry:  true,
		},

		// --- Row 5: 1302/3002, 429 without code, rate_limit_error →
		// rate_limit / COOLING (both channels) ---
		{
			name:       "AB/1302 json cools with default duration",
			channel:    "A+B",
			status:     http.StatusTooManyRequests,
			body:       `{"error":{"code":"1302","message":"rate limited"}}`,
			wantKind:   KindRateLimit,
			wantAction: AccountCooling,
			wantRetry:  true,
			untilMin:   60 * time.Second,
			untilMax:   70 * time.Second,
		},
		{
			name:       "AB/3002 json is rate_limit",
			channel:    "A+B",
			status:     http.StatusTooManyRequests,
			body:       `{"error":{"code":"3002","message":"busy"}}`,
			wantKind:   KindRateLimit,
			wantAction: AccountCooling,
			wantRetry:  true,
			untilMin:   60 * time.Second,
			untilMax:   70 * time.Second,
		},

		// --- Row 5b: 3008/3009/3010 model admission/concurrency →
		// model_busy, account untouched ---
		{
			name:       "A/3010 json is model_busy on any status",
			channel:    "A",
			status:     http.StatusTooManyRequests,
			body:       `{"error":{"code":"3010","message":"model is busy"}}`,
			wantKind:   KindModelBusy,
			wantAction: AccountNone,
			wantRetry:  true,
		},
		{
			name:       "A/3008 sse error event on http 200 is model_busy",
			channel:    "A",
			status:     http.StatusOK,
			body:       "event: error\ndata: {\"error\":{\"code\":\"3008\",\"message\":\"admission limit\"}}\n\n",
			wantKind:   KindModelBusy,
			wantAction: AccountNone,
			wantRetry:  true,
		},
		{
			name:       "A/3009 json is model_busy",
			channel:    "A",
			status:     http.StatusOK,
			body:       `{"error":{"code":"3009","message":"too many concurrent requests"}}`,
			wantKind:   KindModelBusy,
			wantAction: AccountNone,
			wantRetry:  true,
		},

		// --- Row 5c: 429 quota-banner family (1309-1321, minus 1308/1312) →
		// quota_exhausted / EXHAUSTED until next_flush_time ---
		{
			name:       "A/429 with quota family code 1309 exhausts until next_flush_time",
			channel:    "A",
			status:     http.StatusTooManyRequests,
			body:       `{"error":{"code":"1309","message":"weekly cap, next_flush_time=1763638800"}}`,
			wantKind:   KindModelQuotaExhausted,
			wantAction: AccountExhausted,
			wantRetry:  true,
			fixedUntil: flushEpoch,
		},
		{
			name:       "A/429 with quota family code 1310 on http 200",
			channel:    "A",
			status:     http.StatusOK,
			body:       `{"error":{"code":"1310","message":"quota cap"}}`,
			wantKind:   KindModelQuotaExhausted,
			wantAction: AccountExhausted,
			wantRetry:  true,
			untilMin:   59 * time.Minute,
			untilMax:   63 * time.Minute,
		},
		{
			name:       "A/429 with quota family code 1321 sse error event",
			channel:    "A",
			status:     http.StatusOK,
			body:       "event: error\ndata: {\"error\":{\"code\":\"1321\",\"message\":\"fair use, next_flush_time=1763638800\"}}\n\n",
			wantKind:   KindModelQuotaExhausted,
			wantAction: AccountExhausted,
			wantRetry:  true,
			fixedUntil: flushEpoch,
		},
		{
			name:       "A/1312 stays out of the quota family (interruption marker)",
			channel:    "A",
			status:     http.StatusOK,
			body:       `{"error":{"code":"1312","message":"stream interrupted"}}`,
			wantKind:   KindUnknown,
			wantAction: AccountNone,
			wantRetry:  false,
		},
		{
			name:       "AB/429 without any code is rate_limit",
			channel:    "A+B",
			status:     http.StatusTooManyRequests,
			body:       `{"message":"slow down"}`,
			wantKind:   KindRateLimit,
			wantAction: AccountCooling,
			wantRetry:  true,
			untilMin:   60 * time.Second,
			untilMax:   70 * time.Second,
		},
		{
			name:       "AB/429 with nil header map is rate_limit",
			channel:    "A+B",
			status:     http.StatusTooManyRequests,
			body:       `{}`,
			nilHeader:  true,
			wantKind:   KindRateLimit,
			wantAction: AccountCooling,
			wantRetry:  true,
			untilMin:   60 * time.Second,
			untilMax:   70 * time.Second,
		},
		{
			name:       "AB/429 garbage body is rate_limit",
			channel:    "A+B",
			status:     http.StatusTooManyRequests,
			body:       "<html>too many requests</html>",
			wantKind:   KindRateLimit,
			wantAction: AccountCooling,
			wantRetry:  true,
			untilMin:   60 * time.Second,
			untilMax:   70 * time.Second,
		},
		{
			name:       "AB/anthropic rate_limit_error type is rate_limit",
			channel:    "A+B",
			status:     http.StatusTooManyRequests,
			body:       `{"type":"error","error":{"type":"rate_limit_error","message":"Number of requests too high"}}`,
			wantKind:   KindRateLimit,
			wantAction: AccountCooling,
			wantRetry:  true,
			untilMin:   60 * time.Second,
			untilMax:   70 * time.Second,
		},
		{
			name:       "AB/retry-after seconds honored when larger than cooling",
			channel:    "A+B",
			status:     http.StatusTooManyRequests,
			body:       `{"error":{"code":"1302"}}`,
			headers:    map[string]string{"Retry-After": "120"},
			wantKind:   KindRateLimit,
			wantAction: AccountCooling,
			wantRetry:  true,
			untilMin:   120 * time.Second,
			untilMax:   126 * time.Second,
		},
		{
			name:       "AB/retry-after http-date honored",
			channel:    "A+B",
			status:     http.StatusTooManyRequests,
			body:       `{"error":{"code":"1302"}}`,
			headers:    map[string]string{"Retry-After": soon.Format(http.TimeFormat)},
			wantKind:   KindRateLimit,
			wantAction: AccountCooling,
			wantRetry:  true,
			nearUntil:  soon,
		},
		{
			name:       "AB/retry-after below cooling uses the max rule",
			channel:    "A+B",
			status:     http.StatusTooManyRequests,
			body:       `{"error":{"code":"1302"}}`,
			headers:    map[string]string{"Retry-After": "30"},
			wantKind:   KindRateLimit,
			wantAction: AccountCooling,
			wantRetry:  true,
			untilMin:   60 * time.Second,
			untilMax:   70 * time.Second,
		},
		{
			name:       "AB/unparsable retry-after falls back to cooling",
			channel:    "A+B",
			status:     http.StatusTooManyRequests,
			body:       `{"error":{"code":"1302"}}`,
			headers:    map[string]string{"Retry-After": "in a bit"},
			wantKind:   KindRateLimit,
			wantAction: AccountCooling,
			wantRetry:  true,
			untilMin:   60 * time.Second,
			untilMax:   70 * time.Second,
		},
		{
			name:       "AB/past http-date falls back to cooling",
			channel:    "A+B",
			status:     http.StatusTooManyRequests,
			body:       `{"error":{"code":"1302"}}`,
			headers:    map[string]string{"Retry-After": past.Format(http.TimeFormat)},
			wantKind:   KindRateLimit,
			wantAction: AccountCooling,
			wantRetry:  true,
			untilMin:   60 * time.Second,
			untilMax:   70 * time.Second,
		},

		// --- Row 6: 401/403, 1000–1004, authentication/permission → auth ---
		{
			name:       "AB/401 without code disables the account",
			channel:    "A+B",
			status:     http.StatusUnauthorized,
			body:       `{}`,
			wantKind:   KindAuth,
			wantAction: AccountDisabled,
			wantRetry:  true,
		},
		{
			name:       "AB/403 permission_error type disables the account",
			channel:    "A+B",
			status:     http.StatusForbidden,
			body:       `{"error":{"type":"permission_error","message":"forbidden"}}`,
			wantKind:   KindAuth,
			wantAction: AccountDisabled,
			wantRetry:  true,
		},
		{
			name:       "AB/code 1002 is auth regardless of status",
			channel:    "A+B",
			status:     http.StatusOK,
			body:       `{"error":{"code":"1002"}}`,
			wantKind:   KindAuth,
			wantAction: AccountDisabled,
			wantRetry:  true,
		},
		{
			name:       "AB/root numeric code 1004 is auth",
			channel:    "A+B",
			status:     http.StatusForbidden,
			body:       `{"code":1004}`,
			wantKind:   KindAuth,
			wantAction: AccountDisabled,
			wantRetry:  true,
		},
		{
			name:       "AB/authentication_error type is auth",
			channel:    "A+B",
			status:     http.StatusUnauthorized,
			body:       `{"type":"error","error":{"type":"authentication_error","message":"bad key"}}`,
			wantKind:   KindAuth,
			wantAction: AccountDisabled,
			wantRetry:  true,
		},

		// --- Row 7: code 1308 → usage_limit / EXHAUSTED (channel B) ---
		{
			name:       "B/1308 json exhausts the fallback key until next_flush_time",
			channel:    "B",
			status:     http.StatusTooManyRequests,
			body:       `{"error":{"code":"1308","message":"usage limit reached, next_flush_time=1763638800"}}`,
			wantKind:   KindUsageLimit,
			wantAction: AccountExhausted,
			wantRetry:  true,
			fixedUntil: flushEpoch,
		},
		{
			name:       "B/1308 sse error event on http 200",
			channel:    "B",
			status:     http.StatusOK,
			body:       "event: error\ndata: {\"error\":{\"code\":\"1308\",\"message\":\"limit reached next_flush_time=1763638800\"}}\n\n",
			wantKind:   KindUsageLimit,
			wantAction: AccountExhausted,
			wantRetry:  true,
			fixedUntil: flushEpoch,
		},
		{
			name:       "B/1308 bare sse data payload (ReadFirstEvent form)",
			channel:    "B",
			status:     http.StatusOK,
			body:       `{"error":{"code":"1308","message":"usage limit reached (next_flush_time=1763638800)"}}`,
			wantKind:   KindUsageLimit,
			wantAction: AccountExhausted,
			wantRetry:  true,
			fixedUntil: flushEpoch,
		},
		{
			name:       "B/1308 unparseable next_flush_time falls back to exhausted_fallback",
			channel:    "B",
			status:     http.StatusOK,
			body:       `{"error":{"code":"1308","message":"usage limit reached"}}`,
			wantKind:   KindUsageLimit,
			wantAction: AccountExhausted,
			wantRetry:  true,
			untilMin:   59 * time.Minute,
			untilMax:   63 * time.Minute,
		},

		// --- Row 8: code 1113 → no_balance / DISABLED (channel B) ---
		{
			name:       "B/1113 json disables the fallback key",
			channel:    "B",
			status:     http.StatusBadRequest,
			body:       `{"error":{"code":"1113","message":"no balance"}}`,
			wantKind:   KindNoBalance,
			wantAction: AccountDisabled,
			wantRetry:  true,
		},
		{
			name:       "B/1113 sse on http 200 disables the fallback key",
			channel:    "B",
			status:     http.StatusOK,
			body:       "event: error\ndata: {\"error\":{\"code\":\"1113\",\"message\":\"no balance\"}}\n\n",
			wantKind:   KindNoBalance,
			wantAction: AccountDisabled,
			wantRetry:  true,
		},

		// --- Row 9: 500/502/503/504/408/524 → upstream_unavailable /
		// FAIL_STREAK; 1305/529/overloaded → platform overload / NONE ---
		{
			name:       "AB/500 api_error type is upstream_unavailable",
			channel:    "A+B",
			status:     http.StatusInternalServerError,
			body:       `{"error":{"type":"api_error","message":"boom"}}`,
			wantKind:   KindUpstreamUnavailable,
			wantAction: AccountFailStreak,
			wantRetry:  true,
		},
		{
			name:       "AB/code 1305 beats the 429 rate-limit row as platform overload",
			channel:    "A+B",
			status:     http.StatusTooManyRequests,
			body:       `{"error":{"code":"1305"}}`,
			wantKind:   KindUpstreamOverloaded,
			wantAction: AccountNone,
			wantRetry:  true,
		},
		{
			name:       "AB/503 without code is upstream_unavailable",
			channel:    "A+B",
			status:     http.StatusServiceUnavailable,
			body:       `{"message":"upstream error"}`,
			wantKind:   KindUpstreamUnavailable,
			wantAction: AccountFailStreak,
			wantRetry:  true,
		},
		{
			name:       "AB/200 sse error event with 1305 is platform overload",
			channel:    "A+B",
			status:     http.StatusOK,
			body:       "event: error\ndata: {\"error\":{\"code\":\"1305\",\"message\":\"server busy\"}}\n\n",
			wantKind:   KindUpstreamOverloaded,
			wantAction: AccountNone,
			wantRetry:  true,
		},
		{
			name:       "AB/529 overloaded_error type is platform overload",
			channel:    "A+B",
			status:     529,
			body:       `{"error":{"type":"overloaded_error","message":"overloaded"}}`,
			wantKind:   KindUpstreamOverloaded,
			wantAction: AccountNone,
			wantRetry:  true,
		},

		// --- Row 10: 400/404/413/422, invalid_request/not_found →
		// client_error ---
		{
			name:       "AB/400 invalid_request_error",
			channel:    "A+B",
			status:     http.StatusBadRequest,
			body:       `{"type":"error","error":{"type":"invalid_request_error","message":"bad field"}}`,
			wantKind:   KindClientError,
			wantAction: AccountNone,
			wantRetry:  false,
		},
		{
			name:       "AB/404 not_found_error",
			channel:    "A+B",
			status:     http.StatusNotFound,
			body:       `{"error":{"type":"not_found_error","message":"model not found"}}`,
			wantKind:   KindClientError,
			wantAction: AccountNone,
			wantRetry:  false,
		},
		{
			name:       "AB/413 plain-text body without code",
			channel:    "A+B",
			status:     http.StatusRequestEntityTooLarge,
			body:       "payload too large",
			wantKind:   KindClientError,
			wantAction: AccountNone,
			wantRetry:  false,
		},
		{
			name:       "AB/422 invalid_request_error",
			channel:    "A+B",
			status:     http.StatusUnprocessableEntity,
			body:       `{"error":{"type":"invalid_request_error","message":"unprocessable"}}`,
			wantKind:   KindClientError,
			wantAction: AccountNone,
			wantRetry:  false,
		},

		// --- Row 11: 2xx without an error code → ok ---
		{
			name:       "AB/200 json without code is ok",
			channel:    "A+B",
			status:     http.StatusOK,
			body:       `{"type":"message_start","message":{"id":"msg_1"}}`,
			wantKind:   KindOK,
			wantAction: AccountResetStreak,
			wantRetry:  false,
		},
		{
			name:       "AB/200 sse message_start without code is ok",
			channel:    "A+B",
			status:     http.StatusOK,
			body:       "event: message_start\ndata: {\"type\":\"message_start\"}\n\n",
			wantKind:   KindOK,
			wantAction: AccountResetStreak,
			wantRetry:  false,
		},
		{
			name:       "AB/200 garbage body without code is ok",
			channel:    "A+B",
			status:     http.StatusOK,
			body:       "\x1f\x8bpartial binary",
			wantKind:   KindOK,
			wantAction: AccountResetStreak,
			wantRetry:  false,
		},
		{
			name:       "AB/200 with success code 0 is ok",
			channel:    "A+B",
			status:     http.StatusOK,
			body:       `{"code":0,"message":"success"}`,
			wantKind:   KindOK,
			wantAction: AccountResetStreak,
			wantRetry:  false,
		},
		{
			name:       "AB/200 with success code 200 is ok",
			channel:    "A+B",
			status:     http.StatusOK,
			body:       `{"code":"200"}`,
			wantKind:   KindOK,
			wantAction: AccountResetStreak,
			wantRetry:  false,
		},

		// --- Priority and extraction-order pins ---
		{
			name:       "AB/http 200 with error code 1302 is not ok",
			channel:    "A+B",
			status:     http.StatusOK,
			body:       `{"error":{"code":"1302","message":"rate limited"}}`,
			wantKind:   KindRateLimit,
			wantAction: AccountCooling,
			wantRetry:  true,
			untilMin:   60 * time.Second,
			untilMax:   70 * time.Second,
		},
		{
			name:       "AB/400 with rate_limit_error type takes the earlier row",
			channel:    "A+B",
			status:     http.StatusBadRequest,
			body:       `{"error":{"type":"rate_limit_error","message":"limited"}}`,
			wantKind:   KindRateLimit,
			wantAction: AccountCooling,
			wantRetry:  true,
			untilMin:   60 * time.Second,
			untilMax:   70 * time.Second,
		},
		{
			name:       "AB/error.code beats error.type",
			channel:    "A+B",
			status:     http.StatusTooManyRequests,
			body:       `{"error":{"code":"1302","type":"authentication_error","message":"limited"}}`,
			wantKind:   KindRateLimit,
			wantAction: AccountCooling,
			wantRetry:  true,
			untilMin:   60 * time.Second,
			untilMax:   70 * time.Second,
		},
		{
			name:       "A/error.code beats root code (1005 wins over 1113)",
			channel:    "A",
			status:     http.StatusTooManyRequests,
			body:       `{"code":"1113","error":{"code":"1005","message":"next_flush_time=1763638800"}}`,
			wantKind:   KindQuotaExhausted,
			wantAction: AccountExhausted,
			wantRetry:  true,
			fixedUntil: flushEpoch,
		},
		{
			name:       "AB/root code beats error.type",
			channel:    "A+B",
			status:     http.StatusTooManyRequests,
			body:       `{"code":"1113","error":{"type":"rate_limit_error"}}`,
			wantKind:   KindNoBalance,
			wantAction: AccountDisabled,
			wantRetry:  true,
		},
		{
			name:       "AB/401 with rate-limit code 1302 takes the earlier rate-limit row",
			channel:    "A+B",
			status:     http.StatusUnauthorized,
			body:       `{"error":{"code":"1302","message":"limited"}}`,
			wantKind:   KindRateLimit,
			wantAction: AccountCooling,
			wantRetry:  true,
			untilMin:   60 * time.Second,
			untilMax:   70 * time.Second,
		},
		{
			name:       "B/429 with code 1308 is usage_limit not rate_limit",
			channel:    "B",
			status:     http.StatusTooManyRequests,
			body:       `{"error":{"code":"1308","message":"next_flush_time=1763638800"}}`,
			wantKind:   KindUsageLimit,
			wantAction: AccountExhausted,
			wantRetry:  true,
			fixedUntil: flushEpoch,
		},

		// --- Row 11b: 402/405 with risk wording, 408/524 ---
		{
			name:       "AB/402 is a quota exhaustion of the plan",
			channel:    "A+B",
			status:     http.StatusPaymentRequired,
			body:       `{"error":{"message":"payment required, next_flush_time=1763638800"}}`,
			wantKind:   KindModelQuotaExhausted,
			wantAction: AccountExhausted,
			wantRetry:  true,
			fixedUntil: flushEpoch,
		},
		{
			name:       "A/405 with risk wording starts the risk cooldown",
			channel:    "A",
			status:     http.StatusMethodNotAllowed,
			body:       `<html>request blocked: unusual activity detected</html>`,
			wantKind:   KindRiskBlock,
			wantAction: AccountRiskCooldown,
			wantRetry:  true,
			untilMin:   5*time.Hour + 59*time.Minute,
			untilMax:   6*time.Hour + 1*time.Minute,
		},
		{
			name:       "AB/408 read timeout is upstream_unavailable",
			channel:    "A+B",
			status:     http.StatusRequestTimeout,
			body:       `{"message":"timeout"}`,
			wantKind:   KindUpstreamUnavailable,
			wantAction: AccountFailStreak,
			wantRetry:  true,
		},
		{
			name:       "AB/524 origin timeout is upstream_unavailable",
			channel:    "A+B",
			status:     524,
			body:       `<html>origin timed out</html>`,
			wantKind:   KindUpstreamUnavailable,
			wantAction: AccountFailStreak,
			wantRetry:  true,
		},

		// --- Row 12: anything else → unknown ---
		{
			name:       "AB/405 without risk wording is not in the table",
			channel:    "A+B",
			status:     http.StatusMethodNotAllowed,
			body:       `{}`,
			wantKind:   KindUnknown,
			wantAction: AccountNone,
			wantRetry:  false,
		},
		{
			name:       "AB/200 with unrecognized code is unknown not ok",
			channel:    "A+B",
			status:     http.StatusOK,
			body:       `{"error":{"code":"9999"}}`,
			wantKind:   KindUnknown,
			wantAction: AccountNone,
			wantRetry:  false,
		},
	}

	if len(tests) < 20 {
		t.Fatalf("Classify table must cover at least 20 cases, has %d", len(tests))
	}
	channels := map[string]int{}
	for _, tc := range tests {
		channels[tc.channel]++
	}
	for _, ch := range []string{"A", "B", "A+B"} {
		if channels[ch] == 0 {
			t.Errorf("Classify table has no %s cases", ch)
		}
	}

	for _, tc := range tests {
		t.Run(tc.channel+"/"+tc.name, func(t *testing.T) {
			var h http.Header
			if !tc.nilHeader {
				h = http.Header{}
				for k, v := range tc.headers {
					h.Set(k, v)
				}
			}

			// t0 anchors the window checks: Classify computes Until from its
			// own clock reading, which is never before t0, so Until.Sub(t0)
			// is always >= the configured wait.
			t0 := time.Now()
			out := Classify(tc.status, []byte(tc.body), h)

			if out.Kind != tc.wantKind {
				t.Errorf("Kind = %q, want %q", out.Kind, tc.wantKind)
			}
			if out.AccountAction != tc.wantAction {
				t.Errorf("AccountAction = %q, want %q", out.AccountAction, tc.wantAction)
			}
			if out.Retryable != tc.wantRetry {
				t.Errorf("Retryable = %v, want %v", out.Retryable, tc.wantRetry)
			}
			switch {
			case !tc.fixedUntil.IsZero():
				if !out.Until.Equal(tc.fixedUntil) {
					t.Errorf("Until = %s, want %s (next_flush_time)", out.Until, tc.fixedUntil)
				}
			case !tc.nearUntil.IsZero():
				if d := out.Until.Sub(tc.nearUntil); d < -2*time.Second || d > 2*time.Second {
					t.Errorf("Until = %s, want within 2s of %s (Retry-After HTTP-date)", out.Until, tc.nearUntil)
				}
			case tc.untilMin > 0 || tc.untilMax > 0:
				d := out.Until.Sub(t0)
				if d < tc.untilMin || d > tc.untilMax {
					t.Errorf("Until is %s after t0, want between %s and %s", d, tc.untilMin, tc.untilMax)
				}
			default:
				if !out.Until.IsZero() {
					t.Errorf("Until = %s, want the zero time for this row", out.Until)
				}
			}
		})
	}
}

// TestClassifyTuningOverride pins that SetTuning wires COOLING_DURATION,
// EXHAUSTED_FALLBACK and RISK_COOLDOWN into Classify (later phases apply
// runtime settings).
func TestClassifyTuningOverride(t *testing.T) {
	SetTuning(90*time.Second, 45*time.Minute, 2*time.Hour)
	defer SetTuning(DefaultCoolingDuration, DefaultExhaustedFallback, DefaultRiskCooldown)

	// max(Retry-After 30s, COOLING 90s) = 90s.
	out := Classify(http.StatusTooManyRequests, []byte(`{"message":"slow down"}`), nil)
	if out.Kind != KindRateLimit {
		t.Fatalf("Kind = %q, want rate_limit", out.Kind)
	}
	if d := time.Until(out.Until); d < 89*time.Second || d > 95*time.Second {
		t.Errorf("Until is %s from now, want ~90s (overridden COOLING_DURATION)", d)
	}

	// EXHAUSTED_FALLBACK now 45m (code 1005, channel A).
	out = Classify(http.StatusOK, []byte(`{"error":{"code":"1005","message":"limit reached"}}`), nil)
	if out.Kind != KindQuotaExhausted {
		t.Fatalf("Kind = %q, want quota_exhausted", out.Kind)
	}
	if d := time.Until(out.Until); d < 44*time.Minute || d > 47*time.Minute {
		t.Errorf("Until is %s from now, want ~45m (overridden EXHAUSTED_FALLBACK)", d)
	}

	// RISK_COOLDOWN now 2h (code 3012).
	out = Classify(http.StatusOK, []byte(`{"error":{"code":"3012","message":"unusual activity"}}`), nil)
	if out.Kind != KindRiskBlock || out.AccountAction != AccountRiskCooldown {
		t.Fatalf("Classify(3012) = %+v, want risk_block/RISK_COOLDOWN", out)
	}
	if d := time.Until(out.Until); d < 2*time.Hour-time.Minute || d > 2*time.Hour+time.Minute {
		t.Errorf("Until is %s from now, want ~2h (overridden RISK_COOLDOWN)", d)
	}
}

// TestClassifyTransportError pins the row-9 treatment of transport failures
// before COMMIT.
func TestClassifyTransportError(t *testing.T) {
	out := ClassifyTransportError(context.DeadlineExceeded)
	if out.Kind != KindUpstreamUnavailable || out.AccountAction != AccountFailStreak || !out.Retryable {
		t.Fatalf("ClassifyTransportError = %+v, want upstream_unavailable/FAIL_STREAK/retryable", out)
	}
	if !out.Until.IsZero() {
		t.Errorf("Until = %s, want zero", out.Until)
	}
}

// TestClassifyLogsUnknownBodyPrefix pins the WARN log with the first 256 body
// bytes for unknown responses.
func TestClassifyLogsUnknownBodyPrefix(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	body := bytes.Repeat([]byte("x"), 300)
	out := Classify(http.StatusMethodNotAllowed, body, nil)
	if out.Kind != KindUnknown {
		t.Fatalf("Kind = %q, want unknown", out.Kind)
	}

	logs := buf.String()
	if !strings.Contains(logs, "level=WARN") {
		t.Errorf("log level is not WARN: %s", logs)
	}
	if !strings.Contains(logs, "status=405") {
		t.Errorf("log does not carry the status: %s", logs)
	}
	if !strings.Contains(logs, "body_prefix="+strings.Repeat("x", 256)) {
		t.Errorf("log does not carry the first 256 body bytes: %s", logs)
	}
	if strings.Contains(logs, strings.Repeat("x", 257)) {
		t.Errorf("log carries more than 256 body bytes")
	}
}

// TestClassifyLogsNoBalanceWarning pins the WARN that tells the operator to
// check the key type on 1113.
func TestClassifyLogsNoBalanceWarning(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	out := Classify(http.StatusBadRequest, []byte(`{"error":{"code":"1113"}}`), nil)
	if out.Kind != KindNoBalance || out.AccountAction != AccountDisabled {
		t.Fatalf("Classify(1113) = %+v, want no_balance/DISABLED", out)
	}
	logs := buf.String()
	if !strings.Contains(logs, "level=WARN") || !strings.Contains(logs, "1113") {
		t.Errorf("no WARN about checking the key type: %s", logs)
	}
}

// TestClassifyLogsTokenDeadWarning pins the WARN on code 1006 (channel A):
// a dead JWT disables the account loudly.
func TestClassifyLogsTokenDeadWarning(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	out := Classify(http.StatusOK, []byte(`{"error":{"code":"1006","message":"token expired"}}`), nil)
	if out.Kind != KindAuth || out.AccountAction != AccountDisabled {
		t.Fatalf("Classify(1006) = %+v, want auth/DISABLED", out)
	}
	logs := buf.String()
	if !strings.Contains(logs, "level=WARN") || !strings.Contains(logs, "1006") {
		t.Errorf("no WARN about the dead JWT: %s", logs)
	}
}

// TestClassifyConcurrentWithTuning exercises Classify and SetTuning from many
// goroutines under -race.
func TestClassifyConcurrentWithTuning(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = Classify(http.StatusTooManyRequests, []byte(`{"error":{"code":"1302"}}`), nil)
				_ = Classify(http.StatusOK, []byte(`{"error":{"code":"1005","message":"next_flush_time=1763638800"}}`), nil)
				_ = Classify(http.StatusOK, []byte(`{"error":{"code":"3012","message":"unusual activity"}}`), nil)
				_ = Classify(http.StatusOK, []byte(`{}`), nil)
			}
		}()
	}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			SetTuning(61*time.Second, 61*time.Minute, 7*time.Hour)
			SetTuning(DefaultCoolingDuration, DefaultExhaustedFallback, DefaultRiskCooldown)
		}()
	}
	wg.Wait()

	if got := currentTuning(); got.cooling != DefaultCoolingDuration ||
		got.exhaustedFallback != DefaultExhaustedFallback ||
		got.riskCooldown != DefaultRiskCooldown {
		t.Fatalf("tuning not restored: %+v", got)
	}
}
