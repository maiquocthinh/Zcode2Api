package proxy

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"time"

	"zcode2api/internal/pool"
	"zcode2api/internal/upstream"
)

// Anthropic error.type values of the client-facing error table. Every error
// sent to the client uses the Anthropic envelope:
// {"type":"error","error":{"type":...,"message":...}}.
const (
	errTypeAuthentication  = "authentication_error"
	errTypeRequestTooLarge = "request_too_large"
	errTypeNotFound        = "not_found_error"
	errTypeRateLimit       = "rate_limit_error"
	errTypeOverloaded      = "overloaded_error"
	errTypeAPI             = "api_error"
	errTypeInvalidRequest  = "invalid_request_error"
)

// Headers the gateway adds for debugging:
// x-zcode2api-attempts: <n> and x-zcode2api-channel: A|B. The channel header
// names the upstream channel of the attempt that produced (or last touched)
// the response: A = pool JWT account, B = fallback API key. It is set when an
// upstream attempt starts, so committed responses and post-attempt error
// responses both carry it; responses written before any upstream call (client
// auth, body limits, a fully recovering pool) leave it unset.
const headerAttempts = "X-Zcode2api-Attempts"
const headerChannel = "X-Zcode2api-Channel"

// headerRetryAfter is forwarded from upstream and set by the gateway for
// pool-level 429s.
const headerRetryAfter = "Retry-After"

// anthropicErrorBody is the Anthropic error envelope wire format.
type anthropicErrorBody struct {
	Type  string           `json:"type"`
	Error anthropicErrorEl `json:"error"`
}

type anthropicErrorEl struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// WriteError writes an Anthropic-format error response. retryAfter > 0 adds a
// Retry-After header (whole seconds); attempts (>= 0) is advertised in
// x-zcode2api-attempts.
func WriteError(w http.ResponseWriter, status int, errType, message string, retryAfter time.Duration, attempts int) {
	body, err := json.Marshal(anthropicErrorBody{
		Type:  "error",
		Error: anthropicErrorEl{Type: errType, Message: message},
	})
	if err != nil {
		http.Error(w, `{"type":"error","error":{"type":"api_error","message":"internal error"}}`, http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	if retryAfter > 0 {
		h.Set(headerRetryAfter, strconv.Itoa(int(math.Ceil(retryAfter.Seconds()))))
	}
	h.Set(headerAttempts, strconv.Itoa(attempts))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// WriteErrorBody is WriteError with a pre-built error element; used where the
// message is assembled by the caller.
func WriteErrorBody(w http.ResponseWriter, status int, el anthropicErrorEl, retryAfter time.Duration, attempts int) {
	WriteError(w, status, el.Type, el.Message, retryAfter, attempts)
}

// acquireErrorToAnthropic maps a failed pool.Acquire to the client-facing
// error: pool empty / all DISABLED → 503; cooling/exhausted or busy → 429
// (retry-after until the earliest recovery); all tried → 529.
func acquireErrorToAnthropic(e *pool.AcquireError) (status int, el anthropicErrorEl, retryAfter time.Duration) {
	switch e.Failure {
	case pool.AcquireEmpty, pool.AcquireAllDisabled:
		return http.StatusServiceUnavailable,
			anthropicErrorEl{Type: errTypeAPI, Message: "no usable upstream account"}, 0
	case pool.AcquireRecovering:
		return http.StatusTooManyRequests,
			anthropicErrorEl{Type: errTypeRateLimit,
				Message: "all upstream accounts are cooling down or exhausted"}, e.RetryAfter
	case pool.AcquireBusy:
		return http.StatusTooManyRequests,
			anthropicErrorEl{Type: errTypeRateLimit,
				Message: "every upstream account is busy"}, time.Second
	default:
		return statusOverloaded,
			anthropicErrorEl{Type: errTypeOverloaded,
				Message: "upstream is overloaded, all attempts failed"}, 0
	}
}

// statusOverloaded is the Anthropic "overloaded" HTTP status: 529 when
// MAX_ATTEMPTS ran out because upstream was unavailable.
const statusOverloaded = 529

// exhaustedOutcomeToAnthropic maps the last attempt outcome after attempts
// ran out without COMMIT: rate-limit/quota → 429 (retry-after until the last
// failed account recovers); fallback-key drain → 429 "upstream quota
// exhausted"; auth (everything DISABLED) → 503; anything else → 529.
func exhaustedOutcomeToAnthropic(last outcomeResult) (status int, el anthropicErrorEl, retryAfter time.Duration) {
	switch last.outcome.Kind {
	case upstream.KindRateLimit, upstream.KindQuotaExhausted, upstream.KindModelQuotaExhausted, upstream.KindModelBusy:
		return http.StatusTooManyRequests,
			anthropicErrorEl{Type: errTypeRateLimit,
				Message: "all upstream accounts hit their rate or quota limit"}, untilIn(last.outcome.Until)
	case upstream.KindUsageLimit, upstream.KindNoBalance:
		return http.StatusTooManyRequests,
			anthropicErrorEl{Type: errTypeRateLimit,
				Message: "upstream quota exhausted"}, untilIn(last.outcome.Until)
	case upstream.KindAuth:
		return http.StatusServiceUnavailable,
			anthropicErrorEl{Type: errTypeAPI, Message: "no usable upstream account"}, 0
	default:
		return statusOverloaded,
			anthropicErrorEl{Type: errTypeOverloaded,
				Message: "upstream is overloaded, all attempts failed"}, 0
	}
}

// untilIn clamps a recovery time into a positive retry-after duration.
func untilIn(until time.Time) time.Duration {
	if d := time.Until(until); d > 0 {
		return d
	}
	return 0
}
