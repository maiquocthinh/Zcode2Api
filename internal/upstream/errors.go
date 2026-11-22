package upstream

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Kind classifies an upstream response. The business codes are globally
// distinct across both upstream channels, so one parser serves both:
// Classify needs no channel argument.
type Kind string

const (
	KindOK             Kind = "ok"
	KindQuotaExhausted Kind = "quota_exhausted"
	// KindModelQuotaExhausted is the weekly/monthly/fair-use quota family
	// (1309–1321) and HTTP 402 — a real per-model exhaustion, kept apart from
	// the 1005 kind because only 1005 can be the transient minute-level limit.
	KindModelQuotaExhausted Kind = "model_quota_exhausted"
	KindRateLimit           Kind = "rate_limit"
	KindModelBusy           Kind = "model_busy"
	KindAuth                Kind = "auth"
	KindRiskBlock           Kind = "risk_block"
	KindCaptchaRequired     Kind = "captcha_required"
	KindUsageLimit          Kind = "usage_limit"
	KindNoBalance           Kind = "no_balance"
	KindUpstreamUnavailable Kind = "upstream_unavailable"
	KindUpstreamOverloaded  Kind = "upstream_overloaded"
	KindClientError         Kind = "client_error"
	KindUnknown             Kind = "unknown"

	// Claim-only kinds, set by claimScopedOutcome: the /billing/claim
	// endpoint answers about the PLAN, not about the credential, so the
	// shared-table auth/quota rows must not touch the account on them. They
	// never reach the client-facing error tables.
	KindClaimLimit    Kind = "claim_limit"    // daily claim limit used up (claim 1005)
	KindClaimBusiness Kind = "claim_business" // plan missing/expired/not eligible (1001/1002/1004)
	KindClaimAlready  Kind = "claim_already"  // plan already claimed (1003): idempotent, not a new claim
)

// AccountAction tells the pool what to do with the account (or the fallback
// key) that produced the response. The pool maps RISK_COOLDOWN onto its
// COOLING state with Outcome.Until (the pool state machine has exactly four
// states).
type AccountAction string

const (
	// AccountNone keeps the account in its current state.
	AccountNone AccountAction = "NONE"
	// AccountResetStreak clears the account's consecutive-failure counter.
	AccountResetStreak AccountAction = "RESET_STREAK"
	// AccountFailStreak increments the account's consecutive-failure counter.
	// The account is not at fault, so its state does not change; the pool puts
	// it into COOLING for 30s once the counter reaches FAIL_THRESHOLD.
	AccountFailStreak AccountAction = "FAIL_STREAK"
	// AccountCooling puts the account into COOLING until Outcome.Until.
	AccountCooling AccountAction = "COOLING"
	// AccountExhausted puts the account into EXHAUSTED until Outcome.Until.
	AccountExhausted AccountAction = "EXHAUSTED"
	// AccountDisabled disables the account until an operator re-enables it.
	AccountDisabled AccountAction = "DISABLED"
	// AccountRiskCooldown isolates the account until Outcome.Until after a
	// 3012 WAF block (RISK_COOLDOWN, default 6h).
	AccountRiskCooldown AccountAction = "RISK_COOLDOWN"
)

// Outcome is the result of Classify.
type Outcome struct {
	Kind          Kind
	AccountAction AccountAction
	Retryable     bool
	// Until is when the account becomes usable again (COOLING, EXHAUSTED,
	// RISK_COOLDOWN); it is the zero time for every other action.
	Until time.Time
}

// Business codes of the Z.AI error format, in one place so a future code
// change from Z.AI only touches this file.
const (
	// Channel A (free JWT account pool).
	codeQuotaExhausted = "1005"
	codeTokenDead      = "1006"
	codeRiskBlock      = "3012"
	codeCaptcha        = "3007"

	// Rate-limit family (both channels). 3010 lives in the model-busy family
	// below, not here.
	codeRateLimit     = "1302"
	codeRateLimitAlt2 = "3002"

	// Model-busy family: the admission/concurrency limit of ONE model — the
	// request is rejected, the account stays usable and the official client
	// replays after a short pause.
	codeModelBusy     = "3010"
	codeModelBusyAlt1 = "3008"
	codeModelBusyAlt2 = "3009"

	// Channel B (fallback API key).
	codeUsageLimit = "1308"
	codeNoBalance  = "1113"

	// Both channels.
	codeOverloaded = "1305"
)

// quotaFamilyCodes are the 429 business codes of the weekly/monthly/fair-use
// quota banners for one model — the official client's 1308–1321 banner
// family, minus 1308 (own row) and 1312 (reserved as the mid-stream
// interruption marker). A code from this family means the MODEL is exhausted
// on this account: the request must fail over to another account.
var quotaFamilyCodes = map[string]bool{
	"1309": true, "1310": true, "1311": true,
	"1313": true, "1314": true, "1315": true,
	"1316": true, "1317": true, "1318": true,
	"1319": true, "1320": true, "1321": true,
}

// Success codes some Z.AI endpoints carry on 2xx responses: "code 0/200" is
// not an error code, so such a 2xx response counts as OK.
var cleanCodes = map[string]bool{"": true, "0": true, "200": true}

// Anthropic error.type values.
const (
	typeRateLimit      = "rate_limit_error"
	typeOverloaded     = "overloaded_error"
	typeInvalidRequest = "invalid_request_error"
	typeNotFound       = "not_found_error"
	typeAuthentication = "authentication_error"
	typePermission     = "permission_error"
)

// Auth codes 1000–1004 of the Z.AI error format.
const (
	authCodeMin = 1000
	authCodeMax = 1004
)

// platformOverloadStatus is the Anthropic "overloaded" HTTP status — the
// platform-wide overload row (1305/529/overloaded_error), which is about the
// whole platform, never one account.
const platformOverloadStatus = 529

// HTTP statuses referenced by the table. Only these exact statuses match;
// any other status falls through to the row that does list it, or to unknown.
var (
	upstreamUnavailableStatuses = map[int]bool{
		http.StatusInternalServerError: true,
		http.StatusRequestTimeout:      true, // 408: upstream read timeout
		http.StatusBadGateway:          true,
		http.StatusServiceUnavailable:  true,
		http.StatusGatewayTimeout:      true,
		524:                            true, // Cloudflare origin timeout
	}
	clientErrorStatuses = map[int]bool{
		http.StatusBadRequest:            true,
		http.StatusNotFound:              true,
		http.StatusRequestEntityTooLarge: true,
		http.StatusUnprocessableEntity:   true,
	}
)

// Tuning knobs Classify needs from configuration:
// COOLING_DURATION, EXHAUSTED_FALLBACK and RISK_COOLDOWN. They sit behind an
// atomic pointer so runtime settings changes apply without a restart.
// Nothing here is read from the environment directly.
type tuning struct {
	cooling           time.Duration
	exhaustedFallback time.Duration
	riskCooldown      time.Duration
}

// Classify duration defaults.
const (
	DefaultCoolingDuration   = 60 * time.Second
	DefaultExhaustedFallback = time.Hour
	DefaultRiskCooldown      = 6 * time.Hour
)

var tuningState atomic.Pointer[tuning]

func init() {
	tuningState.Store(&tuning{
		cooling:           DefaultCoolingDuration,
		exhaustedFallback: DefaultExhaustedFallback,
		riskCooldown:      DefaultRiskCooldown,
	})
}

// SetTuning updates the durations Classify uses. Non-positive values keep the
// previous setting. Safe for concurrent use.
func SetTuning(cooling, exhaustedFallback, riskCooldown time.Duration) {
	prev := tuningState.Load()
	next := &tuning{cooling: prev.cooling, exhaustedFallback: prev.exhaustedFallback, riskCooldown: prev.riskCooldown}
	if cooling > 0 {
		next.cooling = cooling
	}
	if exhaustedFallback > 0 {
		next.exhaustedFallback = exhaustedFallback
	}
	if riskCooldown > 0 {
		next.riskCooldown = riskCooldown
	}
	tuningState.Store(next)
}

func currentTuning() *tuning { return tuningState.Load() }

// unknownBodyLogBytes caps the body excerpt logged for unknown responses.
// Only upstream *error* bodies are logged, never
// prompts, responses, or secrets.
const unknownBodyLogBytes = 256

// The business code is extracted in priority order (first value found wins):
// error.code, then the root-level code field, then error.type (Anthropic
// format). h may be nil.
func Classify(status int, body []byte, h http.Header) Outcome {
	code := extractCode(body)
	now := time.Now()

	switch {
	case code == codeQuotaExhausted:
		return exhaustedOutcome(KindQuotaExhausted, body, now)

	case code == codeTokenDead:
		slog.Warn("upstream account JWT rejected with code 1006; disabling the account",
			"status", status)
		return Outcome{Kind: KindAuth, AccountAction: AccountDisabled, Retryable: true}

	case code == codeRiskBlock:
		return Outcome{
			Kind:          KindRiskBlock,
			AccountAction: AccountRiskCooldown,
			Retryable:     true,
			Until:         now.Add(currentTuning().riskCooldown),
		}

	case code == codeCaptcha:
		return Outcome{Kind: KindCaptchaRequired, AccountAction: AccountNone, Retryable: true}

	case quotaFamilyCodes[code]:
		// The model's weekly/monthly/fair-use quota ran out on this account:
		// rest until the reported reset, fail over to another account now.
		return exhaustedOutcome(KindModelQuotaExhausted, body, now)

	case code == codeModelBusy || code == codeModelBusyAlt1 || code == codeModelBusyAlt2:
		// Model admission/concurrency limit: the account is fine, the caller
		// replays on the same account after a short pause.
		return Outcome{Kind: KindModelBusy, AccountAction: AccountNone, Retryable: true}

	case code == codeRateLimit || code == codeRateLimitAlt2 ||
		(status == http.StatusTooManyRequests && code == "") || code == typeRateLimit:
		return rateLimitOutcome(h, now)

	case status == http.StatusUnauthorized || status == http.StatusForbidden ||
		inAuthCodeRange(code) || code == typeAuthentication || code == typePermission:
		return Outcome{Kind: KindAuth, AccountAction: AccountDisabled, Retryable: true}

	case code == codeUsageLimit:
		return exhaustedOutcome(KindUsageLimit, body, now)

	case code == codeNoBalance:
		slog.Warn("upstream fallback key returned 1113 (no balance): check the subscription type behind this key")
		return Outcome{Kind: KindNoBalance, AccountAction: AccountDisabled, Retryable: true}

	case status == http.StatusPaymentRequired:
		// 402: the plan behind this account ran out of credit — the same
		// treatment as a quota exhaustion (rest, then retry).
		return exhaustedOutcome(KindModelQuotaExhausted, body, now)

	case status == http.StatusMethodNotAllowed && riskControlText(body):
		// 405 carrying risk wording is the origin WAF blocking the account —
		// the same isolation as 3012. A 405 without risk wording stays
		// unknown: it may be this gateway's own malformed request, which
		// every account would fail alike and retrying would only burn
		// attempts.
		return Outcome{
			Kind:          KindRiskBlock,
			AccountAction: AccountRiskCooldown,
			Retryable:     true,
			Until:         now.Add(currentTuning().riskCooldown),
		}

	case code == codeOverloaded || status == platformOverloadStatus || code == typeOverloaded:
		// Platform-wide overload: the official definition says it has nothing
		// to do with the single account — mark nothing, replay after a short
		// pause. Switching accounts on an overloaded platform only amplifies
		// the traffic, and a fail-streak here would kick every healthy
		// account out of the pool in one bad minute.
		return Outcome{Kind: KindUpstreamOverloaded, AccountAction: AccountNone, Retryable: true}

	case upstreamUnavailableStatuses[status]:
		// Per-egress/per-account upstream failures: the account only takes a
		// failure streak (short COOLING at FAIL_THRESHOLD).
		return Outcome{Kind: KindUpstreamUnavailable, AccountAction: AccountFailStreak, Retryable: true}

	case clientErrorStatuses[status] || code == typeInvalidRequest || code == typeNotFound:
		return Outcome{Kind: KindClientError, AccountAction: AccountNone, Retryable: false}

	case status >= 200 && status < 300 && cleanCodes[code]:
		return Outcome{Kind: KindOK, AccountAction: AccountResetStreak, Retryable: false}

	default:
		slog.Warn("upstream response not recognized by Classify; treating as unknown",
			"status", status, "code", code, "body_prefix", bodyPrefix(body, unknownBodyLogBytes))
		return Outcome{Kind: KindUnknown, AccountAction: AccountNone, Retryable: false}
	}
}

// ClassifyTransportError classifies a transport-level failure — connection
// error, TLS failure, or the first-event timeout — that happened before
// COMMIT. The account is not at fault: the pool only bumps its failure
// streak. err is carried for the caller's own logging.
func ClassifyTransportError(err error) Outcome {
	return Outcome{Kind: KindUpstreamUnavailable, AccountAction: AccountFailStreak, Retryable: true}
}

// exhaustedOutcome builds the EXHAUSTED outcome shared by 1005 (channel A),
// the quota family/402 and 1308 (channel B): the account rests until the
// next_flush_time parsed from the body, or until now + EXHAUSTED_FALLBACK when
// nothing parses.
func exhaustedOutcome(kind Kind, body []byte, now time.Time) Outcome {
	until, ok := parseNextFlush(body)
	if !ok {
		until = now.Add(currentTuning().exhaustedFallback)
	}
	return Outcome{Kind: kind, AccountAction: AccountExhausted, Retryable: true, Until: until}
}

// maxRetryAfter caps a parsed Retry-After so an anomalous value from upstream
// or an intermediate proxy cannot freeze an account for hours. The configured
// COOLING_DURATION floor still applies after the cap, so raising
// COOLING_DURATION stays effective.
const maxRetryAfter = 120 * time.Second

func rateLimitOutcome(h http.Header, now time.Time) Outcome {
	wait := retryAfter(h, now)
	if wait > maxRetryAfter {
		wait = maxRetryAfter
	}
	if wait < currentTuning().cooling {
		wait = currentTuning().cooling
	}
	return Outcome{Kind: KindRateLimit, AccountAction: AccountCooling, Retryable: true, Until: now.Add(wait)}
}

// retryAfter parses the Retry-After header in both forms allowed by HTTP:
// delay-seconds ("120") and HTTP-date ("Wed, 25 Nov 2026 08:00:00 GMT").
// Absent, negative, or unparsable values yield 0; the caller then falls back
// to the configured COOLING_DURATION.
func retryAfter(h http.Header, now time.Time) time.Duration {
	if h == nil {
		return 0
	}
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
		return 0
	}
	return 0
}

// inAuthCodeRange reports whether code is one of the Z.AI auth codes
// 1000–1004.
func inAuthCodeRange(code string) bool {
	n, err := strconv.Atoi(code)
	return err == nil && n >= authCodeMin && n <= authCodeMax
}

func bodyPrefix(body []byte, n int) string {
	if len(body) > n {
		return string(body[:n])
	}
	return string(body)
}

// riskControlRe matches the wording a WAF/risk-control block page carries.
// It gates the HTTP 405 row: only a 405 that reads like a block counts
// against the account.
var riskControlRe = regexp.MustCompile(`(?i)unusual activity|risk control|blocked`)

func riskControlText(body []byte) bool {
	return riskControlRe.Match(body)
}

// extractCode pulls the business code out of the body in the priority order
// of: error.code, root-level code, error.type. It returns "" when
// nothing usable is found (including for bodies that are not JSON at all).
func extractCode(body []byte) string {
	payload := decodeBody(body)
	if len(payload) == 0 {
		return ""
	}
	var parsed struct {
		Code  any `json:"code"`
		Error *struct {
			Code any    `json:"code"`
			Type string `json:"type"`
		} `json:"error"`
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&parsed); err != nil {
		return ""
	}
	if parsed.Error != nil {
		if s := coerceCode(parsed.Error.Code); s != "" {
			return s
		}
	}
	if s := coerceCode(parsed.Code); s != "" {
		return s
	}
	if parsed.Error != nil {
		if t := strings.TrimSpace(parsed.Error.Type); t != "" {
			return t
		}
	}
	return ""
}

// coerceCode normalizes a JSON value that may carry the business code (string
// or number) into its string form; everything else counts as absent.
func coerceCode(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(x)
	case json.Number:
		return x.String()
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	default:
		return ""
	}
}

// decodeBody accepts a plain JSON document or the text of an SSE event block
// ("event: error\ndata: {...}") and returns the JSON payload to parse.
func decodeBody(body []byte) []byte {
	b := bytes.TrimSpace(body)
	if len(b) == 0 || b[0] == '{' || b[0] == '[' {
		return b
	}
	return sseData(b)
}

// sseData extracts the data lines of an SSE event block. Multiple data lines
// of one event are joined with "\n" (standard SSE framing; Anthropic error
// events carry their JSON payload there). CRLF line endings are tolerated.
func sseData(b []byte) []byte {
	var parts [][]byte
	for _, line := range bytes.Split(b, []byte{'\n'}) {
		line = bytes.TrimSuffix(line, []byte{'\r'})
		if rest, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			if v := bytes.TrimSpace(rest); len(v) > 0 {
				parts = append(parts, v)
			}
		}
	}
	return bytes.Join(parts, []byte{'\n'})
}

// next_flush_time parsing. Z.AI reports the
// quota reset time alongside the 1005 (channel A) and 1308 (channel B) codes.
// The exact format may change, so the parser
// is best-effort over the whole body: unix seconds, unix milliseconds, or a
// calendar date following the key. When nothing parses, the caller falls back
// to EXHAUSTED_FALLBACK.
var (
	nextFlushNumRe  = regexp.MustCompile(`(?i)next_flush_time\D{0,12}(\d{9,14})`)
	nextFlushDateRe = regexp.MustCompile(`(?i)next_flush_time\D{0,12}(\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?(?:[Zz]|[+-]\d{2}:?\d{2})?)`)
)

// nextFlushLayouts are the calendar formats accepted after the
// next_flush_time key.
var nextFlushLayouts = []string{
	time.RFC3339,
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04Z07:00",
	"2006-01-02T15:04",
	"2006-01-02 15:04:05Z07:00",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05",
}

// Sanity window for parsed reset times: anything outside it is treated as a
// misparse (e.g. a compact datetime read as unix milliseconds) so the caller
// falls back to EXHAUSTED_FALLBACK instead of freezing an account for
// centuries.
const (
	minPlausibleResetYear = 2020
	maxPlausibleResetYear = 2200
)

// parseNextFlush scans body for next_flush_time and returns the parsed time.
// It serves Classify (the EXHAUSTED rows) and the balance/claim responses.
func parseNextFlush(body []byte) (time.Time, bool) {
	if m := nextFlushNumRe.FindSubmatch(body); m != nil {
		if v, err := strconv.ParseInt(string(m[1]), 10, 64); err == nil {
			if t, ok := nextFlushFromNumber(v); ok {
				return t, true
			}
		}
	}
	if m := nextFlushDateRe.FindSubmatch(body); m != nil {
		raw := string(m[1])
		for _, layout := range nextFlushLayouts {
			if t, err := time.Parse(layout, raw); err == nil && plausibleReset(t) {
				return t, true
			}
		}
	}
	return time.Time{}, false
}

// nextFlushFromNumber interprets a numeric next_flush_time value: values from
// 1e12 on are unix milliseconds, smaller values unix seconds. Implausible
// timestamps are rejected so the caller uses the fallback.
func nextFlushFromNumber(v int64) (time.Time, bool) {
	var t time.Time
	if v >= 1_000_000_000_000 {
		t = time.UnixMilli(v)
	} else {
		t = time.Unix(v, 0)
	}
	if !plausibleReset(t) {
		return time.Time{}, false
	}
	return t, true
}

func plausibleReset(t time.Time) bool {
	y := t.Year()
	return y >= minPlausibleResetYear && y <= maxPlausibleResetYear
}
