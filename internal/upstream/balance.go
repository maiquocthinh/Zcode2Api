package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// balanceCallTimeout bounds the control-plane calls of this file. Unlike the
// streaming data plane, a balance query
// or claim is a short JSON call, so a timeout is safe here — the same
// reasoning as the admin key probe.
const balanceCallTimeout = 30 * time.Second

// balanceBodyLimit caps how much of a billing response is read.
const balanceBodyLimit = 1 << 16

// ChannelAAccount is the per-account material the channel A billing helpers
// need: the free-plan JWT and the stable device id generated at import.
type ChannelAAccount struct {
	JWT       string
	DeviceMid string
}

// CallOption customizes one billing call. Tests point the helpers at an
// httptest.Server with WithOrigin and may inject a client (e.g. one built on
// a proxied TransportFor) with WithHTTPClient; production callers use the
// defaults (ChannelAOrigin, direct-egress transport).
type CallOption func(*callConfig)

type callConfig struct {
	origin string
	client *http.Client
	region string // captcha verify region, sent only with a verify param
}

// WithOrigin overrides the channel A origin for one call.
func WithOrigin(origin string) CallOption {
	return func(c *callConfig) {
		if origin != "" {
			c.origin = origin
		}
	}
}

// WithRegion sets the captcha verify region of one claim call — the region
// the verify param was minted with. The header is omitted when empty.
func WithRegion(region string) CallOption {
	return func(c *callConfig) {
		c.region = region
	}
}

// WithHTTPClient overrides the HTTP client used for one call.
func WithHTTPClient(client *http.Client) CallOption {
	return func(c *callConfig) {
		if client != nil {
			c.client = client
		}
	}
}

func callConfigFor(opts []CallOption) *callConfig {
	c := &callConfig{
		origin: ChannelAOrigin(),
		client: &http.Client{Transport: TransportFor("")},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// Balance is the parsed /billing/balance payload (the admin balance endpoint
// updates remaining_quota, total_quota and plan_expires_at).
// Nil quota fields mean the response did not carry them. ResetAt is the quota
// bucket's expires_at — the instant upstream refills the free quota; a
// balance-confirmed reset is an EXHAUSTED exit path.
type Balance struct {
	RemainingQuota *int64
	TotalQuota     *int64
	PlanExpiresAt  time.Time // zero when absent or implausible
	ResetAt        time.Time // balances[].expires_at; zero when absent
}

// BalanceResult pairs the parsed balance with the Classify outcome of the
// response and any next_flush_time the body carries (the upstream reports
// the quota reset time through claim/balance).
type BalanceResult struct {
	Outcome   Outcome
	Balance   Balance
	NextFlush time.Time // zero when the response carries no parsable next_flush_time
}

// QueryBalance calls GET {UPSTREAM_URL}/api/v1/zcode-plan/billing/balance?app_version=<ZCODE_APP_VERSION>
// with the account JWT and the ZCode source headers. The response goes
// through Classify, so a 401 or a body code
// of 1006 yields Outcome{Kind: auth, AccountAction: DISABLED} instead of a
// generic error. A transport failure returns a nil result and a non-nil error.
func QueryBalance(ctx context.Context, acc ChannelAAccount, opts ...CallOption) (*BalanceResult, error) {
	cfg := callConfigFor(opts)
	appVersion, _ := ChannelAIdentity()

	endpoint := joinEndpoint(cfg.origin, PathChannelABalance) +
		"?app_version=" + url.QueryEscape(appVersion)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("upstream: build balance request: %w", err)
	}
	applyChannelAHeaders(httpReq.Header, acc.JWT, acc.DeviceMid)

	body, resp, err := doBillingCall(ctx, cfg, httpReq)
	if err != nil {
		return nil, err
	}
	out := Classify(resp.StatusCode, body, resp.Header)

	res := &BalanceResult{Outcome: out}
	if t, ok := parseNextFlush(body); ok {
		res.NextFlush = t
	}
	if out.Kind == KindOK {
		res.Balance = parseBalance(body)
	}
	return res, nil
}

// ClaimResult is the outcome of one daily claim call.
type ClaimResult struct {
	Outcome         Outcome
	CaptchaRequired bool      // upstream answered with code 3007: the account must solve a captcha
	NextFlush       time.Time // next_flush_time from the response, zero when absent
	// NextClaimAt is data.plan.ends_at of the claim response — the next-claim
	// instant upstream itself computed (present on success and on the 1005
	// daily-limit answer); zero when the body carries none.
	NextClaimAt time.Time
}

// ClaimQuota calls POST {UPSTREAM_URL}/api/v1/zcode-plan/billing/claim with
// the account JWT and the ZCode source headers. planID is
// the entitlement to claim, sent as the {"plan_id":...} body field; an
// empty planID keeps the empty-object body.

// verifyParam is the captcha verify parameter obtained from the solver:
// a non-empty value sets the param header, and the region header goes with
// it when the caller supplies the param's mint region via WithRegion (the
// upstream sends the pair). A non-empty verifyParam is the ONLY
// situation in the gateway where the captcha header is sent — the chat
// builders never set it.
func ClaimQuota(ctx context.Context, acc ChannelAAccount, planID, verifyParam string, opts ...CallOption) (*ClaimResult, error) {
	cfg := callConfigFor(opts)

	body := []byte("{}")
	if planID != "" {
		encoded, err := json.Marshal(map[string]string{"plan_id": planID})
		if err != nil {
			return nil, fmt.Errorf("upstream: encode claim body: %w", err)
		}
		body = encoded
	}
	httpReq, err := newUpstreamRequest(ctx, joinEndpoint(cfg.origin, PathChannelAClaim), body)
	if err != nil {
		return nil, fmt.Errorf("upstream: build claim request: %w", err)
	}
	applyChannelAHeaders(httpReq.Header, acc.JWT, acc.DeviceMid)
	if verifyParam != "" {
		httpReq.Header.Set(captchaHeader, verifyParam)
		if cfg.region != "" {
			httpReq.Header.Set(captchaRegionHeader, cfg.region)
		}
	}

	b, resp, err := doBillingCall(ctx, cfg, httpReq)
	if err != nil {
		return nil, err
	}
	out := claimScopedOutcome(Classify(resp.StatusCode, b, resp.Header), b)

	res := &ClaimResult{Outcome: out, CaptchaRequired: out.Kind == KindCaptchaRequired}
	if t, ok := parseNextFlush(b); ok {
		res.NextFlush = t
	}
	if t, ok := parseClaimNextAt(b); ok {
		res.NextClaimAt = t
	}
	return res, nil
}

// codeClaimAlready is the claim business code for "this plan was already
// claimed" — idempotent for the claim flow, never counted as a fresh claim.
const codeClaimAlready = "1003"

// claimScopedOutcome rewrites the shared-table outcome for the claim
// endpoint's business answers. Chat treats the 1000–1004 family as a dead
// credential (DISABLED) and 1005 as chat-quota exhaustion (EXHAUSTED); on
// /billing/claim they answer about the PLAN, not the account:
//   - 1003 — already claimed: idempotent, not an error, but not a new claim;
//   - 1005 — the daily claim limit is used up: report only, never bench chat;
//   - 1001/1002/1004 — plan missing/expired/not eligible: failed claim, no
//     state change (the scheduler simply retries tomorrow).
//
// Every other outcome passes through unchanged, so a dead JWT (1006) or a
// WAF block (3012) during claim still reaches the pool state machine.
func claimScopedOutcome(out Outcome, body []byte) Outcome {
	switch extractCode(body) {
	case codeClaimAlready:
		return Outcome{Kind: KindClaimAlready, AccountAction: AccountResetStreak}
	case codeQuotaExhausted: // 1005 on the claim endpoint
		return Outcome{Kind: KindClaimLimit, AccountAction: AccountNone}
	case "1001", "1002", "1004":
		return Outcome{Kind: KindClaimBusiness, AccountAction: AccountNone}
	}
	return out
}

// parseClaimNextAt extracts data.plan.ends_at from a claim response body
// (top-level "plan" accepted as a fallback): unix seconds/milliseconds or an
// RFC3339 string, implausible dates reported as absent.
func parseClaimNextAt(body []byte) (time.Time, bool) {
	payload := decodeBody(body)
	if len(payload) == 0 {
		return time.Time{}, false
	}
	var doc struct {
		Data *struct {
			Plan *struct {
				EndsAt json.RawMessage `json:"ends_at"`
			} `json:"plan"`
		} `json:"data"`
		Plan *struct {
			EndsAt json.RawMessage `json:"ends_at"`
		} `json:"plan"`
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return time.Time{}, false
	}
	var raw json.RawMessage
	if doc.Data != nil && doc.Data.Plan != nil {
		raw = doc.Data.Plan.EndsAt
	} else if doc.Plan != nil {
		raw = doc.Plan.EndsAt
	}
	if len(raw) == 0 {
		return time.Time{}, false
	}
	return parseUnixish(raw)
}

// doBillingCall performs the request with the call timeout, reads the bounded
// body and closes the response.
func doBillingCall(ctx context.Context, cfg *callConfig, httpReq *http.Request) ([]byte, *http.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, balanceCallTimeout)
	defer cancel()
	httpReq = httpReq.WithContext(ctx)

	resp, err := cfg.client.Do(httpReq)
	if err != nil {
		return nil, nil, fmt.Errorf("upstream: billing call: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, balanceBodyLimit))
	if err != nil {
		return nil, nil, fmt.Errorf("upstream: read billing response: %w", err)
	}
	return body, resp, nil
}

// balanceBucket mirrors one quota bucket of the official billing/balance
// contract (ZCode official client bigmodelUsageQuotaProvider):
// data.balances[] carries total_units/used_units/remaining_units and
// expires_at — the instant the daily quota window refills. Scalars may arrive
// as JSON numbers or strings, so every field is captured raw.
type balanceBucket struct {
	TotalUnits     json.RawMessage `json:"total_units"`
	UsedUnits      json.RawMessage `json:"used_units"`
	RemainingUnits json.RawMessage `json:"remaining_units"`
	ExpiresAt      json.RawMessage `json:"expires_at"`
}

// balancePlan mirrors one entry of data.plans[] (ZCode official source
// bigmodelUsageQuotaProvider.ts:1293-1311: the subscription expiry is the
// ends_at of the entry whose status is "active").
type balancePlan struct {
	Status string          `json:"status"`
	EndsAt json.RawMessage `json:"ends_at"`
}

// balancePayload mirrors the /billing/balance response. The
// official shape is {code, data:{plans[], balances[]}}; the flat legacy fields
// (remaining_quota, total_quota, plan_expires_at) are kept as a fallback and
// are also accepted wrapped in "data".
type balancePayload struct {
	RemainingQuota *json.Number    `json:"remaining_quota"`
	TotalQuota     *json.Number    `json:"total_quota"`
	PlanExpiresAt  json.RawMessage `json:"plan_expires_at"`
	Plans          []balancePlan   `json:"plans"`
	Balances       []balanceBucket `json:"balances"`
	Data           *balancePayload `json:"data"`
}

// parseBalance extracts the quota fields from a balance response. The official
// balances[]/plans[] shape is tried first (units summed across buckets, plan
// expiry from the newest active plan), then the flat legacy fields; unknown
// shapes yield zero values. It never fails: the outcome carries any
// protocol-level problem.
func parseBalance(body []byte) Balance {
	payload := decodeBody(body)
	if len(payload) == 0 {
		return Balance{}
	}
	var doc balancePayload
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return Balance{}
	}
	lookup := doc
	if emptyBalance(lookup) && lookup.Data != nil {
		lookup = *lookup.Data
	}

	out := Balance{}
	for _, b := range lookup.Balances {
		if v, ok := rawToInt64(b.RemainingUnits); ok {
			if out.RemainingQuota == nil {
				out.RemainingQuota = new(int64)
			}
			*out.RemainingQuota += v
		}
		if v, ok := rawToInt64(b.TotalUnits); ok {
			if out.TotalQuota == nil {
				out.TotalQuota = new(int64)
			}
			*out.TotalQuota += v
		}
		if t, ok := parseUnixish(b.ExpiresAt); ok {
			// Official semantics (nextResetTime): the EARLIEST bucket expiry
			// is when quota starts coming back, not the latest.
			if out.ResetAt.IsZero() || t.Before(out.ResetAt) {
				out.ResetAt = t
			}
		}
	}
	for _, pl := range lookup.Plans {
		if !strings.EqualFold(strings.TrimSpace(pl.Status), "active") {
			continue
		}
		if t, ok := parseUnixish(pl.EndsAt); ok && t.After(out.PlanExpiresAt) {
			out.PlanExpiresAt = t
		}
	}
	// Flat legacy fields fill anything the official shape left empty.
	if out.RemainingQuota == nil {
		out.RemainingQuota = numberToInt64(lookup.RemainingQuota)
	}
	if out.TotalQuota == nil {
		out.TotalQuota = numberToInt64(lookup.TotalQuota)
	}
	if out.PlanExpiresAt.IsZero() {
		if t, ok := parseUnixish(lookup.PlanExpiresAt); ok {
			out.PlanExpiresAt = t
		}
	}
	return out
}

func emptyBalance(p balancePayload) bool {
	return p.RemainingQuota == nil && p.TotalQuota == nil && len(p.PlanExpiresAt) == 0 &&
		len(p.Plans) == 0 && len(p.Balances) == 0
}

// rawToInt64 reads a JSON scalar (number or quoted number) as int64.
func rawToInt64(raw json.RawMessage) (int64, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return 0, false
	}
	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return 0, false
		}
		v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		return v, err == nil
	}
	var n json.Number
	if err := json.Unmarshal(trimmed, &n); err != nil {
		return 0, false
	}
	v, err := n.Int64()
	return v, err == nil
}

// numberToInt64 converts a JSON number into an int64 pointer; nil stays nil.
func numberToInt64(n *json.Number) *int64 {
	if n == nil {
		return nil
	}
	if v, err := strconv.ParseInt(n.String(), 10, 64); err == nil {
		return &v
	}
	// Float-shaped numbers ("1e7", "12.0") still count.
	if f, err := strconv.ParseFloat(n.String(), 64); err == nil {
		v := int64(f)
		return &v
	}
	return nil
}

// parseUnixish interprets a plan_expires_at JSON value: a unix-seconds or
// unix-milliseconds number, or an RFC3339 string. Implausible dates are
// reported as absent so a misparse cannot poison plan_expires_at.
func parseUnixish(raw json.RawMessage) (time.Time, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return time.Time{}, false
	}
	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return time.Time{}, false
		}
		if t, err := time.Parse(time.RFC3339, s); err == nil && plausibleReset(t) {
			return t, true
		}
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return unixishTime(v)
		}
		return time.Time{}, false
	}
	var v int64
	if err := json.Unmarshal(trimmed, &v); err != nil {
		return time.Time{}, false
	}
	return unixishTime(v)
}

func unixishTime(v int64) (time.Time, bool) {
	if v >= 1_000_000_000_000 { // unix milliseconds
		v /= 1000
	}
	t := time.Unix(v, 0)
	if !plausibleReset(t) {
		return time.Time{}, false
	}
	return t, true
}
