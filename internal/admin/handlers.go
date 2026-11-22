package admin

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"zcode2api/internal/config"
	"zcode2api/internal/pool"
	"zcode2api/internal/upstream"
)

// maxAdminBodyBytes bounds /api request bodies (the largest legitimate
// payload is a bulk key import; 1 MiB is generous).
const maxAdminBodyBytes = 1 << 20

// proxyTestTimeout bounds one egress-proxy latency measurement,
// and proxyTestParallel caps concurrent tests at 8.
const (
	proxyTestTimeout   = 10 * time.Second
	proxyTestParallel  = 8
	proxyStatusHealthy = "HEALTHY"
	proxyStatusDead    = "DEAD"
)

// AccountTestFunc runs the one-shot upstream probe behind POST
// /api/accounts/{id}/test: a minimal /v1/messages call
// charged to the account, whose outcome updates the account state. It is
// injected by internal/server so this package does not depend on the data
// plane.
type AccountTestFunc func(ctx context.Context, accountID string) (ok bool, kind string, latencyMS int64, err error)

// Handlers serves every /api route of the admin dashboard.
type Handlers struct {
	pool    *pool.Pool
	mapping *pool.Mapping
	store   *pool.Store
	logs    *Logs
	auth    *Auth
	cfg     *config.Config

	claims           *pool.Claimer
	accountTest      AccountTestFunc
	onSettingsChange func()
}

// Option configures optional handler dependencies.
type Option func(*Handlers)

// WithAccountTest injects the upstream account probe (see AccountTestFunc).
func WithAccountTest(f AccountTestFunc) Option { return func(h *Handlers) { h.accountTest = f } }

// WithClaimer injects the daily claim flow behind the /api/accounts/claim
// endpoints. Nil claims disables those endpoints.
func WithClaimer(c *pool.Claimer) Option { return func(h *Handlers) { h.claims = c } }

// WithSettingsChanged registers a callback applied after PUT /api/settings so
// the runtime knobs reach the pool, the upstream classifier and the proxy.
func WithSettingsChanged(f func()) Option { return func(h *Handlers) { h.onSettingsChange = f } }

// NewHandlers builds the control-plane handlers and the session store.
func NewHandlers(cfg *config.Config, pl *pool.Pool, mp *pool.Mapping, st *pool.Store, lg *Logs, opts ...Option) *Handlers {
	h := &Handlers{
		pool:    pl,
		mapping: mp,
		store:   st,
		logs:    lg,
		auth:    NewAuth(),
		cfg:     cfg,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Register wires every /api route. POST /api/login is the
// only unauthenticated route; everything else goes through the session
// middleware, which also enforces the CSRF header on write methods.
func (h *Handlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/login", h.handleLogin)

	guarded := func(pattern string, fn http.HandlerFunc) {
		mux.Handle(pattern, h.auth.Middleware(limitBody(fn, maxAdminBodyBytes)))
	}
	guarded("POST /api/logout", h.handleLogout)
	guarded("GET /api/session", h.handleSession)
	guarded("GET /api/stats", h.handleStats)

	guarded("GET /api/accounts", h.handleAccountsList)
	guarded("POST /api/accounts", h.handleAccountsImport)
	guarded("PATCH /api/accounts/{id}", h.handleAccountPatch)
	guarded("DELETE /api/accounts/{id}", h.handleAccountDelete)
	guarded("POST /api/accounts/{id}/test", h.handleAccountTest)
	guarded("POST /api/accounts/{id}/balance", h.handleAccountBalance)
	guarded("POST /api/accounts/{id}/claim", h.handleAccountClaim)
	guarded("POST /api/accounts/claim", h.handleAccountsClaimAll)
	guarded("POST /api/accounts/refresh", h.handleAccountsRefresh)

	guarded("GET /api/mappings", h.handleMappingsList)
	guarded("POST /api/mappings", h.handleMappingCreate)
	guarded("PUT /api/mappings/{id}", h.handleMappingUpdate)
	guarded("DELETE /api/mappings/{id}", h.handleMappingDelete)
	guarded("POST /api/mappings/reset-default", h.handleMappingResetDefault)
	guarded("GET /api/mappings/resolve", h.handleMappingResolve)

	guarded("GET /api/proxies", h.handleProxiesList)
	guarded("POST /api/proxies", h.handleProxyCreate)
	guarded("POST /api/proxies/batch", h.handleProxyBatch)
	guarded("POST /api/proxies/test", h.handleProxiesTest)
	guarded("DELETE /api/proxies/{id}", h.handleProxyDelete)

	guarded("GET /api/logs", h.handleLogsList)
	guarded("GET /api/logs/stream", http.HandlerFunc(h.logs.ServeStream))
	guarded("DELETE /api/logs", h.handleLogsDelete)

	guarded("GET /api/settings", h.handleSettingsGet)
	guarded("PUT /api/settings", h.handleSettingsPut)
	guarded("GET /api/settings/export", h.handleSettingsExport)
	guarded("POST /api/settings/vacuum", h.handleSettingsVacuum)
}

// --- shared helpers -----------------------------------------------------------
func writeAdminError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func respondJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func limitBody(next http.Handler, max int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, max)
		next.ServeHTTP(w, r)
	})
}

// readJSON decodes the request body; failures answer 400 {"error":...}.
func readJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

func unixOf(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

// --- session ------------------------------------------------------------

func (h *Handlers) handleLogin(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Password string `json:"password"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	cookie, ok, limited := h.auth.Login(body.Password, h.cfg.AdminPassword, r)
	switch {
	case limited:
		writeAdminError(w, http.StatusTooManyRequests, "too many login attempts, try again later")
	case !ok:
		writeAdminError(w, http.StatusUnauthorized, "invalid password")
	default:
		http.SetCookie(w, cookie)
		w.WriteHeader(http.StatusNoContent)
	}
}

func (h *Handlers) handleLogout(w http.ResponseWriter, r *http.Request) {
	h.auth.Logout(r)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) handleSession(w http.ResponseWriter, r *http.Request) {
	// The middleware already verified the cookie.
	respondJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// --- stats ---------------------------------------------------------------

func (h *Handlers) handleStats(w http.ResponseWriter, r *http.Request) {
	accounts := h.pool.Accounts()
	counts := h.pool.StatusCounts()
	var requests, failures, inTok, outTok int64
	for _, a := range accounts {
		requests += a.RequestsTotal
		failures += a.FailuresTotal
		inTok += a.InputTokens
		outTok += a.OutputTokens
	}
	stats := map[string]any{
		"accounts": map[string]int{
			"total":     h.pool.NumAccounts(),
			"active":    counts[pool.StatusActive],
			"cooling":   counts[pool.StatusCooling],
			"exhausted": counts[pool.StatusExhausted],
			"disabled":  counts[pool.StatusDisabled],
		},
		"inflight":        h.pool.InflightTotal(),
		"requests_total":  requests,
		"failures_total":  failures,
		"failovers_total": h.logs.FailoversTotal(),
		"input_tokens":    inTok,
		"output_tokens":   outTok,
		"uptime_seconds":  h.logs.UptimeSeconds(),
		"version":         upstream.Version,
		// FALLBACK_API_KEY indicator: only whether the channel is configured,
		// never the key itself.
		"fallback_configured": h.fallbackAPIKey() != "",
	}
	if h.claims != nil {
		stats["claims_total"] = h.claims.ClaimsTotal()
		stats["claims_captcha_skipped"] = h.claims.CaptchaSkipped()
	}
	respondJSON(w, http.StatusOK, stats)
}

// fallbackAPIKey resolves the channel B key: the runtime setting wins over
// ENV. It is write-only — never returned to a
// client.
func (h *Handlers) fallbackAPIKey() string {
	if v, ok, err := h.store.GetSetting("fallback_api_key"); err == nil && ok && v != "" {
		return v
	}
	return h.cfg.FallbackAPIKey
}

// --- accounts ------------------------------------------------------------

// accountView is the dashboard representation of an account. The JWT never
// leaves the pool: only the "…hint" mask is shown.
type accountView struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	Token          string `json:"token"` // masked, e.g. "…a1b2"
	DeviceMid      string `json:"device_mid"`
	ProxyID        string `json:"proxy_id,omitempty"`
	Status         string `json:"status"`
	StatusReason   string `json:"status_reason,omitempty"`
	CooldownUntil  int64  `json:"cooldown_until,omitempty"`
	PlanExpiresAt  int64  `json:"plan_expires_at,omitempty"`
	RemainingQuota *int64 `json:"remaining_quota,omitempty"`
	TotalQuota     *int64 `json:"total_quota,omitempty"`
	QuotaCheckedAt int64  `json:"quota_checked_at,omitempty"`
	Inflight       int    `json:"inflight"`
	RequestsTotal  int64  `json:"requests_total"`
	FailuresTotal  int64  `json:"failures_total"`
	InputTokens    int64  `json:"input_tokens"`
	OutputTokens   int64  `json:"output_tokens"`
	LastClaimAt    int64  `json:"last_claim_at,omitempty"`
	LastClaimKind  string `json:"last_claim_kind,omitempty"`
	LastUsedAt     int64  `json:"last_used_at,omitempty"`
	CreatedAt      int64  `json:"created_at"`
}

func accountViewOf(a pool.Account) accountView {
	return accountView{
		ID:             a.ID,
		Name:           a.Name,
		Token:          a.MaskedToken(),
		DeviceMid:      a.DeviceMid,
		ProxyID:        a.ProxyID,
		Status:         string(a.Status),
		StatusReason:   a.StatusReason,
		CooldownUntil:  unixOf(a.CooldownUntil),
		PlanExpiresAt:  unixOf(a.PlanExpiresAt),
		RemainingQuota: a.RemainingQuota,
		TotalQuota:     a.TotalQuota,
		QuotaCheckedAt: unixOf(a.QuotaCheckedAt),
		Inflight:       a.Inflight,
		RequestsTotal:  a.RequestsTotal,
		FailuresTotal:  a.FailuresTotal,
		InputTokens:    a.InputTokens,
		OutputTokens:   a.OutputTokens,
		LastClaimAt:    unixOf(a.LastClaimAt),
		LastClaimKind:  a.LastClaimKind,
		LastUsedAt:     unixOf(a.LastUsedAt),
		CreatedAt:      unixOf(a.CreatedAt),
	}
}

func (h *Handlers) handleAccountsList(w http.ResponseWriter, r *http.Request) {
	statusFilter := r.URL.Query().Get("status")
	q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	accounts := h.pool.Accounts()
	out := make([]accountView, 0, len(accounts))
	for _, a := range accounts {
		if statusFilter != "" && string(a.Status) != statusFilter {
			continue
		}
		if q != "" &&
			!strings.Contains(strings.ToLower(a.Name), q) &&
			!strings.Contains(strings.ToLower(a.ID), q) &&
			!strings.Contains(strings.ToLower(a.DeviceMid), q) &&
			!strings.Contains(strings.ToLower(a.TokenHint), q) {
			continue
		}
		out = append(out, accountViewOf(a))
	}
	respondJSON(w, http.StatusOK, map[string]any{"accounts": out})
}

func (h *Handlers) handleAccountsImport(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Lines string `json:"lines"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	added, duplicates, invalid, err := h.pool.ImportAccounts(body.Lines)
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"added": added, "duplicates": duplicates, "invalid": invalid,
	})
}

func (h *Handlers) handleAccountPatch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		Name    *string `json:"name"`
		ProxyID *string `json:"proxy_id"`
		Enabled *bool   `json:"enabled"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	if body.Name != nil {
		if err := h.pool.RenameAccount(id, *body.Name); err != nil {
			h.writeAccountError(w, err)
			return
		}
	}
	if body.ProxyID != nil {
		if *body.ProxyID != "" && !h.proxyExists(*body.ProxyID) {
			writeAdminError(w, http.StatusBadRequest, "no such proxy")
			return
		}
		if err := h.pool.SetAccountProxy(id, *body.ProxyID); err != nil {
			h.writeAccountError(w, err)
			return
		}
	}
	if body.Enabled != nil {
		if err := h.pool.SetAccountEnabled(id, *body.Enabled); err != nil {
			h.writeAccountError(w, err)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) handleAccountDelete(w http.ResponseWriter, r *http.Request) {
	if err := h.pool.DeleteAccount(r.PathValue("id")); err != nil {
		h.writeAccountError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) handleAccountTest(w http.ResponseWriter, r *http.Request) {
	if h.accountTest == nil {
		writeAdminError(w, http.StatusNotImplemented, "account testing is not configured")
		return
	}
	ok, kind, latency, err := h.accountTest(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeAccountError(w, err)
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"ok": ok, "kind": kind, "latency_ms": latency,
	})
}

// handleAccountBalance implements POST /api/accounts/{id}/balance:
// the free /billing/balance call updates the quota snapshot and the
// account state.
func (h *Handlers) handleAccountBalance(w http.ResponseWriter, r *http.Request) {
	balance, _, err := h.pool.RefreshBalance(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeAccountError(w, err)
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"remaining_quota": balance.RemainingQuota,
		"total_quota":     balance.TotalQuota,
		"plan_expires_at": unixOf(balance.PlanExpiresAt),
	})
}

// handleAccountClaim implements POST /api/accounts/{id}/claim:
// claim one account right now.
func (h *Handlers) handleAccountClaim(w http.ResponseWriter, r *http.Request) {
	if h.claims == nil {
		writeAdminError(w, http.StatusNotImplemented, "claiming is not configured")
		return
	}
	report := h.claims.ClaimOne(r.Context(), r.PathValue("id"))
	respondJSON(w, http.StatusOK, map[string]any{
		"ok": report.OK, "kind": report.Kind,
		"claimed": report.Claimed, "captcha_required": report.CaptchaRequired,
	})
}

// handleAccountsClaimAll implements POST /api/accounts/claim:
// claim-all runs in the background, account by account;
// progress is reported through `event: claim` on the log stream. The pass is
// single-flight: when one is already running (a second click, or a click that
// lands on the scheduled 00:05 run) the answer is started=false and no second
// pass is stacked.
func (h *Handlers) handleAccountsClaimAll(w http.ResponseWriter, r *http.Request) {
	if h.claims == nil {
		writeAdminError(w, http.StatusNotImplemented, "claiming is not configured")
		return
	}
	started, _ := h.claims.TryStartClaimAll(context.WithoutCancel(r.Context()))
	respondJSON(w, http.StatusOK, map[string]bool{"started": started})
}

// handleAccountsRefresh implements POST /api/accounts/refresh:
// refresh the balance of every ACTIVE account in the background
// (free calls — no quota consumed).
func (h *Handlers) handleAccountsRefresh(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	go h.pool.RefreshBalances(context.WithoutCancel(ctx))
	respondJSON(w, http.StatusOK, map[string]bool{"started": true})
}

func (h *Handlers) writeAccountError(w http.ResponseWriter, err error) {
	if errors.Is(err, pool.ErrAccountNotFound) {
		writeAdminError(w, http.StatusNotFound, "no such account")
		return
	}
	writeAdminError(w, http.StatusInternalServerError, err.Error())
}

func (h *Handlers) proxyExists(id string) bool {
	proxies, err := h.store.ListProxies()
	if err != nil {
		return false
	}
	for _, p := range proxies {
		if p.ID == id {
			return true
		}
	}
	return false
}

// --- model mappings ------------------------------------------------------

type mappingView struct {
	ID        string `json:"id"`
	Pattern   string `json:"pattern"`
	Target    string `json:"target,omitempty"`
	Mode      string `json:"mode"`
	CreatedAt int64  `json:"created_at"`
}

func (h *Handlers) handleMappingsList(w http.ResponseWriter, r *http.Request) {
	rules := h.mapping.Rules()
	out := make([]mappingView, 0, len(rules))
	for _, rule := range rules {
		out = append(out, mappingView{
			ID: rule.ID, Pattern: rule.Pattern, Target: rule.Target,
			Mode: rule.Mode, CreatedAt: unixOf(rule.CreatedAt),
		})
	}
	respondJSON(w, http.StatusOK, map[string]any{"mappings": out})
}

type mappingBody struct {
	Pattern string `json:"pattern"`
	Target  string `json:"target"`
	Mode    string `json:"mode"`
}

func (h *Handlers) handleMappingCreate(w http.ResponseWriter, r *http.Request) {
	var body mappingBody
	if !readJSON(w, r, &body) {
		return
	}
	id, err := h.mapping.Add(body.Pattern, body.Target, body.Mode, time.Now())
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"id": id})
}

func (h *Handlers) handleMappingUpdate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body mappingBody
	if !readJSON(w, r, &body) {
		return
	}
	// Validate first so an invalid body never deletes the existing rule.
	if err := pool.ValidatePattern(body.Pattern); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}
	mode := strings.ToUpper(strings.TrimSpace(body.Mode))
	if mode != pool.ModeMapped && mode != pool.ModePassthrough {
		writeAdminError(w, http.StatusBadRequest, "mode must be MAPPED or PASSTHROUGH")
		return
	}
	if mode == pool.ModeMapped && strings.TrimSpace(body.Target) == "" {
		writeAdminError(w, http.StatusBadRequest, "MAPPED rules need a non-empty target")
		return
	}

	// PUT replaces the rule identified by id: drop it, then upsert the new
	// pattern (Add's upsert merges with an existing identical pattern).
	var found bool
	for _, rule := range h.mapping.Rules() {
		if rule.ID == id {
			found = true
			break
		}
	}
	if !found {
		writeAdminError(w, http.StatusNotFound, "no such mapping")
		return
	}
	if err := h.mapping.Delete(id); err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if _, err := h.mapping.Add(body.Pattern, body.Target, body.Mode, time.Now()); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) handleMappingDelete(w http.ResponseWriter, r *http.Request) {
	if err := h.mapping.Delete(r.PathValue("id")); err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) handleMappingResetDefault(w http.ResponseWriter, r *http.Request) {
	if err := h.mapping.ResetDefault(h.cfg); err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) handleMappingResolve(w http.ResponseWriter, r *http.Request) {
	model := r.URL.Query().Get("model")
	out, ruleID := h.mapping.Resolve(model)
	respondJSON(w, http.StatusOK, map[string]string{
		"input": model, "output": out, "rule_id": ruleID,
	})
}

// --- egress proxies ------------------------------------------------------

type proxyView struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	URL       string `json:"url"` // credentials masked
	LatencyMS int64  `json:"latency_ms"`
	Status    string `json:"status"`
	CreatedAt int64  `json:"created_at"`
}

func proxyViewOf(p pool.ProxyRecord) proxyView {
	return proxyView{
		ID: p.ID, Name: p.Name,
		URL:       pool.MaskProxyURL(p.URL),
		LatencyMS: p.LatencyMS, Status: p.Status, CreatedAt: unixOf(p.CreatedAt),
	}
}

func (h *Handlers) handleProxiesList(w http.ResponseWriter, r *http.Request) {
	records, err := h.store.ListProxies()
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]proxyView, 0, len(records))
	for _, p := range records {
		out = append(out, proxyViewOf(p))
	}
	respondJSON(w, http.StatusOK, map[string]any{"proxies": out})
}

// validateProxyURL enforces the schemes of (http, https,
// socks5) and a non-empty host. Error text never embeds the raw URL because
// it may carry credentials.
func validateProxyURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return errors.New("proxy URL does not parse")
	}
	switch u.Scheme {
	case "http", "https", "socks5", "socks5h":
	default:
		return errors.New("unsupported proxy scheme (allowed: http, https, socks5)")
	}
	if u.Host == "" {
		return errors.New("proxy URL has no host")
	}
	return nil
}

func (h *Handlers) handleProxyCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
		URL  string `json:"url"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	id, invalid := h.addProxy(body.Name, body.URL)
	if invalid != "" {
		writeAdminError(w, http.StatusBadRequest, invalid)
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"id": id})
}

func (h *Handlers) handleProxyBatch(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Lines string `json:"lines"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	added, duplicates := 0, 0
	var invalid []int
	for i, line := range strings.Split(body.Lines, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if line == "" {
			continue // blank lines carry no meaning
		}
		name := ""
		if namePart, urlPart, ok := strings.Cut(line, "|"); ok {
			name, line = strings.TrimSpace(namePart), strings.TrimSpace(urlPart)
		}
		id, problem := h.addProxy(name, line)
		if problem != "" {
			if problem == "duplicate" {
				duplicates++
			} else {
				invalid = append(invalid, i+1)
			}
			continue
		}
		_ = id
		added++
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"added": added, "duplicates": duplicates, "invalid": invalid,
	})
}

// addProxy inserts one proxy (after duplicate pre-check) and refreshes the
// pool's proxy table. problem is "" on success, "duplicate" for an existing
// URL, or a validation message.
func (h *Handlers) addProxy(name, rawURL string) (id, problem string) {
	if err := validateProxyURL(rawURL); err != nil {
		return "", err.Error()
	}
	existing, err := h.store.ListProxies()
	if err != nil {
		return "", err.Error()
	}
	for _, p := range existing {
		if p.URL == rawURL {
			return "", "duplicate"
		}
	}
	id, err = h.store.AddProxy(name, rawURL, time.Now())
	if err != nil {
		return "", err.Error()
	}
	if err := h.pool.RefreshProxies(); err != nil {
		return "", err.Error()
	}
	return id, ""
}

func (h *Handlers) handleProxiesTest(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IDs []string `json:"ids"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	records, err := h.store.ListProxies()
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	wanted := make(map[string]bool, len(body.IDs))
	for _, id := range body.IDs {
		wanted[id] = true
	}
	target := upstreamHostPort(h.cfg.UpstreamURL)

	type result struct {
		ID        string `json:"id"`
		LatencyMS int64  `json:"latency_ms"`
		Status    string `json:"status"`
		// Detail names the failure point of a dead probe (proxy_dial /
		// proxy_connect / target_connect / tls_handshake) plus the transport
		// error; empty for a healthy proxy.
		Detail string `json:"detail,omitempty"`
	}
	results := make([]result, len(records))
	var wg sync.WaitGroup
	sem := make(chan struct{}, proxyTestParallel)
	for i, rec := range records {
		if len(wanted) > 0 && !wanted[rec.ID] {
			results[i] = result{ID: rec.ID, LatencyMS: rec.LatencyMS, Status: rec.Status}
			continue
		}
		wg.Add(1)
		go func(i int, rec pool.ProxyRecord) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			latency, err := measureViaProxy(r.Context(), rec.URL, target)
			status, latencyMS, detail := proxyStatusHealthy, latency, ""
			if err != nil {
				status, latencyMS, detail = proxyStatusDead, -1, err.Error()
			}
			if uerr := h.store.UpdateProxyCheck(rec.ID, latencyMS, status, time.Now()); uerr != nil {
				// Storage hiccup must not fail the whole test round.
				status, latencyMS = proxyStatusDead, -1
			}
			results[i] = result{ID: rec.ID, LatencyMS: latencyMS, Status: status, Detail: detail}
		}(i, rec)
	}
	wg.Wait()
	// Reload the pool's proxy table so the round's verdict takes effect at
	// once: DEAD probes drop out of egress (reloadLocked skips them) and a
	// recovered one re-enters without a restart. Best-effort — the response
	// still reports the results.
	if err := h.pool.RefreshProxies(); err != nil {
		slog.Warn("admin: reload proxies after test", "err", err)
	}
	respondJSON(w, http.StatusOK, map[string]any{"results": results})
}

func (h *Handlers) handleProxyDelete(w http.ResponseWriter, r *http.Request) {
	if err := h.store.DeleteProxy(r.PathValue("id")); err != nil {
		writeAdminError(w, http.StatusNotFound, "no such proxy")
		return
	}
	if err := h.pool.RefreshProxies(); err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// upstreamHostPort extracts host:port of UPSTREAM_URL for the proxy latency
// test, defaulting the port by scheme.
func upstreamHostPort(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "api.z.ai:443"
	}
	port := u.Port()
	if port == "" {
		if u.Scheme == "http" {
			port = "80"
		} else {
			port = "443"
		}
	}
	return net.JoinHostPort(u.Hostname(), port)
}

// measureViaProxy times the TCP + TLS handshake to target through the given
// egress proxy. Supported schemes: direct, http, https,
// socks5/socks5h (CONNECT / SOCKS5 CONNECT, then TLS to the target). The
// measurement never sends application traffic to the upstream. Errors name
// their failure point: proxy_dial, proxy_connect, target_connect, or
// tls_handshake.
func measureViaProxy(ctx context.Context, proxyURL, target string) (int64, error) {
	start := time.Now()
	deadline := start.Add(proxyTestTimeout)

	dial := func(addr string) (net.Conn, error) {
		d := &net.Dialer{Deadline: deadline}
		return d.DialContext(ctx, "tcp", addr)
	}
	var conn net.Conn
	var err error
	if proxyURL == "" {
		conn, err = dial(target)
		if err != nil {
			return 0, fmt.Errorf("target_connect: %w", err)
		}
	} else {
		conn, err = dialViaProxy(ctx, proxyURL, target, dial, deadline)
		if err != nil {
			return 0, err // already staged: proxy_dial / proxy_connect
		}
	}
	defer conn.Close()

	tlsConn := tls.Client(conn, &tls.Config{ServerName: hostOnly(target)})
	tlsConn.SetDeadline(deadline)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return 0, fmt.Errorf("tls_handshake: %w", err)
	}
	_ = tlsConn.Close() // measurement only; the raw conn is closed by the defer
	return time.Since(start).Milliseconds(), nil
}

// dialViaProxy establishes the tunnel to target through proxyURL: plain TCP
// for http://, TLS-to-proxy for https://, SOCKS5 handshake for socks5://.
func dialViaProxy(ctx context.Context, proxyURL, target string, dial func(string) (net.Conn, error), deadline time.Time) (net.Conn, error) {
	u, err := url.Parse(proxyURL)
	if err != nil || u.Host == "" {
		return nil, errors.New("invalid proxy url")
	}
	conn, err := dial(u.Host)
	if err != nil {
		return nil, fmt.Errorf("proxy_dial: %w", err)
	}
	conn.SetDeadline(deadline)

	switch u.Scheme {
	case "https":
		pc := tls.Client(conn, &tls.Config{ServerName: u.Hostname()})
		if err := pc.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, fmt.Errorf("proxy_connect: %w", err)
		}
		conn = pc
		fallthrough
	case "http":
		if err := proxyConnect(conn, target); err != nil {
			conn.Close()
			return nil, fmt.Errorf("proxy_connect: %w", err)
		}
	case "socks5", "socks5h":
		if err := socks5Connect(conn, target); err != nil {
			conn.Close()
			return nil, fmt.Errorf("proxy_connect: %w", err)
		}
	default:
		conn.Close()
		return nil, fmt.Errorf("proxy_connect: unsupported proxy scheme %q", u.Scheme)
	}
	return conn, nil
}

// proxyConnect issues an HTTP CONNECT through an established proxy connection.
func proxyConnect(conn net.Conn, target string) error {
	req := "CONNECT " + target + " HTTP/1.1\r\nHost: " + target + "\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		return err
	}
	br := bufio.NewReader(conn)
	connectReq, _ := http.NewRequest(http.MethodConnect, "https://"+target, nil)
	resp, err := http.ReadResponse(br, connectReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("proxy CONNECT failed: %s", resp.Status)
	}
	return nil
}

// socks5Connect performs the no-auth SOCKS5 CONNECT handshake (RFC 1928) so
// latency testing works without extra dependencies.
func socks5Connect(conn net.Conn, target string) error {
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return errors.New("socks5: bad target port")
	}
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil { // VER, 1 method, NO-AUTH
		return err
	}
	greeting := make([]byte, 2)
	if _, err := io.ReadFull(conn, greeting); err != nil {
		return err
	}
	if greeting[0] != 0x05 || greeting[1] != 0x00 {
		return errors.New("socks5: no acceptable auth method")
	}
	req := make([]byte, 0, 7+len(host))
	req = append(req, 0x05, 0x01, 0x00, 0x03, byte(len(host))) // VER, CMD=CONNECT, RSV, ATYP=domain
	req = append(req, host...)
	req = append(req, byte(port>>8), byte(port&0xff))
	if _, err := conn.Write(req); err != nil {
		return err
	}
	reply := make([]byte, 4)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return err
	}
	if reply[1] != 0x00 {
		return fmt.Errorf("socks5: connect failed (code %d)", reply[1])
	}
	var rest int
	switch reply[3] { // bound address type
	case 0x01:
		rest = 4 // IPv4 + port
	case 0x03:
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return err
		}
		rest = int(lenBuf[0]) + 2
	case 0x04:
		rest = 16 // IPv6 + port
	default:
		return errors.New("socks5: unknown address type in reply")
	}
	rest += 2 // port
	drain := make([]byte, rest)
	if _, err := io.ReadFull(conn, drain); err != nil {
		return err
	}
	return nil
}

func hostOnly(hostPort string) string {
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		return hostPort
	}
	return host
}

// --- logs ----------------------------------------------------------------

func (h *Handlers) handleLogsList(w http.ResponseWriter, r *http.Request) {
	limit := 200
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	entries := h.logs.Snapshot(limit)
	type logLine struct {
		Event string          `json:"event"`
		Data  json.RawMessage `json:"data"`
	}
	out := make([]logLine, 0, len(entries))
	for _, e := range entries {
		out = append(out, logLine{Event: e.Event, Data: e.Data})
	}
	respondJSON(w, http.StatusOK, map[string]any{"logs": out})
}

func (h *Handlers) handleLogsDelete(w http.ResponseWriter, r *http.Request) {
	h.logs.Reset()
	w.WriteHeader(http.StatusNoContent)
}

// --- settings and maintenance --------------------------------------------

// settingSpec describes one runtime setting: how to parse/validate the JSON
// value and what string to persist.
type settingSpec struct {
	apply func(v any) (string, error)
}

func durationSetting(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", errors.New("must be a duration string like 60s, 30m, 1h")
	}
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil || d <= 0 {
		return "", errors.New("must be a positive duration like 60s, 30m, 1h")
	}
	return d.String(), nil
}

func intSetting(min, max int) func(any) (string, error) {
	return func(v any) (string, error) {
		n, ok := v.(float64) // encoding/json numbers arrive as float64
		if !ok || n != float64(int64(n)) {
			return "", errors.New("must be an integer")
		}
		i := int(n)
		if i < min || i > max {
			return "", fmt.Errorf("must be between %d and %d", min, max)
		}
		return strconv.Itoa(i), nil
	}
}

func boolSetting(v any) (string, error) {
	b, ok := v.(bool)
	if !ok {
		return "", errors.New("must be a boolean")
	}
	return strconv.FormatBool(b), nil
}

// stringSetting stores a non-empty string value (claim_time, solver URLs).
func stringSetting(v any) (string, error) {
	s, ok := v.(string)
	if !ok || strings.TrimSpace(s) == "" {
		return "", errors.New("must be a non-empty string")
	}
	return strings.TrimSpace(s), nil
}

// claimTimeSetting validates the HH:MM grammar of CLAIM_TIME.
func claimTimeSetting(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", errors.New("must be an HH:MM string")
	}
	if _, _, err := config.ParseClaimTime(strings.TrimSpace(s)); err != nil {
		return "", errors.New("must be a valid HH:MM 24-hour time (e.g. 00:05)")
	}
	return strings.TrimSpace(s), nil
}

// runtimeSettingSpecs mirrors the editable runtime settings list, with
// validation bounds. fallback_api_key and claim_solver_key
// are write-only secrets: they are persisted but never returned by GET.
var runtimeSettingSpecs = map[string]settingSpec{
	"cooling_duration":   {apply: durationSetting},
	"risk_cooldown":      {apply: durationSetting},
	"exhausted_fallback": {apply: durationSetting},
	"inflight_limit":     {apply: intSetting(1, 32)},
	"max_attempts":       {apply: intSetting(1, 10)},
	"affinity_enabled":   {apply: boolSetting},
	"affinity_ttl":       {apply: durationSetting},
	"strict_models":      {apply: boolSetting},
	"daily_claim":        {apply: boolSetting},
	"claim_time":         {apply: claimTimeSetting},
	"claim_solver_url":   {apply: stringSetting},
	"claim_solver_key":   {apply: stringSetting},
	"fallback_api_key":   {apply: stringSetting},
}

func (h *Handlers) handleSettingsGet(w http.ResponseWriter, r *http.Request) {
	stored, err := h.store.AllSettings()
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	cfg := h.cfg
	duration := func(key string, def time.Duration) string {
		if v, ok := stored[key]; ok {
			if d, err := time.ParseDuration(v); err == nil && d > 0 {
				return d.String()
			}
		}
		return def.String()
	}
	number := func(key string, def int) int {
		if v, ok := stored[key]; ok {
			if n, err := strconv.Atoi(v); err == nil {
				return n
			}
		}
		return def
	}
	boolean := func(key string, def bool) bool {
		if v, ok := stored[key]; ok {
			if b, err := strconv.ParseBool(v); err == nil {
				return b
			}
		}
		return def
	}
	stringVal := func(key, def string) string {
		if v, ok := stored[key]; ok && v != "" {
			return v
		}
		return def
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"runtime": map[string]any{
			"cooling_duration":   duration("cooling_duration", cfg.CoolingDuration),
			"risk_cooldown":      duration("risk_cooldown", cfg.RiskCooldown),
			"exhausted_fallback": duration("exhausted_fallback", cfg.ExhaustedFallback),
			"inflight_limit":     number("inflight_limit", cfg.InflightLimit),
			"max_attempts":       number("max_attempts", cfg.MaxAttempts),
			"affinity_enabled":   boolean("affinity_enabled", cfg.AffinityEnabled),
			"affinity_ttl":       duration("affinity_ttl", cfg.AffinityTTL),
			"strict_models":      boolean("strict_models", cfg.StrictModels),
			"daily_claim":        boolean("daily_claim", cfg.DailyClaim),
			"claim_time":         stringVal("claim_time", cfg.ClaimTime),
			"claim_solver_url":   stringVal("claim_solver_url", cfg.ClaimSolverURL),
			// claim_solver_key and fallback_api_key are write-only: never
			// returned, only their configured state is.
			"claim_solver_key_set": h.claimSolverKey() != "",
			"fallback_api_key_set": h.fallbackAPIKey() != "",
		},
		"system": map[string]any{
			"host":                cfg.Host,
			"port":                cfg.Port,
			"upstream_url":        cfg.UpstreamURL,
			"fallback_url":        cfg.FallbackURL,
			"fallback_configured": h.fallbackAPIKey() != "",
			"db_path":             cfg.DBPath,
			"version":             upstream.Version,
			"secret_key_set":      cfg.SecretKey != "",
		},
	})
}

// claimSolverKey resolves the solver key: the runtime setting wins over ENV.
// Write-only — never returned to a client.
func (h *Handlers) claimSolverKey() string {
	if v, ok, err := h.store.GetSetting("claim_solver_key"); err == nil && ok && v != "" {
		return v
	}
	return h.cfg.ClaimSolverKey
}

func (h *Handlers) handleSettingsPut(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !readJSON(w, r, &body) {
		return
	}
	persist := make(map[string]string, len(body))
	for key, value := range body {
		spec, ok := runtimeSettingSpecs[key]
		if !ok {
			writeAdminError(w, http.StatusBadRequest, fmt.Sprintf("unknown or read-only setting %q", key))
			return
		}
		stored, err := spec.apply(value)
		if err != nil {
			writeAdminError(w, http.StatusBadRequest, fmt.Sprintf("%s: %v", key, err))
			return
		}
		persist[key] = stored
	}
	now := time.Now()
	// One transaction for the whole batch: a store failure between keys rolls
	// everything back instead of persisting a partial PUT.
	if err := h.store.SetSettings(persist, now); err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if h.onSettingsChange != nil {
		h.onSettingsChange()
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) handleSettingsExport(w http.ResponseWriter, r *http.Request) {
	rules := h.mapping.Rules()
	mappings := make([]map[string]string, 0, len(rules))
	for _, rule := range rules {
		mappings = append(mappings, map[string]string{
			"pattern": rule.Pattern, "target": rule.Target, "mode": rule.Mode,
		})
	}
	records, err := h.store.ListProxies()
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	proxies := make([]map[string]any, 0, len(records))
	for _, p := range records {
		proxies = append(proxies, map[string]any{
			"name":       p.Name,
			"url":        pool.MaskProxyURL(p.URL), // password masked
			"latency_ms": p.LatencyMS,
			"status":     p.Status,
		})
	}
	accounts := h.pool.Accounts()
	accountList := make([]map[string]any, 0, len(accounts))
	for _, a := range accounts {
		// The export carries name, token_hint, device_mid and status only —
		// never the JWT.
		accountList = append(accountList, map[string]any{
			"name":       a.Name,
			"token_hint": a.TokenHint,
			"device_mid": a.DeviceMid,
			"status":     string(a.Status),
		})
	}
	settings, err := h.store.AllSettings()
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Secrets never reach the export file.
	for _, secret := range []string{"fallback_api_key", "claim_solver_key"} {
		delete(settings, secret)
	}

	payload, err := json.Marshal(map[string]any{
		"version":     upstream.Version,
		"exported_at": time.Now().Unix(),
		"mappings":    mappings,
		"proxies":     proxies,
		"accounts":    accountList,
		"settings":    settings,
	})
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", `attachment; filename="zcode2api-export.json"`)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

func (h *Handlers) handleSettingsVacuum(w http.ResponseWriter, r *http.Request) {
	before, after, err := h.store.Vacuum()
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]int64{
		"before_bytes": before, "after_bytes": after,
	})
}
