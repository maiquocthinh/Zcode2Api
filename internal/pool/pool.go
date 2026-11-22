package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"zcode2api/internal/config"
	"zcode2api/internal/upstream"
)

// MaskProxyURL hides credentials in a proxy URL for dashboard output.
func MaskProxyURL(raw string) string { return maskProxyURL(raw) }

// EventState is the event name emitted for every account state transition;
// the admin log stream sends it to the dashboard as `event: state`.
const EventState = "state"

// Background job intervals: sweeper every 15 seconds; counters flushed every
// 60 seconds and at shutdown.
const (
	DefaultSweepInterval = 15 * time.Second
	DefaultFlushInterval = 60 * time.Second
)

// proxyStatusDead matches the stored proxy status an admin probe writes for
// a line that failed its handshake: such proxies are excluded from egress.
const proxyStatusDead = "DEAD"

// failStreakCooling is the short COOLING pause when an account's consecutive
// upstream-unavailable failures reach FAIL_THRESHOLD. Upstream-side failures
// are not the account's fault, so the account is only rested briefly and
// never disabled.
const failStreakCooling = 30 * time.Second

// Transient-1005 recheck: a chat 1005 on a snapshot that still shows quota is
// the minute-level limit, not a real exhaustion. Two consecutive strikes bench
// the account for two minutes and trigger a free billing verification instead
// of freezing it until next_flush_time (which once benched accounts with
// millions of remaining units for a month).
const (
	quotaStrikeThreshold = 2
	quotaRecheckBench    = 2 * time.Minute
)

// StateEvent is one state transition, emitted after the DB write so the
// dashboard shows what the store already persisted. The JSON shape is the
// `event: state` payload.
type StateEvent struct {
	Ts         time.Time `json:"ts"`
	AccountID  string    `json:"acc"`
	AccountNme string    `json:"name,omitempty"`
	From       Status    `json:"from"`
	To         Status    `json:"to"`
	Until      time.Time `json:"until,omitempty"`
	Kind       string    `json:"kind,omitempty"`
}

// StateHandler receives state events. Handlers run after Pool.mu has been
// released and must not block; a handler that needs pool data should snapshot
// it asynchronously.
type StateHandler func(StateEvent)

// ErrPoolExhausted is the sentinel carried by every Acquire failure: the pool
// has no usable account for this request (empty, all disabled, recovering, or
// all tried). The attempt loop (internal/proxy) triggers the fallback channel
// B on errors.Is(err, ErrPoolExhausted).
var ErrPoolExhausted = errors.New("pool: no usable upstream account")

// AcquireFailure distinguishes why Acquire found no account, so the proxy
// error mapping can pick the right Anthropic error.
type AcquireFailure string

const (
	AcquireEmpty       AcquireFailure = "empty"        // pool has no account at all -> 503
	AcquireAllDisabled AcquireFailure = "all_disabled" // every account DISABLED -> 503
	AcquireRecovering  AcquireFailure = "recovering"   // remaining accounts COOLING/EXHAUSTED -> 429
	AcquireBusy        AcquireFailure = "busy"         // ACTIVE accounts all at INFLIGHT_LIMIT -> 429
	AcquireTriedAll    AcquireFailure = "tried"        // ACTIVE accounts exist but all tried -> end of attempts
)

// AcquireError is returned by Acquire when no account can serve a request. It
// always matches ErrPoolExhausted (see Is).
type AcquireError struct {
	Failure    AcquireFailure
	RetryAfter time.Duration // > 0 only for AcquireRecovering
}

func (e *AcquireError) Error() string {
	switch e.Failure {
	case AcquireEmpty:
		return "pool: no upstream account configured"
	case AcquireAllDisabled:
		return "pool: no usable upstream account (all accounts are DISABLED)"
	case AcquireRecovering:
		return "pool: all upstream accounts are cooling down or exhausted"
	case AcquireBusy:
		return "pool: every active account is at INFLIGHT_LIMIT"
	case AcquireTriedAll:
		return "pool: no untried upstream account left for this request"
	default:
		return fmt.Sprintf("pool: no account available (%s)", e.Failure)
	}
}

// Is reports whether target is ErrPoolExhausted — every acquire failure means
// the pool cannot serve the request, which is exactly the condition under
// which the gateway may spend its one fallback attempt.
func (e *AcquireError) Is(target error) bool { return target == ErrPoolExhausted }

// ErrPoolClosed is returned by Acquire after Stop.
var ErrPoolClosed = errors.New("pool: pool is closed")

// Config carries the pool knobs. AffinityEnabled is a plain bool: the caller
// passes config.AffinityEnabled (default true) explicitly. Now is injectable
// so tests drive cooldowns with a fake clock; OnState receives state events.
// UpstreamURL is the channel A origin used by the billing helpers
// (balance/claim) — read-only system configuration. QuotaRefreshInterval is
// the background quota-refresh period (0 = off); production wiring sets it
// from QUOTA_REFRESH_MIN.
type Config struct {
	InflightLimit        int
	FailThreshold        int
	AffinityEnabled      bool
	AffinityTTL          time.Duration
	DefaultProxyURL      string
	UpstreamURL          string
	SweepInterval        time.Duration
	FlushInterval        time.Duration
	QuotaRefreshInterval time.Duration
	Now                  func() time.Time
	OnState              StateHandler
}

func (c *Config) setDefaults() {
	if c.InflightLimit <= 0 {
		c.InflightLimit = config.DefaultInflightLimit
	}
	if c.FailThreshold <= 0 {
		c.FailThreshold = config.DefaultFailThreshold
	}
	if c.AffinityTTL <= 0 {
		c.AffinityTTL = config.DefaultAffinityTTL
	}
	if c.SweepInterval <= 0 {
		c.SweepInterval = DefaultSweepInterval
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = DefaultFlushInterval
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

// Pool is the thread-safe account manager. It keeps the
// accounts in RAM and writes state changes through to SQLite; counters are
// flushed periodically, not per request.
type Pool struct {
	mu        sync.Mutex
	accounts  []*Account          // stable creation order (round-robin basis)
	byID      map[string]*Account // id -> account
	proxyURLs map[string]string   // proxy id -> URL
	rr        int                 // round-robin cursor over tied candidates

	store *Store
	aff   *Affinity
	cfg   Config // guarded by mu except Now/SweepInterval/FlushInterval (set once)

	onState StateHandler // guarded by mu
	stop    chan struct{}
	wg      sync.WaitGroup
	closed  bool
}

// NewPool loads the stored accounts into RAM. The store must already be open.
func NewPool(store *Store, cfg Config) (*Pool, error) {
	cfg.setDefaults()
	p := &Pool{
		store:     store,
		aff:       NewAffinity(cfg.AffinityTTL, MaxAffinityEntries),
		cfg:       cfg,
		onState:   cfg.OnState,
		byID:      make(map[string]*Account),
		proxyURLs: make(map[string]string),
	}
	// Not yet shared with any goroutine, so no lock is needed here.
	if err := p.reloadLocked(); err != nil {
		return nil, err
	}
	return p, nil
}

// reloadLocked rebuilds the RAM snapshot from the store. Values persisted per
// request (status, cooldown, quota, claim, name, proxy) come from the DB;
// values that only live in RAM (inflight, fail streak, counters since the
// last flush) survive a reload through the previous snapshot.
func (p *Pool) reloadLocked() error {
	recs, err := p.store.ListAccounts()
	if err != nil {
		return err
	}
	proxies, err := p.store.ListProxies()
	if err != nil {
		return err
	}
	exhausted, err := p.store.ListExhaustedModels(p.now())
	if err != nil {
		return err
	}
	urls := make(map[string]string, len(proxies))
	for _, pr := range proxies {
		if pr.Status == proxyStatusDead {
			// A proxy the probe found dead stays out of egress until a later
			// test round proves it healthy again (the test handler reloads
			// the table, so recovery re-enters without a restart).
			continue
		}
		urls[pr.ID] = pr.URL
	}
	prev := make(map[string]*Account, len(p.accounts))
	for _, a := range p.accounts {
		prev[a.ID] = a
	}
	accounts := make([]*Account, 0, len(recs))
	byID := make(map[string]*Account, len(recs))
	for _, r := range recs {
		a := &Account{
			ID:              r.ID,
			Name:            r.Name,
			JWT:             string(r.Token),
			TokenHint:       r.TokenHint,
			Fingerprint:     r.Fingerprint,
			DeviceMid:       r.DeviceMid,
			ProxyID:         r.ProxyID,
			CreatedAt:       r.CreatedAt,
			PlanExpiresAt:   r.PlanExpiresAt,
			Status:          Status(r.Status),
			StatusReason:    r.StatusReason,
			CooldownUntil:   r.CooldownUntil,
			RemainingQuota:  r.RemainingQuota,
			TotalQuota:      r.TotalQuota,
			QuotaCheckedAt:  r.QuotaCheckedAt,
			LastClaimAt:     r.LastClaimAt,
			LastClaimKind:   r.LastClaimKind,
			RequestsTotal:   r.RequestsTotal,
			FailuresTotal:   r.FailuresTotal,
			InputTokens:     r.InputTokens,
			OutputTokens:    r.OutputTokens,
			LastUsedAt:      r.LastUsedAt,
			ExhaustedModels: exhausted[r.ID],
		}
		if !a.Status.valid() {
			slog.Warn("pool: stored account has unknown status, treating as ACTIVE", "account_id", a.ID, "status", r.Status)
			a.Status = StatusActive
		}
		if old, ok := prev[a.ID]; ok {
			a.Inflight = old.Inflight
			a.FailStreak = old.FailStreak
			a.QuotaStrikes = old.QuotaStrikes
			a.RequestsTotal = old.RequestsTotal
			a.FailuresTotal = old.FailuresTotal
			a.InputTokens = old.InputTokens
			a.OutputTokens = old.OutputTokens
			a.LastUsedAt = old.LastUsedAt
		}
		accounts = append(accounts, a)
		byID[a.ID] = a
	}
	p.accounts = accounts
	p.byID = byID
	p.proxyURLs = urls
	return nil
}

func (p *Pool) reload() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reloadLocked()
}

func (p *Pool) now() time.Time { return p.cfg.Now() }

// SetEventHandler replaces the state-event receiver. Safe for concurrent use.
func (p *Pool) SetEventHandler(h StateHandler) {
	p.mu.Lock()
	p.onState = h
	p.mu.Unlock()
}

// emit delivers events to the handler after Pool.mu was released, so a
// handler may safely call pool accessors.
func (p *Pool) emit(evs []StateEvent) {
	if len(evs) == 0 {
		return
	}
	p.mu.Lock()
	h := p.onState
	p.mu.Unlock()
	if h == nil {
		return
	}
	for _, ev := range evs {
		h(ev)
	}
}

// --- selection and leases ---------------------------------------------------

// Acquire checks out one account for a request. sessionKey is
// metadata.user_id ("" when the client did
// not send one — never guessed); tried holds the ids of accounts already
// tried in this request and they are never returned; model is the resolved
// outbound model ("" when unknown) — accounts whose quota for that model is
// marked exhausted are skipped. On success inflight is
// already incremented; the caller must eventually call Lease.Release exactly
// once, even on error paths.
func (p *Pool) Acquire(sessionKey string, tried map[string]bool, model string) (*Lease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrPoolClosed
	}
	now := p.now()
	a := selectAccount(p.accounts, sessionKey, tried, p.aff, p.cfg.AffinityEnabled, p.cfg.InflightLimit, now, &p.rr, model)
	if a == nil {
		return nil, p.acquireFailureLocked(now, tried, model)
	}
	a.Inflight++
	a.LastUsedAt = now
	return &Lease{p: p, a: a, model: model, proxyURL: p.proxyURLForLocked(a)}, nil
}

// acquireFailureLocked classifies an empty selection. An untried ACTIVE
// account whose quota for model is marked exhausted counts as recovering
// until its per-model deadline, not as busy.
func (p *Pool) acquireFailureLocked(now time.Time, tried map[string]bool, model string) error {
	if len(p.accounts) == 0 {
		return &AcquireError{Failure: AcquireEmpty}
	}
	var (
		earliest      time.Time
		anyRecovering bool
		anyActive     bool
		anyUntried    bool
	)
	for _, a := range p.accounts {
		switch a.Status {
		case StatusCooling, StatusExhausted:
			anyRecovering = true
			if a.CooldownUntil.After(now) && (earliest.IsZero() || a.CooldownUntil.Before(earliest)) {
				earliest = a.CooldownUntil
			}
		case StatusActive:
			anyActive = true
			switch {
			case tried[a.ID]:
			case a.ModelExhausted(model, now):
				anyRecovering = true
				if until := a.ExhaustedModels[model]; earliest.IsZero() || until.Before(earliest) {
					earliest = until
				}
			default:
				anyUntried = true // exists but failed step 1 on INFLIGHT_LIMIT
			}
		}
	}
	if anyRecovering {
		retry := earliest.Sub(now)
		if retry < 0 {
			retry = 0
		}
		return &AcquireError{Failure: AcquireRecovering, RetryAfter: retry}
	}
	if !anyActive {
		return &AcquireError{Failure: AcquireAllDisabled}
	}
	if !anyUntried {
		return &AcquireError{Failure: AcquireTriedAll}
	}
	return &AcquireError{Failure: AcquireBusy}
}

func (p *Pool) proxyURLForLocked(a *Account) string {
	if a.ProxyID != "" {
		if u, ok := p.proxyURLs[a.ProxyID]; ok && u != "" {
			return u
		}
	}
	return p.cfg.DefaultProxyURL
}

// Lease is one checked-out account. Account/AccountID/AccountName stay
// valid through the request; JWT, DeviceMid and ProxyURL are read-only for
// the caller. Release must be called exactly once (it is idempotent beyond
// that). model is the resolved outbound model of the request ("" when
// unknown) — it scopes quota exhaustions to the (account, model) pair.
type Lease struct {
	p        *Pool
	a        *Account
	model    string
	proxyURL string
	inTok    int64
	outTok   int64
	released bool
}

// Account returns the leased account. Only ID/Name/JWT/TokenHint/DeviceMid/
// Fingerprint/ProxyID are stable fields; status fields must be read via the
// pool.
func (l *Lease) Account() *Account { return l.a }

func (l *Lease) AccountID() string {
	if l.a == nil {
		return ""
	}
	return l.a.ID
}

func (l *Lease) AccountName() string {
	if l.a == nil {
		return ""
	}
	return l.a.Name
}

// JWT is the plaintext login token of the leased account — the credential of
// the channel A request.
func (l *Lease) JWT() string {
	if l.a == nil {
		return ""
	}
	return l.a.JWT
}

// DeviceMid is the stable device id of the leased account (header
// X-Device-Mid).
func (l *Lease) DeviceMid() string {
	if l.a == nil {
		return ""
	}
	return l.a.DeviceMid
}

// ProxyURL is the resolved egress proxy for this request: the account's
// dedicated proxy, else the global default, else "" (direct).
func (l *Lease) ProxyURL() string { return l.proxyURL }

// AddUsage records token usage read from the upstream response.
func (l *Lease) AddUsage(inTok, outTok int64) {
	l.p.mu.Lock()
	l.inTok += inTok
	l.outTok += outTok
	l.p.mu.Unlock()
}

// Release returns the lease and applies the upstream outcome to the account:
// counters, fail streak and the state machine. It is idempotent: a second
// Release does nothing.
//
// Chat quota exhaustions take two special paths here: a 1005 on a snapshot
// that still shows quota is the transient minute-level limit (strike
// counting, see quotaStrikeThreshold), and any other model-scoped exhaustion
// benches only the (account, model) pair instead of the whole account.
func (l *Lease) Release(outcome upstream.Outcome) {
	p := l.p
	p.mu.Lock()
	if l.released {
		p.mu.Unlock()
		return
	}
	l.released = true
	a := l.a
	now := p.now()

	if a.Inflight > 0 {
		a.Inflight--
	}
	a.RequestsTotal++
	if isFailureKind(outcome.Kind) {
		a.FailuresTotal++
	}
	a.InputTokens += l.inTok
	a.OutputTokens += l.outTok

	if outcome.AccountAction == upstream.AccountExhausted && l.model != "" {
		if outcome.Kind == upstream.KindQuotaExhausted && a.RemainingQuota != nil && *a.RemainingQuota > 0 {
			// The snapshot still shows quota: this 1005 is the minute-level
			// limit. The first strike only skips the request; the second
			// benches the account briefly and triggers a free billing
			// verification.
			a.QuotaStrikes++
			if a.QuotaStrikes >= quotaStrikeThreshold {
				a.QuotaStrikes = 0
				outcome.Until = now.Add(quotaRecheckBench)
				a.QuotaBenchUntil = outcome.Until
				go p.recheckQuota(a.ID)
				// fall through: the short EXHAUSTED bench below stands.
			} else {
				outcome = upstream.Outcome{Kind: outcome.Kind, AccountAction: upstream.AccountNone, Retryable: true}
			}
		} else {
			// A real model-scoped exhaustion: bench only the (account, model)
			// pair — the account keeps serving its other models.
			if err := p.markExhaustedModelLocked(a, l.model, defensiveUntil(now, outcome.Until)); err != nil {
				slog.Warn("pool: mark model exhausted", "account_id", a.ID, "model", l.model, "err", err)
			}
			outcome = upstream.Outcome{Kind: outcome.Kind, AccountAction: upstream.AccountNone, Retryable: true}
		}
	}

	var evs []StateEvent
	p.applyOutcomeLocked(a, outcome, now, &evs)
	p.mu.Unlock()
	p.emit(evs)
}

// markExhaustedModelLocked records the (account, model) exhaustion deadline in
// RAM and in the store. Must hold Pool.mu.
func (p *Pool) markExhaustedModelLocked(a *Account, model string, until time.Time) error {
	if a.ExhaustedModels == nil {
		a.ExhaustedModels = make(map[string]time.Time)
	}
	a.ExhaustedModels[model] = until
	return p.store.MarkExhaustedModel(a.ID, model, until)
}

// Feedback applies the outcome of an upstream interaction that did not go
// through a lease — claim results, balance queries, admin probes — to the
// account state. acc is an account object previously handed out by the pool
// (its ID is what matters). Unlike Lease.Release there is no model, so an
// EXHAUSTED outcome benches the whole account.
func (p *Pool) Feedback(acc *Account, outcome upstream.Outcome) {
	if acc == nil {
		return
	}
	p.mu.Lock()
	a, ok := p.byID[acc.ID]
	if !ok {
		p.mu.Unlock() // deleted meanwhile; nothing to update
		return
	}
	now := p.now()
	var evs []StateEvent
	p.applyOutcomeLocked(a, outcome, now, &evs)
	p.mu.Unlock()
	p.emit(evs)
}

// applyOutcomeLocked applies the AccountAction of an outcome to an account
// through the state machine. Must hold Pool.mu; it is the single mapping
// shared by Lease.Release, Feedback, RefreshBalance and the claim flow.
func (p *Pool) applyOutcomeLocked(a *Account, outcome upstream.Outcome, now time.Time, evs *[]StateEvent) {
	switch outcome.AccountAction {
	case upstream.AccountResetStreak:
		a.FailStreak = 0
		a.QuotaStrikes = 0

	case upstream.AccountFailStreak:
		// Upstream-side failure: the account is not disabled, but repeated
		// failures put it into a short COOLING.
		a.FailStreak++
		if p.cfg.FailThreshold > 0 && a.FailStreak >= p.cfg.FailThreshold && a.Status == StatusActive {
			until := outcome.Until
			if !until.After(now) {
				until = now.Add(failStreakCooling)
			}
			if err := p.transitionLocked(a, StatusCooling, until, string(outcome.Kind), now, evs); err != nil {
				slog.Warn("pool: apply fail-streak cooldown", "account_id", a.ID, "err", err)
			}
		}

	case upstream.AccountCooling:
		// Rate limit (429/1302/3002): Classify already folded Retry-After and
		// COOLING_DURATION into Outcome.Until.
		if err := p.transitionLocked(a, StatusCooling, defensiveUntil(now, outcome.Until), string(outcome.Kind), now, evs); err != nil {
			slog.Warn("pool: apply cooling outcome", "account_id", a.ID, "err", err)
		}

	case upstream.AccountRiskCooldown:
		// 3012 WAF risk block: isolate the account for Outcome.Until
		// (RISK_COOLDOWN, default 6h) as a COOLING entry.
		if err := p.transitionLocked(a, StatusCooling, defensiveUntil(now, outcome.Until), string(outcome.Kind), now, evs); err != nil {
			slog.Warn("pool: apply risk cooldown outcome", "account_id", a.ID, "err", err)
		}

	case upstream.AccountExhausted:
		// 1005: EXHAUSTED until the parsed next_flush_time (Outcome.Until) or
		// EXHAUSTED_FALLBACK when nothing parsed.
		if err := p.transitionLocked(a, StatusExhausted, defensiveUntil(now, outcome.Until), string(outcome.Kind), now, evs); err != nil {
			slog.Warn("pool: apply exhausted outcome", "account_id", a.ID, "err", err)
		}

	case upstream.AccountDisabled:
		// 1006 / 401/403 / 1000-1004: the JWT is dead; an operator re-enables.
		if err := p.transitionLocked(a, StatusDisabled, time.Time{}, string(outcome.Kind), now, evs); err != nil {
			slog.Warn("pool: apply disabled outcome", "account_id", a.ID, "err", err)
		}

	case upstream.AccountNone:
	}
}

// WithAccount runs fn with the plaintext JWT, the device id and the resolved
// egress proxy URL of account id under the same inflight accounting as a
// lease, then applies the returned outcome through the normal state machine.
// It backs POST /api/accounts/{id}/test: the probe must
// update account state exactly like a proxied request does. fn runs outside
// the pool lock and must not call back into the pool.
func (p *Pool) WithAccount(id string, fn func(acc upstream.ChannelAAccount, proxyURL string) upstream.Outcome) error {
	p.mu.Lock()
	a, ok := p.byID[id]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrAccountNotFound, id)
	}
	acc := upstream.ChannelAAccount{
		JWT:       a.JWT, // never handed out by Pool.Accounts; here it must reach the probe
		DeviceMid: a.DeviceMid,
	}
	proxyURL := p.proxyURLForLocked(a)
	now := p.now()
	a.Inflight++
	a.LastUsedAt = now
	p.mu.Unlock()

	outcome := fn(acc, proxyURL)

	p.mu.Lock()
	if a.Inflight > 0 {
		a.Inflight--
	}
	a.RequestsTotal++
	if isFailureKind(outcome.Kind) {
		a.FailuresTotal++
	}
	var evs []StateEvent
	p.applyOutcomeLocked(a, outcome, now, &evs)
	p.mu.Unlock()
	p.emit(evs)
	return nil
}

// defensiveUntil guards against an Outcome without a usable Until: an account
// may never enter COOLING/EXHAUSTED with a zero cooldown, or only the sweeper
// could release it.
func defensiveUntil(now, until time.Time) time.Time {
	if until.After(now) {
		return until
	}
	return now.Add(failStreakCooling)
}

// isFailureKind decides whether an outcome counts towards failures_total.
// ok is success and client_error is the caller's fault, not the account's.
func isFailureKind(kind upstream.Kind) bool {
	switch kind {
	case upstream.KindOK, upstream.KindClientError, "":
		return false
	default:
		return true
	}
}

// --- balance -----------

// RefreshBalance calls the free /billing/balance endpoint for one account,
// stores the quota snapshot (remaining_quota, total_quota, plan_expires_at)
// and applies the response outcome through the state machine — a 1006 balance
// answer disables the account exactly like a chat answer would. The call is
// free and does not consume quota. The outcome is also returned for callers
// that want the classification.
func (p *Pool) RefreshBalance(ctx context.Context, id string) (upstream.Balance, upstream.Outcome, error) {
	p.mu.Lock()
	a, ok := p.byID[id]
	if !ok {
		p.mu.Unlock()
		return upstream.Balance{}, upstream.Outcome{}, fmt.Errorf("%w: %s", ErrAccountNotFound, id)
	}
	acc := upstream.ChannelAAccount{JWT: a.JWT, DeviceMid: a.DeviceMid}
	proxyURL := p.proxyURLForLocked(a)
	origin := p.cfg.UpstreamURL
	p.mu.Unlock()

	client := billingClient(proxyURL)
	res, err := upstream.QueryBalance(ctx, acc, upstream.WithOrigin(origin), upstream.WithHTTPClient(client))
	if err != nil {
		return upstream.Balance{}, upstream.Outcome{}, fmt.Errorf("pool: balance query: %w", err)
	}

	p.mu.Lock()
	a, ok = p.byID[id]
	if ok {
		// Store what the response carried; nil fields mean the response did
		// not include the quota.
		if res.Balance.RemainingQuota != nil {
			a.RemainingQuota = res.Balance.RemainingQuota
		}
		if res.Balance.TotalQuota != nil {
			a.TotalQuota = res.Balance.TotalQuota
		}
		if !res.Balance.PlanExpiresAt.IsZero() {
			a.PlanExpiresAt = res.Balance.PlanExpiresAt
		}
		a.QuotaCheckedAt = p.now()
		if err := p.store.UpdateAccountQuota(id, a.RemainingQuota, a.TotalQuota, a.PlanExpiresAt, a.QuotaCheckedAt); err != nil {
			slog.Warn("pool: persist balance snapshot", "account_id", id, "err", err)
		}
		// The outcome still goes through the state machine: a 401/1006 on the
		// free balance call is proof of a dead JWT.
		var evs []StateEvent
		p.applyOutcomeLocked(a, res.Outcome, p.now(), &evs)
		// A successful balance answer whose quota bucket has already refilled
		// (balances[].expires_at in the past) proves a reset: an EXHAUSTED
		// account returns to ACTIVE through the single transition path instead
		// of waiting for the sweeper's midnight boundary.
		if res.Outcome.Kind == upstream.KindOK && a.Status == StatusExhausted &&
			!res.Balance.ResetAt.IsZero() && res.Balance.ResetAt.Before(p.now()) {
			if err := p.transitionLocked(a, StatusActive, time.Time{}, "quota_reset_confirmed", p.now(), &evs); err != nil {
				slog.Warn("pool: balance confirmed quota reset", "account_id", id, "err", err)
			}
			p.clearExhaustedModelsLocked(a)
		}
		// A successful balance answer that still shows quota restores an
		// EXHAUSTED account, reset bucket or not — except during a
		// transient-1005 strike bench: the balance cannot see the minute-level
		// limit, so the bench must run out before the account serves again.
		if res.Outcome.Kind == upstream.KindOK && a.Status == StatusExhausted &&
			a.RemainingQuota != nil && *a.RemainingQuota > 0 && !a.QuotaBenchUntil.After(p.now()) {
			if err := p.transitionLocked(a, StatusActive, time.Time{}, "quota_recheck_confirmed", p.now(), &evs); err != nil {
				slog.Warn("pool: balance recheck restored account", "account_id", id, "err", err)
			}
		}
		p.mu.Unlock()
		p.emit(evs)
	} else {
		p.mu.Unlock()
	}
	return res.Balance, res.Outcome, nil
}

// recheckQuota verifies a transient-looking 1005 with a free billing call:
// quota still present → the strike bench runs out and the sweeper restores
// the account; quota really gone → the balance outcome keeps it EXHAUSTED
// until the reported reset.
func (p *Pool) recheckQuota(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, _, err := p.RefreshBalance(ctx, id); err != nil {
		slog.Warn("pool: quota recheck failed", "account_id", id, "err", err)
	}
}

// RefreshBalances refreshes the balance of every ACTIVE account, sequentially.
// It returns the number of accounts whose balance was updated.
func (p *Pool) RefreshBalances(ctx context.Context) int {
	p.mu.Lock()
	ids := make([]string, 0, len(p.accounts))
	for _, a := range p.accounts {
		if a.Status == StatusActive {
			ids = append(ids, a.ID)
		}
	}
	p.mu.Unlock()

	n := 0
	for _, id := range ids {
		if ctx.Err() != nil {
			return n
		}
		if _, _, err := p.RefreshBalance(ctx, id); err == nil {
			n++
		}
	}
	return n
}

// --- state machine ----------------------------------------------------------

// transitionLocked is the single code path for every account status change:
// validated edge, RAM update, DB write, one "state" event. from == to
// only extends a longer cooldown; a cooldown is never shortened by a later
// outcome of the same kind. Must hold Pool.mu.
func (p *Pool) transitionLocked(a *Account, to Status, until time.Time, reason string, now time.Time, evs *[]StateEvent) error {
	if !to.valid() {
		return fmt.Errorf("pool: unknown target status %q", to)
	}
	from := a.Status
	if from == to {
		if to == StatusActive {
			a.QuotaStrikes = 0
		}
		if (to == StatusCooling || to == StatusExhausted) && until.After(a.CooldownUntil) {
			a.CooldownUntil = until
			if err := p.store.UpdateAccountState(a.ID, string(to), a.StatusReason, until); err != nil {
				slog.Warn("pool: persist cooldown extension", "account_id", a.ID, "err", err)
			}
		}
		return nil
	}
	if !transitionAllowed[from][to] {
		return fmt.Errorf("pool: transition %s -> %s is not allowed for account %s", from, to, a.ID)
	}
	a.Status = to
	if to == StatusActive {
		a.StatusReason = ""
		// Per-model exhaustions survive: elapsed cooldown time proves nothing
		// about a weekly/monthly model quota. Only a confirmed reset or an
		// operator re-enable clears them (clearExhaustedModelsLocked).
		a.QuotaStrikes = 0
	} else {
		a.StatusReason = reason
	}
	a.CooldownUntil = until
	if err := p.store.UpdateAccountState(a.ID, string(to), a.StatusReason, until); err != nil {
		slog.Warn("pool: persist state transition", "account_id", a.ID, "from", from, "to", to, "err", err)
	}
	*evs = append(*evs, StateEvent{
		Ts: now, AccountID: a.ID, AccountNme: a.Name, From: from, To: to, Until: until, Kind: a.StatusReason,
	})
	return nil
}

// clearExhaustedModelsLocked drops every per-model exhaustion of an account,
// in RAM and in the store. Must hold Pool.mu.
func (p *Pool) clearExhaustedModelsLocked(a *Account) {
	if len(a.ExhaustedModels) == 0 {
		return
	}
	a.ExhaustedModels = nil
	if err := p.store.ClearExhaustedModels(a.ID); err != nil {
		slog.Warn("pool: clear exhausted models", "account_id", a.ID, "err", err)
	}
}

// --- admin operations (used by the /api/accounts handlers) ------------------

// SetAccountEnabled implements PATCH /api/accounts/{id} "enabled":
// false disables the account for the operator to handle;
// true returns it to ACTIVE and clears the cooldown and fail streak.
// DISABLED only ever leaves through this path.
func (p *Pool) SetAccountEnabled(id string, enabled bool) error {
	p.mu.Lock()
	a, ok := p.byID[id]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrAccountNotFound, id)
	}
	now := p.now()
	var evs []StateEvent
	if enabled {
		a.FailStreak = 0
		err := p.transitionLocked(a, StatusActive, time.Time{}, "", now, &evs)
		p.clearExhaustedModelsLocked(a)
		p.mu.Unlock()
		p.emit(evs)
		if err != nil {
			slog.Warn("pool: enable account", "account_id", id, "err", err)
		}
		return nil
	}
	err := p.transitionLocked(a, StatusDisabled, time.Time{}, "disabled_by_admin", now, &evs)
	p.mu.Unlock()
	p.emit(evs)
	if err != nil {
		slog.Warn("pool: disable account", "account_id", id, "err", err)
	}
	return nil
}

// RenameAccount updates the display name (PATCH /api/accounts/{id} "name").
func (p *Pool) RenameAccount(id, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	a, ok := p.byID[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrAccountNotFound, id)
	}
	if err := p.store.UpdateAccountMeta(id, name, a.ProxyID); err != nil {
		return err
	}
	a.Name = name
	return nil
}

// SetAccountProxy assigns the dedicated egress proxy ("" = unassign) and
// refreshes the RAM proxy table so the next lease resolves the URL.
func (p *Pool) SetAccountProxy(id, proxyID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	a, ok := p.byID[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrAccountNotFound, id)
	}
	if err := p.store.UpdateAccountMeta(id, a.Name, proxyID); err != nil {
		return err
	}
	proxies, err := p.store.ListProxies()
	if err != nil {
		return err
	}
	urls := make(map[string]string, len(proxies))
	for _, pr := range proxies {
		if pr.Status == proxyStatusDead {
			// A proxy the probe found dead stays out of egress until a later
			// test round proves it healthy again (the test handler reloads
			// the table, so recovery re-enters without a restart).
			continue
		}
		urls[pr.ID] = pr.URL
	}
	p.proxyURLs = urls
	a.ProxyID = proxyID
	return nil
}

// DeleteAccount removes an account from the store and the RAM snapshot.
func (p *Pool) DeleteAccount(id string) error {
	if err := p.store.DeleteAccount(id); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	a, ok := p.byID[id]
	if !ok {
		return nil
	}
	delete(p.byID, id)
	for i, aa := range p.accounts {
		if aa == a {
			p.accounts = append(p.accounts[:i], p.accounts[i+1:]...)
			break
		}
	}
	return nil
}

// ImportAccounts parses an import payload ("Name | eyJ..." or bare JWT per
// line), inserts new accounts, counts duplicates by fingerprint and returns
// the line numbers of invalid lines. device_mid UUID v4
// values are generated by the store at insert time.
func (p *Pool) ImportAccounts(raw string) (added, duplicates int, invalid []int, err error) {
	for _, ln := range ParseAccountLines(raw) {
		if ln.Token == "" || !jwtLooksValid(ln.Token) {
			invalid = append(invalid, ln.Line)
			continue
		}
		if _, err := p.store.AddAccount(ln.Name, []byte(ln.Token), "", p.now()); err != nil {
			if errors.Is(err, errDuplicateAccount) {
				duplicates++
				continue
			}
			return added, duplicates, invalid, err
		}
		added++
	}
	if added > 0 {
		if err := p.reload(); err != nil {
			return added, duplicates, invalid, err
		}
	}
	return added, duplicates, invalid, nil
}

// RefreshProxies reloads the proxy URL table (after admin proxy CRUD).
func (p *Pool) RefreshProxies() error { return p.reload() }

// ApplyRuntime applies the runtime settings without a restart. Non-positive
// numeric values keep the previous setting.
func (p *Pool) ApplyRuntime(inflightLimit, failThreshold int, affinityEnabled bool, affinityTTL time.Duration, defaultProxyURL string) {
	p.mu.Lock()
	if inflightLimit > 0 {
		p.cfg.InflightLimit = inflightLimit
	}
	if failThreshold > 0 {
		p.cfg.FailThreshold = failThreshold
	}
	p.cfg.AffinityEnabled = affinityEnabled
	if affinityTTL > 0 {
		p.cfg.AffinityTTL = affinityTTL
		p.aff.SetTTL(affinityTTL)
	}
	p.cfg.DefaultProxyURL = defaultProxyURL
	p.mu.Unlock()
}

// --- introspection (stats endpoints) ----------------------------------------

// NumAccounts returns the number of pooled accounts.
func (p *Pool) NumAccounts() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.accounts)
}

// InflightTotal returns the number of requests currently served by the pool.
func (p *Pool) InflightTotal() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	total := 0
	for _, a := range p.accounts {
		total += a.Inflight
	}
	return total
}

// StatusCounts returns the number of accounts per state.
func (p *Pool) StatusCounts() map[Status]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[Status]int, 4)
	for _, a := range p.accounts {
		out[a.Status]++
	}
	return out
}

// Accounts returns copies of the pooled accounts for display. JWT is cleared
// — full tokens never leave the pool.
func (p *Pool) Accounts() []Account {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Account, 0, len(p.accounts))
	for _, a := range p.accounts {
		c := *a
		c.JWT = ""
		out = append(out, c)
	}
	return out
}

// AccountByID returns a copy of one account without its JWT.
func (p *Pool) AccountByID(id string) (Account, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	a, ok := p.byID[id]
	if !ok {
		return Account{}, false
	}
	c := *a
	c.JWT = ""
	return c, true
}

// claimCandidates snapshots the accounts the claim flow may touch,
// in creation order, with their JWTs (claim.go is in-package). Only ACTIVE
// accounts are claimable — DISABLED/EXHAUSTED are not claimed on their behalf
// unless includeExhausted is set.
func (p *Pool) claimCandidates(includeExhausted bool) []Account {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Account, 0, len(p.accounts))
	for _, a := range p.accounts {
		if a.Status == StatusActive || (includeExhausted && a.Status == StatusExhausted) {
			out = append(out, *a)
		}
	}
	return out
}

// claimAccount returns a copy of one account including its JWT (claim.go is
// in-package: the JWT must reach the claim call, but it never leaves the
// pool package). AccountByID clears the JWT for the dashboard; this does not.
func (p *Pool) claimAccount(id string) (Account, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	a, ok := p.byID[id]
	if !ok {
		return Account{}, false
	}
	return *a, true
}

// setClaimResult records the claim outcome in RAM and in the store.
func (p *Pool) setClaimResult(id string, at time.Time, kind string) {
	p.mu.Lock()
	if a, ok := p.byID[id]; ok {
		a.LastClaimAt = at
		a.LastClaimKind = kind
	}
	p.mu.Unlock()
	if err := p.store.UpdateAccountClaim(id, at, kind); err != nil {
		slog.Warn("claim: persist claim result", "account_id", id, "err", err)
	}
}

// --- background jobs --------------------------------------------------------

// Start launches the sweeper (default 15s: expired COOLING/EXHAUSTED accounts
// return to ACTIVE; no upstream probe is ever sent),
// the flusher (default 60s: counters to SQLite) and, when
// QuotaRefreshInterval is set, the background quota refresh — the free billing
// call that keeps the selector's RemainingQuota from going stale (the
// "ghost-richest" stickiness).
func (p *Pool) Start() {
	p.mu.Lock()
	if p.closed || p.stop != nil {
		p.mu.Unlock()
		return
	}
	stop := make(chan struct{})
	p.stop = stop
	sweepEvery := p.cfg.SweepInterval
	flushEvery := p.cfg.FlushInterval
	quotaEvery := p.cfg.QuotaRefreshInterval
	p.mu.Unlock()

	p.wg.Add(2)
	go p.runLoop(stop, sweepEvery, func() { p.Sweep(p.now()) })
	go p.runLoop(stop, flushEvery, func() { _ = p.Flush() })
	if quotaEvery > 0 {
		// The refresh context dies with stop, so Stop never waits out a whole
		// pass over the pool.
		qctx, qcancel := context.WithCancel(context.Background())
		go func() {
			<-stop
			qcancel()
		}()
		p.wg.Add(1)
		go p.runLoop(stop, quotaEvery, func() { _ = p.RefreshBalances(qctx) })
	}
}

func (p *Pool) runLoop(stop <-chan struct{}, every time.Duration, fn func()) {
	defer p.wg.Done()
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			fn()
		}
	}
}

// Stop stops the background jobs, marks the pool closed (Acquire fails) and
// flushes the counters one last time.
func (p *Pool) Stop() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	stop := p.stop
	p.stop = nil
	p.mu.Unlock()
	if stop != nil {
		close(stop)
	}
	p.wg.Wait()
	if err := p.Flush(); err != nil {
		slog.Warn("pool: final counter flush failed", "err", err)
	}
}

// --- sweeper and flusher (also called directly by tests/fake clocks) --------

// Sweep returns every COOLING/EXHAUSTED account whose cooldown has passed to
// ACTIVE and returns how many accounts recovered.
// DISABLED accounts are never touched — only an operator re-enables them.
func (p *Pool) Sweep(now time.Time) int {
	p.mu.Lock()
	var evs []StateEvent
	n := 0
	for _, a := range p.accounts {
		if !a.CooldownExpired(now) {
			continue
		}
		a.FailStreak = 0
		if err := p.transitionLocked(a, StatusActive, time.Time{}, "", now, &evs); err != nil {
			slog.Warn("pool: sweep account back to ACTIVE", "account_id", a.ID, "err", err)
		}
		n++
	}
	p.mu.Unlock()
	p.emit(evs)
	return n
}

// Flush writes the RAM counters of every account to SQLite.
func (p *Pool) Flush() error {
	type counterSnapshot struct {
		id                 string
		requests, failures int64
		inTok, outTok      int64
		lastUsed           time.Time
	}
	p.mu.Lock()
	snaps := make([]counterSnapshot, 0, len(p.accounts))
	for _, a := range p.accounts {
		snaps = append(snaps, counterSnapshot{
			id:       a.ID,
			requests: a.RequestsTotal,
			failures: a.FailuresTotal,
			inTok:    a.InputTokens,
			outTok:   a.OutputTokens,
			lastUsed: a.LastUsedAt,
		})
	}
	p.mu.Unlock()

	var errs []error
	for _, s := range snaps {
		if err := p.store.FlushAccountCounters(s.id, s.requests, s.failures, s.inTok, s.outTok, s.lastUsed); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// billingClient returns an HTTP client on the shared transport for the given
// egress proxy. Billing calls are short JSON requests
// bounded by their own context timeout inside the upstream package.
func billingClient(proxyURL string) *http.Client {
	return &http.Client{Transport: upstream.TransportFor(proxyURL)}
}

// BillingOrigin returns the pool's channel A origin for the billing helpers
// ("" when unwired — callers that need a real origin must check this first;
// RefreshBalance falls back to the default origin on an empty value).
func (p *Pool) BillingOrigin() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cfg.UpstreamURL
}
