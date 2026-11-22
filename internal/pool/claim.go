package pool

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"zcode2api/internal/captcha"
	"zcode2api/internal/config"
	"zcode2api/internal/upstream"
)

// EventClaim is the event name emitted for every per-account claim result;
// the admin log stream sends it to the dashboard as `event: claim`.
const EventClaim = "claim"

// DefaultClaimTime is the CLAIM_TIME default in GMT+8.
const DefaultClaimTime = config.DefaultClaimTime

// claimPauseBetween is the pause between two accounts in a claim-all run.
const claimPauseBetween = 2 * time.Second

// claimSlotWait bounds how long a scheduled claim pass waits for an
// on-demand pass to finish before giving up on that run — the scheduler must
// not skip the day silently, but it must not stack a second pass either.
const claimSlotWait = 30 * time.Second

// solverTimeout bounds one call to the external captcha solver (optional,
// external HTTP) and the embedded node solver per attempt.
const solverTimeout = 30 * time.Second

// captchaTimeout bounds one embedded solver mint attempt: 40s covers the CDN
// bundle load plus the po token.
const captchaTimeout = 40 * time.Second

// solverBodyLimit caps how much of a solver response is read.
const solverBodyLimit = 1 << 16

// claimZone is the fixed GMT+8 timezone the claim schedule lives in —
// CLAIM_TIME is interpreted in GMT+8 regardless of the host timezone.
var claimZone = time.FixedZone("GMT+8", 8*3600)

// ClaimEvent is one per-account claim result, the `event: claim` payload:
// {"ts","acc","ok","kind","captcha_required","next_claim_at"}.
type ClaimEvent struct {
	Ts              int64  `json:"ts"`
	AccountID       string `json:"acc"`
	OK              bool   `json:"ok"`
	Kind            string `json:"kind"`
	CaptchaRequired bool   `json:"captcha_required"`
	// NextClaimAt is data.plan.ends_at of the claim response (unix seconds,
	// 0 when the body carried none): the next-claim instant upstream itself
	// computed.
	NextClaimAt int64 `json:"next_claim_at,omitempty"`
}

// ClaimHandler receives claim events after the account's row was updated.
// Handlers must not block.
type ClaimHandler func(ClaimEvent)

// ClaimJobEvent is one claim-all progress payload — the `event: claim_job`
// stream: phase start (with the account total), tick after each account,
// done/aborted when the pass ends.
type ClaimJobEvent struct {
	Phase string `json:"phase"` // start | tick | done | aborted
	Done  int    `json:"done"`  // processed accounts
	Total int    `json:"total"`
}

// ClaimJobHandler receives claim-all progress events. Handlers must not
// block.
type ClaimJobHandler func(ClaimJobEvent)

// ClaimConfig carries the knobs of the daily claim flow.
// Now/Rand01/Wait are injectable so tests drive the schedule without
// sleeping; HTTPClient points the solver calls at a test server.
type ClaimConfig struct {
	// Enabled is DAILY_CLAIM. A disabled
	// claimer never schedules; ClaimOne/ClaimAll still work on demand.
	Enabled bool
	// ClaimTime is "HH:MM" 24-hour, interpreted in GMT+8.
	ClaimTime string
	// Jitter is CLAIM_JITTER: the maximum random delay added to the schedule
	// so many deployments do not claim in the same second.
	Jitter time.Duration
	// SolverURL is CLAIM_SOLVER_URL — the optional external captcha solver.
	// Empty means accounts that answer 3007 are skipped for the day
	// (counted in claims_captcha_skipped).
	SolverURL string
	// SolverKey is CLAIM_SOLVER_KEY, sent to the solver as a Bearer token when
	// set.
	SolverKey string
	// IncludeExhausted is CLAIM_EXHAUSTED: also claim accounts currently
	// EXHAUSTED.
	IncludeExhausted bool
	// UpstreamURL is the channel A origin for the claim call
	// ({UPSTREAM_URL}/api/v1/zcode-plan/billing/claim).
	UpstreamURL string
	// PlanID is the entitlement the claim posts ({"plan_id":...} body field).
	// Empty keeps the empty-object body.
	PlanID string
	// DataDir is the gateway data directory; the embedded captcha solver
	// materializes its node tree under <DataDir>/captcha_node on first use.
	DataDir string

	// PauseBetween is the pause between two accounts in ClaimAll (default 2s).
	PauseBetween time.Duration

	// Injectable clock pieces.
	Now    func() time.Time
	Rand01 func() float64
	Wait   func(ctx context.Context, d time.Duration) error
	// HTTPClient is used for solver calls; nil = a default client.
	HTTPClient *http.Client
}

func (c *ClaimConfig) setDefaults() error {
	if c.ClaimTime == "" {
		c.ClaimTime = DefaultClaimTime
	}
	if _, _, err := config.ParseClaimTime(c.ClaimTime); err != nil {
		return fmt.Errorf("pool: claim time %q is not HH:MM: %w", c.ClaimTime, err)
	}
	if c.PauseBetween <= 0 {
		c.PauseBetween = claimPauseBetween
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Rand01 == nil {
		c.Rand01 = cryptoRand01
	}
	if c.Wait == nil {
		c.Wait = defaultWait
	}
	return nil
}

// cryptoRand01 returns one float in [0,1) from crypto/rand.
func cryptoRand01() float64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0
	}

	v := binary.LittleEndian.Uint64(b[:]) >> 11
	return float64(v) / (1 << 53)
}

// defaultWait sleeps for d, interruptible by ctx.
func defaultWait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Claimer runs the daily claim flow: one claim call per ACTIVE account at
// CLAIM_TIME (GMT+8) + jitter, with the optional captcha solver for 3007
// answers. Counters (claims_total, claims_captcha_skipped) live in RAM here.
type Claimer struct {
	pool *Pool

	mu      sync.Mutex
	cfg     ClaimConfig
	onClaim ClaimHandler
	onJob   ClaimJobHandler

	solver *captcha.Solver

	// running is the single-flight gate of claim passes: two overlapping
	// passes would call the same billing endpoint for the same accounts
	// (risk-control bait, interleaved events).
	running atomic.Bool

	claimsTotal  int64
	captchaSkips int64

	stop chan struct{}
	wg   sync.WaitGroup
}

// NewClaimer validates the claim configuration. The claimer is inert until
// Run starts the scheduler; on-demand claims (ClaimNow/ClaimOne/ClaimAll)
// work regardless of cfg.Enabled.
func NewClaimer(p *Pool, cfg ClaimConfig) (*Claimer, error) {
	if p == nil {
		return nil, errors.New("pool: NewClaimer: nil pool")
	}
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	// The embedded captcha solver is available whenever a data directory is
	// configured (node stays an external runtime — checked lazily at Solve).
	var solver *captcha.Solver
	if cfg.DataDir != "" {
		solver = captcha.New(captchaTimeout)
	}
	return &Claimer{pool: p, cfg: cfg, solver: solver}, nil
}

// SetClaimHandler registers the event receiver for `event: claim`.
// Safe for concurrent use.
func (c *Claimer) SetClaimHandler(h ClaimHandler) {
	c.mu.Lock()
	c.onClaim = h
	c.mu.Unlock()
}

// SetJobHandler registers the claim-all progress receiver (`event:
// claim_job`). Safe for concurrent use.
func (c *Claimer) SetJobHandler(h ClaimJobHandler) {
	c.mu.Lock()
	c.onJob = h
	c.mu.Unlock()
}

// emitJob delivers one claim-all progress event (nil handler = nobody
// listening). Handlers must not block, so this never holds c.mu while
// calling.
func (c *Claimer) emitJob(ev ClaimJobEvent) {
	c.mu.Lock()
	h := c.onJob
	c.mu.Unlock()
	if h != nil {
		h(ev)
	}
}

// ApplyRuntime applies the runtime settings to the claimer without a restart.
// Empty strings keep the previous value.
func (c *Claimer) ApplyRuntime(enabled bool, claimTime, solverURL, solverKey string, includeExhausted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cfg.Enabled = enabled
	if claimTime != "" {
		if _, _, err := config.ParseClaimTime(claimTime); err == nil {
			c.cfg.ClaimTime = claimTime
		}
	}
	if solverURL != "" {
		c.cfg.SolverURL = solverURL
	}
	if solverKey != "" {
		c.cfg.SolverKey = solverKey
	}
	c.cfg.IncludeExhausted = includeExhausted
}

// ClaimsTotal reports the number of successful claims.
func (c *Claimer) ClaimsTotal() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.claimsTotal
}

// CaptchaSkipped reports claims_captcha_skipped: 3007
// answers that were skipped because no solver was configured (or the solver
// failed).
func (c *Claimer) CaptchaSkipped() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.captchaSkips
}

// Start launches the daily scheduler goroutine. It does nothing when the
// claimer is disabled (DAILY_CLAIM=false) or already running. Stop with Close.
func (c *Claimer) Start(ctx context.Context) {
	c.mu.Lock()
	if c.stop != nil || !c.cfg.Enabled {
		c.mu.Unlock()
		return
	}
	stop := make(chan struct{})
	c.stop = stop
	c.mu.Unlock()

	c.wg.Add(1)
	go c.run(ctx, stop)
}

// Close stops the scheduler goroutine (on-demand claims keep working).
func (c *Claimer) Close() {
	c.mu.Lock()
	stop := c.stop
	c.stop = nil
	c.mu.Unlock()
	if stop != nil {
		close(stop)
	}
	c.wg.Wait()
}

// run is the scheduler loop: compute the next CLAIM_TIME (GMT+8) + jitter,
// wait, claim all eligible accounts, repeat. Each iteration re-reads the
// config so runtime settings changes apply from the next day on.
func (c *Claimer) run(ctx context.Context, stop <-chan struct{}) {
	defer c.wg.Done()
	for {
		c.mu.Lock()
		cfg := c.cfg
		c.mu.Unlock()

		now := cfg.Now()
		hour, minute, _ := config.ParseClaimTime(cfg.ClaimTime)
		next := nextRunAfter(now, hour, minute, cfg.Jitter, cfg.Rand01)

		waitCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			select {
			case <-stop:
				cancel()
			case <-done:
			}
		}()
		err := cfg.Wait(waitCtx, next.Sub(now))
		cancel()
		close(done)
		if err != nil {
			return
		}

		c.ClaimAll(ctx)
	}
}

// nextRunAfter returns the next claim instant strictly after now: the first
// occurrence of hour:minute in GMT+8 at or after now (today's occurrence when
// still ahead, else tomorrow's), delayed by a random jitter in [0, jitter).
func nextRunAfter(now time.Time, hour, minute int, jitter time.Duration, rand01 func() float64) time.Time {
	n := now.In(claimZone)
	run := time.Date(n.Year(), n.Month(), n.Day(), hour, minute, 0, 0, claimZone)
	if !run.After(now) {
		run = run.AddDate(0, 0, 1)
	}
	if jitter > 0 && rand01 != nil {
		run = run.Add(time.Duration(rand01() * float64(jitter)))
	}
	return run
}

// ClaimReport is the result of one account's claim attempt — the payload of
// POST /api/accounts/{id}/claim.
type ClaimReport struct {
	OK              bool   `json:"ok"`
	Kind            string `json:"kind"`
	Claimed         bool   `json:"claimed"`
	CaptchaRequired bool   `json:"captcha_required"`
	// NextClaimAt is data.plan.ends_at of the claim response, zero when the
	// body carried none.
	NextClaimAt time.Time `json:"-"`
}

// ClaimNow runs one full claim pass over every eligible account and returns
// the number of successful claims. It backs `zcode2api claim --now`
// and POST /api/accounts/claim. When a pass is already running it starts
// nothing and reports 0.
func (c *Claimer) ClaimNow(ctx context.Context) int {
	started, done := c.TryStartClaimAll(ctx)
	if !started {
		return 0
	}
	return <-done
}

// TryStartClaimAll takes the single-flight claim gate and, on success, runs
// the whole pass on a fresh goroutine. started=false means a pass is already
// running and nothing was started — the on-demand entry point (the admin
// handler answers "started": false instead of stacking a second pass). The
// claimed count arrives on done when the pass finishes.
func (c *Claimer) TryStartClaimAll(ctx context.Context) (started bool, done <-chan int) {
	if !c.running.CompareAndSwap(false, true) {
		return false, nil
	}
	ch := make(chan int, 1)
	go func() {
		defer c.running.Store(false)
		ch <- c.runClaimPass(ctx)
	}()
	return true, ch
}

// ClaimAll claims every eligible account sequentially, pausing between
// accounts — the scheduler's entry point. Only ACTIVE accounts are claimed —
// DISABLED/EXHAUSTED are never claimed on their behalf unless CLAIM_EXHAUSTED
// is set. A pass never runs concurrently with another: when a pass is already
// running, this waits up to claimSlotWait for it to finish instead of
// skipping the scheduled run silently.
func (c *Claimer) ClaimAll(ctx context.Context) int {
	if !c.running.CompareAndSwap(false, true) {
		if !c.awaitClaimSlot(ctx) {
			return 0
		}
	}
	defer c.running.Store(false)
	return c.runClaimPass(ctx)
}

// awaitClaimSlot polls the single-flight gate until it frees or the bounded
// wait elapses (the gate is taken on success — the caller must release it).
func (c *Claimer) awaitClaimSlot(ctx context.Context) bool {
	c.mu.Lock()
	cfg := c.cfg
	c.mu.Unlock()
	deadline := cfg.Now().Add(claimSlotWait)
	for {
		if err := cfg.Wait(ctx, 250*time.Millisecond); err != nil {
			return false
		}
		if c.running.CompareAndSwap(false, true) {
			return true
		}
		if cfg.Now().After(deadline) {
			return false
		}
	}
}

// runClaimPass is the claim loop proper; the caller holds the single-flight
// gate and releases it afterwards.
func (c *Claimer) runClaimPass(ctx context.Context) int {
	c.mu.Lock()
	cfg := c.cfg
	c.mu.Unlock()

	accounts := c.pool.claimCandidates(cfg.IncludeExhausted)
	c.emitJob(ClaimJobEvent{Phase: "start", Done: 0, Total: len(accounts)})
	claimed := 0
	for i, acc := range accounts {
		if ctx.Err() != nil {
			c.emitJob(ClaimJobEvent{Phase: "aborted", Done: i, Total: len(accounts)})
			return claimed
		}
		if i > 0 && cfg.PauseBetween > 0 {
			if err := cfg.Wait(ctx, cfg.PauseBetween); err != nil {
				c.emitJob(ClaimJobEvent{Phase: "aborted", Done: i, Total: len(accounts)})
				return claimed
			}
		}
		report := c.ClaimOne(ctx, acc.ID)
		if report.Claimed {
			claimed++
		}
		c.emitJob(ClaimJobEvent{Phase: "tick", Done: i + 1, Total: len(accounts)})
	}
	c.emitJob(ClaimJobEvent{Phase: "done", Done: len(accounts), Total: len(accounts)})
	return claimed
}

// ClaimOne claims for one account. With CLAIM_PLAN_ID set it posts exactly
// that plan (the legacy single-claim behavior). With it empty the account's
// own discovery drives the flow: activation telemetry, then every plan the
// marketing delivery offers (each with its captcha retry), each success
// confirmed against its campaign. The result is recorded and emitted as a
// `claim` event; a 1006 during claim disables the account exactly like
// during chat.
func (c *Claimer) ClaimOne(ctx context.Context, id string) ClaimReport {
	c.mu.Lock()
	cfg := c.cfg
	c.mu.Unlock()

	acc, ok := c.pool.claimAccount(id)
	if !ok {
		return ClaimReport{OK: false, Kind: string(upstream.KindUnknown)}
	}
	now := cfg.Now()

	var report ClaimReport
	if cfg.PlanID != "" {
		report = c.claimPlanWithCaptcha(ctx, cfg, acc, cfg.PlanID, "")
	} else {
		report = c.claimDiscoveredPlans(ctx, cfg, acc)
	}
	c.recordClaim(id, now, report)
	return report
}

// claimDiscoveredPlans runs the discovery-driven claim flow of an account
// with CLAIM_PLAN_ID empty:
//  1. activation telemetry (app_launch/app_daily_active/app_login_success) —
//     a failed round never fails the claim, it only skips the report;
//  2. marketing/touch discovery — a transport failure falls back to today's
//     legacy empty-body claim, while a 0-delivery answer stops the flow (no
//     campaign right now is transient and never a cooldown-worthy failure);
//  3. claim every offered plan (the upstream dedups via the idempotent 1003),
//     confirming each success against its delivery's campaign.
//
// The account's report is the first fresh claim, else the last failure, else
// (every plan already claimed) the last idempotent answer.
func (c *Claimer) claimDiscoveredPlans(ctx context.Context, cfg ClaimConfig, acc Account) ClaimReport {
	accA := upstream.ChannelAAccount{JWT: acc.JWT, DeviceMid: acc.DeviceMid}
	if err := upstream.ReportActivationEvents(ctx, accA, upstream.WithOrigin(cfg.UpstreamURL)); err != nil {
		slog.Warn("claim: activation events failed; continuing", "account_id", acc.ID, "err", err)
	}
	plans, err := upstream.FetchOfferedPlans(ctx, accA, upstream.WithOrigin(cfg.UpstreamURL))
	if err != nil {
		slog.Warn("claim: plan discovery failed; falling back to the legacy claim", "account_id", acc.ID, "err", err)
		return c.claimPlanWithCaptcha(ctx, cfg, acc, "", "")
	}
	if len(plans) == 0 {
		slog.Info("claim: no campaign delivery for the account right now", "account_id", acc.ID)
		return ClaimReport{OK: false, Kind: "no_offer"}
	}
	var (
		anyClaimed, anyFailed bool
		best, failed, last    ClaimReport
	)
	for _, plan := range plans {
		if ctx.Err() != nil {
			break
		}
		r := c.claimPlanWithCaptcha(ctx, cfg, acc, plan.PlanID, plan.CampaignID)
		if r.Claimed && !anyClaimed {
			best, anyClaimed = r, true
		}
		if !r.OK {
			failed, anyFailed = r, true
		}
		last = r
	}
	switch {
	case anyClaimed:
		return best
	case anyFailed:
		return failed
	}
	return last
}

// claimPlanWithCaptcha posts one billing/claim and, on 3007, mints a verify
// parameter with the configured solver (embedded first, then the external
// CLAIM_SOLVER_URL) and retries — the retry sends the region header the
// param was minted with. campaignID (empty on the legacy path) gets the
// confirm receipt after a successful claim. Account-state handling follows
// the shared table: a dead JWT or a WAF block during claim still reaches the
// pool, while the claim-scoped business codes (1003/1005/1001/1002/1004) do
// not.
func (c *Claimer) claimPlanWithCaptcha(ctx context.Context, cfg ClaimConfig, acc Account, planID, campaignID string) ClaimReport {
	accA := upstream.ChannelAAccount{JWT: acc.JWT, DeviceMid: acc.DeviceMid}

	res, err := upstream.ClaimQuota(ctx, accA, planID, "", upstream.WithOrigin(cfg.UpstreamURL))
	if err != nil {
		return ClaimReport{OK: false, Kind: string(upstream.KindUpstreamUnavailable)}
	}

	out := res.Outcome
	captcha := res.CaptchaRequired
	if captcha {
		if c.solver != nil && cfg.HTTPClient == nil {
			param, region, serr := c.solver.Solve(ctx, cfg.DataDir)
			if serr == nil {
				slog.Info("claim: embedded captcha solver minted a verify param", "account_id", acc.ID)
				res2, err2 := upstream.ClaimQuota(ctx, accA, planID, param,
					upstream.WithOrigin(cfg.UpstreamURL), upstream.WithRegion(region))
				if err2 == nil {
					return c.claimResult(ctx, cfg, acc, accA, campaignID, res2.Outcome, res2.CaptchaRequired, res2.NextClaimAt)
				}
				slog.Warn("claim: claim retry after embedded solve failed", "account_id", acc.ID)
			} else {
				slog.Warn("claim: embedded captcha solver failed", "account_id", acc.ID, "err", serr)
			}
		}
		if cfg.SolverURL == "" {
			slog.Info("claim: upstream requires a captcha but no solver is configured; skipping account",
				"account_id", acc.ID)
			c.mu.Lock()
			c.captchaSkips++
			c.mu.Unlock()
			return ClaimReport{OK: false, Kind: string(upstream.KindCaptchaRequired), CaptchaRequired: true}
		}
		param, region, serr := c.solveCaptcha(ctx, acc.JWT)
		if region == "" {
			region = solverFallbackRegion()
		}
		if serr != nil {
			slog.Warn("claim: captcha solver failed; skipping account", "account_id", acc.ID)
			c.mu.Lock()
			c.captchaSkips++
			c.mu.Unlock()
			return ClaimReport{OK: false, Kind: string(upstream.KindCaptchaRequired), CaptchaRequired: true}
		}
		res2, err2 := upstream.ClaimQuota(ctx, accA, planID, param,
			upstream.WithOrigin(cfg.UpstreamURL), upstream.WithRegion(region))
		if err2 != nil {
			return ClaimReport{OK: false, Kind: string(upstream.KindUpstreamUnavailable), CaptchaRequired: true}
		}
		return c.claimResult(ctx, cfg, acc, accA, campaignID, res2.Outcome, res2.CaptchaRequired, res2.NextClaimAt)
	}
	return c.claimResult(ctx, cfg, acc, accA, campaignID, out, captcha, res.NextClaimAt)
}

// claimResult finishes one claim answer: the report, the pool feedback for
// the shared-table kinds, and the campaign confirm receipt on a successful
// claim (the receipt only feeds attribution — a lost one must never fail the
// claim).
func (c *Claimer) claimResult(ctx context.Context, cfg ClaimConfig, acc Account, accA upstream.ChannelAAccount, campaignID string, out upstream.Outcome, captcha bool, nextClaimAt time.Time) ClaimReport {
	report := ClaimReport{
		OK:   out.Kind == upstream.KindOK || out.Kind == upstream.KindClaimAlready,
		Kind: string(out.Kind), CaptchaRequired: captcha, NextClaimAt: nextClaimAt,
	}
	if out.Kind == upstream.KindOK {
		report.Claimed = true
		if campaignID != "" {
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
			defer cancel()
			if err := upstream.ConfirmCampaign(cctx, accA, campaignID, upstream.WithOrigin(cfg.UpstreamURL)); err != nil {
				slog.Warn("claim: campaign confirm receipt failed (claim stands)",
					"account_id", acc.ID, "campaign_id", campaignID, "err", err)
			}
		}
	}
	switch out.Kind {
	case upstream.KindAuth, upstream.KindQuotaExhausted, upstream.KindModelQuotaExhausted, upstream.KindRiskBlock, upstream.KindRateLimit:
		c.pool.Feedback(&acc, out)
	}
	return report
}

// recordClaim persists last_claim_at/last_claim_kind, emits the `claim`
// event, then bumps claims_total and refreshes the account's quota snapshot
// right away on success (a successful claim changed the quota upstream — the
// same semantics as the admin scan button, where a dead JWT on the free call
// still disables through the state machine). The event comes first: it is
// the subscriber's signal, and the counting/refresh must not delay it.
func (c *Claimer) recordClaim(id string, now time.Time, report ClaimReport) {
	c.pool.setClaimResult(id, now, report.Kind)
	c.mu.Lock()
	h := c.onClaim
	c.mu.Unlock()
	if h != nil {
		var nextClaim int64
		if !report.NextClaimAt.IsZero() {
			nextClaim = report.NextClaimAt.Unix()
		}
		h(ClaimEvent{
			Ts: now.Unix(), AccountID: id,
			OK: report.OK, Kind: report.Kind, CaptchaRequired: report.CaptchaRequired,
			NextClaimAt: nextClaim,
		})
	}
	if report.Claimed {
		c.mu.Lock()
		c.claimsTotal++
		c.mu.Unlock()
		// The refresh only runs when the pool has a billing origin wired:
		// without one RefreshBalance would fall back to the real channel A
		// origin (the unit setups wire a claim-only fake upstream).
		if c.pool.BillingOrigin() != "" {
			if _, _, err := c.pool.RefreshBalance(context.Background(), id); err != nil {
				slog.Warn("claim: post-claim balance refresh", "account_id", id, "err", err)
			}
		}
	}
}

// solveCaptcha asks the external solver (CLAIM_SOLVER_URL): POST
// {"token": "<JWT>", "endpoint": "claim"}, read verify_param — and the
// optional region the param was minted for — from the answer.
func (c *Claimer) solveCaptcha(ctx context.Context, jwt string) (string, string, error) {
	c.mu.Lock()
	solverURL, solverKey, client := c.cfg.SolverURL, c.cfg.SolverKey, c.cfg.HTTPClient
	c.mu.Unlock()
	if solverURL == "" {
		return "", "", errors.New("claim: no solver URL configured")
	}
	if client == nil {
		client = &http.Client{Timeout: solverTimeout}
	}

	body, err := json.Marshal(map[string]string{"token": jwt, "endpoint": "claim"})
	if err != nil {
		return "", "", fmt.Errorf("claim: encode solver request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, solverURL, bytes.NewReader(body))
	if err != nil {
		return "", "", fmt.Errorf("claim: build solver request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if solverKey != "" {
		req.Header.Set("Authorization", "Bearer "+solverKey)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("claim: solver call: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, solverBodyLimit))
	if err != nil {
		return "", "", fmt.Errorf("claim: read solver response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", fmt.Errorf("claim: solver answered %s", resp.Status)
	}

	var parsed struct {
		VerifyParam string `json:"verify_param"`
		Region      string `json:"region"`
		Data        *struct {
			VerifyParam string `json:"verify_param"`
			Region      string `json:"region"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", "", fmt.Errorf("claim: solver response is not JSON: %w", err)
	}
	if parsed.VerifyParam != "" {
		return parsed.VerifyParam, parsed.Region, nil
	}
	if parsed.Data != nil && parsed.Data.VerifyParam != "" {
		return parsed.Data.VerifyParam, parsed.Data.Region, nil
	}
	return "", "", errors.New("claim: solver response carries no verify_param")
}

// solverFallbackRegion pairs a verify param from a solver that named no
// region with the last known captcha scene region.
func solverFallbackRegion() string { return captcha.CachedRegion() }
