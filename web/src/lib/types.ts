// Shapes of the Admin API responses. Field names match the
// Go structs in internal/admin/handlers.go and internal/admin/logs.go exactly.
//
// The pool holds JWT accounts of ZCode free plans, so every "key" shape of
// the legacy schema became an "account"
// shape: masked JWT (`token`), `device_mid`, `plan_expires_at`, quota
// snapshot and `last_claim_*`. The fallback channel is only ever reported as
// a boolean — the FALLBACK_API_KEY itself never crosses the API.

export type AccountStatus = 'ACTIVE' | 'COOLING' | 'EXHAUSTED' | 'DISABLED';

export interface Stats {
	accounts: { total: number; active: number; cooling: number; exhausted: number; disabled: number };
	/** Channel B indicator: true when FALLBACK_API_KEY is configured. */
	fallback_configured: boolean;
	inflight: number;
	requests_total: number;
	failures_total: number;
	failovers_total: number;
	claims_total?: number;
	claims_captcha_skipped?: number;
	input_tokens: number;
	output_tokens: number;
	uptime_seconds: number;
	version: string;
}

// GET /api/accounts: JWT masked as "…a1b2"; quota fields
// are null until the first /billing/balance check.
export interface AccountView {
	id: string;
	name: string;
	token: string; // masked JWT, e.g. "…a1b2" — never the full token
	device_mid: string;
	proxy_id?: string;
	status: AccountStatus;
	status_reason?: string;
	cooldown_until?: number; // unix seconds, 0/absent = none
	plan_expires_at?: number; // unix seconds, 0/absent = unknown
	remaining_quota?: number | null;
	total_quota?: number | null;
	quota_checked_at?: number;
	inflight: number;
	requests_total: number;
	failures_total: number;
	input_tokens: number;
	output_tokens: number;
	last_claim_at?: number;
	last_claim_kind?: string;
	last_used_at?: number;
	created_at: number;
}

export interface ImportResult {
	added: number;
	duplicates: number;
	invalid: number[];
}

export interface AccountTestResult {
	ok: boolean;
	kind: string;
	latency_ms: number;
}

// POST /api/accounts/{id}/balance response.
export interface BalanceResult {
	remaining_quota: number | null;
	total_quota: number | null;
	plan_expires_at: number;
}

// POST /api/accounts/{id}/claim response.
export interface ClaimResult {
	ok: boolean;
	kind: string;
	claimed: boolean;
	captcha_required: boolean;
}

export interface MappingView {
	id: string;
	pattern: string;
	target?: string;
	mode: 'MAPPED' | 'PASSTHROUGH';
	created_at: number;
}

export interface ResolveResult {
	input: string;
	output: string;
	rule_id: string;
}

export interface ProxyView {
	id: string;
	name: string;
	url: string; // credentials masked
	latency_ms: number; // -1 = untested
	status: 'UNKNOWN' | 'HEALTHY' | 'DEAD';
	created_at: number;
}

export interface ProxyTestResult {
	id: string;
	latency_ms: number;
	status: string;
}

// GET /api/settings. claim_solver_key and
// fallback_api_key are write-only: GET only reports their *_set booleans.
export interface SettingsView {
	runtime: {
		cooling_duration: string;
		risk_cooldown: string;
		exhausted_fallback: string;
		inflight_limit: number;
		max_attempts: number;
		affinity_enabled: boolean;
		affinity_ttl: string;
		strict_models: boolean;
		daily_claim: boolean;
		claim_time: string;
		claim_solver_url: string;
		claim_solver_key_set: boolean;
		fallback_api_key_set: boolean;
	};
	system: {
		host: string;
		port: number;
		upstream_url: string;
		fallback_url: string;
		fallback_configured: boolean;
		db_path: string;
		version: string;
		secret_key_set: boolean;
	};
}

// Editable runtime values accepted by PUT /api/settings. The two
// write-only secrets are only sent when the operator actually typed a value.
export interface SettingsPatch {
	cooling_duration?: string;
	risk_cooldown?: string;
	exhausted_fallback?: string;
	inflight_limit?: number;
	max_attempts?: number;
	affinity_enabled?: boolean;
	affinity_ttl?: string;
	strict_models?: boolean;
	daily_claim?: boolean;
	claim_time?: string;
	claim_solver_url?: string;
	claim_solver_key?: string;
	fallback_api_key?: string;
}

// SSE payloads. The request
// event carries the channel letter ("A" = JWT pool, "B" = fallback key).
export interface RequestEvent {
	ts: number;
	rid: string;
	acc: string; // account id ("" for a fallback-channel attempt)
	name?: string;
	channel?: 'A' | 'B';
	model_in: string;
	model_out: string;
	stream: boolean;
	status: number;
	attempts: number;
	ms: number;
	in_tok: number;
	out_tok: number;
}

export interface FailoverEvent {
	ts: number;
	rid: string;
	from: string;
	to: string;
	kind?: string;
}

export interface StateEvent {
	ts: number;
	acc: string;
	from: string;
	to: string;
	until?: number;
	kind?: string;
}

export interface ClaimEvent {
	ts: number;
	acc: string;
	ok: boolean;
	kind: string;
	captcha_required: boolean;
	/** data.plan.ends_at of the claim response (unix seconds, absent when none). */
	next_claim_at?: number;
}

// `event: claim_job` progress frame: start (with the account total), tick
// after each account, done/aborted when the pass ends.
export interface ClaimJobEvent {
	phase: 'start' | 'tick' | 'done' | 'aborted';
	done: number;
	total: number;
}

// GET /api/logs returns { logs: [{ event, data }] } with data as raw JSON.
export interface LogEntry {
	event: string;
	data: unknown;
}
