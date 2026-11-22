// Shared dashboard state (Svelte 5 runes) + realtime log buffer.
//
// OperationsView/AccountsView consume stats/accounts/logs live; the SSE
// stream (stream.ts) pushes request/failover/state/claim events into the log
// buffer and patches the account table so state changes appear without
// waiting for the next poll.

import { api } from './api';
import { t } from './i18n.svelte';
import { LogStream } from './stream';
import { fmtClock, fmtNum } from './format';
import type {
	AccountView,
	ClaimEvent,
	ClaimJobEvent,
	FailoverEvent,
	LogEntry,
	ProxyView,
	RequestEvent,
	StateEvent,
	Stats,
} from './types';

export type LogLevel = 'INFO' | 'WARN' | 'FAILOVER' | 'CLAIM';

export interface UiLog {
	ts: number; // unix seconds
	timeStr: string; // HH:MM:SS
	level: LogLevel;
	text: string;
}

const LOG_CAP = 500;

class AppState {
	stats = $state<Stats | null>(null);
	accounts = $state<AccountView[]>([]);
	proxies = $state<ProxyView[]>([]);
	logs = $state<UiLog[]>([]);
	streamOk = $state(false);
	inflightLimit = $state(3);
	/** Live claim-all progress (`event: claim_job`); null when no pass runs. */
	claimJob = $state<ClaimJobEvent | null>(null);
	/** Count of `claim_job` frames received; lets callers tell whether SSE already reported a pass. */
	claimJobFrames = 0;

	private stream: LogStream | null = null;
	private pollTimer: ReturnType<typeof setInterval> | null = null;
	private statsTimer: ReturnType<typeof setTimeout> | null = null;

	// --- log buffer ------------------------------------------------------------

	private pushLog(level: LogLevel, tsSec: number, text: string): void {
		const entry: UiLog = { ts: tsSec, timeStr: fmtClock(tsSec), level, text };
		const next = this.logs.length >= LOG_CAP ? this.logs.slice(1) : this.logs.slice();
		next.push(entry);
		this.logs = next;
	}

	/** Replace the buffer with GET /api/logs (called once after login). */
	async loadLogs(): Promise<void> {
		try {
			const res = await api.logs(200);
			const out: UiLog[] = [];
			for (const e of res.logs as LogEntry[]) {
				if (e.event === 'request') this.appendRequest(out, e.data as RequestEvent);
				else if (e.event === 'failover') this.appendFailover(out, e.data as FailoverEvent);
				else if (e.event === 'state') this.appendState(out, e.data as StateEvent);
				else if (e.event === 'claim') this.appendClaim(out, e.data as ClaimEvent);
			}
			this.logs = out.slice(-LOG_CAP);
		} catch {
			// Logs are best-effort; an empty console is fine.
		}
	}

	clearLogs(): void {
		this.logs = [];
	}

	private appendRequest(list: UiLog[], e: RequestEvent): void {
		const level: LogLevel = e.status >= 400 ? 'WARN' : 'INFO';
		list.push({
			ts: e.ts,
			timeStr: fmtClock(e.ts),
			level,
			text: t('log_request', {
				model: e.model_in + '→' + e.model_out,
				acc: e.name || e.acc || 'fallback',
				ch: e.channel ? ` ch=${e.channel}` : '',
				status: e.status,
				attempts: e.attempts,
				ms: e.ms,
				in: fmtNum(e.in_tok),
				out: fmtNum(e.out_tok),
			}),
		});
	}

	private appendFailover(list: UiLog[], e: FailoverEvent): void {
		list.push({
			ts: e.ts,
			timeStr: fmtClock(e.ts),
			level: 'FAILOVER',
			text: t('log_failover', { rid: e.rid, from: e.from, to: e.to, kind: e.kind ?? '?' }),
		});
	}

	private appendState(list: UiLog[], e: StateEvent): void {
		const until = e.until ? t('log_state_until', { t: fmtClock(e.until) }) : '';
		const kind = e.kind ? t('log_kind', { k: e.kind }) : '';
		list.push({
			ts: e.ts,
			timeStr: fmtClock(e.ts),
			level: e.to === 'ACTIVE' ? 'INFO' : 'WARN',
			text: t('log_state', { acc: e.acc, from: e.from, to: e.to, until, kind }),
		});
	}

	private appendClaim(list: UiLog[], e: ClaimEvent): void {
		// Claim results are their own telemetry class: captcha_required accounts were skipped.
		let text: string;
		if (e.ok) text = t('log_claim_ok', { acc: e.acc, kind: e.kind });
		else if (e.captcha_required) text = t('log_claim_captcha', { acc: e.acc });
		else text = t('log_claim_fail', { acc: e.acc, kind: e.kind });
		list.push({ ts: e.ts, timeStr: fmtClock(e.ts), level: 'CLAIM', text });
	}

	// --- data loading ----------------------------------------------------------

	async refreshStats(): Promise<void> {
		try {
			this.stats = await api.stats();
		} catch {
			// handled by the unauthorized hook in api.ts
		}
	}

	/**
	 * Coalesce stats refreshes: SSE bursts (request/failover/state/claim) mark
	 * dirty and one trailing fetch runs ~1s later instead of one GET per event;
	 * the 10s poll remains the floor.
	 */
	private scheduleStatsRefresh(): void {
		if (this.statsTimer !== null) return;
		this.statsTimer = setTimeout(() => {
			this.statsTimer = null;
			void this.refreshStats();
		}, 1000);
	}

	async refreshAccounts(): Promise<void> {
		try {
			const res = await api.accounts();
			this.accounts = res.accounts;
		} catch {
			// handled by the unauthorized hook in api.ts
		}
	}

	async refreshProxies(): Promise<void> {
		try {
			const res = await api.proxies();
			this.proxies = res.proxies;
		} catch {
			// handled by the unauthorized hook in api.ts
		}
	}

	/** Poll /api/stats + /api/accounts every 10s (SSE covers fine-grained deltas). */
	private startPolling(): void {
		this.stopPolling();
		this.pollTimer = setInterval(() => {
			void this.refreshStats();
			void this.refreshAccounts();
		}, 10_000);
	}

	private stopPolling(): void {
		if (this.pollTimer !== null) {
			clearInterval(this.pollTimer);
			this.pollTimer = null;
		}
	}

	// --- SSE wiring ------------------------------------------------------------

	startRealtime(): void {
		this.stopRealtime();
		this.stream = new LogStream({
			request: (e) => {
				this.pushLog(e.status >= 400 ? 'WARN' : 'INFO', e.ts, this.requestText(e));
				this.scheduleStatsRefresh();
			},
			failover: (e) => {
				const list: UiLog[] = this.logs.slice();
				this.appendFailover(list, e);
				this.logs = list.slice(-LOG_CAP);
				this.scheduleStatsRefresh();
			},
			state: (e) => {
				const list: UiLog[] = this.logs.slice();
				this.appendState(list, e);
				this.logs = list.slice(-LOG_CAP);
				this.patchAccountState(e);
				this.scheduleStatsRefresh();
			},
			claim: (e) => {
				const list: UiLog[] = this.logs.slice();
				this.appendClaim(list, e);
				this.logs = list.slice(-LOG_CAP);
				this.patchAccountClaim(e);
				this.scheduleStatsRefresh();
			},
			claim_job: (e) => {
				this.claimJobFrames++;
				// Progress bar state; per-account details still arrive as `claim` events.
				this.claimJob = e.phase === 'done' || e.phase === 'aborted' ? null : e;
			},
			reconnected: () => {
				// Resync anything missed while the stream was down.
				void this.loadLogs();
				void this.refreshStats();
				void this.refreshAccounts();
			},
			status: (ok) => {
				this.streamOk = ok;
			},
		});
		this.stream.start();
		this.startPolling();
		void this.loadInflightLimit();
	}

	/** Read INFLIGHT_LIMIT once from /api/settings for the metric sub-line. */
	async loadInflightLimit(): Promise<void> {
		try {
			const s = await api.settings();
			this.inflightLimit = s.runtime.inflight_limit;
		} catch {
			// keep the default 3
		}
	}

	private requestText(e: RequestEvent): string {
		return t('log_request', {
			model: e.model_in + '→' + e.model_out,
			acc: e.name || e.acc || 'fallback',
			ch: e.channel ? ` ch=${e.channel}` : '',
			status: e.status,
			attempts: e.attempts,
			ms: e.ms,
			in: fmtNum(e.in_tok),
			out: fmtNum(e.out_tok),
		});
	}

	/** Patch one account's status in the table from a `state` event. */
	private patchAccountState(e: StateEvent): void {
		const idx = this.accounts.findIndex((a) => a.id === e.acc);
		if (idx === -1) {
			void this.refreshAccounts();
			return;
		}
		const next = this.accounts.slice();
		next[idx] = {
			...next[idx],
			status: e.to as AccountView['status'],
			status_reason: e.kind ?? next[idx].status_reason,
			cooldown_until: e.until ?? next[idx].cooldown_until,
		};
		this.accounts = next;
	}

	/** Patch one account's claim bookkeeping from a `claim` event. */
	private patchAccountClaim(e: ClaimEvent): void {
		const idx = this.accounts.findIndex((a) => a.id === e.acc);
		if (idx === -1) return;
		const next = this.accounts.slice();
		next[idx] = { ...next[idx], last_claim_at: e.ts, last_claim_kind: e.kind };
		this.accounts = next;
	}

	stopRealtime(): void {
		this.stream?.stop();
		this.stream = null;
		this.stopPolling();
		if (this.statsTimer !== null) {
			clearTimeout(this.statsTimer);
			this.statsTimer = null;
		}
		this.claimJob = null;
		this.streamOk = false;
	}
}

export const store = new AppState();
