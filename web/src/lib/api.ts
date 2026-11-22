// Admin API client.
//
// - Every request carries `credentials: 'same-origin'` so the z2a_session
// HttpOnly cookie travels with it.
// - Every write (POST/PUT/PATCH/DELETE) carries the CSRF header
// `X-Requested-With: zcode2api` — the server rejects
// writes without it with 403.
// - A 401 anywhere flips the UI to the Login screen through the
// onUnauthorized callback installed by App.svelte.
//
// Routes: the pool is /api/accounts (JWT free accounts), with the claim
// (+ /claim, /{id}/claim) and free balance endpoints (/refresh, /{id}/balance).
// No /api/keys route exists anymore.

import type {
 AccountTestResult,
 AccountView,
 BalanceResult,
 ClaimResult,
 ImportResult,
 LogEntry,
 MappingView,
 ProxyTestResult,
 ProxyView,
 ResolveResult,
 SettingsPatch,
 SettingsView,
 Stats
} from './types';

export class ApiError extends Error {
 readonly status: number;
 constructor(status: number, message: string) {
 super(message);
 this.name = 'ApiError';
 this.status = status;
 }
}

type UnauthorizedHandler = () => void;

let unauthorizedHandler: UnauthorizedHandler | null = null;

/** Installed once by App.svelte: switches the UI to the Login screen. */
export function onUnauthorized(fn: UnauthorizedHandler): void {
 unauthorizedHandler = fn;
}

const CSRF_HEADER = 'X-Requested-With';
const CSRF_VALUE = 'zcode2api';

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
 const init: RequestInit = {
 method,
 credentials: 'same-origin',
 headers: { [CSRF_HEADER]: CSRF_VALUE } as Record<string, string>
 };
 if (body !== undefined) {
 (init.headers as Record<string, string>)['Content-Type'] = 'application/json';
 init.body = JSON.stringify(body);
 }

 let res: Response;
 try {
 res = await fetch(path, init);
 } catch {
 // Network failure (server down). Treated as an error, not a logout.
 throw new ApiError(0, 'network');
 }

 if (res.status === 401 && !path.endsWith('/api/login')) {
 unauthorizedHandler?.();
 throw new ApiError(401, 'unauthorized');
 }

 if (res.status === 204) return undefined as T;

 let payload: unknown = null;
 const text = await res.text();
 if (text) {
 try {
 payload = JSON.parse(text);
 } catch {
 payload = null;
 }
 }

 if (!res.ok) {
 const msg =
 payload && typeof payload === 'object' && 'error' in payload
 ? String((payload as { error: unknown }).error): `HTTP ${res.status}`;
 throw new ApiError(res.status, msg);
 }

 return payload as T;
}

export const api = {
 // --- session ---
 login(password: string): Promise<void> {
 return request<void>('POST', '/api/login', { password });
 },
 logout(): Promise<void> {
 return request<void>('POST', '/api/logout');
 },
 session(): Promise<{ ok: boolean }> {
 return request<{ ok: boolean }>('GET', '/api/session');
 },

 // --- stats ---
 stats(): Promise<Stats> {
 return request<Stats>('GET', '/api/stats');
 },

 // --- JWT accounts ---
 accounts(status = '', q = ''): Promise<{ accounts: AccountView[] }> {
 const p = new URLSearchParams();
 if (status) p.set('status', status);
 if (q) p.set('q', q);
 const qs = p.toString();
 return request<{ accounts: AccountView[] }>('GET', '/api/accounts' + (qs ? `?${qs}`: ''));
 },
 /** Bulk import: each line is "Name | eyJhbGci…" or a bare JWT. */
 importAccounts(lines: string): Promise<ImportResult> {
 return request<ImportResult>('POST', '/api/accounts', { lines });
 },
 patchAccount(
 id: string,
 patch: { name?: string; proxy_id?: string; enabled?: boolean }): Promise<void> {
 return request<void>('PATCH', `/api/accounts/${encodeURIComponent(id)}`, patch);
 },
 /** Free upstream call: refresh quota snapshot + plan expiry of one account. */
 refreshBalance(id: string): Promise<BalanceResult> {
 return request<BalanceResult>('POST', `/api/accounts/${encodeURIComponent(id)}/balance`);
 },
 /** Free background sweep: refresh the balance of every ACTIVE account. */
 refreshAllBalances(): Promise<{ started: boolean }> {
 return request<{ started: boolean }>('POST', '/api/accounts/refresh');
 },
 /** Charged probe: minimal /v1/messages call through one account. */
 testAccount(id: string): Promise<AccountTestResult> {
 return request<AccountTestResult>('POST', `/api/accounts/${encodeURIComponent(id)}/test`);
 },
 /** Claim quota for one account right now. */
 claimAccount(id: string): Promise<ClaimResult> {
 return request<ClaimResult>('POST', `/api/accounts/${encodeURIComponent(id)}/claim`);
 },
 /** Claim-all runs in the background; progress arrives via `event: claim_job`. */
 claimAll(): Promise<{ started: boolean }> {
 return request<{ started: boolean }>('POST', '/api/accounts/claim');
 },
 deleteAccount(id: string): Promise<void> {
 return request<void>('DELETE', `/api/accounts/${encodeURIComponent(id)}`);
 },

 // --- mappings ---
 mappings(): Promise<{ mappings: MappingView[] }> {
 return request<{ mappings: MappingView[] }>('GET', '/api/mappings');
 },
 addMapping(pattern: string, target: string, mode: string): Promise<{ id: string }> {
 return request<{ id: string }>('POST', '/api/mappings', { pattern, target, mode });
 },
 updateMapping(id: string, pattern: string, target: string, mode: string): Promise<void> {
 return request<void>('PUT', `/api/mappings/${encodeURIComponent(id)}`, {
 pattern,
 target,
 mode
 });
 },
 deleteMapping(id: string): Promise<void> {
 return request<void>('DELETE', `/api/mappings/${encodeURIComponent(id)}`);
 },
 resetDefaultMappings(): Promise<void> {
 return request<void>('POST', '/api/mappings/reset-default');
 },
 resolve(model: string): Promise<ResolveResult> {
 return request<ResolveResult>('GET', `/api/mappings/resolve?model=${encodeURIComponent(model)}`);
 },

 // --- proxies ---
 proxies(): Promise<{ proxies: ProxyView[] }> {
 return request<{ proxies: ProxyView[] }>('GET', '/api/proxies');
 },
 addProxy(name: string, url: string): Promise<{ id: string }> {
 return request<{ id: string }>('POST', '/api/proxies', { name, url });
 },
 addProxiesBatch(lines: string): Promise<ImportResult> {
 return request<ImportResult>('POST', '/api/proxies/batch', { lines });
 },
 testProxies(ids: string[] = []): Promise<{ results: ProxyTestResult[] }> {
 return request<{ results: ProxyTestResult[] }>('POST', '/api/proxies/test', { ids });
 },
 deleteProxy(id: string): Promise<void> {
 return request<void>('DELETE', `/api/proxies/${encodeURIComponent(id)}`);
 },

 // --- logs ---
 logs(limit = 200): Promise<{ logs: LogEntry[] }> {
 return request<{ logs: LogEntry[] }>('GET', `/api/logs?limit=${limit}`);
 },
 clearLogs(): Promise<void> {
 return request<void>('DELETE', '/api/logs');
 },

 // --- settings & maintenance ---
 settings(): Promise<SettingsView> {
 return request<SettingsView>('GET', '/api/settings');
 },
 /** Runtime values only; write-only secrets are sent only when non-empty. */
 putSettings(values: SettingsPatch): Promise<void> {
 return request<void>('PUT', '/api/settings', values);
 },
 exportUrl(): string {
 return '/api/settings/export';
 },
 vacuum(): Promise<{ before_bytes: number; after_bytes: number }> {
 return request<{ before_bytes: number; after_bytes: number }>(
 'POST',
 '/api/settings/vacuum');
 }
};

/** Human-readable message for an ApiError, given the current dictionary. */
export function describeError(e: unknown, fmt: (status: number, msg: string) => string): string {
 if (e instanceof ApiError) {
 if (e.status === 0) return 'network';
 return fmt(e.status, e.message);
 }
 return String(e);
}
