<script lang="ts">
 // Operations view: four metric cards
 // (Active / Cooling / Exhausted-1005 / Total balance), the JWT account
 // table, claim status + FALLBACK indicator, and the SSE console.
 // - metric 1308 → 1005 (quota exhausted).
 // - CLAIM QUOTA button (claim-all; per-account CLAIM lives in the rows)
 // plus REFRESH BALANCE (free background sweep).
 // - FALLBACK_API_KEY indicator: configuration status only, never the key.
 import { t } from '../i18n.svelte';
 import { api, ApiError } from '../api';
 import { store } from '../state.svelte';
 import { fmtAgo, fmtClock, fmtCompact, fmtDaysLeft, fmtNum } from '../format';
 import { autoscroll } from '../autoscroll';
 import { showToast } from '../toast.svelte';
 import type { AccountStatus } from '../types';

 interface Props {
 onOpenExpanded: () => void;
 onOpenImport: () => void;
 }
 let { onOpenExpanded, onOpenImport }: Props = $props();

 let consoleEl: HTMLDivElement | undefined = $state();
 let claiming = $state(false);
 let refreshing = $state(false);
 let rowBusyId = $state('');

 const stats = $derived(store.stats);
 const accounts = $derived(store.accounts);

 const fallback = $derived(stats?.fallback_configured ?? false);

 // Quota snapshot totals (null until the first balance check).
 const quota = $derived.by(() => {
 let rem = 0;
 let tot = 0;
 let known = false;
 for (const a of accounts) {
 if (a.remaining_quota != null && a.total_quota != null && a.total_quota > 0) {
 rem += a.remaining_quota;
 tot += a.total_quota;
 known = true;
 }
 }
 const pct = tot > 0 ? Math.round((rem / tot) * 1000) / 10: 0;
 return { rem, tot, known, pct };
 });

 // Most recent claim across the pool (claim-all runs account by account).
 const lastClaim = $derived.by(() => {
 let best = 0;
 let kind = '';
 for (const a of accounts) {
 if ((a.last_claim_at ?? 0) > best) {
 best = a.last_claim_at ?? 0;
 kind = a.last_claim_kind ?? '';
 }
 }
 return { at: best, kind };
 });

 function badgeClass(status: AccountStatus): string {
 switch (status) {
 case 'ACTIVE':
 return 'badge badge-active';
 case 'COOLING':
 return 'badge badge-cooling';
 case 'EXHAUSTED':
 return 'badge badge-exhausted';
 default:
 return 'badge'; // DISABLED: neutral border like the generic badge
 }
 }

 const claimPct = $derived.by(() => {
 const job = store.claimJob;
 if (!job || job.total <= 0) return 0;
 return Math.round((job.done / job.total) * 100);
 });

 function describe(err: unknown): string {
 if (err instanceof ApiError) {
 if (err.status === 0) return t('err_network');
 return t('err_http', { status: err.status, msg: err.message });
 }
 return String(err);
 }

 /** Claim-all runs in the background; progress arrives via `event: claim_job`. */
 async function claimAll(): Promise<void> {
 claiming = true;
 try {
 const framesBefore = store.claimJobFrames;
 const r = await api.claimAll();
 if (r.started) {
 showToast(t('toast_claim_started'));
 // Optimistic bar until the `claim_job` start frame arrives over SSE;
 // skipped once any frame arrived (a fast pass may already be done), so
 // live SSE state is never clobbered or resurrected.
 if (store.claimJobFrames === framesBefore && !store.claimJob && store.accounts.length > 0) {
 store.claimJob = { phase: 'start', done: 0, total: store.accounts.length };
 }
 } else {
 showToast(t('toast_claim_already_running'));
 }
 await store.refreshStats();
 } catch (err) {
 showToast(describe(err));
 } finally {
 claiming = false;
 }
 }

 /** Free background sweep of every ACTIVE account. */
 async function refreshBalances(): Promise<void> {
 refreshing = true;
 try {
 await api.refreshAllBalances();
 showToast(t('toast_balance_started'));
 } catch (err) {
 showToast(describe(err));
 } finally {
 refreshing = false;
 }
 }

 /** Per-row: claim one account right now. */
 async function claimOne(id: string, name: string): Promise<void> {
 rowBusyId = id;
 try {
 const r = await api.claimAccount(id);
 if (r.captcha_required && !r.ok) showToast(t('toast_claim_one_captcha', { name }));
 else if (r.ok) showToast(t('toast_claim_one_ok', { name, kind: r.kind }));
 else showToast(t('toast_claim_one_fail', { name, kind: r.kind }));
 await Promise.all([store.refreshAccounts(), store.refreshStats()]);
 } catch (err) {
 showToast(describe(err));
 } finally {
 rowBusyId = '';
 }
 }

 /** Per-row: free balance probe — refreshes quota + plan expiry. */
 async function balanceOne(id: string, name: string): Promise<void> {
 rowBusyId = id;
 try {
 const b = await api.refreshBalance(id);
 showToast(
 t('toast_balance_ok', {
 name,
 rem: fmtNum(b.remaining_quota ?? 0),
 tot: fmtNum(b.total_quota ?? 0),
 exp: b.plan_expires_at ? fmtClock(b.plan_expires_at): t('plan_unknown')
 }));
 await store.refreshAccounts();
 } catch (err) {
 showToast(describe(err));
 } finally {
 rowBusyId = '';
 }
 }
</script>

<div class="view-content active">
 <!-- Page Heading -->
 <div class="page-heading">
 <div>
 <span class="eyebrow">{t('op_eyebrow')}</span>
 <h1>{t('op_title')}</h1>
 <p>{t('op_desc')}</p>
 </div>
 <div class="heading-actions">
 <button class="btn" onclick={() => void claimAll()} disabled={claiming}>
 <svg class="svg-icon" viewBox="0 0 24 24"><rect x="3" y="11" width="18" height="11" /><path d="M7 11V7a5 5 0 0 1 10 0v4" /></svg>
 <span>{t('btn_claim_quota')}</span>
 </button>
 <button class="btn" onclick={() => void refreshBalances()} disabled={refreshing}>
 <svg class="svg-icon" viewBox="0 0 24 24"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" /></svg>
 <span>{t('btn_check_all')}</span>
 </button>
 <button class="btn btn-primary" onclick={onOpenImport}>
 <svg class="svg-icon" viewBox="0 0 24 24"><line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" /></svg>
 <span>{t('btn_add_tokens')}</span>
 </button>
 </div>
 </div>

 <!-- Claim status + FALLBACK indicator (configuration state only) -->
 <div class="quick-connect-panel">
 <div class="quick-connect-grid">
 <div>
 <span class="muted-label">{t('claims_total', { n: stats?.claims_total ?? 0 })} · {t('claims_captcha_skipped', { n: stats?.claims_captcha_skipped ?? 0 })}</span>
 <div class="endpoint-row">
 <span class="status-dot" class:pulse={lastClaim.at > 0}></span>
 <span style="font-size: 12px; color: var(--ink);">
 {lastClaim.at
 ? t('claim_last', { t: fmtClock(lastClaim.at) }) + t('claim_last_kind', { k: lastClaim.kind || 'ok' }): t('claim_never')}
 </span>
 </div>
 </div>
 <div class="quick-connect-right">
 <span class="system-status" title={t('fallback_hint')}>
 <span class="status-dot" style={fallback ? '': 'background: var(--subtle);'}></span>
 {fallback ? t('fallback_on'): t('fallback_off')}
 </span>
 <span class="hint" style="font-size: 10px;">{t('fallback_hint')}</span>
 </div>
 </div>
 {#if store.claimJob}
 <!-- Claim-all progress (event: claim_job): one pass over the pool. -->
 <div class="claim-progress">
 <div class="claim-progress-bar">
 <div class="claim-progress-fill" style="width: {claimPct}%;"></div>
 </div>
 <span class="claim-progress-label">{store.claimJob.done} / {store.claimJob.total}</span>
 </div>
 {/if}
 </div>

 <!-- Metric Grid (2x2 on mobile, 4 cards on desktop) -->
 <div class="metric-grid">
 <div class="metric-card">
 <span class="metric-label">
 <span>{t('metric_active')}</span>
 <svg class="svg-icon" viewBox="0 0 24 24"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" /><polyline points="22 4 12 14.01 9 11.01" /></svg>
 </span>
 <div class="metric-val" style="color: var(--accent-text);">
 {stats ? `${stats.accounts.active} / ${stats.accounts.total}`: '—'}
 </div>
 <em class="metric-sub positive">
 {t('metric_active_sub', { n: stats?.accounts.active ?? 0, m: store.inflightLimit })}
 </em>
 </div>

 <div class="metric-card">
 <span class="metric-label">
 <span>{t('metric_cooling')}</span>
 <svg class="svg-icon" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" /></svg>
 </span>
 <div class="metric-val" style="color: var(--warning-text);">{stats?.accounts.cooling ?? '—'}</div>
 <em class="metric-sub warning">{t('metric_cooling_sub')}</em>
 </div>

 <div class="metric-card">
 <span class="metric-label">
 <span>{t('metric_exhausted')}</span>
 <svg class="svg-icon" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10" /><line x1="15" y1="9" x2="9" y2="15" /><line x1="9" y1="9" x2="15" y2="15" /></svg>
 </span>
 <div class="metric-val" style="color: var(--danger-text);">{stats?.accounts.exhausted ?? '—'}</div>
 <em class="metric-sub danger">{t('metric_exhausted_sub')}</em>
 </div>

 <div class="metric-card">
 <span class="metric-label">
 <span>{t('metric_balance')}</span>
 <svg class="svg-icon" viewBox="0 0 24 24"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z" /></svg>
 </span>
 <div class="metric-val" style="color: var(--ink);">
 {#if quota.known}
 {fmtCompact(quota.rem)}
 <span style="font-size: 13px; color: var(--muted); font-weight: 400;">/ {fmtCompact(quota.tot)}</span>
 {:else}
 —
 {/if}
 </div>
 <em class="metric-sub {quota.pct >= 20 ? 'positive': 'warning'}">
 {quota.known ? t('metric_balance_sub', { pct: quota.pct }): t('metric_quota_unknown')}
 </em>
 </div>
 </div>

 <!-- Managed JWT Account Pool Table -->
 <div class="panel">
 <div class="panel-header">
 <div>
 <span class="eyebrow">{t('tbl_eyebrow')}</span>
 <h2>{t('tbl_title', { n: accounts.length })}</h2>
 </div>
 </div>
 <div class="table-responsive">
 <table>
 <thead>
 <tr>
 <th style="width: 40px;">#</th>
 <th>{t('th_acc_id')}</th>
 <th>{t('th_status')}</th>
 <th>{t('th_inflight')}</th>
 <th>{t('th_quota')}</th>
 <th>{t('th_expires')}</th>
 <th>{t('th_used')}</th>
 <th style="text-align: right;">{t('th_actions')}</th>
 </tr>
 </thead>
 <tbody>
 {#if accounts.length === 0}
 <tr><td colspan="8" style="color: var(--muted); text-align: center;">{t('accounts_empty')}</td></tr>
 {:else}
 {#each accounts as a, i (a.id)}
 <tr>
 <td style="color: var(--subtle);">{String(i + 1).padStart(2, '0')}</td>
 <td>
 <strong style="color: var(--ink); font-size: 12px;">{a.name || a.id}</strong>
 <div style="color: var(--muted); font-size: 10px; font-family: var(--font-mono); margin-top: 2px;">
 {a.id} · <span style="opacity: 0.8;">{a.token}</span>
 </div>
 </td>
 <td>
 <span class={badgeClass(a.status)}>
 {#if a.status === 'ACTIVE'}<span class="status-dot"></span>{/if}
 {a.status}
 </span>
 </td>
 <td>
 <span style="color: {a.inflight > 0 ? 'var(--accent-text)': 'var(--muted)'}; font-weight: 700;">
 {a.inflight} / {store.inflightLimit}
 </span>
 </td>
 <td>
 {#if a.remaining_quota != null && a.total_quota != null && a.total_quota > 0}
 {@const pct = Math.round((a.remaining_quota / a.total_quota) * 100)}
 <div class="quota-bar-wrapper">
 <div class="quota-bar">
 <div class="quota-fill" class:low={pct < 20} class:empty={pct === 0} style="width: {pct}%;"></div>
 </div>
 <strong>{fmtNum(a.remaining_quota)}</strong>
 <span style="color: var(--muted); font-size: 10px;">({pct}%)</span>
 </div>
 {:else}
 <span style="color: var(--muted);">{t('quota_unknown')}</span>
 {/if}
 </td>
 <td style="color: var(--muted);">
 {#if a.plan_expires_at}
 {fmtDaysLeft(a.plan_expires_at)}d
 {:else}
 {t('plan_unknown')}
 {/if}
 </td>
 <td style="color: var(--muted);">{fmtAgo(a.last_used_at)}</td>
 <td style="text-align: right;">
 <div style="display: inline-flex; gap: 6px;">
 <button class="btn btn-sm" onclick={() => void claimOne(a.id, a.name || a.id)} disabled={rowBusyId === a.id}>
 {rowBusyId === a.id ? t('claiming'): t('btn_claim_one')}
 </button>
 <button class="btn btn-sm" onclick={() => void balanceOne(a.id, a.name || a.id)} disabled={rowBusyId === a.id}>
 {t('btn_balance')}
 </button>
 </div>
 </td>
 </tr>
 {/each}
 {/if}
 </tbody>
 </table>
 </div>
 </div>

 <!-- Realtime Console (SSE: request/failover/state/claim) -->
 <div class="panel">
 <div class="panel-header console-header-wrap">
 <div class="console-header-top">
 <div>
 <span class="eyebrow">{t('log_eyebrow')}</span>
 <h2>{t('log_title')}</h2>
 </div>
 <button class="btn btn-sm btn-primary" onclick={onOpenExpanded} title={t('exp_log_title')} style="flex-shrink: 0;">
 <svg class="svg-icon" viewBox="0 0 24 24"><polyline points="15 3 21 3 21 9" /><polyline points="9 21 3 21 3 15" /><line x1="21" y1="3" x2="14" y2="10" /><line x1="3" y1="21" x2="10" y2="14" /></svg>
 <span>{t('btn_expand_log')}</span>
 </button>
 </div>
 <div class="console-header-controls">
 <button class="btn btn-sm" onclick={() => store.clearLogs()}>
 <svg class="svg-icon" viewBox="0 0 24 24"><polyline points="3 6 5 6 21 6" /><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /></svg>
 {t('btn_clear_console')}
 </button>
 </div>
 </div>
 <div class="console-box" id="console-stream" bind:this={consoleEl} use:autoscroll={{ deps: store.logs.length }}>
 {#each store.logs as line, i (i)}
 <div class="log-line">
 <span class="log-time">{line.timeStr}</span>
 <span class="log-{line.level.toLowerCase()}">[{line.level}]</span>
 <span class="log-text">{line.text}</span>
 </div>
 {/each}
 </div>
 </div>
</div>
