<script lang="ts">
 // Settings view: accordion with the chevron on
 // the left.
 // - host / port / upstream / fallback_url are READ-ONLY (from ENV).
 // - no captcha header on chat, no request hold.
 // - daily_claim is a runtime toggle (section 3) alongside claim_time,
 // CLAIM_SOLVER_URL/KEY and the write-only FALLBACK_API_KEY.
 // - risk_cooldown stays a runtime knob — fixed
 // isolation for 3012, no proxy rotation.
 import { t } from '../i18n.svelte';
 import { api, ApiError } from '../api';
 import { store } from '../state.svelte';
 import { copyText, downloadFile, fmtNum } from '../format';
 import { showToast } from '../toast.svelte';
 import type { MappingView, SettingsPatch, SettingsView as Settings } from '../types';

 interface Props {
 onOpenMapping: (m: MappingView | null) => void;
 }
 let { onOpenMapping }: Props = $props();

 let settings = $state<Settings | null>(null);
 let loadErr = $state('');

 // Editable copies of the pool-runtime values (section 2).
 let cooling = $state('60s');
 let riskCooldown = $state('6h');
 let exhaustedFallback = $state('1h');
 let inflight = $state(3);
 let maxAttempts = $state(3);
 let affinityEnabled = $state(true);
 let affinityTtl = $state('30m');
 let strictModels = $state(false);
 let saving = $state(false);

 // Claim & fallback (section 3). The two secrets are write-only: they are
 // only included in the PUT when the operator actually typed a value.
 let dailyClaim = $state(true);
 let claimTime = $state('00:05');
 let solverUrl = $state('');
 let solverKey = $state('');
 let fallbackKey = $state('');
 let savingClaim = $state(false);

 // Mappings
 let mappings = $state<MappingView[]>([]);

 // Resolve try-out box.
 let resolveInput = $state('');
 let resolveOut = $state('');
 let resolveOk = $state<boolean | null>(null);
 let resolving = $state(false);

 // Accordion sections (open by default).
 let open1 = $state(true);
 let open2 = $state(true);
 let open3 = $state(true);
 let open4 = $state(true);
 let open5 = $state(true);

 function describe(err: unknown): string {
 if (err instanceof ApiError) {
 if (err.status === 0) return t('err_network');
 return t('err_http', { status: err.status, msg: err.message });
 }
 return String(err);
 }

 async function load(): Promise<void> {
 try {
 const s = await api.settings();
 settings = s;
 loadErr = '';
 cooling = s.runtime.cooling_duration;
 riskCooldown = s.runtime.risk_cooldown;
 exhaustedFallback = s.runtime.exhausted_fallback;
 inflight = s.runtime.inflight_limit;
 maxAttempts = s.runtime.max_attempts;
 affinityEnabled = s.runtime.affinity_enabled;
 affinityTtl = s.runtime.affinity_ttl;
 strictModels = s.runtime.strict_models;
 dailyClaim = s.runtime.daily_claim;
 claimTime = s.runtime.claim_time;
 solverUrl = s.runtime.claim_solver_url;
 solverKey = '';
 fallbackKey = '';
 store.inflightLimit = s.runtime.inflight_limit;
 const m = await api.mappings();
 mappings = m.mappings;
 } catch (err) {
 loadErr = describe(err);
 }
 }

 // Reload when the view mounts.
 $effect(() => {
 void load();
 });

 async function saveRuntime(): Promise<void> {
 saving = true;
 try {
 await api.putSettings({
 cooling_duration: cooling,
 risk_cooldown: riskCooldown,
 exhausted_fallback: exhaustedFallback,
 inflight_limit: inflight,
 max_attempts: maxAttempts,
 affinity_enabled: affinityEnabled,
 affinity_ttl: affinityTtl,
 strict_models: strictModels
 });
 showToast(t('toast_runtime_saved'));
 store.inflightLimit = inflight;
 await load();
 } catch (err) {
 showToast(describe(err));
 } finally {
 saving = false;
 }
 }

 async function saveClaimSettings(): Promise<void> {
 savingClaim = true;
 try {
 const patch: SettingsPatch = {
 daily_claim: dailyClaim,
 claim_time: claimTime,
 claim_solver_url: solverUrl
 };
 // Write-only secrets: send only when a new value was typed.
 if (solverKey.trim()) patch.claim_solver_key = solverKey.trim();
 if (fallbackKey.trim()) patch.fallback_api_key = fallbackKey.trim();
 await api.putSettings(patch);
 showToast(t('toast_runtime_saved'));
 await load();
 } catch (err) {
 showToast(describe(err));
 } finally {
 savingClaim = false;
 }
 }

 async function deleteMapping(m: MappingView): Promise<void> {
 try {
 await api.deleteMapping(m.id);
 showToast(t('toast_mapping_deleted', { pattern: m.pattern }));
 await load();
 } catch (err) {
 showToast(describe(err));
 }
 }

 async function resetDefaults(): Promise<void> {
 try {
 await api.resetDefaultMappings();
 showToast(t('toast_mapping_reset'));
 await load();
 } catch (err) {
 showToast(describe(err));
 }
 }

 async function resolve(): Promise<void> {
 if (!resolveInput.trim()) return;
 resolving = true;
 try {
 const r = await api.resolve(resolveInput.trim());
 resolveOut = r.rule_id ? t('resolve_result', { in: r.input, out: r.output }): t('resolve_no_rule');
 resolveOk = !!r.rule_id;
 } catch (err) {
 resolveOut = describe(err);
 resolveOk = false;
 } finally {
 resolving = false;
 }
 }

 async function exportJson(): Promise<void> {
 try {
 // Fetch through the same-origin session; the response is the file.
 const res = await fetch(api.exportUrl(), { credentials: 'same-origin' });
 if (!res.ok) throw new ApiError(res.status, `HTTP ${res.status}`);
 const text = await res.text();
 downloadFile('zcode2api-export.json', text, 'application/json');
 showToast(t('toast_export'));
 } catch (err) {
 showToast(describe(err));
 }
 }

 async function vacuum(): Promise<void> {
 try {
 const r = await api.vacuum();
 showToast(t('toast_vacuum', { b: fmtNum(r.before_bytes), a: fmtNum(r.after_bytes) }));
 } catch (err) {
 showToast(describe(err));
 }
 }

 async function clearServerLogs(): Promise<void> {
 try {
 await api.clearLogs();
 store.clearLogs();
 showToast(t('toast_logs_cleared'));
 } catch (err) {
 showToast(describe(err));
 }
 }

 async function copyValue(v: string): Promise<void> {
 if (await copyText(v)) showToast(t('toast_copied'));
 }
</script>

<div class="view-content active">
 <div class="page-heading">
 <div>
 <span class="eyebrow">{t('set_eyebrow')}</span>
 <h1>{t('set_title')}</h1>
 <p>{t('set_desc')}</p>
 </div>
 <div class="heading-actions">
 <button class="btn btn-sm" onclick={() => { open1 = open2 = open3 = open4 = open5 = true; }}>
 <svg class="svg-icon" viewBox="0 0 24 24"><polyline points="7 13 12 18 17 13" /><polyline points="7 6 12 11 17 6" /></svg>
 <span>{t('btn_expand_all')}</span>
 </button>
 <button class="btn btn-sm" onclick={() => { open1 = open2 = open3 = open4 = open5 = false; }}>
 <svg class="svg-icon" viewBox="0 0 24 24"><polyline points="17 11 12 6 7 11" /><polyline points="17 18 12 13 7 18" /></svg>
 <span>{t('btn_collapse_all')}</span>
 </button>
 </div>
 </div>

 {#if loadErr}
 <div class="login-error">{loadErr}</div>
 {/if}

 <!-- Section 1: read-only system & upstream information -->
 <div class="settings-section" class:collapsed={!open1}>
 <div
 class="settings-section-head"
 role="button"
 tabindex="0"
 onclick={() => (open1 = !open1)}
 onkeydown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); open1 = !open1; } }}
 >
 <div class="title-group">
 <svg class="svg-icon chevron-left" viewBox="0 0 24 24"><polyline points="9 18 15 12 9 6" /></svg>
 <svg class="svg-icon" viewBox="0 0 24 24"><rect x="2" y="2" width="20" height="8" rx="2" ry="2" /><rect x="2" y="14" width="20" height="8" rx="2" ry="2" /><line x1="6" y1="6" x2="6.01" y2="6" /><line x1="6" y1="18" x2="6.01" y2="18" /></svg>
 <h3>{t('sec1_title')}</h3>
 </div>
 </div>
 <div class="settings-body">
 <div class="settings-grid">
 <div class="form-group">
 <label>{t('lbl_host')}</label>
 <input type="text" class="form-control" readonly value={settings?.system.host ?? '—'} />
 <span class="hint">{t('hint_host')}</span>
 </div>
 <div class="form-group">
 <label>{t('lbl_port')}</label>
 <input type="text" class="form-control" readonly value={settings ? String(settings.system.port): '—'} />
 <span class="hint">{t('hint_port')}</span>
 </div>
 <div class="form-group">
 <label>{t('lbl_upstream')}</label>
 <input type="text" class="form-control" readonly value={settings?.system.upstream_url ?? '—'} />
 <span class="hint">{t('hint_upstream')}</span>
 </div>
 <div class="form-group">
 <label>{t('lbl_fallback')}</label>
 <input type="text" class="form-control" readonly value={settings?.system.fallback_url ?? '—'} />
 <span class="hint">{t('hint_fallback')}</span>
 </div>
 <div class="form-group">
 <label>{t('lbl_fallback_key')}</label>
 <input
 type="text"
 class="form-control"
 readonly
 value={settings ? (settings.system.fallback_configured ? t('fallback_key_set'): t('fallback_key_missing')): '—'}
 style="color: {settings?.system.fallback_configured ? 'var(--accent-text)': 'var(--subtle)'};"
 />
 <span class="hint">{t('hint_fallback_key')}</span>
 </div>
 <div class="form-group">
 <label>{t('lbl_db_path')}</label>
 <input type="text" class="form-control" readonly value={settings?.system.db_path ?? '—'} />
 <span class="hint">{t('hint_db_path')}</span>
 </div>
 <div class="form-group">
 <label>{t('lbl_version')}</label>
 <input type="text" class="form-control" readonly value={settings?.system.version ?? '—'} />
 </div>
 <div class="form-group">
 <label>{t('lbl_secret_key')}</label>
 <input
 type="text"
 class="form-control"
 readonly
 value={settings ? (settings.system.secret_key_set ? t('secret_key_set'): t('secret_key_missing')): '—'}
 style="color: {settings?.system.secret_key_set ? 'var(--accent-text)': 'var(--warning-text)'};"
 />
 </div>
 <div class="form-group">
 <label>{t('lbl_client_keys')}</label>
 <input type="text" class="form-control" readonly value="ENV (masked)" />
 <span class="hint">{t('hint_client_keys')}</span>
 </div>
 </div>
 </div>
 </div>

 <!-- Section 2: pool runtime parameters -->
 <div class="settings-section" class:collapsed={!open2}>
 <div
 class="settings-section-head"
 role="button"
 tabindex="0"
 onclick={() => (open2 = !open2)}
 onkeydown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); open2 = !open2; } }}
 >
 <div class="title-group">
 <svg class="svg-icon chevron-left" viewBox="0 0 24 24"><polyline points="9 18 15 12 9 6" /></svg>
 <svg class="svg-icon" viewBox="0 0 24 24"><path d="M4 15s1-1 4-1 5 2 8 2 4-1 4-1V3s-1 1-4 1-5-2-8-2-4 1-4 1z" /><line x1="4" y1="22" x2="4" y2="15" /></svg>
 <h3>{t('sec2_title')}</h3>
 </div>
 <div class="sec-actions" role="presentation" onclick={(e) => e.stopPropagation()}>
 <button class="btn btn-sm btn-primary" onclick={() => void saveRuntime()} disabled={saving}>
 <svg class="svg-icon" viewBox="0 0 24 24"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z" /><polyline points="17 21 17 13 7 13 7 21" /><polyline points="7 3 7 8 15 8" /></svg>
 <span>{t('btn_save_runtime')}</span>
 </button>
 </div>
 </div>
 <div class="settings-body">
 <div class="settings-grid">
 <div class="form-group">
 <label>{t('lbl_cooling')}</label>
 <input type="text" class="form-control" bind:value={cooling} />
 <span class="hint">{t('hint_cooling')}</span>
 </div>
 <div class="form-group">
 <label>{t('lbl_risk')}</label>
 <input type="text" class="form-control" bind:value={riskCooldown} />
 <span class="hint">{t('hint_risk')}</span>
 </div>
 <div class="form-group">
 <label>{t('lbl_exhausted')}</label>
 <input type="text" class="form-control" bind:value={exhaustedFallback} />
 </div>
 <div class="form-group">
 <label>{t('lbl_inflight')}</label>
 <input type="number" min="1" max="32" class="form-control" bind:value={inflight} />
 <span class="hint">{t('hint_inflight')}</span>
 </div>
 <div class="form-group">
 <label>{t('lbl_max_attempts')}</label>
 <input type="number" min="1" max="10" class="form-control" bind:value={maxAttempts} />
 </div>
 <div class="form-group">
 <label>{t('lbl_affinity_ttl')}</label>
 <input type="text" class="form-control" bind:value={affinityTtl} />
 </div>
 <div class="form-group">
 <label style="visibility: hidden;">.</label>
 <span class="hint">{t('runtime_hint')}</span>
 </div>
 </div>

 <div class="toggle-row">
 <div class="toggle-info">
 <strong>{t('tog_affinity')}</strong>
 <span>{t('tog_affinity_sub')}</span>
 </div>
 <label class="switch">
 <input type="checkbox" bind:checked={affinityEnabled} />
 <span class="slider"></span>
 </label>
 </div>

 <div class="toggle-row">
 <div class="toggle-info">
 <strong>{t('tog_strict_models')}</strong>
 <span>{t('tog_strict_models_sub')}</span>
 </div>
 <label class="switch">
 <input type="checkbox" bind:checked={strictModels} />
 <span class="slider"></span>
 </label>
 </div>
 </div>
 </div>

 <!-- Section 3: quota claim & fallback channel -->
 <div class="settings-section" class:collapsed={!open3}>
 <div
 class="settings-section-head"
 role="button"
 tabindex="0"
 onclick={() => (open3 = !open3)}
 onkeydown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); open3 = !open3; } }}
 >
 <div class="title-group">
 <svg class="svg-icon chevron-left" viewBox="0 0 24 24"><polyline points="9 18 15 12 9 6" /></svg>
 <svg class="svg-icon" viewBox="0 0 24 24"><polygon points="12 2 2 7 12 12 22 7 12 2" /><polyline points="2 17 12 22 22 17" /><polyline points="2 12 12 17 22 12" /></svg>
 <h3>{t('sec_claim_title')}</h3>
 </div>
 <div class="sec-actions" role="presentation" onclick={(e) => e.stopPropagation()}>
 <button class="btn btn-sm btn-primary" onclick={() => void saveClaimSettings()} disabled={savingClaim}>
 <svg class="svg-icon" viewBox="0 0 24 24"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z" /><polyline points="17 21 17 13 7 13 7 21" /><polyline points="7 3 7 8 15 8" /></svg>
 <span>{t('btn_save_runtime')}</span>
 </button>
 </div>
 </div>
 <div class="settings-body">
 <div class="settings-grid">
 <div class="form-group">
 <label>{t('lbl_claim_time')}</label>
 <input type="text" class="form-control" bind:value={claimTime} placeholder="00:05" />
 <span class="hint">{t('hint_claim_time')}</span>
 </div>
 <div class="form-group">
 <label>{t('lbl_solver_url')}</label>
 <input type="text" class="form-control" bind:value={solverUrl} placeholder="https://solver.example/verify" />
 <span class="hint">{t('hint_solver_url')}</span>
 </div>
 <div class="form-group">
 <label>{t('lbl_solver_key')}</label>
 <input type="password" class="form-control" bind:value={solverKey} placeholder="•••••••• ({settings?.runtime.claim_solver_key_set ? t('solver_key_set'): t('solver_key_missing')})" />
 <span class="hint">{t('hint_solver_key')}</span>
 </div>
 <div class="form-group">
 <label>{t('lbl_fallback_key')}</label>
 <input type="password" class="form-control" bind:value={fallbackKey} placeholder="•••••••• ({settings ? (settings.runtime.fallback_api_key_set ? t('fallback_key_set'): t('fallback_key_missing')): ''})" />
 <span class="hint">{t('hint_fallback_key')}</span>
 </div>
 </div>

 <!-- daily_claim: runtime toggle, no restart needed -->
 <div class="toggle-row">
 <div class="toggle-info">
 <strong>{t('tog_claim')}</strong>
 <span>{t('tog_claim_sub')}</span>
 </div>
 <label class="switch">
 <input type="checkbox" bind:checked={dailyClaim} />
 <span class="slider"></span>
 </label>
 </div>
 </div>
 </div>

 <!-- Section 4: model mapping CRUD + resolve try-out -->
 <div class="settings-section" class:collapsed={!open4}>
 <div
 class="settings-section-head"
 role="button"
 tabindex="0"
 onclick={() => (open4 = !open4)}
 onkeydown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); open4 = !open4; } }}
 >
 <div class="title-group">
 <svg class="svg-icon chevron-left" viewBox="0 0 24 24"><polyline points="9 18 15 12 9 6" /></svg>
 <svg class="svg-icon" viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" /><polyline points="14 2 14 8 20 8" /><line x1="16" y1="13" x2="8" y2="13" /><line x1="16" y1="17" x2="8" y2="17" /><polyline points="10 9 9 9 8 9" /></svg>
 <h3>{t('sec_map_title')}</h3>
 </div>
 <div class="sec-actions" role="presentation" onclick={(e) => e.stopPropagation()}>
 <button class="btn btn-sm" onclick={() => void resetDefaults()}>
 <svg class="svg-icon" viewBox="0 0 24 24"><polyline points="1 4 1 10 7 10" /><path d="M3.51 15a9 9 0 1 0 2.13-9.36L1 10" /></svg>
 <span>{t('btn_load_defaults')}</span>
 </button>
 <button class="btn btn-sm btn-primary" onclick={() => onOpenMapping(null)}>
 <svg class="svg-icon" viewBox="0 0 24 24"><line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" /></svg>
 <span>{t('btn_add_mapping')}</span>
 </button>
 </div>
 </div>
 <div class="settings-body">
 <!-- "Try resolve" box -->
 <div class="quick-connect-panel">
 <div class="quick-connect-grid">
 <div>
 <span class="muted-label">{t('lbl_resolve')}</span>
 <div class="endpoint-row">
 <input
 type="text"
 class="search-input"
 style="flex: 1; min-width: 180px;"
 bind:value={resolveInput}
 placeholder={t('resolve_placeholder')}
 onkeydown={(e) => { if (e.key === 'Enter') void resolve(); }}
 />
 <button class="btn btn-sm btn-primary" onclick={() => void resolve()} disabled={resolving}>
 {t('btn_resolve')}
 </button>
 </div>
 </div>
 {#if resolveOut}
 <div>
 <span class="muted-label">&rarr;</span>
 <div class="endpoint-row">
 <code class="endpoint-code" style="color: {resolveOk ? 'var(--accent-text)': 'var(--warning-text)'};">{resolveOut}</code>
 </div>
 </div>
 {/if}
 </div>
 </div>

 <div class="table-responsive">
 <table>
 <thead>
 <tr>
 <th>{t('th_inbound')}</th>
 <th style="width: 30px;"></th>
 <th>{t('th_outbound')}</th>
 <th>{t('th_map_status')}</th>
 <th style="width: 140px; text-align: right;">{t('th_actions')}</th>
 </tr>
 </thead>
 <tbody>
 {#each mappings as m (m.id)}
 <tr>
 <td><code>{m.pattern}</code></td>
 <td style="color: var(--muted); text-align: center;">&rarr;</td>
 <td style="color: var(--accent-text);"><code>{m.mode === 'PASSTHROUGH' ? '—': m.target}</code></td>
 <td>
 <span class="badge {m.mode === 'MAPPED' ? 'badge-active': 'badge-cooling'}">{m.mode}</span>
 </td>
 <td style="text-align: right;">
 <div style="display: inline-flex; gap: 6px;">
 <button class="btn btn-sm" onclick={() => onOpenMapping(m)}>EDIT</button>
 <button class="btn btn-sm btn-danger" onclick={() => void deleteMapping(m)}>&times;</button>
 </div>
 </td>
 </tr>
 {/each}
 </tbody>
 </table>
 </div>
 </div>
 </div>

 <!-- Section 5: storage & maintenance -->
 <div class="settings-section" class:collapsed={!open5}>
 <div
 class="settings-section-head"
 role="button"
 tabindex="0"
 onclick={() => (open5 = !open5)}
 onkeydown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); open5 = !open5; } }}
 >
 <div class="title-group">
 <svg class="svg-icon chevron-left" viewBox="0 0 24 24"><polyline points="9 18 15 12 9 6" /></svg>
 <svg class="svg-icon" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><polyline points="7 10 12 15 17 10" /><line x1="12" y1="15" x2="12" y2="3" /></svg>
 <h3>{t('sec_storage_title')}</h3>
 </div>
 </div>
 <div class="settings-body">
 <div class="db-actions-group">
 <button class="btn" onclick={() => void exportJson()}>
 <svg class="svg-icon" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><polyline points="7 10 12 15 17 10" /><line x1="12" y1="15" x2="12" y2="3" /></svg>
 <span>{t('btn_export_json')}</span>
 </button>
 <button class="btn" onclick={() => void vacuum()}>
 <svg class="svg-icon" viewBox="0 0 24 24"><path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z" /></svg>
 <span>{t('btn_vacuum')}</span>
 </button>
 <button class="btn btn-danger" onclick={() => void clearServerLogs()}>
 <svg class="svg-icon" viewBox="0 0 24 24"><polyline points="3 6 5 6 21 6" /><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" /></svg>
 <span>{t('btn_clear_logs')}</span>
 </button>
 </div>
 </div>
 </div>
</div>
