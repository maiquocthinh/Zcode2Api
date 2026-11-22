<script lang="ts">
	// Accounts view: full inventory of JWT free accounts —
	// search, status filter, rename, proxy assignment, claim, balance, enable/
	// disable, probe and delete. The JWT column only ever
	// shows the server-provided mask ("…a1b2").
	import { t } from '../i18n.svelte';
	import { api, ApiError } from '../api';
	import { store } from '../state.svelte';
	import { fmtAgo, fmtDate, fmtDaysLeft, fmtNum } from '../format';
	import { showToast } from '../toast.svelte';
	import type { AccountStatus, AccountView } from '../types';

	interface Props {
		onOpenImport: () => void;
		onOpenRename: (a: AccountView) => void;
	}
	let { onOpenImport, onOpenRename }: Props = $props();

	let query = $state('');
	let statusFilter = $state<'ALL' | AccountStatus>('ALL');
	let busyId = $state('');
	let busyKind = $state(''); // 'test' | 'claim' | 'balance' | '' — picks the busy label

	const filtered = $derived(
		store.accounts.filter((a) => {
			const matchesStatus = statusFilter === 'ALL' || a.status === statusFilter;
			const q = query.trim().toLowerCase();
			const matchesQuery =
				q === '' ||
				a.name.toLowerCase().includes(q) ||
				a.id.toLowerCase().includes(q) ||
				a.device_mid.toLowerCase().includes(q) ||
				a.token.toLowerCase().includes(q);
			return matchesStatus && matchesQuery;
		}),
	);

	const FILTERS: ('ALL' | AccountStatus)[] = ['ALL', 'ACTIVE', 'COOLING', 'EXHAUSTED', 'DISABLED'];

	function badgeClass(status: AccountStatus): string {
		switch (status) {
			case 'ACTIVE':
				return 'badge badge-active';
			case 'COOLING':
				return 'badge badge-cooling';
			case 'EXHAUSTED':
				return 'badge badge-exhausted';
			default:
				return 'badge';
		}
	}

	function describe(err: unknown): string {
		if (err instanceof ApiError) {
			if (err.status === 0) return t('err_network');
			return t('err_http', { status: err.status, msg: err.message });
		}
		return String(err);
	}

	async function toggleAccount(a: AccountView): Promise<void> {
		const enable = a.status === 'DISABLED';
		busyId = a.id;
		try {
			await api.patchAccount(a.id, { enabled: enable });
			showToast(
				t('toast_account_toggled', {
					name: a.name || a.id,
					store: t(enable ? 'state_enabled' : 'state_disabled'),
				}),
			);
			await store.refreshAccounts();
		} catch (err) {
			showToast(describe(err));
		} finally {
			busyId = '';
		}
	}

	async function testAccount(a: AccountView): Promise<void> {
		busyId = a.id;
		busyKind = 'test';
		try {
			const res = await api.testAccount(a.id);
			if (res.ok) {
				showToast(t('toast_test_ok', { name: a.name || a.id, ms: res.latency_ms }));
			} else {
				showToast(t('toast_test_fail', { name: a.name || a.id, kind: res.kind }));
			}
			await store.refreshAccounts();
		} catch (err) {
			showToast(describe(err));
		} finally {
			busyId = '';
			busyKind = '';
		}
	}

	/** Free balance probe: quota snapshot + plan expiry. */
	async function balanceAccount(a: AccountView): Promise<void> {
		busyId = a.id;
		busyKind = 'balance';
		try {
			const b = await api.refreshBalance(a.id);
			showToast(
				t('toast_balance_ok', {
					name: a.name || a.id,
					rem: fmtNum(b.remaining_quota ?? 0),
					tot: fmtNum(b.total_quota ?? 0),
					exp: fmtDate(b.plan_expires_at) || t('plan_unknown'),
				}),
			);
			await store.refreshAccounts();
		} catch (err) {
			showToast(describe(err));
		} finally {
			busyId = '';
			busyKind = '';
		}
	}

	/** Claim quota for one account right now (; captcha → solver or skip). */
	async function claimAccount(a: AccountView): Promise<void> {
		busyId = a.id;
		busyKind = 'claim';
		try {
			const r = await api.claimAccount(a.id);
			const name = a.name || a.id;
			if (r.captcha_required && !r.ok) showToast(t('toast_claim_one_captcha', { name }));
			else if (r.ok) showToast(t('toast_claim_one_ok', { name, kind: r.kind }));
			else showToast(t('toast_claim_one_fail', { name, kind: r.kind }));
			await Promise.all([store.refreshAccounts(), store.refreshStats()]);
		} catch (err) {
			showToast(describe(err));
		} finally {
			busyId = '';
			busyKind = '';
		}
	}

	async function deleteAccount(a: AccountView): Promise<void> {
		if (!window.confirm(t('confirm_delete_account', { name: a.name || a.id }))) return;
		busyId = a.id;
		try {
			await api.deleteAccount(a.id);
			showToast(t('toast_account_deleted', { name: a.name || a.id }));
			await store.refreshAccounts();
		} catch (err) {
			showToast(describe(err));
		} finally {
			busyId = '';
		}
	}

	async function assignProxy(a: AccountView, proxyId: string): Promise<void> {
		busyId = a.id;
		try {
			await api.patchAccount(a.id, { proxy_id: proxyId });
			showToast(t('toast_proxy_assigned', { name: a.name || a.id }));
			await store.refreshAccounts();
		} catch (err) {
			showToast(describe(err));
		} finally {
			busyId = '';
		}
	}
</script>

<div class="view-content active">
	<div class="page-heading">
		<div>
			<span class="eyebrow">{t('acc_eyebrow')}</span>
			<h1>{t('acc_title')}</h1>
			<p>{t('acc_desc')}</p>
		</div>
		<div class="heading-actions">
			<button class="btn btn-primary" onclick={onOpenImport}>
				<svg class="svg-icon" viewBox="0 0 24 24">
					<line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" />
				</svg>
				<span>{t('btn_batch_import')}</span>
			</button>
		</div>
	</div>

	<div class="panel">
		<div class="panel-header">
			<h2>{t('inv_store_title')}</h2>
			<div class="panel-header-actions">
				<button
					class="btn btn-sm"
					onclick={() =>
						void api.refreshAllBalances().then(() => showToast(t('toast_balance_started')))}
				>
					{t('btn_check_all')}
				</button>
			</div>
		</div>
		<div class="panel-header">
			<div class="table-filter-bar">
				<input
					type="text"
					class="search-input"
					bind:value={query}
					placeholder={t('search_placeholder')}
				/>
				<div class="filter-btn-group">
					{#each FILTERS as f (f)}
						<button
							class="btn btn-sm"
							class:btn-primary={statusFilter === f}
							onclick={() => (statusFilter = f)}
						>
							{f === 'ALL' ? t('filter_all') : f}
						</button>
					{/each}
				</div>
			</div>
		</div>
		<div class="table-responsive">
			<table>
				<thead>
					<tr>
						<th style="width: 40px;">#</th>
						<th>{t('th_acc_id')}</th>
						<th>{t('th_jwt')}</th>
						<th>{t('th_device_mid')}</th>
						<th>{t('th_status')}</th>
						<th>{t('th_inflight')}</th>
						<th>{t('th_quota')}</th>
						<th>{t('th_expires')}</th>
						<th>{t('th_used')}</th>
						<th>{t('no_proxy')}</th>
						<th style="text-align: right;">{t('th_actions')}</th>
					</tr>
				</thead>
				<tbody>
					{#if filtered.length === 0}
						<tr>
							<td colspan="11" style="color: var(--muted); text-align: center;">
								{store.accounts.length === 0 ? t('accounts_empty') : '—'}
							</td>
						</tr>
					{:else}
						{#each filtered as a, i (a.id)}
							<tr>
								<td style="color: var(--subtle);">{String(i + 1).padStart(2, '0')}</td>
								<td>
									<div style="display: flex; align-items: center; gap: 6px;">
										<strong style="color: var(--ink); font-size: 12px;">{a.name || a.id}</strong>
										<button
											class="btn btn-sm"
											style="padding: 1px 5px; font-size: 10px; min-height: 20px;"
											onclick={() => onOpenRename(a)}
											title={t('btn_rename')}
										>
											<svg class="svg-icon" style="width: 11px; height: 11px;" viewBox="0 0 24 24">
												<path d="M12 20h9" /><path
													d="M16.5 3.5a2.12 2.12 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"
												/>
											</svg>
										</button>
									</div>
									<div
										style="color: var(--muted); font-size: 10px; font-family: var(--font-mono); margin-top: 2px;"
									>
										{a.id}
									</div>
								</td>
								<td style="color: var(--muted); font-family: var(--font-mono);">{a.token}</td>
								<td style="color: var(--muted); font-family: var(--font-mono); font-size: 11px;">
									{a.device_mid}
								</td>
								<td>
									<span class={badgeClass(a.status)}>
										{#if a.status === 'ACTIVE'}<span class="status-dot"></span>{/if}
										{a.status}
									</span>
								</td>
								<td>
									<span
										style="color: {a.inflight > 0
											? 'var(--accent-text)'
											: 'var(--muted)'}; font-weight: 700;"
									>
										{a.inflight} / {store.inflightLimit}
									</span>
								</td>
								<td>
									{#if a.remaining_quota != null && a.total_quota != null && a.total_quota > 0}
										{@const pct = Math.round((a.remaining_quota / a.total_quota) * 100)}
										<div class="quota-bar-wrapper">
											<div class="quota-bar">
												<div
													class="quota-fill"
													class:low={pct < 20}
													class:empty={pct === 0}
													style="width: {pct}%;"
												></div>
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
										{fmtDate(a.plan_expires_at)}
										<span style="color: var(--subtle); font-size: 10px;">
											({fmtDaysLeft(a.plan_expires_at)}d)
										</span>
									{:else}
										{t('plan_unknown')}
									{/if}
								</td>
								<td style="color: var(--muted);">{fmtAgo(a.last_used_at)}</td>
								<td>
									<select
										class="form-control"
										style="min-width: 130px; padding: 4px 6px; font-size: 11px;"
										value={a.proxy_id ?? ''}
										disabled={busyId === a.id}
										onchange={(e) => assignProxy(a, (e.currentTarget as HTMLSelectElement).value)}
									>
										<option value="">{t('no_proxy')}</option>
										{#each store.proxies as p (p.id)}
											<option value={p.id}>{p.name || p.url}</option>
										{/each}
									</select>
								</td>
								<td style="text-align: right;">
									<div style="display: inline-flex; gap: 6px;">
										<button
											class="btn btn-sm"
											onclick={() => void claimAccount(a)}
											disabled={busyId === a.id}
										>
											{busyId === a.id && busyKind === 'claim' ? t('claiming') : t('btn_claim_one')}
										</button>
										<button
											class="btn btn-sm"
											onclick={() => void balanceAccount(a)}
											disabled={busyId === a.id}
										>
											{busyId === a.id && busyKind === 'balance'
												? t('balancing')
												: t('btn_balance')}
										</button>
										<button
											class="btn btn-sm"
											onclick={() => void testAccount(a)}
											disabled={busyId === a.id}
										>
											{busyId === a.id && busyKind === 'test' ? t('testing') : t('btn_test')}
										</button>
										<button
											class="btn btn-sm"
											onclick={() => void toggleAccount(a)}
											disabled={busyId === a.id}
										>
											{a.status === 'DISABLED' ? t('btn_enable') : t('btn_disable')}
										</button>
										<button
											class="btn btn-sm btn-danger"
											onclick={() => void deleteAccount(a)}
											disabled={busyId === a.id}
										>
											&times;
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
</div>
