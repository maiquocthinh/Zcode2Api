<script lang="ts">
	// Proxies view: static egress proxies assigned
	// per account — add one, batch import, latency test, delete.
	import { t } from '../i18n.svelte';
	import { api, ApiError } from '../api';
	import { store } from '../state.svelte';
	import { fmtLatency } from '../format';
	import { showToast } from '../toast.svelte';

	interface Props {
		onOpenProxy: () => void;
		onOpenBatchProxy: () => void;
	}
	let { onOpenProxy, onOpenBatchProxy }: Props = $props();

	let testing = $state(false);
	let busyId = $state('');

	function describe(err: unknown): string {
		if (err instanceof ApiError) {
			if (err.status === 0) return t('err_network');
			return t('err_http', { status: err.status, msg: err.message });
		}
		return String(err);
	}

	function badgeClass(status: string): string {
		if (status === 'HEALTHY') return 'badge badge-active';
		if (status === 'DEAD') return 'badge badge-exhausted';
		return 'badge badge-cooling'; // UNKNOWN
	}

	async function testProxies(): Promise<void> {
		testing = true;
		try {
			await api.testProxies([]);
			await store.refreshProxies();
			showToast(t('btn_test_latency') + ': OK');
		} catch (err) {
			showToast(describe(err));
		} finally {
			testing = false;
		}
	}

	async function deleteProxy(id: string, name: string): Promise<void> {
		if (!window.confirm(t('confirm_delete_proxy', { name: name || id }))) return;
		busyId = id;
		try {
			await api.deleteProxy(id);
			showToast(t('toast_proxy_deleted', { name: name || id }));
			await store.refreshProxies();
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
			<span class="eyebrow">{t('proxy_eyebrow')}</span>
			<h1>{t('proxy_title')}</h1>
			<p>{t('proxy_desc')}</p>
		</div>
		<div class="heading-actions">
			<button class="btn" onclick={onOpenBatchProxy}>
				<svg class="svg-icon" viewBox="0 0 24 24">
					<polygon points="12 2 2 7 12 12 22 7 12 2" /><polyline
						points="2 17 12 22 22 17"
					/><polyline points="2 12 12 17 22 12" />
				</svg>
				<span>{t('btn_batch_add_proxy')}</span>
			</button>
			<button class="btn btn-primary" onclick={onOpenProxy}>
				<svg class="svg-icon" viewBox="0 0 24 24">
					<line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" />
				</svg>
				<span>{t('btn_add_proxy')}</span>
			</button>
		</div>
	</div>

	<div class="panel">
		<div class="panel-header">
			<h2>{t('proxy_lines_title')}</h2>
			<div class="panel-header-actions">
				<button class="btn btn-sm" onclick={() => void testProxies()} disabled={testing}>
					{t('btn_test_latency')}
				</button>
			</div>
		</div>
		<div class="table-responsive">
			<table>
				<thead>
					<tr>
						<th style="width: 40px;">#</th>
						<th>{t('th_proxy_name')}</th>
						<th>{t('th_proxy_url')}</th>
						<th>{t('th_latency')}</th>
						<th>{t('th_proxy_status')}</th>
						<th style="text-align: right;">{t('th_actions')}</th>
					</tr>
				</thead>
				<tbody>
					{#if store.proxies.length === 0}
						<tr>
							<td colspan="6" style="color: var(--muted); text-align: center;">
								{t('proxies_empty')}
							</td>
						</tr>
					{:else}
						{#each store.proxies as px, i (px.id)}
							<tr>
								<td style="color: var(--subtle);">{String(i + 1).padStart(2, '0')}</td>
								<td><strong style="color: var(--ink);">{px.name || px.id}</strong></td>
								<td style="color: var(--muted); font-family: var(--font-mono);">{px.url}</td>
								<td
									style="color: {px.latency_ms >= 0
										? 'var(--accent-text)'
										: 'var(--muted)'}; font-weight: 700;"
								>
									{fmtLatency(px.latency_ms)}
								</td>
								<td><span class={badgeClass(px.status)}>{px.status}</span></td>
								<td style="text-align: right;">
									<button
										class="btn btn-sm btn-danger"
										onclick={() => void deleteProxy(px.id, px.name)}
										disabled={busyId === px.id}
									>
										&times;
									</button>
								</td>
							</tr>
						{/each}
					{/if}
				</tbody>
			</table>
		</div>
	</div>
</div>
