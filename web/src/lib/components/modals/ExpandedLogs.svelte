<script lang="ts">
	// Fullscreen telemetry console: filters ALL / INFO / WARN /
	// FAILOVER / CLAIM, text search, auto-scroll toggle, download and clear.
	// Failover happens by reading the first SSE event / first-event JSON,
	// and `event: claim_job` carries the claim-all progress.
	import { t } from '../../i18n.svelte';
	import { store, type LogLevel } from '../../state.svelte';
	import { autoscroll } from '../../autoscroll';
	import { downloadFile } from '../../format';
	import { api, ApiError } from '../../api';
	import { showToast } from '../../toast.svelte';

	interface Props {
		onClose: () => void;
	}
	let { onClose }: Props = $props();

	type Filter = 'ALL' | LogLevel;
	let filter = $state<Filter>('ALL');
	let query = $state('');
	let autoScroll = $state(true);
	let consoleEl: HTMLDivElement | undefined = $state();

	const filtered = $derived(
		store.logs.filter((entry) => {
			const matchesFilter = filter === 'ALL' || entry.level === filter;
			const q = query.trim().toLowerCase();
			const matchesQuery =
				q === '' || entry.text.toLowerCase().includes(q) || entry.level.toLowerCase().includes(q);
			return matchesFilter && matchesQuery;
		}),
	);

	const totalFailover = $derived(store.logs.filter((l) => l.level === 'FAILOVER').length);
	const totalWarn = $derived(store.logs.filter((l) => l.level === 'WARN').length);
	const totalClaim = $derived(store.logs.filter((l) => l.level === 'CLAIM').length);

	const FILTERS: Filter[] = ['ALL', 'FAILOVER', 'WARN', 'INFO', 'CLAIM'];

	function filterLabel(f: Filter): string {
		switch (f) {
			case 'ALL':
				return t('filter_all');
			case 'INFO':
				return t('filter_info');
			case 'WARN':
				return t('filter_warn');
			case 'CLAIM':
				return t('filter_claim');
			default:
				return t('filter_failover');
		}
	}

	function describe(err: unknown): string {
		if (err instanceof ApiError) {
			if (err.status === 0) return t('err_network');
			return t('err_http', { status: err.status, msg: err.message });
		}
		return String(err);
	}

	function download(): void {
		const text = store.logs.map((l) => `${l.timeStr} [${l.level}] ${l.text}`).join('\n');
		downloadFile(`zcode2api_telemetry_${Date.now()}.log`, text, 'text/plain;charset=utf-8');
		showToast(t('btn_download_log') + ': OK');
	}

	async function clearAll(): Promise<void> {
		try {
			await api.clearLogs();
			store.clearLogs();
			showToast(t('toast_logs_cleared'));
		} catch (err) {
			showToast(describe(err));
		}
	}

	function backdrop(e: MouseEvent): void {
		if (e.target === e.currentTarget) onClose();
	}
</script>

<div
	class="modal-overlay has-fullscreen active"
	role="dialog"
	aria-modal="true"
	tabindex="-1"
	onclick={backdrop}
	onkeydown={(e) => {
		if (e.key === 'Escape') onClose();
	}}
>
	<div class="modal modal-fullscreen">
		<div class="modal-head">
			<div style="display: flex; align-items: center; gap: 10px;">
				<span class="brand-mark" style="width: 10px; height: 10px;"></span>
				<div>
					<span class="eyebrow">{t('exp_log_eyebrow')}</span>
					<strong style="font-size: 13px;">{t('exp_log_title')}</strong>
				</div>
			</div>
			<div class="modal-head-actions">
				<button class="btn btn-sm" onclick={download}>
					<svg class="svg-icon" viewBox="0 0 24 24">
						<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><polyline
							points="7 10 12 15 17 10"
						/><line x1="12" y1="15" x2="12" y2="3" />
					</svg>
					<span>{t('btn_download_log')}</span>
				</button>
				<button class="btn btn-sm" onclick={onClose} aria-label="Close modal">
					<svg class="svg-icon" viewBox="0 0 24 24">
						<line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
					</svg>
					<span>{t('btn_close_exp')}</span>
				</button>
			</div>
		</div>
		<div class="modal-body">
			<div class="expanded-toolbar">
				<input
					type="text"
					class="search-input"
					bind:value={query}
					placeholder={t('search_logs_placeholder')}
				/>
				<div class="expanded-filter-row">
					{#each FILTERS as f (f)}
						<button
							class="btn btn-sm"
							class:btn-primary={filter === f}
							style={f === 'FAILOVER'
								? 'color: var(--log-failover);'
								: f === 'WARN'
									? 'color: var(--warning-text);'
									: f === 'CLAIM'
										? 'color: var(--info-text);'
										: ''}
							onclick={() => (filter = f)}
						>
							{filterLabel(f)}
						</button>
					{/each}
				</div>
				<div class="expanded-actions-row">
					<button class="btn btn-sm" onclick={() => (autoScroll = !autoScroll)}>
						{autoScroll ? t('auto_scroll_on') : t('auto_scroll_off')}
					</button>
					<button class="btn btn-sm" onclick={() => void clearAll()}>
						{t('btn_clear_console')}
					</button>
				</div>
			</div>

			<div
				class="expanded-console-box"
				bind:this={consoleEl}
				use:autoscroll={{ deps: filtered.length, enabled: () => autoScroll }}
			>
				{#each filtered as line, i (i)}
					<div class="log-line">
						<span class="log-time">{line.timeStr}</span>
						<span class="log-{line.level.toLowerCase()}">[{line.level}]</span>
						<span class="log-text">{line.text}</span>
					</div>
				{/each}
			</div>

			<div class="expanded-footer-bar">
				<div style="display: flex; gap: 14px; align-items: center; flex-wrap: wrap;">
					<span>
						<span class="status-dot" class:pulse={store.streamOk}></span>
						<strong style="color: var(--accent-text);">
							{store.streamOk ? t('stream_live') : t('stream_reconnecting')}
						</strong>
					</span>
					<span>{t('stat_total', { n: store.logs.length })}</span>
					<span style="color: var(--log-failover);">
						{t('stat_failover', { n: totalFailover })}
					</span>
					<span style="color: var(--warning-text);">{t('stat_warn', { n: totalWarn })}</span>
					<span style="color: var(--info-text);">{t('stat_claim', { n: totalClaim })}</span>
				</div>
				<div>
					<!-- Failover engine note -->
					<span>
						Failover Engine: <strong style="color: var(--accent-text);">
							ACTIVE (FIRST SSE EVENT)
						</strong>
					</span>
				</div>
			</div>
		</div>
	</div>
</div>
