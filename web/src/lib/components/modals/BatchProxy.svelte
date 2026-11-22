<script lang="ts">
	// Batch proxy import (POST /api/proxies/batch {lines}).
	import { t } from '../../i18n.svelte';
	import { api, ApiError } from '../../api';
	import { store } from '../../state.svelte';
	import { showToast } from '../../toast.svelte';

	interface Props {
		onClose: () => void;
	}
	let { onClose }: Props = $props();

	let lines = $state('');
	let busy = $state(false);

	const placeholder =
		'Singapore Highspeed | http://user:pass@1.2.3.4:8080\nTokyo Fast | socks5://5.6.7.8:1080\nhttp://11.22.33.44:3128';

	function describe(err: unknown): string {
		if (err instanceof ApiError) {
			if (err.status === 0) return t('err_network');
			return t('err_http', { status: err.status, msg: err.message });
		}
		return String(err);
	}

	async function submit(): Promise<void> {
		if (busy || !lines.trim()) return;
		busy = true;
		try {
			const res = await api.addProxiesBatch(lines);
			showToast(t('toast_batch_proxy', { a: res.added, d: res.duplicates, i: res.invalid.length }));
			await store.refreshProxies();
			onClose();
		} catch (err) {
			showToast(describe(err));
		} finally {
			busy = false;
		}
	}

	function backdrop(e: MouseEvent): void {
		if (e.target === e.currentTarget) onClose();
	}
</script>

<div
	class="modal-overlay active"
	role="dialog"
	aria-modal="true"
	tabindex="-1"
	onclick={backdrop}
	onkeydown={(e) => {
		if (e.key === 'Escape') onClose();
	}}
>
	<div class="modal">
		<div class="modal-head">
			<div>
				<span class="eyebrow">BATCH EGRESS LOADER</span>
				<strong style="font-size: 12px;">{t('modal_batch_proxy_title')}</strong>
			</div>
			<button class="btn btn-sm" onclick={onClose} aria-label="Close modal">&times;</button>
		</div>
		<div class="modal-body">
			<p style="margin: 0 0 8px 0; color: var(--muted); font-size: 12px;">
				{t('modal_batch_proxy_desc')}
			</p>
			<textarea bind:value={lines} {placeholder}></textarea>
			<div style="display: flex; justify-content: flex-end; gap: 8px; margin-top: 14px;">
				<button
					class="btn btn-primary"
					onclick={() => void submit()}
					disabled={busy || !lines.trim()}
				>
					<svg class="svg-icon" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12" /></svg>
					{t('btn_confirm_batch_proxy')}
				</button>
				<button class="btn" onclick={onClose}>{t('btn_cancel')}</button>
			</div>
		</div>
	</div>
</div>
