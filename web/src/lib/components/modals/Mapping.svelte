<script lang="ts">
	// Model mapping add/edit (POST /api/mappings, PUT /api/mappings/{id}).
	// Only MAPPED / PASSTHROUGH exist — a DISABLED mode is not
	// supported by the backend.
	import { t } from '../../i18n.svelte';
	import { api, ApiError } from '../../api';
	import { showToast } from '../../toast.svelte';
	import type { MappingView } from '../../types';

	interface Props {
		mapping: MappingView | null;
		onClose: () => void;
	}
	let { mapping, onClose }: Props = $props();

	let pattern = $state(mapping?.pattern ?? '');
	let target = $state(mapping?.target ?? '');
	let mode = $state<'MAPPED' | 'PASSTHROUGH'>(mapping?.mode ?? 'MAPPED');
	let busy = $state(false);

	function describe(err: unknown): string {
		if (err instanceof ApiError) {
			if (err.status === 0) return t('err_network');
			return t('err_http', { status: err.status, msg: err.message });
		}
		return String(err);
	}

	async function submit(): Promise<void> {
		if (busy) return;
		if (!pattern.trim()) return;
		if (mode === 'MAPPED' && !target.trim()) return;
		busy = true;
		try {
			if (mapping) {
				await api.updateMapping(mapping.id, pattern.trim(), target.trim(), mode);
			} else {
				await api.addMapping(pattern.trim(), target.trim(), mode);
			}
			showToast(t('toast_mapping_saved', { pattern: pattern.trim() }));
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
				<span class="eyebrow">MODEL ROUTING</span>
				<strong style="font-size: 12px;">
					{mapping ? t('modal_mapping_edit') : t('modal_mapping_add')}
				</strong>
			</div>
			<button class="btn btn-sm" onclick={onClose} aria-label="Close modal">&times;</button>
		</div>
		<div class="modal-body">
			<div class="form-group" style="margin-bottom: 12px;">
				<label>{t('lbl_inbound_model')}</label>
				<input
					type="text"
					class="form-control"
					bind:value={pattern}
					placeholder="claude-sonnet-4-5 or claude-sonnet-*"
				/>
				<span class="hint">{t('hint_inbound_model')}</span>
			</div>
			<div class="form-group" style="margin-bottom: 12px;">
				<label>{t('lbl_outbound_model')}</label>
				<input
					type="text"
					class="form-control"
					bind:value={target}
					placeholder="glm-4.7"
					disabled={mode === 'PASSTHROUGH'}
				/>
				<span class="hint">{t('hint_outbound_model')}</span>
			</div>
			<div class="form-group" style="margin-bottom: 14px;">
				<label>{t('lbl_map_mode')}</label>
				<select class="form-control" bind:value={mode}>
					<option value="MAPPED">{t('mode_mapped')}</option>
					<option value="PASSTHROUGH">{t('mode_passthrough')}</option>
				</select>
			</div>
			<div style="display: flex; justify-content: flex-end; gap: 8px;">
				<button
					class="btn btn-primary"
					onclick={() => void submit()}
					disabled={busy || !pattern.trim() || (mode === 'MAPPED' && !target.trim())}
				>
					<svg class="svg-icon" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12" /></svg>
					{t('btn_save_mapping')}
				</button>
				<button class="btn" onclick={onClose}>{t('btn_cancel')}</button>
			</div>
		</div>
	</div>
</div>
