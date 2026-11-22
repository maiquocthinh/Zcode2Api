<script lang="ts">
  // Rename modal (PATCH /api/accounts/{id} {name}) — account alias
  // dialog for the JWT account pool.
  import { t } from '../../i18n.svelte';
  import { api, ApiError } from '../../api';
  import { store } from '../../state.svelte';
  import { showToast } from '../../toast.svelte';
  import type { AccountView } from '../../types';

  interface Props {
    target: AccountView;
    onClose: () => void;
  }
  let { target, onClose }: Props = $props();

  let name = $state(target.name || '');
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
    busy = true;
    try {
      await api.patchAccount(target.id, { name: name.trim() });
      showToast(t('toast_renamed', { name: name.trim() || target.id }));
      await store.refreshAccounts();
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

<div class="modal-overlay active" role="dialog" aria-modal="true" tabindex="-1" onclick={backdrop} onkeydown={(e) => { if (e.key === 'Escape') onClose(); }}>
  <div class="modal" style="max-width: 480px;">
    <div class="modal-head">
      <div>
        <span class="eyebrow">ACCOUNT ALIAS</span>
        <strong style="font-size: 12px;">{t('modal_rename_title')}</strong>
      </div>
      <button class="btn btn-sm" onclick={onClose} aria-label="Close modal">&times;</button>
    </div>
    <div class="modal-body">
      <div class="form-group" style="margin-bottom: 14px;">
        <label>{t('lbl_acc_name')}</label>
        <input
          type="text"
          class="form-control"
          bind:value={name}
          placeholder="Cursor Dev VIP, Acc công ty..."
        />
        <span class="hint">{t('hint_acc_name')}</span>
      </div>
      <div style="display: flex; justify-content: flex-end; gap: 8px;">
        <button class="btn btn-primary" onclick={() => void submit()} disabled={busy}>
          <svg class="svg-icon" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12" /></svg>
          {t('btn_save_name')}
        </button>
        <button class="btn" onclick={onClose}>{t('btn_cancel')}</button>
      </div>
    </div>
  </div>
</div>
