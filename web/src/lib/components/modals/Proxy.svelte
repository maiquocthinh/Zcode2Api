<script lang="ts">
  // Add one egress proxy (POST /api/proxies {name,url}).
  import { t } from '../../i18n.svelte';
  import { api, ApiError } from '../../api';
  import { store } from '../../state.svelte';
  import { showToast } from '../../toast.svelte';

  interface Props {
    onClose: () => void;
  }
  let { onClose }: Props = $props();

  let name = $state('');
  let url = $state('');
  let busy = $state(false);

  function describe(err: unknown): string {
    if (err instanceof ApiError) {
      if (err.status === 0) return t('err_network');
      return t('err_http', { status: err.status, msg: err.message });
    }
    return String(err);
  }

  async function submit(): Promise<void> {
    if (busy || !url.trim()) return;
    busy = true;
    try {
      await api.addProxy(name.trim(), url.trim());
      showToast(t('toast_proxy_added', { name: name.trim() || url.trim() }));
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

<div class="modal-overlay active" role="dialog" aria-modal="true" tabindex="-1" onclick={backdrop} onkeydown={(e) => { if (e.key === 'Escape') onClose(); }}>
  <div class="modal">
    <div class="modal-head">
      <div>
        <span class="eyebrow">EGRESS LINE</span>
        <strong style="font-size: 12px;">{t('modal_proxy_title')}</strong>
      </div>
      <button class="btn btn-sm" onclick={onClose} aria-label="Close modal">&times;</button>
    </div>
    <div class="modal-body">
      <div class="form-group" style="margin-bottom: 12px;">
        <label>{t('lbl_proxy_name')}</label>
        <input type="text" class="form-control" bind:value={name} placeholder="Singapore Highspeed" />
        <span class="hint">{t('hint_proxy_name')}</span>
      </div>
      <div class="form-group" style="margin-bottom: 14px;">
        <label>{t('lbl_proxy_url')}</label>
        <input type="text" class="form-control" bind:value={url} placeholder="http://user:pass@1.2.3.4:8080 or socks5://1.2.3.4:1080" />
        <span class="hint">{t('hint_proxy_url')}</span>
      </div>
      <div style="display: flex; justify-content: flex-end; gap: 8px;">
        <button class="btn btn-primary" onclick={() => void submit()} disabled={busy || !url.trim()}>
          <svg class="svg-icon" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12" /></svg>
          {t('btn_save_proxy')}
        </button>
        <button class="btn" onclick={onClose}>{t('btn_cancel')}</button>
      </div>
    </div>
  </div>
</div>
