<script lang="ts">
 // Endpoint / integration modal:
 // shows the gateway endpoints and generates client snippets for
 // Claude Code / Cursor / Cline plus a curl stream test.
 // The client key is never displayed — snippets reference the ENV value.
 import { t } from '../../i18n.svelte';
 import { copyText } from '../../format';
 import { showToast } from '../../toast.svelte';

 interface Props {
 onClose: () => void;
 }
 let { onClose }: Props = $props();

 type Tab = 'claude' | 'cursor' | 'cline' | 'curl';
 let tab = $state<Tab>('claude');

 const base = typeof window !== 'undefined' ? window.location.origin: 'http://127.0.0.1:3101';
 const messagesUrl = base + '/v1/messages';

 const claudeSnippet = $derived(`{
 "env": {
 "ANTHROPIC_BASE_URL": "${base}",
 "ANTHROPIC_AUTH_TOKEN": "<CLIENT_API_KEYS>",
 "API_TIMEOUT_MS": "3000000"
 }
}`);

 const cursorSnippet = $derived(`# Cursor → Settings → Models → Override Anthropic Base URL
ANTHROPIC_BASE_URL = ${base}
ANTHROPIC_API_KEY = <CLIENT_API_KEYS> # one value from ENV CLIENT_API_KEYS`);

 const clineSnippet = $derived(`# Cline (VS Code) → API Provider: "Anthropic Compatible"
Base URL: ${base}
API Key: <CLIENT_API_KEYS> # one value from ENV CLIENT_API_KEYS`);

 const curlSnippet = $derived(`curl -N ${messagesUrl} \\
 -H "x-api-key: <CLIENT_API_KEYS>" \\
 -H "anthropic-version: 2023-06-01" \\
 -H "content-type: application/json" \\
 -d '{"model":"claude-sonnet-4-5","max_tokens":1024,"messages":[{"role":"user","content":"Hi"}]}'`);

 const TABS: { id: Tab; label: string; hint: string }[] = [
 { id: 'claude', label: t('ep_tab_claude'), hint: t('ep_claude_hint') },
 { id: 'cursor', label: t('ep_tab_cursor'), hint: t('ep_cursor_hint') },
 { id: 'cline', label: t('ep_tab_cline'), hint: t('ep_cline_hint') },
 { id: 'curl', label: t('ep_tab_curl'), hint: t('ep_curl_hint') }
 ];

 const snippet = $derived.by(() => {
 switch (tab) {
 case 'claude':
 return claudeSnippet;
 case 'cursor':
 return cursorSnippet;
 case 'cline':
 return clineSnippet;
 default:
 return curlSnippet;
 }
 });

 async function copy(value: string): Promise<void> {
 if (await copyText(value)) showToast(t('toast_copied'));
 }

 function backdrop(e: MouseEvent): void {
 if (e.target === e.currentTarget) onClose();
 }
</script>

<div class="modal-overlay active" role="dialog" aria-modal="true" tabindex="-1" onclick={backdrop} onkeydown={(e) => { if (e.key === 'Escape') onClose(); }}>
 <div class="modal" style="max-width: 560px;">
 <div class="modal-head">
 <div style="display: flex; align-items: center; gap: 8px;">
 <span style="color: var(--accent-text); font-weight: 800; font-family: var(--font-mono);">&lt;/&gt;</span>
 <strong style="font-size: 12px;">{t('modal_endpoint_title')}</strong>
 </div>
 <button class="btn btn-sm" onclick={onClose} aria-label="Close modal">&times;</button>
 </div>
 <div class="modal-body" style="padding: 16px;">
 <div class="form-group" style="margin-bottom: 12px;">
 <label style="display: flex; justify-content: space-between;">
 <span>{t('ep_messages_label')}</span>
 <span style="color: var(--accent-text); font-size: 10px;">HOT PATH</span>
 </label>
 <div style="display: flex; gap: 6px;">
 <input type="text" class="form-control" readonly value={messagesUrl} />
 <button class="btn btn-sm" onclick={() => void copy(messagesUrl)}>{t('btn_copy')}</button>
 </div>
 <span class="hint">{t('ep_messages_hint')}</span>
 </div>

 <div class="form-group" style="margin-bottom: 12px;">
 <label style="display: flex; justify-content: space-between;">
 <span>{t('ep_base_label')}</span>
 <span style="color: var(--muted); font-size: 10px;">OVERRIDE URL</span>
 </label>
 <div style="display: flex; gap: 6px;">
 <input type="text" class="form-control" readonly value={base} />
 <button class="btn btn-sm" onclick={() => void copy(base)}>{t('btn_copy')}</button>
 </div>
 <span class="hint">{t('ep_base_hint')}</span>
 </div>

 <div class="form-group" style="margin-bottom: 14px;">
 <label style="display: flex; justify-content: space-between;">
 <span>{t('ep_key_label')}</span>
 <span style="color: var(--warning-text); font-size: 10px;">SECRET</span>
 </label>
 <div style="display: flex; gap: 6px;">
 <input type="text" class="form-control" readonly value="<CLIENT_API_KEYS> (ENV)" />
 </div>
 <span class="hint">{t('ep_key_hint')}</span>
 </div>

 <!-- Client tabs with generated snippets -->
 <div class="code-container" style="margin-bottom: 14px;">
 <div class="code-header" style="padding: 0;">
 <div class="client-tabs" style="border-bottom: none; padding: 4px 8px 0 8px; flex: 1;">
 {#each TABS as tb (tb.id)}
 <button class="client-tab" class:active={tab === tb.id} onclick={() => (tab = tb.id)}>
 {tb.label}
 </button>
 {/each}
 </div>
 <button class="btn btn-sm" style="margin: 4px 8px; flex-shrink: 0;" onclick={() => void copy(snippet)}>
 {t('btn_copy')}
 </button>
 </div>
 <div class="code-content">
 <pre>{snippet}</pre>
 </div>
 <div style="padding: 6px 12px; border-top: 1px solid var(--line); font-size: 10px; color: var(--muted);">
 {TABS.find((x) => x.id === tab)?.hint}
 </div>
 </div>

 <div style="display: flex; justify-content: flex-end;">
 <button class="btn" onclick={onClose}>{t('btn_close_modal')}</button>
 </div>
 </div>
 </div>
</div>
