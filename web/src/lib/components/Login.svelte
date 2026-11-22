<script lang="ts">
 // Admin login — POST /api/login with the ADMIN_PASSWORD; the server answers
 // 204 + z2a_session cookie. All 4 languages and
 // the theme toggle stay available on this screen.
 import { t, getLang, LANGS, LANG_NAMES, setLang, type Lang } from '../i18n.svelte';
 import { ApiError } from '../api';
 import { showToast } from '../toast.svelte';

 interface Props {
 onLogin: (password: string) => Promise<void>;
 }

 let { onLogin }: Props = $props();

 let password = $state('');
 let busy = $state(false);
 let error = $state('');
 let menuOpen = $state(false);

 async function submit(e: SubmitEvent): Promise<void> {
 e.preventDefault();
 if (busy || !password) return;
 busy = true;
 error = '';
 try {
 await onLogin(password);
 password = '';
 } catch (err) {
 if (err instanceof ApiError && (err.status === 401 || err.status === 429)) {
 error = t('login_error');
 } else if (err instanceof ApiError && err.status === 0) {
 error = t('err_network');
 } else {
 error = t('err_http', { status: err instanceof ApiError ? err.status: '?', msg: String(err) });
 }
 } finally {
 busy = false;
 }
 }

 function pickLang(lang: Lang): void {
 setLang(lang);
 localStorage.setItem('z2a_lang', lang);
 menuOpen = false;
 showToast(t('toast_lang', { lang: lang.toUpperCase() }));
 }
</script>

<div class="login-wrap">
 <div class="login-card">
 <div class="login-brand">
 <span class="brand-mark"></span>
 <strong class="brand-title">ZCODE2API</strong>
 </div>
 <span class="eyebrow">{t('login_eyebrow')}</span>
 <h1>{t('login_title')}</h1>
 <span class="brand-title"><span class="tagline">{t('tagline')}</span></span>
 <p class="login-desc">{t('login_desc')}</p>

 {#if error}
 <div class="login-error">{error}</div>
 {/if}

 <form onsubmit={submit}>
 <div class="form-group" style="margin-bottom: 14px;">
 <label for="login-password">{t('lbl_password')}</label>
 <input
 id="login-password"
 type="password"
 class="form-control"
 autocomplete="current-password"
 bind:value={password}
 disabled={busy}
 />
 </div>
 <button class="btn btn-primary" type="submit" style="width: 100%;" disabled={busy || !password}>
 <svg class="svg-icon" viewBox="0 0 24 24">
 <rect x="3" y="11" width="18" height="11" rx="0" ry="0" /><path d="M7 11V7a5 5 0 0 1 10 0v4" />
 </svg>
 {t('btn_login')}
 </button>
 </form>

 <div class="lang-dropdown-wrapper" style="margin-top: 16px;">
 <button class="btn btn-sm lang-dropdown-btn" type="button" onclick={() => (menuOpen = !menuOpen)}>
 <svg class="svg-icon" viewBox="0 0 24 24"><circle cx="12" cy="12" r="10" /><line x1="2" y1="12" x2="22" y2="12" /><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z" /></svg>
 <strong style="color: var(--accent-text);">{getLang().toUpperCase()}</strong>
 </button>
 {#if menuOpen}
 <div class="lang-menu open" style="left: 0; right: auto;">
 <div class="lang-menu-head">// SELECT LOCALE</div>
 {#each LANGS as lang (lang)}
 <button class="lang-option" class:active={lang === getLang()} type="button" onclick={() => pickLang(lang)}>
 <span class="lang-tag">{lang.toUpperCase()}</span>
 <span class="lang-name">{LANG_NAMES[lang]}</span>
 <span class="lang-check">✓</span>
 </button>
 {/each}
 </div>
 {/if}
 </div>
 </div>
</div>
