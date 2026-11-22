<script lang="ts">
 // Root component: session gate (cookie + /api/session), theme & language
 // persistence, view switching and modal orchestration.
 import { onMount } from 'svelte';
 import { api, onUnauthorized } from './lib/api';
 import { setLang, getLang, t, type Lang } from './lib/i18n.svelte';
 import { store } from './lib/state.svelte';
 import { showToast, toastState } from './lib/toast.svelte';
 import type { AccountView, MappingView } from './lib/types';
 import Login from './lib/components/Login.svelte';
 import Header from './lib/components/Header.svelte';
 import BottomNav from './lib/components/BottomNav.svelte';
 import OperationsView from './lib/components/OperationsView.svelte';
 import AccountsView from './lib/components/AccountsView.svelte';
 import ProxiesView from './lib/components/ProxiesView.svelte';
 import SettingsView from './lib/components/SettingsView.svelte';
 import ImportAccountsModal from './lib/components/modals/ImportAccounts.svelte';
 import RenameModal from './lib/components/modals/Rename.svelte';
 import MappingModal from './lib/components/modals/Mapping.svelte';
 import ProxyModal from './lib/components/modals/Proxy.svelte';
 import BatchProxyModal from './lib/components/modals/BatchProxy.svelte';
 import EndpointModal from './lib/components/modals/Endpoint.svelte';
 import ExpandedLogsModal from './lib/components/modals/ExpandedLogs.svelte';

 export type ViewName = 'operations' | 'accounts' | 'proxies' | 'settings';

 type Modal =
 | { kind: 'importAccounts' }
 | { kind: 'rename'; target: AccountView }
 | { kind: 'mapping'; mapping: MappingView | null }
 | { kind: 'proxy' }
 | { kind: 'batchProxy' }
 | { kind: 'endpoint' }
 | { kind: 'expandedLogs' }
 | null;

 type Session = 'checking' | 'out' | 'in';

 let session = $state<Session>('checking');
 let view = $state<ViewName>('operations');
 let modal = $state<Modal>(null);
 let theme = $state<'dark' | 'light'>(
 localStorage.getItem('z2a_theme') === 'light' ? 'light': 'dark');

 // Restore language before first paint of texts.
 const savedLang = localStorage.getItem('z2a_lang') as Lang | null;
 if (savedLang) setLang(savedLang);

 // Apply theme to <html data-theme="...">.
 $effect(() => {
 document.documentElement.dataset.theme = theme;
 localStorage.setItem('z2a_theme', theme);
 });

 // 401 anywhere (expired session, logged out elsewhere) → Login screen.
 onUnauthorized(() => {
 becomeLoggedOut();
 });

 function becomeLoggedOut(): void {
 session = 'out';
 modal = null;
 store.stopRealtime();
 store.stats = null;
 store.accounts = [];
 store.logs = [];
 }

 async function checkSession(): Promise<void> {
 try {
 await api.session();
 await becomeLoggedIn();
 } catch {
 session = 'out';
 }
 }

 async function becomeLoggedIn(): Promise<void> {
 session = 'in';
 store.stopRealtime();
 store.logs = [];
 await store.loadLogs();
 await Promise.all([store.refreshStats(), store.refreshAccounts(), store.refreshProxies()]);
 store.startRealtime();
 }

 async function handleLogin(password: string): Promise<void> {
 await api.login(password);
 await becomeLoggedIn();
 }

 async function handleLogout(): Promise<void> {
 try {
 await api.logout();
 } catch {
 // The session cookie is cleared server-side; ignore errors.
 }
 becomeLoggedOut();
 }

 function toggleTheme(): void {
 const target = theme === 'dark' ? 'light': 'dark';
 theme = target;
 showToast(t('toast_theme', { mode: t(target === 'dark' ? 'theme_dark': 'theme_light') }));
 }

 function selectLang(lang: Lang): void {
 if (lang === getLang()) return;
 setLang(lang);
 localStorage.setItem('z2a_lang', lang);
 showToast(t('toast_lang', { lang: lang.toUpperCase() }));
 }

 function switchView(v: ViewName): void {
 view = v;
 // .app-main is the scroll container.
 document.querySelector('.app-main')?.scrollTo({ top: 0 });
 }

 // --- modal helpers passed down to the views ---------------------------------

 function openImportAccounts(): void {
 modal = { kind: 'importAccounts' };
 }
 function openRename(target: AccountView): void {
 modal = { kind: 'rename', target };
 }
 function openMapping(mapping: MappingView | null): void {
 modal = { kind: 'mapping', mapping };
 }
 function openProxy(): void {
 modal = { kind: 'proxy' };
 }
 function openBatchProxy(): void {
 modal = { kind: 'batchProxy' };
 }
 function openEndpoint(): void {
 modal = { kind: 'endpoint' };
 }
 function openExpandedLogs(): void {
 modal = { kind: 'expandedLogs' };
 }
 function closeModal(): void {
 modal = null;
 }

 // Escape closes any open modal.
 function onKeydown(e: KeyboardEvent): void {
 if (e.key === 'Escape' && modal !== null) {
 closeModal();
 }
 }

 onMount(() => {
 void checkSession();
 });
</script>

<svelte:window onkeydown={onKeydown} />

{#if session === 'checking'}
 <div class="login-wrap">
 <div class="login-card">
 <div class="login-brand">
 <span class="brand-mark"></span>
 <span class="eyebrow">ZCODE2API</span>
 </div>
 <p class="login-desc">{t('login_checking')}</p>
 </div>
 </div>
{:else if session === 'out'}
 <Login onLogin={handleLogin} />
{:else}
 <div class="app-layout">
 <Header
 {view}
 {theme}
 streamOk={store.streamOk}
 version={store.stats?.version ?? ''}
 onView={switchView}
 onOpenEndpoint={openEndpoint}
 onToggleTheme={toggleTheme}
 onSelectLang={selectLang}
 onLogout={handleLogout}
 />

 <main class="app-main">
 <div class="main-container">
 {#if view === 'operations'}
 <OperationsView onOpenExpanded={openExpandedLogs} onOpenImport={openImportAccounts} />
 {:else if view === 'accounts'}
 <AccountsView onOpenImport={openImportAccounts} onOpenRename={openRename} />
 {:else if view === 'proxies'}
 <ProxiesView onOpenProxy={openProxy} onOpenBatchProxy={openBatchProxy} />
 {:else}
 <SettingsView onOpenMapping={openMapping} />
 {/if}
 </div>
 </main>

 <BottomNav {view} onView={switchView} />
 </div>

 {#if modal?.kind === 'importAccounts'}
 <ImportAccountsModal onClose={closeModal} />
 {:else if modal?.kind === 'rename'}
 <RenameModal target={$state.snapshot(modal.target)} onClose={closeModal} />
 {:else if modal?.kind === 'mapping'}
 <MappingModal
 mapping={modal.mapping ? $state.snapshot(modal.mapping): null}
 onClose={closeModal}
 />
 {:else if modal?.kind === 'proxy'}
 <ProxyModal onClose={closeModal} />
 {:else if modal?.kind === 'batchProxy'}
 <BatchProxyModal onClose={closeModal} />
 {:else if modal?.kind === 'endpoint'}
 <EndpointModal onClose={closeModal} />
 {:else if modal?.kind === 'expandedLogs'}
 <ExpandedLogsModal onClose={closeModal} />
 {/if}

 <div class="toast-box" role="alert" aria-live="polite">
 {#each toastState.items as item (item.id)}
 <div class="toast">
 <svg class="svg-icon" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12" /></svg>
 <span>{item.msg}</span>
 </div>
 {/each}
 </div>
{/if}
