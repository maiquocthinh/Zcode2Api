<script lang="ts">
	// Top app bar: brand, desktop nav tabs, endpoint quick view,
	// language dropdown (VI/EN/ZH/JA) and the light/dark theme toggle.
	import { t, getLang, LANGS, LANG_NAMES, type Lang } from '../i18n.svelte';
	import type { ViewName } from '../../App.svelte';

	interface Props {
		view: ViewName;
		theme: 'dark' | 'light';
		streamOk: boolean;
		version: string;
		onView: (v: ViewName) => void;
		onOpenEndpoint: () => void;
		onToggleTheme: () => void;
		onSelectLang: (l: Lang) => void;
		onLogout: () => Promise<void>;
	}

	let {
		view,
		theme,
		streamOk,
		version,
		onView,
		onOpenEndpoint,
		onToggleTheme,
		onSelectLang,
		onLogout,
	}: Props = $props();

	let menuOpen = $state(false);

	const TABS: {
		id: ViewName;
		icon: string;
		label: 'nav_operations' | 'nav_accounts' | 'nav_proxies' | 'nav_settings';
	}[] = [
		{
			id: 'operations',
			icon: '<rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/>',
			label: 'nav_operations',
		},
		{
			id: 'accounts',
			icon: '<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/>',
			label: 'nav_accounts',
		},
		{
			id: 'proxies',
			icon: '<circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>',
			label: 'nav_proxies',
		},
		{
			id: 'settings',
			icon: '<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/>',
			label: 'nav_settings',
		},
	];

	// Close the language menu on any outside click.
	function onWindowClick(e: MouseEvent): void {
		const wrap = document.querySelector('.lang-dropdown-wrapper');
		if (menuOpen && wrap && !wrap.contains(e.target as Node)) {
			menuOpen = false;
		}
	}
</script>

<svelte:window onclick={onWindowClick} />

<header class="app-header">
	<div class="brand-section">
		<span class="brand-mark"></span>
		<div class="brand-title">
			ZCODE2API
			<span class="tagline">{t('tagline')}</span>
		</div>
		<!-- Desktop Navigation Tabs (>= 900px; below that the BottomNav takes over) -->
		<nav class="nav-tabs" aria-label="Desktop Navigation">
			{#each TABS as tab (tab.id)}
				<button class="nav-tab" class:active={view === tab.id} onclick={() => onView(tab.id)}>
					<svg class="svg-icon" viewBox="0 0 24 24">{@html tab.icon}</svg>
					<span>{t(tab.label)}</span>
				</button>
			{/each}
		</nav>
	</div>

	<div class="header-actions">
		<!-- Live status pill: SSE stream state + version -->
		<span class="system-status">
			<span
				class="status-dot"
				class:pulse={streamOk}
				style={streamOk ? '' : 'background: var(--danger);'}
			></span>
			{streamOk ? t('sys_online') : t('sys_offline')}
			{#if version}
				<span style="color: var(--subtle);">v{version}</span>
			{/if}
		</span>

		<!-- Endpoint quick view -->
		<button class="btn btn-sm" onclick={onOpenEndpoint} title={t('modal_endpoint_title')}>
			<span
				style="font-weight: 800; font-family: var(--font-mono); color: var(--accent-text); font-size: 11px;"
			>
				&lt;/&gt;
			</span>
		</button>

		<div class="lang-dropdown-wrapper">
			<button
				class="btn btn-sm lang-dropdown-btn"
				onclick={() => (menuOpen = !menuOpen)}
				title="Select Language"
			>
				<svg class="svg-icon" viewBox="0 0 24 24">
					<circle cx="12" cy="12" r="10" /><line x1="2" y1="12" x2="22" y2="12" /><path
						d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"
					/>
				</svg>
				<strong style="color: var(--accent-text);">{getLang().toUpperCase()}</strong>
				<svg class="svg-icon" viewBox="0 0 24 24" style="width: 10px; height: 10px; opacity: 0.7;">
					<polyline points="6 9 12 15 18 9" />
				</svg>
			</button>
			{#if menuOpen}
				<div class="lang-menu open">
					<div class="lang-menu-head">// SELECT LOCALE</div>
					{#each LANGS as lang (lang)}
						<button
							class="lang-option"
							class:active={lang === getLang()}
							onclick={() => {
								onSelectLang(lang);
								menuOpen = false;
							}}
						>
							<span class="lang-tag">{lang.toUpperCase()}</span>
							<span class="lang-name">{LANG_NAMES[lang]}</span>
							<span class="lang-check">✓</span>
						</button>
					{/each}
				</div>
			{/if}
		</div>

		<!-- Light / dark theme toggle (sun / moon icon) -->
		<button
			class="btn btn-sm"
			onclick={onToggleTheme}
			title={t('toast_theme', { mode: theme === 'dark' ? t('theme_light') : t('theme_dark') })}
		>
			{#if theme === 'dark'}
				<svg class="svg-icon" viewBox="0 0 24 24">
					<circle cx="12" cy="12" r="5" /><line x1="12" y1="1" x2="12" y2="3" /><line
						x1="12"
						y1="21"
						x2="12"
						y2="23"
					/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64" /><line
						x1="18.36"
						y1="18.36"
						x2="19.78"
						y2="19.78"
					/><line x1="1" y1="12" x2="3" y2="12" /><line x1="21" y1="12" x2="23" y2="12" /><line
						x1="4.22"
						y1="19.78"
						x2="5.64"
						y2="18.36"
					/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
				</svg>
				<span>LIGHT</span>
			{:else}
				<svg class="svg-icon" viewBox="0 0 24 24">
					<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
				</svg>
				<span>DARK</span>
			{/if}
		</button>

		<button class="btn btn-sm" onclick={() => void onLogout()} title={t('btn_logout')}>
			<svg class="svg-icon" viewBox="0 0 24 24">
				<path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" /><polyline
					points="16 17 21 12 16 7"
				/><line x1="21" y1="12" x2="9" y2="12" />
			</svg>
			<span>{t('btn_logout')}</span>
		</button>
	</div>
</header>
