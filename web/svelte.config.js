import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

// Svelte 5 with TypeScript preprocessing (no Tailwind, no PostCSS — the UI
// uses the plain CSS tokens of demo/index.html,).
export default {
 preprocess: vitePreprocess()
};
