import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

// Svelte 5 SPA for the Zcode2Api dashboard.
// - Build output goes to web/dist which is embedded into the Go binary
// (web/embed.go, //go:embed all:dist).
// - In dev mode /api (control plane) and /v1 (data plane) are proxied to the
// Go gateway on localhost:3101.
export default defineConfig({
 plugins: [svelte()],
 build: {
 outDir: 'dist',
 emptyOutDir: true,
 target: 'es2022'
 },
 server: {
 proxy: {
 '/api': {
 target: 'http://localhost:3101',
 changeOrigin: false
 },
 '/v1': {
 target: 'http://localhost:3101',
 changeOrigin: false
 }
 }
 }
});
