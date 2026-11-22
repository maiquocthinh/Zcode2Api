// Small formatting helpers shared by the views.

import { t } from './i18n.svelte';

const numberFmt = new Intl.NumberFormat('en-US');

/** 1234567 → "1,234,567". */
export function fmtNum(n: number | undefined): string {
  if (n === undefined || Number.isNaN(n)) return '0';
  return numberFmt.format(n);
}

/** 1300000 → "1.3M", 482000 → "482K". */
export function fmtCompact(n: number | undefined): string {
  if (n === undefined || Number.isNaN(n)) return '0';
  if (n >= 1_000_000) return trim(n / 1_000_000) + 'M';
  if (n >= 1_000) return trim(n / 1_000) + 'K';
  return String(n);
}

function trim(v: number): string {
  return (Math.round(v * 10) / 10).toString();
}

/** Proxy latency from /api/proxies: -1 means untested. */
export function fmtLatency(ms: number): string {
  if (ms < 0) return '—';
  return ms + 'ms';
}

/** HH:MM:SS for log timestamps. */
export function fmtClock(unixSec: number): string {
  const d = new Date(unixSec * 1000);
  const p = (v: number) => String(v).padStart(2, '0');
  return p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds());
}

/** Cooldown/reset deadline as a wall-clock time string. */
export function fmtUntil(unixSec: number | undefined): string {
  if (!unixSec) return '';
  const d = new Date(unixSec * 1000);
  const p = (v: number) => String(v).padStart(2, '0');
  return p(d.getHours()) + ':' + p(d.getMinutes());
}

/** ISO date (YYYY-MM-DD) for plan-expiry / claim timestamps. */
export function fmtDate(unixSec: number | undefined | null): string {
  if (!unixSec) return '';
  const d = new Date(unixSec * 1000);
  const p = (v: number) => String(v).padStart(2, '0');
  return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate());
}

/** Whole days until a deadline (ceil, min 0); 0 = today/overdue. */
export function fmtDaysLeft(unixSec: number | undefined | null): number {
  if (!unixSec) return 0;
  return Math.max(0, Math.ceil((unixSec * 1000 - Date.now()) / 86_400_000));
}

/** Relative "x ago" using the active dictionary. */
export function fmtAgo(unixSec: number | undefined): string {
  if (!unixSec) return t('time_never');
  const s = Math.max(0, Math.floor(Date.now() / 1000 - unixSec));
  if (s < 10) return t('time_now');
  if (s < 60) return t('time_s_ago', { n: s });
  if (s < 3600) return t('time_m_ago', { n: Math.floor(s / 60) });
  if (s < 86400) return t('time_h_ago', { n: Math.floor(s / 3600) });
  return t('time_d_ago', { n: Math.floor(s / 86400) });
}

/** Copy to clipboard with fallback for non-secure contexts. */
export async function copyText(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    // fall through to the legacy path
  }
  try {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand('copy');
    ta.remove();
    return ok;
  } catch {
    return false;
  }
}

/** Trigger a JSON/text file download (export backup, log dump). */
export function downloadFile(filename: string, content: string, mime: string): void {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
