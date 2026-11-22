// SSE subscriber for GET /api/logs/stream.
//
// The server sends `event: request | failover | state | claim` frames plus
// `:keepalive` comments every 20s. EventSource ignores comment frames by
// itself; the named events are dispatched to the callbacks registered here.
//
// Reconnection: EventSource retries automatically on transient network drops,
// but a failed HTTP status (e.g. 401 after session expiry) makes it stop. To
// make recovery deterministic in both cases we close the EventSource on error
// and schedule our own reconnect with capped exponential backoff (1s → 8s).

import type { ClaimEvent, ClaimJobEvent, FailoverEvent, RequestEvent, StateEvent } from './types';

export interface StreamCallbacks {
 request: (e: RequestEvent) => void;
 failover: (e: FailoverEvent) => void;
 state: (e: StateEvent) => void;
 claim: (e: ClaimEvent) => void;
 /** Claim-all pass progress (start/tick/done/aborted). */
 claim_job?: (e: ClaimJobEvent) => void;
 /** Connection is up again after at least one error. */
 reconnected?: () => void;
 /** true when the stream opened, false on a connection error. */
 status?: (ok: boolean) => void;
}

export class LogStream {
 private es: EventSource | null = null;
 private retryTimer: ReturnType<typeof setTimeout> | null = null;
 private attempts = 0;
 private everConnected = false;
 private closed = false;

 constructor(private readonly cb: StreamCallbacks) {}

 get connected(): boolean {
 return this.es !== null && this.es.readyState === EventSource.OPEN;
 }

 start(): void {
 this.closed = false;
 this.open();
 }

 stop(): void {
 this.closed = true;
 if (this.retryTimer !== null) {
 clearTimeout(this.retryTimer);
 this.retryTimer = null;
 }
 this.es?.close();
 this.es = null;
 this.attempts = 0;
 this.everConnected = false;
 }

 private open(): void {
 if (this.closed) return;
 // Fresh EventSource each time: cookies are sent same-origin by default.
 const es = new EventSource('/api/logs/stream');
 this.es = es;

 es.onopen = () => {
 this.cb.status?.(true);
 if (!this.everConnected) {
 this.everConnected = true;
 } else {
 this.cb.reconnected?.();
 }
 this.attempts = 0;
 };

 es.addEventListener('request', (ev) => {
 this.dispatch(this.cb.request, ev as MessageEvent<string>);
 });
 es.addEventListener('failover', (ev) => {
 this.dispatch(this.cb.failover, ev as MessageEvent<string>);
 });
 es.addEventListener('state', (ev) => {
 this.dispatch(this.cb.state, ev as MessageEvent<string>);
 });
 es.addEventListener('claim', (ev) => {
 this.dispatch(this.cb.claim, ev as MessageEvent<string>);
 });
 es.addEventListener('claim_job', (ev) => {
 this.dispatch(this.cb.claim_job ?? (() => {}), ev as MessageEvent<string>);
 });

 es.onerror = () => {
 es.close();
 this.es = null;
 this.cb.status?.(false);
 if (this.closed) return;
 // Capped exponential backoff: 1s, 2s, 4s, 8s, 8s,...
 const delay = Math.min(1000 * 2 ** this.attempts, 8000);
 this.attempts++;
 this.retryTimer = setTimeout(() => this.open(), delay);
 };
 }

 private dispatch(fn: (e: never) => void, ev: MessageEvent<string>): void {
 try {
 fn(JSON.parse(ev.data) as never);
 } catch {
 // Malformed frame: ignore rather than kill the stream.
 }
 }
}
