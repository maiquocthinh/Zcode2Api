# Zcode2Api

A self-hosted gateway that exposes an **Anthropic Messages API** (`/v1/messages`) backed by a pool of **ZCode free accounts (Z.AI Coding Plan)**.

Import free-account JWTs into the pool. The gateway rotates between them, claims the daily free quota (captcha included), and switches to another account mid-request when one fails. Clients see a single endpoint and a single API key.

```
                    ┌──────────────────────────────────────────────────────┐
                    │                    Zcode2Api                         │
                    │                                                      │
  Claude Code       │   1. Auth          CLIENT_API_KEYS                   │
  Cursor      ──────►   2. Model map     claude-sonnet-4-5 → GLM-5.3-Flash │
  Cline             │   3. Pool          pick an ACTIVE account            │
                    │   4. Forward       rebuild as an official ZCode client│
                    │        │                                             │
                    │        ▼                                             │
                    │   zcode.z.ai (free quota)      api.z.ai (paid key,   │
                    │        │                       optional fallback)    │
                    │   5. First SSE event is an error? → mark account,    │
                    │      retry with the next one (client sees nothing)   │
                    │      First event OK?  → stream straight to client    │
                    │                                                      │
                    │   Background: daily claim + captcha solver,          │
                    │   account sweeper, SSE telemetry, web dashboard      │
                    └──────────────────────────────────────────────────────┘
```

The gateway has two upstream channels:

- **Channel A (default):** the pool of ZCode free accounts (`UPSTREAM_URL`, `zcode.z.ai`).
- **Channel B (optional):** a paid Z.AI API key (`FALLBACK_API_KEY`, `api.z.ai`). It is used only when the pool has no usable account left. Leave it empty to disable it.

## Features

| Concern | How it is handled |
|---|---|
| Dead account or exhausted quota | The first SSE event is checked before anything is sent to the client. A failing account is marked and the request is retried on another one. |
| Daily free quota | Scheduled claim in GMT+8 with random jitter. The captcha is solved by an embedded Node solver, or by an external solver URL. |
| Model names | Mapping rules, seeded from `DEFAULT_MODEL_*` on first boot and editable later. |
| Client identity | Requests are rebuilt to match the official ZCode wire format, which the upstream WAF requires. |
| Secrets | JWTs are encrypted at rest with AES-256-GCM when `SECRET_KEY` is set. Logs never contain prompts, responses, or tokens. |
| Fallback | Optional paid Z.AI API key, used only when the pool has no usable account. |
| Observability | Web dashboard with account states, real quota, live SSE logs, and token usage. |

## Requirements

- Go **1.25+** to build from source (see `go.mod`)
- Node.js 22+ **only** if you want the built-in captcha solver to run locally (`npm` is needed for the web UI build and the solver's dependencies)
- Or Docker, see below

## Quick start

### From source

```bash
# 1. Build the web UI (embedded into the binary)
cd web && npm ci && npm run build && cd ..

# 2. Build the gateway
go build -o zcode2api ./cmd/zcode2api

# 3. Configure (see the table below or .env.example)
export CLIENT_API_KEYS="client-key-16-chars-min"
export ADMIN_PASSWORD="admin-pass-16-chars-min"
export DEFAULT_MODEL_SONNET="GLM-5.3-Flash"
export DEFAULT_MODEL_OPUS="GLM-5.3"
export DEFAULT_MODEL_HAIKU="GLM-5.3-Flash"
export CLAIM_PLAN_ID="zcode-v3-start-plan-wk-1120-1"   # from GET /billing/balance

# 4. Run
./zcode2api
```

### With Docker

The image is built from `scratch` (no shell, no Node.js). Persist the database on a volume.

```bash
docker build -t zcode2api .

docker run -d --name zcode2api -p 3101:3101 \
  -e CLIENT_API_KEYS="client-key-16-chars-min" \
  -e ADMIN_PASSWORD="admin-pass-16-chars-min" \
  -e DEFAULT_MODEL_SONNET="GLM-5.3-Flash" \
  -e DEFAULT_MODEL_OPUS="GLM-5.3" \
  -e DEFAULT_MODEL_HAIKU="GLM-5.3-Flash" \
  -e CLAIM_PLAN_ID="zcode-v3-start-plan-wk-1120-1" \
  -v zcode2api-data:/data \
  zcode2api
```

The container has no Node.js, so the built-in captcha solver cannot run inside it. Use `CLAIM_SOLVER_URL` with an external solver, or claim manually (see [Daily claim](#daily-claim)).

### First-time setup

1. Open `http://localhost:3101` and log in with `ADMIN_PASSWORD`.
2. Import accounts, one per line: `Name | eyJ...` (the JWT of a logged-in ZCode client).
3. Point your client at the gateway:

```json
{
  "env": {
    "ANTHROPIC_BASE_URL": "http://localhost:3101",
    "ANTHROPIC_AUTH_TOKEN": "<one of CLIENT_API_KEYS>"
  }
}
```

Cursor and Cline work the same way: set the Anthropic base URL and the client key.

## API

| Method | Path | Auth | Purpose |
|---|---|---|---|
| `POST` | `/v1/messages` | client key | Chat completions, streaming and non-streaming |
| `POST` | `/v1/messages/count_tokens` | client key | Token counting |
| `GET` | `/v1/models` | client key | List the mapped model names |
| `GET` | `/healthz` | none | Returns `{"status":"ok"}` |
| `/api/*` | | admin session | Admin API used by the dashboard |

Clients authenticate with either `x-api-key: <key>` or `Authorization: Bearer <key>`. Invalid or missing keys get a `401` in the Anthropic error format.

## Configuration

Values are read from environment variables. Runtime settings saved from the dashboard take precedence over environment values, and most of them apply without a restart. A complete, commented list is in [`.env.example`](.env.example) and [docs/TECH_SPEC.md §8](docs/TECH_SPEC.md).

### Required

| Variable | Rule |
|---|---|
| `CLIENT_API_KEYS` | Comma-separated. Each key must be at least 16 characters. |
| `ADMIN_PASSWORD` | At least 16 characters. |

Any secret containing `change-me` is rejected. That is the placeholder marker used in `.env.example`, so the gateway will not start with the sample values.

### Main options

| Variable | Default | Notes |
|---|---|---|
| `SECRET_KEY` | empty | Encrypts stored JWTs. Changing it makes existing tokens unreadable. |
| `HOST` / `PORT` | `0.0.0.0` / `3101` | Listen address. |
| `DB_PATH` | `data/zcode2api.db` | SQLite database. Docker uses `/data/zcode2api.db`. |
| `FALLBACK_API_KEY` | empty | Paid Z.AI key, used only when the pool is exhausted. Empty disables it. |
| `FALLBACK_URL` | `https://api.z.ai` | Origin for the fallback channel. |
| `UPSTREAM_URL` | `https://zcode.z.ai` | Origin for the free-account pool. |
| `DEFAULT_MODEL_OPUS` / `_SONNET` / `_HAIKU` | empty | Upstream model names used to seed the mapping. |
| `CLAIM_PLAN_ID` | empty | Entitlement ID posted when claiming. Read it from `GET /billing/balance`. |
| `RISK_COOLDOWN` | `6h` | Cooldown after upstream code `3012`. `10m` is recommended, since the WAF window is minutes. |
| `CORS_ORIGINS` | empty | Comma-separated allowlist. Empty means same-origin only. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error`. |

### Pool and failover

| Variable | Default | Notes |
|---|---|---|
| `INFLIGHT_LIMIT` | `3` | Concurrent requests per account (1–32). |
| `MAX_ATTEMPTS` | `3` | Accounts tried per request before giving up (1–10). |
| `FIRST_EVENT_TIMEOUT` | `90s` | How long to wait for the first SSE event. |
| `COOLING_DURATION` | `60s` | Cooldown after 429, 3010, or 3002. |
| `EXHAUSTED_FALLBACK` | `1h` | Fallback recheck interval for accounts marked exhausted. |
| `FAIL_THRESHOLD` | `3` | Failures before an account is marked. |
| `AFFINITY_ENABLED` / `AFFINITY_TTL` | `true` / `30m` | Keep a session on the same account. |
| `DEFAULT_PROXY_URL` | empty | Egress proxy (`http`, `https`, or `socks5`). |

### Daily claim

| Variable | Default | Notes |
|---|---|---|
| `DAILY_CLAIM` | `true` | Enable the scheduled claim. |
| `CLAIM_TIME` | `00:05` | Time in GMT+8 (`HH:MM`). |
| `CLAIM_JITTER` | `10m` | Random delay added to the schedule. |
| `CLAIM_SOLVER_URL` / `CLAIM_SOLVER_KEY` | empty | External captcha solver. When set, it takes precedence over the embedded solver. |
| `CLAIM_EXHAUSTED` | `false` | Also claim accounts currently marked exhausted. |

### Identity

Keep the defaults `ZCODE_APP_VERSION=3.16.3` and `ZCODE_PLATFORM=darwin-x64`. They were verified against the upstream WAF. Changing them can cause requests to be rejected.

## Daily claim

Claims are made once a day for each active account. Outside Docker you can also run a single pass manually, for example from cron with `DAILY_CLAIM=false`:

```bash
./zcode2api claim --now
```

## Account states

| State | Entered when | Left when |
|---|---|---|
| `ACTIVE` | Account is added or recovers | A 1005, 3012, 1006, or 429 is seen, or an admin disables it |
| `COOLING` | 429, 3010, or 3002 | `COOLING_DURATION` elapses |
| `EXHAUSTED` | 1005 (quota used up) | Upstream reports a `next_flush_time`, or it passes 00:00 GMT+8 |
| `DISABLED` | 1006 (JWT dead), or disabled by an admin | An admin re-enables it, or a new JWT is imported |

Account selection (in order): keep only `ACTIVE` accounts under `INFLIGHT_LIMIT`, prefer the session's current account (affinity), skip accounts already tried in this request, prefer the one with the most quota, then round-robin among equals.

For the full request flow and failover rules, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Project layout

```
cmd/zcode2api/       entry point: server, `claim --now`, `healthcheck`
internal/config/     environment loading and validation
internal/server/     HTTP server, auth, CORS, SPA hosting
internal/proxy/      /v1/messages handling, first-event check, retries
internal/pool/       account pool, state machine, selection, claim scheduler, mapping
internal/upstream/   upstream clients, request identity, error classification
internal/admin/      admin API and log stream
internal/captcha/    captcha solver integration
web/                 Svelte dashboard (embedded at build time)
docs/                architecture, technical spec, implementation plan
```

## Testing

```bash
go test ./...
```

## Security notes

- Client keys, the admin password, and stored JWTs are compared or encrypted as described above. Use strong, unique values.
- Put the gateway behind HTTPS if it is reachable from outside your machine.
- The gateway uses the official ZCode identity. Pooled free-account usage must follow Z.AI's terms. That responsibility belongs to the operator.

## License

No public license yet. Contact the owner before reusing this code.
