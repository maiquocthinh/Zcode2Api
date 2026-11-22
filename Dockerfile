# syntax=docker/dockerfile:1
#
# Zcode2Api — 3-stage build (IMPLEMENTATION_PLAN.md Phase 6):
#   1. node:22-alpine        builds the Svelte SPA into web/dist
#   2. golang:1.25-alpine    builds a static binary (CGO_ENABLED=0, -trimpath,
#                            -ldflags "-s -w -X main.version=…")
#   3. scratch               binary + CA certificates only; USER 65534, VOLUME
#                            /data, HEALTHCHECK via `zcode2api healthcheck`
#                            (scratch has no shell, curl or wget).
#
# Release size target: image < 25MB, binary < 15MB (ARCHITECTURE.md goals).

# --- Stage 1: Web UI -----------------------------------------------------------------
FROM node:22-alpine AS ui

WORKDIR /app/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

# --- Stage 2: Go binary ---------------------------------------------------------------
FROM golang:1.25-alpine AS build

# ca-certificates.crt is copied into the scratch image; install it explicitly so
# its presence never depends on base-image drift.
RUN apk add --no-cache ca-certificates

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY internal/ ./internal/
COPY cmd/ ./cmd/
COPY web/embed.go ./web/embed.go
COPY --from=ui /app/web/dist ./web/dist

# VERSION is the release string stamped into main.version and reported by the
# outbound User-Agent ("Zcode2Api/<version>") and the dashboard. Override with:
#   docker build --build-arg VERSION=x.y.z .
ARG VERSION=3.1.0
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/zcode2api ./cmd/zcode2api

# /data must exist and be writable by uid 65534 (nobody): scratch cannot RUN
# mkdir/chown, so the directory is prepared here and copied with --chown.
RUN mkdir -p /data && chown 65534:65534 /data

# --- Stage 3: runtime ------------------------------------------------------------------
FROM scratch

COPY --from=build /out/zcode2api /zcode2api

# TLS roots are mandatory: both upstream channels are HTTPS (TECH_SPEC.md §2 —
# channel A UPSTREAM_URL, default https://zcode.z.ai, and the optional channel
# B FALLBACK_URL, default https://api.z.ai).
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# zoneinfo is deliberately NOT copied: the codebase performs no named-zone
# lookups (no time.LoadLocation / ParseInLocation; "zoneinfo nếu cần",
# IMPLEMENTATION_PLAN.md Phase 6 — it is not needed).

COPY --from=build --chown=65534:65534 /data /data

# Never run as root (IMPLEMENTATION_PLAN.md Phase 6).
USER 65534:65534

# SQLite database lives on the volume; override DB_PATH only together with a
# matching mount.
VOLUME ["/data"]
ENV DB_PATH=/data/zcode2api.db \
    HOST=0.0.0.0 \
    PORT=3101

EXPOSE 3101

ENTRYPOINT ["/zcode2api"]

# Probe /healthz with the binary itself (subcommand "healthcheck") because the
# scratch image has no shell or curl (TECH_SPEC.md §1.2: 200 {"status":"ok"}).
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD ["/zcode2api", "healthcheck"]
