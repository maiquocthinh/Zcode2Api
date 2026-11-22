#!/bin/bash
# Build the Svelte dashboard and the Go binary for release platforms.
# Mirrors the Dockerfile: web/dist -> go:embed -> binary.
# Artifacts go to build/ (gitignored).

echo "Building Zcode2Api dashboard (web/dist)..."
echo

# Reuse existing node_modules: npm ci wipes them, and a running editor
# language server (e.g. svelte-language-server) holds the native rollup
# .node file, which fails the wipe with EPERM on Windows. Install only
# when the tree is missing or stale relative to package-lock.json.
if ! (cd web && npm ci --dry-run >/dev/null 2>&1); then
	echo "node_modules missing or stale — running npm ci..."
	(cd web && npm ci) || exit 1
fi
(cd web && npm run build) || exit 1

echo
echo "Building Zcode2Api binaries..."
echo

# Stamp the release version from web/package.json.
VERSION=$(sed -n 's/.*"version": "\([^"]*\)".*/\1/p' web/package.json | head -n 1)
LDFLAGS="-s -w -X main.version=${VERSION}"

mkdir - build

echo "[1/4] Building Windows AMD64..."
GOOS=windows GOARCH=amd64 go build -trimpath -ldflags="${LDFLAGS}" -o build/zcode2api-windows-amd64.exe ./cmd/zcode2api || exit 1

echo "[2/4] Building Linux AMD64..."
GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="${LDFLAGS}" -o build/zcode2api-linux-amd64 ./cmd/zcode2api || exit 1

echo "[3/4] Building macOS AMD64 (Intel)..."
GOOS=darwin GOARCH=amd64 go build -trimpath -ldflags="${LDFLAGS}" -o build/zcode2api-macos-amd64 ./cmd/zcode2api || exit 1

echo "[4/4] Building macOS ARM64 (Apple Silicon)..."
GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags="${LDFLAGS}" -o build/zcode2api-macos-arm64 ./cmd/zcode2api || exit 1

echo
echo "✓ All builds completed successfully!"
echo
echo "Final build artifacts:"
ls -lh build/
echo
echo "Ready for GitHub release!"
