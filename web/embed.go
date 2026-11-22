// Package web embeds the built Svelte SPA (web/dist) into the Go binary:
// main.go serves DistFS through fs.Sub(..., "dist") to server.NewServer,
// which answers every path outside /api and /v1 with dist/index.html (SPA
// fallback). A minimal dist/index.html placeholder is committed so `go build`
// works before the first `npm run build`.
package web

import "embed"

//go:embed all:dist
var DistFS embed.FS
