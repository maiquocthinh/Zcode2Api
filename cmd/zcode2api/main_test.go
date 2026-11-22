package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHealthcheckURL pins the probe target used by the Docker HEALTHCHECK of
// the scratch image: PORT/HOST from the
// environment, loopback for wildcard or empty hosts, path /healthz.
func TestHealthcheckURL(t *testing.T) {
	tests := []struct {
		name    string
		hostEnv string
		portEnv string
		want    string
	}{
		{"defaults", "", "", "http://127.0.0.1:3101/healthz"},
		{"wildcard host", "0.0.0.0", "8080", "http://127.0.0.1:8080/healthz"},
		{"ipv6 wildcard host", "::", "3101", "http://[::1]:3101/healthz"},
		{"specific host", "192.0.2.10", "3101", "http://192.0.2.10:3101/healthz"},
		{"blank port falls back to default", "127.0.0.1", "  ", "http://127.0.0.1:3101/healthz"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := healthcheckURL(tc.hostEnv, tc.portEnv); got != tc.want {
				t.Errorf("healthcheckURL(%q, %q) = %q, want %q", tc.hostEnv, tc.portEnv, got, tc.want)
			}
		})
	}
}

// TestProbeHealthcheck exercises probeHealthcheck against a local test server
// only (no real network — rule: upstream is always an
// httptest.Server in tests). Exit 0 only on HTTP 200 from /healthz.
func TestProbeHealthcheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	if code := probeHealthcheck(srv.URL + "/healthz"); code != 0 {
		t.Errorf("probeHealthcheck(ok) = %d, want 0", code)
	}
	if code := probeHealthcheck(srv.URL + "/other"); code != 1 {
		t.Errorf("probeHealthcheck(404 path) = %d, want 1", code)
	}
	// Connection refused: an unreachable port on loopback.
	if code := probeHealthcheck("http://127.0.0.1:1/healthz"); code != 1 {
		t.Errorf("probeHealthcheck(unreachable) = %d, want 1", code)
	}
}

// TestVersionPinned guards the link-time override contract: main.version feeds
// upstream.Version (User-Agent "Zcode2Api/<version>",).
func TestVersionPinned(t *testing.T) {
	if version == "" {
		t.Fatal("version is empty; -ldflags -X main.version would produce an empty User-Agent version")
	}
	if !strings.HasPrefix(version, "3.") {
		t.Errorf("version = %q, want a 3.x default matching upstream.Version", version)
	}
}
