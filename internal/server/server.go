// Package server wires the data plane (internal/proxy), the control plane
// (internal/admin) and the embedded SPA behind one http.Server: Go 1.22
// ServeMux routes, middleware (request-id, body limits, panic recovery, slog
// access log), CORS allowlist and the SPA fallback for everything outside
// /api and /v1.
package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"zcode2api/internal/admin"
	"zcode2api/internal/config"
	"zcode2api/internal/pool"
	"zcode2api/internal/proxy"
	"zcode2api/internal/upstream"
)

// ShutdownTimeout bounds graceful shutdown: running streams get up to 30
// seconds to finish before the server returns.
const ShutdownTimeout = 30 * time.Second

// Deps are the collaborators built by main or by tests. The server
// package never imports the web UI package: the SPA arrives as fs.FS.
type Deps struct {
	Pool    *pool.Pool
	Mapping *pool.Mapping
	Store   *pool.Store
	Logs    *admin.Logs
	Claims  *pool.Claimer // optional; nil disables the claim endpoints
}

// NewServer builds the complete http.Server (not started). spa is the
// embedded UI filesystem (web/dist); nil disables SPA serving so tests and
// headless builds do not need web/dist.
func NewServer(cfg *config.Config, deps Deps, spa fs.FS) *http.Server {
	configureLogging(cfg.LogLevel)

	// Account state transitions become `event: state` on the dashboard stream.
	// Pool emits after releasing its own lock, so this
	// handler never deadlocks against pool.Acquire.
	deps.Pool.SetEventHandler(func(ev pool.StateEvent) {
		deps.Logs.State(admin.StateEntry{
			Ts:    ev.Ts.Unix(),
			Acc:   ev.AccountID,
			From:  string(ev.From),
			To:    string(ev.To),
			Until: unixOrZero(ev.Until),
			Kind:  ev.Kind,
		})
	})
	if deps.Claims != nil {
		// Claim progress becomes `event: claim` on the dashboard stream.
		deps.Claims.SetClaimHandler(func(ev pool.ClaimEvent) {
			deps.Logs.Claim(admin.ClaimEntry{
				Ts: ev.Ts, Acc: ev.AccountID,
				OK: ev.OK, Kind: ev.Kind, CaptchaRequired: ev.CaptchaRequired,
				NextClaimAt: ev.NextClaimAt,
			})
		})
		// Claim-all progress becomes `event: claim_job` (start/tick/done) for
		// the dashboard's progress bar.
		deps.Claims.SetJobHandler(func(ev pool.ClaimJobEvent) {
			deps.Logs.ClaimJob(admin.ClaimJobEntry{Phase: ev.Phase, Done: ev.Done, Total: ev.Total})
		})
	}

	proxyH := proxy.NewHandler(cfg, deps.Pool, deps.Mapping, deps.Logs)
	adminH := admin.NewHandlers(cfg, deps.Pool, deps.Mapping, deps.Store, deps.Logs,
		admin.WithAccountTest(newAccountTester(cfg, deps.Pool)),
		admin.WithClaimer(deps.Claims),
		admin.WithSettingsChanged(func() { applyRuntimeSettings(cfg, deps, proxyH) }),
	)

	// Runtime settings stored in the settings table override ENV values,
	// both at startup and after PUT /api/settings.
	applyRuntimeSettings(cfg, deps, proxyH)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	auth := clientAuth(cfg.ClientAPIKeys)
	mux.Handle("POST /v1/messages", auth(http.HandlerFunc(proxyH.ServeMessages)))
	mux.Handle("POST /v1/messages/count_tokens", auth(http.HandlerFunc(proxyH.ServeCountTokens)))
	mux.Handle("GET /v1/models", auth(http.HandlerFunc(proxyH.ServeModels)))

	adminH.Register(mux)

	if spa != nil {
		mux.Handle("/", spaHandler(spa))
	} else {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			http.NotFound(w, r)
		})
	}

	handler := observe(mux)
	handler = cors(cfg.CORSOrigins, handler)

	return &http.Server{
		Addr:    net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Handler: handler,
		// Streams run for minutes: no WriteTimeout, generous idle timeout.
		// ReadHeaderTimeout only protects the header read.
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

// Shutdown gracefully stops srv, giving in-flight streams up to
// ShutdownTimeout (30s) to finish before force-closing the rest.
func Shutdown(srv *http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer cancel()
	return srv.Shutdown(ctx)
}

// --- client authentication ---------------------------------

// clientAuth guards /v1/* with the CLIENT_API_KEYS: x-api-key header or
// "Authorization: Bearer <key>". Wrong or missing keys get 401
// authentication_error in the Anthropic format.
func clientAuth(keys []string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("X-Api-Key")
			if key == "" {
				scheme, token, _ := strings.Cut(r.Header.Get("Authorization"), " ")
				if strings.EqualFold(scheme, "Bearer") {
					key = strings.TrimSpace(token)
				}
			}
			if !keyMatches(keys, key) {
				proxy.WriteError(w, http.StatusUnauthorized, "authentication_error",
					"invalid or missing API key", 0, 0)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// keyMatches compares against every configured key with a constant-time
// comparison. All keys are compared to keep timing flat.
func keyMatches(keys []string, key string) bool {
	if key == "" {
		return false
	}
	found := false
	for _, k := range keys {
		if subtle.ConstantTimeCompare([]byte(k), []byte(key)) == 1 {
			found = true
		}
	}
	return found
}

// --- CORS ------------------------------------------------------

// cors applies the CORS_ORIGINS allowlist; an empty list means same-origin
// only — no CORS headers at all.
func cors(origins []string, next http.Handler) http.Handler {
	if len(origins) == 0 {
		return next
	}
	allowed := make(map[string]bool, len(origins))
	for _, o := range origins {
		allowed[strings.TrimSpace(o)] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && allowed[origin] {
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			h.Add("Vary", "Origin")
			h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Content-Type, X-Api-Key, Authorization, X-Requested-With, Anthropic-Version, Anthropic-Beta")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// --- request id, panic recovery, access log --------------------------------------
type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(code int) {
	if w.wrote {
		return // a second WriteHeader would panic in net/http
	}
	w.status = code
	w.wrote = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.wrote = true
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// observe wraps the mux with request-id propagation, panic recovery and one
// slog access-log line per request. Log lines carry method, path, status,
// duration and rid — never bodies, keys, or prompt content.
func observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rid := admin.NewRequestID()
		r = r.WithContext(admin.WithRequestID(r.Context(), rid))
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}

		defer func() {
			if p := recover(); p != nil {
				slog.Error("panic recovered",
					"rid", rid, "method", r.Method, "path", r.URL.Path,
					"panic", fmt.Sprint(p), "stack", string(debug.Stack()))
				if !sw.wrote {
					proxy.WriteError(sw, http.StatusInternalServerError,
						"api_error", "internal error", 0, 0)
				}
				sw.status = http.StatusInternalServerError
			}
			slog.Info("request",
				"method", r.Method, "path", r.URL.Path, "status", sw.status,
				"ms", time.Since(start).Milliseconds(), "rid", rid)
		}()

		next.ServeHTTP(sw, r)
	})
}

// --- SPA (fs.FS, no web package import) -------------------------------------------

// spaHandler serves the embedded UI: real files from fsys, and index.html for
// every unknown path so client-side routing works.
func spaHandler(fsys fs.FS) http.Handler {
	fileServer := http.FileServerFS(fsys)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/")
		if p == "" || !fs.ValidPath(p) {
			serveIndex(w, fsys)
			return
		}
		st, err := fs.Stat(fsys, p)
		if err != nil || st.IsDir() {
			serveIndex(w, fsys) // SPA fallback
			return
		}
		fileServer.ServeHTTP(w, r)
	})
}

func serveIndex(w http.ResponseWriter, fsys fs.FS) {
	data, err := fs.ReadFile(fsys, "index.html")
	if err != nil {
		http.NotFound(w, nil)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// --- account test (POST /api/accounts/{id}/test) ----------------

// newAccountTester builds the admin account probe: one minimal /v1/messages
// request (max_tokens 1, DEFAULT_MODEL_HAIKU) sent on channel A with the
// account's own JWT and device_mid through its egress proxy, classified like
// any other upstream response. It runs inside Pool.WithAccount so counters
// and the state machine behave exactly as for a proxied request.
func newAccountTester(cfg *config.Config, pl *pool.Pool) admin.AccountTestFunc {
	return func(ctx context.Context, id string) (ok bool, kind string, latencyMS int64, err error) {
		if cfg.DefaultModelHaiku == "" {
			return false, "", 0, fmt.Errorf("DEFAULT_MODEL_HAIKU is not configured")
		}
		buildErr := error(nil)
		werr := pl.WithAccount(id, func(acc upstream.ChannelAAccount, proxyURL string) upstream.Outcome {
			body, merr := json.Marshal(map[string]any{
				"model":      cfg.DefaultModelHaiku,
				"max_tokens": 1,
				"messages":   []map[string]any{{"role": "user", "content": "ping"}},
			})
			if merr != nil {
				buildErr = merr
				return upstream.Outcome{}
			}
			// Channel A probe: a model request, so it uses
			// the same parity set as the data plane (attribution, body
			// metadata.user_id with the account fingerprint, official system
			// blocks) — no captcha header.
			attr := upstream.NewAttribution()
			probeBody := upstream.InjectModelIdentity(body, acc.DeviceMid, attr)
			req, berr := upstream.BuildChannelARequest(ctx, cfg.UpstreamURL,
				acc.JWT, attr, nil, probeBody)
			if berr != nil {
				buildErr = berr
				return upstream.Outcome{}
			}
			// A probe response is tiny and non-streaming, so a timeout is
			// safe here (unlike the streaming data-plane client).
			client := &http.Client{
				Transport: upstream.TransportFor(proxyURL),
				Timeout:   30 * time.Second,
			}
			t0 := time.Now()
			resp, derr := client.Do(req)
			latencyMS = time.Since(t0).Milliseconds()
			if derr != nil {
				out := upstream.ClassifyTransportError(derr)
				kind = string(out.Kind)
				return out
			}
			respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
			resp.Body.Close()
			out := upstream.Classify(resp.StatusCode, respBody, resp.Header)
			kind = string(out.Kind)
			ok = out.Kind == upstream.KindOK
			return out
		})
		if werr != nil {
			return false, "", 0, werr // account not found
		}
		if buildErr != nil {
			return false, "", latencyMS, buildErr
		}
		return ok, kind, latencyMS, nil
	}
}

// --- runtime settings -------------------------------------

// applyRuntimeSettings reads the persisted runtime settings and applies them
// to the upstream classifier, the pool, the claimer and the proxy handler
// immediately, without a restart. ENV values (cfg) are the defaults; stored
// values win.
func applyRuntimeSettings(cfg *config.Config, deps Deps, proxyH *proxy.Handler) {
	stored, err := deps.Store.AllSettings()
	if err != nil {
		slog.Warn("server: read runtime settings", "err", err)
		stored = map[string]string{}
	}
	duration := func(key string, def time.Duration) time.Duration {
		if v, ok := stored[key]; ok {
			if d, perr := time.ParseDuration(v); perr == nil && d > 0 {
				return d
			}
		}
		return def
	}
	number := func(key string, def int) int {
		if v, ok := stored[key]; ok {
			if n, perr := strconv.Atoi(v); perr == nil {
				return n
			}
		}
		return def
	}
	boolean := func(key string, def bool) bool {
		if v, ok := stored[key]; ok {
			if b, perr := strconv.ParseBool(v); perr == nil {
				return b
			}
		}
		return def
	}
	stringVal := func(key, def string) string {
		if v, ok := stored[key]; ok && v != "" {
			return v
		}
		return def
	}

	upstream.SetTuning(
		duration("cooling_duration", cfg.CoolingDuration),
		duration("exhausted_fallback", cfg.ExhaustedFallback),
		duration("risk_cooldown", cfg.RiskCooldown),
	)
	deps.Pool.ApplyRuntime(
		number("inflight_limit", cfg.InflightLimit),
		cfg.FailThreshold,
		boolean("affinity_enabled", cfg.AffinityEnabled),
		duration("affinity_ttl", cfg.AffinityTTL),
		cfg.DefaultProxyURL,
	)
	proxyH.ApplyRuntime(
		number("max_attempts", cfg.MaxAttempts),
		boolean("strict_models", cfg.StrictModels),
		stringVal("fallback_api_key", cfg.FallbackAPIKey),
	)
	if deps.Claims != nil {
		deps.Claims.ApplyRuntime(
			boolean("daily_claim", cfg.DailyClaim),
			stringVal("claim_time", cfg.ClaimTime),
			stringVal("claim_solver_url", cfg.ClaimSolverURL),
			stringVal("claim_solver_key", cfg.ClaimSolverKey),
			boolean("claim_exhausted", cfg.ClaimExhausted),
		)
	}
}

// --- helpers ------------------------------------------------------------------------
func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

// configureLogging installs the JSON slog handler with LOG_LEVEL.
func configureLogging(level string) {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l})))
}
