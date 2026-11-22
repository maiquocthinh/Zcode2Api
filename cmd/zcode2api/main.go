// zcode2api healthcheck GET /healthz on the local server; exits 0 on HTTP
// 200. Used by the Docker HEALTHCHECK because the
// scratch image has no shell, curl or wget.
// zcode2api claim --now run one daily-claim pass and
// exit; for external cron setups.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"zcode2api/internal/admin"
	"zcode2api/internal/config"
	"zcode2api/internal/pool"
	"zcode2api/internal/server"
	"zcode2api/internal/upstream"
	"zcode2api/web"
)

// main assigns it to upstream.Version before serving so the channel B
// User-Agent ("Zcode2Api/<version>" — the gateway never
// impersonates another client) and every dashboard
// payload report the same release.
var version = "3.1.0"

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "healthcheck":
			runHealthcheck()
			return
		case "claim":
			runClaimOnce(os.Args[2:])
			return
		default:
			fmt.Fprintf(os.Stderr, "zcode2api: unknown subcommand %q\n\n"+
				"Usage:\n  zcode2api              run the gateway\n"+
				"  zcode2api claim --now  run one claim pass and exit\n"+
				"  zcode2api healthcheck  probe /healthz (Docker HEALTHCHECK)\n", os.Args[1])
			os.Exit(2)
		}
	}
	run()
}

// run wires every component and blocks until SIGINT/SIGTERM.
func run() {
	upstream.Version = version

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "zcode2api: invalid configuration:", err)
		os.Exit(1)
	}

	store, err := pool.Open(cfg.DBPath, cfg.SecretKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, "zcode2api: open store:", err)
		os.Exit(1)
	}

	pl, err := pool.NewPool(store, pool.Config{
		InflightLimit:        cfg.InflightLimit,
		FailThreshold:        cfg.FailThreshold,
		AffinityEnabled:      cfg.AffinityEnabled,
		AffinityTTL:          cfg.AffinityTTL,
		DefaultProxyURL:      cfg.DefaultProxyURL,
		UpstreamURL:          cfg.UpstreamURL,
		QuotaRefreshInterval: time.Duration(cfg.QuotaRefreshMin) * time.Minute,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "zcode2api: init pool:", err)
		store.Close()
		os.Exit(1)
	}

	mapping, err := pool.NewMapping(store)
	if err != nil {
		fmt.Fprintln(os.Stderr, "zcode2api: init mapping:", err)
		store.Close()
		os.Exit(1)
	}
	if len(mapping.Rules()) == 0 {
		if err := mapping.ResetDefault(cfg); err != nil {
			fmt.Fprintln(os.Stderr, "zcode2api: WARNING empty model mapping, cannot seed defaults:", err)
		}
	}

	claimer, err := pool.NewClaimer(pl, pool.ClaimConfig{
		Enabled:          cfg.DailyClaim,
		ClaimTime:        cfg.ClaimTime,
		Jitter:           cfg.ClaimJitter,
		SolverURL:        cfg.ClaimSolverURL,
		SolverKey:        cfg.ClaimSolverKey,
		IncludeExhausted: cfg.ClaimExhausted,
		UpstreamURL:      cfg.UpstreamURL,
		PlanID:           cfg.ClaimPlanID,
		DataDir:          filepath.Dir(cfg.DBPath),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "zcode2api: init claimer:", err)
		pl.Stop()
		store.Close()
		os.Exit(1)
	}

	logs := admin.NewLogs()

	upstream.SetHeaderTimeout(cfg.UpstreamHeaderTimeout)

	upstream.SetChannelAIdentity(cfg.ZcodeAppVersion, cfg.ZcodePlatform)
	upstream.SetChannelAOrigin(cfg.UpstreamURL)

	srv := server.NewServer(cfg, server.Deps{
		Pool:    pl,
		Mapping: mapping,
		Store:   store,
		Logs:    logs,
		Claims:  claimer,
	}, distFS())

	pl.Start()
	claimer.Start(context.Background())

	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "zcode2api: listen:", err)
		pl.Stop()
		store.Close()
		os.Exit(1)
	}

	slog.Info("zcode2api starting",
		"version", version,
		"addr", srv.Addr,
		"db_path", cfg.DBPath,
		"upstream_url", cfg.UpstreamURL,
		"client_keys", len(cfg.ClientAPIKeys),
		"pool_accounts", pl.NumAccounts(),
		"daily_claim", cfg.DailyClaim,
		"mapping_rules", len(mapping.Rules()),
	)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		slog.Info("shutting down", "signal", sig.String())
		forceCh := make(chan os.Signal, 1)
		signal.Notify(forceCh, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-forceCh
			slog.Warn("second signal, forcing exit")
			os.Exit(1)
		}()
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server failed", "err", err)
		}
	}

	if err := server.Shutdown(srv); err != nil {
		slog.Warn("server shutdown timed out or failed", "err", err)
	}

	claimer.Close()
	pl.Stop()
	if err := store.Close(); err != nil {
		slog.Warn("store close", "err", err)
	}
	slog.Info("stopped")
}

// runClaimOnce implements `zcode2api claim --now`:
// one claim pass over the pool, then exit. Intended for external cron setups
// with DAILY_CLAIM=false. It never crashes on an empty pool and never logs
// secret material.
func runClaimOnce(args []string) {
	if len(args) == 0 || args[0] != "--now" {
		fmt.Fprintln(os.Stderr, "zcode2api claim: use `zcode2api claim --now`")
		os.Exit(2)
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "zcode2api: invalid configuration:", err)
		os.Exit(1)
	}
	store, err := pool.Open(cfg.DBPath, cfg.SecretKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, "zcode2api: open store:", err)
		os.Exit(1)
	}
	defer store.Close()

	pl, err := pool.NewPool(store, pool.Config{
		InflightLimit:        cfg.InflightLimit,
		FailThreshold:        cfg.FailThreshold,
		AffinityEnabled:      cfg.AffinityEnabled,
		AffinityTTL:          cfg.AffinityTTL,
		DefaultProxyURL:      cfg.DefaultProxyURL,
		UpstreamURL:          cfg.UpstreamURL,
		QuotaRefreshInterval: time.Duration(cfg.QuotaRefreshMin) * time.Minute,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "zcode2api: init pool:", err)
		os.Exit(1)
	}
	defer pl.Stop()

	upstream.SetChannelAIdentity(cfg.ZcodeAppVersion, cfg.ZcodePlatform)
	claimer, err := pool.NewClaimer(pl, pool.ClaimConfig{
		Enabled:          true,
		ClaimTime:        cfg.ClaimTime,
		Jitter:           cfg.ClaimJitter,
		SolverURL:        cfg.ClaimSolverURL,
		SolverKey:        cfg.ClaimSolverKey,
		IncludeExhausted: cfg.ClaimExhausted,
		UpstreamURL:      cfg.UpstreamURL,
		PlanID:           cfg.ClaimPlanID,
		DataDir:          filepath.Dir(cfg.DBPath),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "zcode2api: init claimer:", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	claimed := claimer.ClaimNow(ctx)
	fmt.Printf("claim: %d account(s) claimed, %d captcha skip(s)\n",
		claimed, claimer.CaptchaSkipped())
}

// distFS returns the embedded SPA filesystem rooted at web/dist
// (web/embed.go). The placeholder index.html committed in web/dist keeps this
// buildable before the first `npm run build`.
func distFS() fs.FS {
	sub, err := fs.Sub(web.DistFS, "dist")
	if err != nil {
		fmt.Fprintln(os.Stderr, "zcode2api: embed web/dist:", err)
		os.Exit(1)
	}
	return sub
}

// runHealthcheck implements `zcode2api healthcheck` — the probe used by the
// HEALTHCHECK of the scratch image (scratch has no curl). It GETs /healthz on
// the locally served port and exits 0 only
// on HTTP 200. It reads PORT/HOST directly from the environment so a probe can
// never fail merely because the required secrets are absent from a `docker
// exec`-style environment; the main server itself still refuses to start
// without them.
func runHealthcheck() {
	os.Exit(probeHealthcheck(healthcheckURL(os.Getenv("HOST"), os.Getenv("PORT"))))
}

// healthcheckURL builds the probe URL from the HOST/PORT environment values
// (PORT defaults to config.DefaultPort; wildcard or empty hosts probe the
// loopback interface).
func healthcheckURL(hostEnv, portEnv string) string {
	port := strings.TrimSpace(portEnv)
	if port == "" {
		port = strconv.Itoa(config.DefaultPort)
	}
	host := strings.TrimSpace(hostEnv)
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/healthz"
}

// probeHealthcheck performs one GET /healthz and returns the process exit
// code: 0 on HTTP 200, 1 on any transport failure or other status.
func probeHealthcheck(url string) int {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck: request failed:", err)
		return 1
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck: /healthz returned %s\n", resp.Status)
		return 1
	}

	fmt.Printf("healthcheck: ok %s\n", strings.TrimSpace(string(body)))
	return 0
}
