// Package captcha mints Aliyun captcha verify parameters for the daily claim
// flow using the browserless happy-dom node solver in
// internal/captcha/captcha_node (solver.js plus its package manifest,
// embedded and materialized to <data>/captcha_node on first use). Node itself
// stays an external runtime dependency.
package captcha

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha512"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"zcode2api/internal/upstream"
)

//go:embed all:captcha_node
var solverAssets embed.FS

// defaultCfg is the baked Aliyun scene of the ZCode claim flow — the
// fallback when the origin's live client/configs answer is unavailable. The
// baked region has gone stale before (an upstream region rotation), so the
// live fetch is preferred whenever it works.
var defaultCfg = cfg{Enabled: true, Prefix: "no8xfe", Region: "cn", SceneID: "11xygtvd"}

type cfg struct {
	Enabled bool
	Prefix  string
	Region  string
	SceneID string
}

// captchaCfgTTL bounds the live client/configs cache.
const captchaCfgTTL = 5 * time.Minute

// captchaCfgTimeout bounds one live fetch.
const captchaCfgTimeout = 15 * time.Second

var (
	captchaCfgMu  sync.Mutex
	captchaCfg    cfg
	captchaCfgAt  time.Time
	captchaCfgSet bool
)

// liveCfg returns the captcha scene of the origin's public client/configs
// endpoint, cached for captchaCfgTTL (a failed fetch also caches the fallback
// so a broken endpoint is not hammered on every mint). The gateway mints
// reactively — only on a claim 3007 — so an origin-side disable surfaces as a
// clear error at Solve instead of minting a param nobody accepts.
func (s *Solver) liveCfg(ctx context.Context) cfg {
	captchaCfgMu.Lock()
	if captchaCfgSet && time.Since(captchaCfgAt) < captchaCfgTTL {
		c := captchaCfg
		captchaCfgMu.Unlock()
		return c
	}
	captchaCfgMu.Unlock()

	c, ok := fetchCaptchaCfg(ctx)
	captchaCfgMu.Lock()
	defer captchaCfgMu.Unlock()
	if ok {
		captchaCfg, captchaCfgAt, captchaCfgSet = c, time.Now(), true
		return c
	}
	if !captchaCfgSet {
		captchaCfg, captchaCfgSet = defaultCfg, true
	}
	captchaCfgAt = time.Now()
	return captchaCfg
}

// CachedRegion returns the captcha region of the last known scene (live
// client/configs when fetched, else the baked default) without any network
// call — the region to pair with a verify param whose solver named none.
func CachedRegion() string {
	captchaCfgMu.Lock()
	defer captchaCfgMu.Unlock()
	if captchaCfgSet && captchaCfg.Region != "" {
		return captchaCfg.Region
	}
	return defaultCfg.Region
}

// fetchCaptchaCfg reads data.configs.captcha (data.captcha accepted as a
// fallback shape) from the origin's public client/configs endpoint with the
// channel A identity. ok=false on any transport/parse problem — the caller
// keeps its previous scene.
func fetchCaptchaCfg(ctx context.Context) (cfg, bool) {
	appVersion, platform := upstream.ChannelAIdentity()
	endpoint := joinURL(upstream.ChannelAOrigin(), "/api/v1/client/configs") +
		"?app_version=" + url.QueryEscape(appVersion) +
		"&platform=" + url.QueryEscape(platform)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return cfg{}, false
	}
	req.Header.Set("User-Agent", "ZCode/"+appVersion)
	req.Header.Set("X-ZCode-App-Version", appVersion)
	client := &http.Client{Timeout: captchaCfgTimeout, Transport: upstream.TransportFor("")}
	resp, err := client.Do(req)
	if err != nil {
		return cfg{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return cfg{}, false
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return cfg{}, false
	}
	var doc struct {
		Data struct {
			Configs *struct {
				Captcha *sceneJSON `json:"captcha"`
			} `json:"configs"`
			Captcha *sceneJSON `json:"captcha"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &doc) != nil {
		return cfg{}, false
	}
	var scene *sceneJSON
	if doc.Data.Configs != nil {
		scene = doc.Data.Configs.Captcha
	}
	if scene == nil {
		scene = doc.Data.Captcha
	}
	if scene == nil || scene.SceneID == "" || scene.Region == "" || scene.Prefix == "" {
		return cfg{}, false
	}
	// An omitted enabled flag with a full scene means enabled (the presence
	// of the scene is the delivery).
	enabled := scene.Enabled == nil || *scene.Enabled
	return cfg{Enabled: enabled, Prefix: scene.Prefix, Region: scene.Region, SceneID: scene.SceneID}, true
}

type sceneJSON struct {
	Enabled *bool  `json:"enabled"`
	Prefix  string `json:"prefix"`
	Region  string `json:"region"`
	SceneID string `json:"sceneId"`
}

// joinURL joins an origin and a path (the upstream package keeps its own
// copy private; this is the same one-liner).
func joinURL(baseURL, path string) string {
	return strings.TrimRight(baseURL, "/") + "/" + strings.TrimLeft(path, "/")
}

// Solver mints verify params by spawning the embedded node solver.
type Solver struct {
	dir      string // materialized solver directory ("" until first use)
	timeout  time.Duration
	nodePath string
}

// New builds a solver with the given per-attempt timeout.
func New(timeout time.Duration) *Solver {
	if timeout <= 0 {
		timeout = 40 * time.Second
	}
	return &Solver{timeout: timeout, nodePath: "node"}
}

// assetsSum fingerprints the embedded tree so a changed asset re-extracts.
func assetsSum() (string, error) {
	h := sha512.New()
	err := fs.WalkDir(solverAssets, "captcha_node", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		f, ferr := solverAssets.Open(p)
		if ferr != nil {
			return ferr
		}
		defer f.Close()
		if _, cerr := io.Copy(h, f); cerr != nil {
			return cerr
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ensureDir materializes the embedded solver to <dataDir>/captcha_node and
// returns that directory. Re-extracts only when the embedded tree changed.
func (s *Solver) ensureDir(dataDir string) (string, error) {
	dir := filepath.Join(dataDir, "captcha_node")
	marker := filepath.Join(dir, ".embedded-sha")
	if _, err := os.Stat(filepath.Join(dir, "solver.js")); err == nil {
		want, werr := assetsSum()
		if werr == nil {
			if got, rerr := os.ReadFile(marker); rerr == nil && strings.TrimSpace(string(got)) == want {
				return dir, nil
			}
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("captcha: create solver dir: %w", err)
	}
	sum, err := assetsSum()
	if err != nil {
		return "", fmt.Errorf("captcha: fingerprint assets: %w", err)
	}
	err = fs.WalkDir(solverAssets, "captcha_node", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, rerr := filepath.Rel("captcha_node", p)
		if rerr != nil {
			return rerr
		}
		if rel == "." {
			return nil
		}
		dst := filepath.Join(dir, filepath.FromSlash(rel))
		if d.IsDir() {
			return os.MkdirAll(dst, 0o755)
		}
		src, oerr := solverAssets.Open(p)
		if oerr != nil {
			return oerr
		}
		defer src.Close()
		return os.WriteFile(dst, readAll(src), 0o644)
	})
	if err != nil {
		return "", fmt.Errorf("captcha: extract solver: %w", err)
	}
	if err := os.WriteFile(marker, []byte(sum), 0o644); err != nil {
		return "", fmt.Errorf("captcha: write marker: %w", err)
	}
	return dir, nil
}

func readAll(r io.Reader) []byte {
	b, _ := io.ReadAll(r)
	return b
}

// childEnv strips mitm/proxy/CA variables so the solver talks to
// zcode.z.ai and aliyuncs.com directly from this host.
func childEnv() []string {
	drop := map[string]bool{
		"http_proxy": true, "https_proxy": true, "all_proxy": true,
		"HTTP_PROXY": true, "HTTPS_PROXY": true, "ALL_PROXY": true,
		"SSL_CERT_FILE": true, "SSL_CERT_DIR": true,
		"REQUESTS_CA_BUNDLE": true, "CURL_CA_BUNDLE": true,
		"NODE_EXTRA_CA_CERTS": true, "NODE_TLS_REJECT_UNAUTHORIZED": true,
	}
	env := os.Environ()[:0]
	for _, kv := range os.Environ() {
		if i := strings.IndexByte(kv, '='); i > 0 && !drop[kv[:i]] {
			env = append(env, kv)
		}
	}
	return env
}

// Solver tuning: a first-run "pe-stall" is normal solver lifecycle, not
// failure, so one mint gets a bounded retry loop; "exit status 1" usually
// means a stale CDN bundle, wiped (throttled) before the next attempt; a
// start with less than solverMinBudget left on the clock cannot succeed and
// is skipped instead of wiping the cache for nothing.
const (
	solverAttempts    = 3 // first mint + up to two retries
	solverMinBudget   = 5 * time.Second
	cacheWipeThrottle = 30 * time.Second
)

// errSolverCancelled marks a spawn aborted by its context: Solve maps it to
// ctx.Err() instead of treating it as a solver failure.
var errSolverCancelled = errors.New("captcha: solver cancelled")

// lastCacheWipe throttles the CDN cache wipe to one per 30s across callers.
var lastCacheWipe atomic.Int64

// Solve mints one verify parameter and returns it with the mint region — the
// claim retry must send the region header the param was minted with. dataDir
// is the gateway data
// directory (DB_PATH's parent semantics): the solver tree materializes under
// it and node_modules installs on first use (happy-dom + undici). The output
// contract is solver.js's: a line `VERIFY_PARAM=<param>`.
func (s *Solver) Solve(ctx context.Context, dataDir string) (string, string, error) {
	dir, err := s.ensureDir(dataDir)
	if err != nil {
		return "", "", err
	}
	if err := s.ensureModules(ctx, dir); err != nil {
		return "", "", err
	}
	sc := s.liveCfg(ctx)
	if !sc.Enabled {
		return "", "", errors.New("captcha: disabled by the origin's client/configs")
	}
	var lastErr error
	for attempt := 1; attempt <= solverAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", "", err
		}
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < solverMinBudget {
			break
		}
		param, err := s.spawn(ctx, dir, sc)
		if err == nil {
			return param, sc.Region, nil
		}
		if errors.Is(err, errSolverCancelled) {
			return "", "", ctx.Err()
		}
		lastErr = err
		if strings.Contains(err.Error(), "exit status 1") {
			wipeCDNCache()
		}
	}
	if lastErr == nil {
		return "", "", fmt.Errorf("captcha: solver skipped: not enough time left on the clock")
	}
	return "", "", lastErr
}

// spawn runs the node solver once with the live scene and parses its
// VERIFY_PARAM output.
func (s *Solver) spawn(ctx context.Context, dir string, sc cfg) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, s.nodePath, "solver.js", sc.SceneID, sc.Region, sc.Prefix)
	cmd.Dir = dir
	cmd.Env = childEnv()
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return "", errSolverCancelled
		}
		tail := strings.TrimSpace(errb.String())
		if len(tail) > 200 {
			tail = tail[len(tail)-200:]
		}
		return "", fmt.Errorf("captcha: solver exit: %v %s", err, tail)
	}
	scanner := bufio.NewScanner(&out)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "VERIFY_PARAM=") {
			continue
		}
		p := strings.TrimSpace(strings.TrimPrefix(line, "VERIFY_PARAM="))
		if p == "" {
			return "", fmt.Errorf("captcha: empty verify param")
		}
		if err := Ready(p); err != nil {
			return "", err
		}
		return p, nil
	}
	return "", fmt.Errorf("captcha: no VERIFY_PARAM in solver output")
}

// cdnCacheDir is where the node solver caches its CDN bundle (its default;
// the gateway child env sets no override).
func cdnCacheDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".zcode-captcha-cdn-cache")
}

// wipeCDNCache removes the solver's CDN bundle cache so the next attempt
// re-downloads it instead of running a stale bundle. Throttled: at most one
// wipe per 30s across all callers.
func wipeCDNCache() {
	dir := cdnCacheDir()
	if dir == "" {
		return
	}
	now := time.Now().Unix()
	if prev := lastCacheWipe.Load(); now-prev < int64(cacheWipeThrottle/time.Second) {
		return
	}
	lastCacheWipe.Store(now)
	_ = os.RemoveAll(dir)
}

// Ready validates a verify param the way the official client does:
// base64 JSON carrying certifyId + isSign + a
// securityToken of at least 50 chars. A short token is the SDK's degraded
// no-device result — upstream answers it 3007, so it is not usable.
func Ready(param string) error {
	raw, err := base64.StdEncoding.DecodeString(param)
	if err != nil {
		return fmt.Errorf("captcha: param is not base64: %w", err)
	}
	var p struct {
		SecurityToken string `json:"securityToken"`
		CertifyID     string `json:"certifyId"`
		IsSign        bool   `json:"isSign"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return fmt.Errorf("captcha: param is not JSON")
	}
	if p.CertifyID == "" || !p.IsSign || len(p.SecurityToken) < 50 {
		return fmt.Errorf("captcha: degraded param (certifyId=%t isSign=%t tokenLen=%d)",
			p.CertifyID != "", p.IsSign, len(p.SecurityToken))
	}
	return nil
}

// ensureModules installs the solver's npm dependencies once (happy-dom +
// undici, ~a few MB into the data dir). Missing node is reported here with a
// clear message instead of a confusing solver spawn failure later. The
// embedded lockfile pins npmmirror.com "remote" tarballs that many npm
// configs refuse (EALLOWREMOTE), so it is normalized to the official
// registry before install.
func (s *Solver) ensureModules(ctx context.Context, dir string) error {
	if _, err := os.Stat(filepath.Join(dir, "node_modules", "happy-dom")); err == nil {
		return nil // already installed
	}
	if _, err := exec.LookPath("npm"); err != nil {
		return fmt.Errorf("captcha: npm not on PATH — install Node.js to enable captcha claims: %w", err)
	}
	lockPath := filepath.Join(dir, "package-lock.json")
	if raw, err := os.ReadFile(lockPath); err == nil && bytes.Contains(raw, []byte("registry.npmmirror.com")) {
		_ = os.WriteFile(lockPath, bytes.ReplaceAll(raw,
			[]byte("https://registry.npmmirror.com/"), []byte("https://registry.npmjs.org/")), 0o644)
	}
	ictx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ictx, "npm", "install", "--no-audit", "--no-fund", "--loglevel=error")
	cmd.Dir = dir
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		tail := out.String()
		if len(tail) > 200 {
			tail = tail[len(tail)-200:]
		}
		return fmt.Errorf("captcha: npm install: %v %s", err, tail)
	}
	return nil
}
