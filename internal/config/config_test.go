package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Valid secrets: long enough and free of the "change-me" placeholder marker.
const (
	validClientKey = "z2a-test-client-key-0123456789abcdef"
	validAdminPass = "z2a-test-admin-password-0123456789"
)

// envKeys lists every variable of the table, in table order.
// config.Load reads exactly these keys.
var envKeys = []string{
	"HOST", "PORT",
	"CLIENT_API_KEYS", "ADMIN_PASSWORD", "SECRET_KEY",
	"DB_PATH",
	"UPSTREAM_URL", "ZCODE_APP_VERSION", "ZCODE_PLATFORM",
	"FALLBACK_URL", "FALLBACK_API_KEY",
	"DAILY_CLAIM", "CLAIM_TIME", "CLAIM_JITTER", "CLAIM_SOLVER_URL", "CLAIM_SOLVER_KEY", "CLAIM_EXHAUSTED",
	"DEFAULT_PROXY_URL",
	"DEFAULT_MODEL_OPUS", "DEFAULT_MODEL_SONNET", "DEFAULT_MODEL_HAIKU",
	"STRICT_MODELS",
	"INFLIGHT_LIMIT", "MAX_ATTEMPTS",
	"ATTEMPT_DEADLINE", "FIRST_EVENT_TIMEOUT", "FIRST_EVENT_MAX_BYTES",
	"UPSTREAM_HEADER_TIMEOUT", "COOLING_DURATION", "RISK_COOLDOWN", "EXHAUSTED_FALLBACK",
	"FAIL_THRESHOLD", "AFFINITY_ENABLED", "AFFINITY_TTL",
	"MAX_BODY_BYTES",
	"CORS_ORIGINS", "LOG_LEVEL",
}

// spec8Keys is the same list used to verify that .env.example reflects the
// whole table.
var spec8Keys = append([]string(nil), envKeys...)

// clearEnv unsets every configuration variable for the duration of the test so
// each test starts from the documented defaults.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range envKeys {
		old, had := os.LookupEnv(k)
		if had {
			os.Unsetenv(k)
			t.Cleanup(func() { os.Setenv(k, old) })
		}
	}
}

// mustLoad is load-or-fatal for tests that expect success.
func mustLoad(t *testing.T) *Config {
	t.Helper()
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned unexpected error: %v", err)
	}
	return cfg
}

// wantLoadError asserts Load fails and the message mentions the given fragment.
func wantLoadError(t *testing.T, wantFragment string) {
	t.Helper()
	cfg, err := Load()
	if err == nil {
		t.Fatalf("Load() = %v, want error containing %q", cfg, wantFragment)
	}
	if cfg != nil {
		t.Fatalf("Load() returned a config alongside an error: %v", cfg)
	}
	if !strings.Contains(err.Error(), wantFragment) {
		t.Fatalf("Load() error = %v, want it to contain %q", err, wantFragment)
	}
}

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	t.Setenv("CLIENT_API_KEYS", validClientKey)
	t.Setenv("ADMIN_PASSWORD", validAdminPass)

	cfg := mustLoad(t)

	// Every default below comes from the table in
	if cfg.Host != DefaultHost {
		t.Errorf("Host = %q, want %q", cfg.Host, DefaultHost)
	}
	if cfg.Port != DefaultPort {
		t.Errorf("Port = %d, want %d", cfg.Port, DefaultPort)
	}
	if cfg.DBPath != DefaultDBPath {
		t.Errorf("DBPath = %q, want %q", cfg.DBPath, DefaultDBPath)
	}

	// Channel A — the free JWT pool is the primary upstream.
	if cfg.UpstreamURL != DefaultUpstreamURL {
		t.Errorf("UpstreamURL = %q, want %q", cfg.UpstreamURL, DefaultUpstreamURL)
	}
	if cfg.UpstreamURL != "https://zcode.z.ai" {
		t.Errorf("UpstreamURL = %q, want the channel A origin \"https://zcode.z.ai\"", cfg.UpstreamURL)
	}
	if cfg.ZcodeAppVersion != DefaultZcodeAppVersion {
		t.Errorf("ZcodeAppVersion = %q, want %q", cfg.ZcodeAppVersion, DefaultZcodeAppVersion)
	}
	if cfg.ZcodePlatform != DefaultZcodePlatform {
		t.Errorf("ZcodePlatform = %q, want %q", cfg.ZcodePlatform, DefaultZcodePlatform)
	}
	if cfg.DefaultProxyURL != "" {
		t.Errorf("DefaultProxyURL = %q, want empty", cfg.DefaultProxyURL)
	}

	// Channel B — optional fallback, off by default.
	if cfg.FallbackURL != DefaultFallbackURL {
		t.Errorf("FallbackURL = %q, want %q", cfg.FallbackURL, DefaultFallbackURL)
	}
	if cfg.FallbackURL != "https://api.z.ai" {
		t.Errorf("FallbackURL = %q, want \"https://api.z.ai\"", cfg.FallbackURL)
	}
	if cfg.FallbackAPIKey != "" {
		t.Errorf("FallbackAPIKey = %q, want empty (channel B disabled)", cfg.FallbackAPIKey)
	}

	// Daily claim defaults.
	if !cfg.DailyClaim {
		t.Error("DailyClaim = false, want true")
	}
	if cfg.ClaimTime != DefaultClaimTime {
		t.Errorf("ClaimTime = %q, want %q", cfg.ClaimTime, DefaultClaimTime)
	}
	if cfg.ClaimTime != "00:05" {
		t.Errorf("ClaimTime = %q, want \"00:05\"", cfg.ClaimTime)
	}
	if cfg.ClaimJitter != DefaultClaimJitter {
		t.Errorf("ClaimJitter = %s, want %s", cfg.ClaimJitter, DefaultClaimJitter)
	}
	if cfg.ClaimJitter != 10*time.Minute {
		t.Errorf("ClaimJitter = %s, want 10m", cfg.ClaimJitter)
	}
	if cfg.ClaimSolverURL != "" || cfg.ClaimSolverKey != "" {
		t.Errorf("ClaimSolverURL/Key = %q/%q, want both empty", cfg.ClaimSolverURL, cfg.ClaimSolverKey)
	}
	if cfg.ClaimExhausted {
		t.Error("ClaimExhausted = true, want false")
	}
	if cfg.RiskCooldown != DefaultRiskCooldown {
		t.Errorf("RiskCooldown = %s, want %s", cfg.RiskCooldown, DefaultRiskCooldown)
	}
	if cfg.RiskCooldown != 6*time.Hour {
		t.Errorf("RiskCooldown = %s, want 6h", cfg.RiskCooldown)
	}

	if cfg.SecretKey != "" {
		t.Errorf("SecretKey = %q, want empty", cfg.SecretKey)
	}
	if cfg.DefaultModelOpus != "" || cfg.DefaultModelSonnet != "" || cfg.DefaultModelHaiku != "" {
		t.Errorf("default models = %q/%q/%q, want all empty",
			cfg.DefaultModelOpus, cfg.DefaultModelSonnet, cfg.DefaultModelHaiku)
	}
	if cfg.StrictModels {
		t.Error("StrictModels = true, want false")
	}
	if cfg.InflightLimit != DefaultInflightLimit {
		t.Errorf("InflightLimit = %d, want %d", cfg.InflightLimit, DefaultInflightLimit)
	}
	if cfg.MaxAttempts != DefaultMaxAttempts {
		t.Errorf("MaxAttempts = %d, want %d", cfg.MaxAttempts, DefaultMaxAttempts)
	}
	if cfg.AttemptDeadline != 60*time.Second {
		t.Errorf("AttemptDeadline = %s, want 60s", cfg.AttemptDeadline)
	}
	if cfg.FirstEventTimeout != 90*time.Second {
		t.Errorf("FirstEventTimeout = %s, want 90s", cfg.FirstEventTimeout)
	}
	if cfg.FirstEventMaxBytes != 65536 {
		t.Errorf("FirstEventMaxBytes = %d, want 65536", cfg.FirstEventMaxBytes)
	}
	if cfg.UpstreamHeaderTimeout != 120*time.Second {
		t.Errorf("UpstreamHeaderTimeout = %s, want 120s", cfg.UpstreamHeaderTimeout)
	}
	if cfg.CoolingDuration != 60*time.Second {
		t.Errorf("CoolingDuration = %s, want 60s", cfg.CoolingDuration)
	}
	if cfg.ExhaustedFallback != time.Hour {
		t.Errorf("ExhaustedFallback = %s, want 1h", cfg.ExhaustedFallback)
	}
	if cfg.FailThreshold != 3 {
		t.Errorf("FailThreshold = %d, want 3", cfg.FailThreshold)
	}
	if !cfg.AffinityEnabled {
		t.Error("AffinityEnabled = false, want true")
	}
	if cfg.AffinityTTL != 30*time.Minute {
		t.Errorf("AffinityTTL = %s, want 30m", cfg.AffinityTTL)
	}
	if cfg.MaxBodyBytes != 20971520 {
		t.Errorf("MaxBodyBytes = %d, want 20971520", cfg.MaxBodyBytes)
	}
	if cfg.CORSOrigins != nil {
		t.Errorf("CORSOrigins = %q, want empty", cfg.CORSOrigins)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, DefaultLogLevel)
	}
	if len(cfg.ClientAPIKeys) != 1 || cfg.ClientAPIKeys[0] != validClientKey {
		t.Errorf("ClientAPIKeys = %q, want [%q]", cfg.ClientAPIKeys, validClientKey)
	}
	if cfg.AdminPassword != validAdminPass {
		t.Errorf("AdminPassword = %q, want %q", cfg.AdminPassword, validAdminPass)
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("CLIENT_API_KEYS", "override-client-key-aaaaaaaaaaaa, override-client-key-bbbbbbbbbbbb")
	t.Setenv("ADMIN_PASSWORD", "override-admin-password-9876543210")
	t.Setenv("SECRET_KEY", "override-secret-key-for-aes-gcm")
	t.Setenv("HOST", "127.0.0.1")
	t.Setenv("PORT", "8443")
	t.Setenv("DB_PATH", "/tmp/z2a-test.db")
	// Channel A: UPSTREAM_URL is only the origin.
	t.Setenv("UPSTREAM_URL", "https://upstream.example.com/")
	t.Setenv("ZCODE_APP_VERSION", "9.9.9")
	t.Setenv("ZCODE_PLATFORM", "win-x64")
	t.Setenv("DEFAULT_PROXY_URL", "socks5://proxy.example.com:1080")
	// Channel B.
	t.Setenv("FALLBACK_URL", "https://fallback.example.com/")
	t.Setenv("FALLBACK_API_KEY", "override-fallback-key-0123456789")
	// Claim.
	t.Setenv("DAILY_CLAIM", "false")
	t.Setenv("CLAIM_TIME", "07:30")
	t.Setenv("CLAIM_JITTER", "2m30s")
	t.Setenv("CLAIM_SOLVER_URL", "https://solver.example.com/captcha")
	t.Setenv("CLAIM_SOLVER_KEY", "override-solver-key-0123456789")
	t.Setenv("CLAIM_EXHAUSTED", "true")
	t.Setenv("RISK_COOLDOWN", "30m")
	t.Setenv("DEFAULT_MODEL_OPUS", "test-model-opus")
	t.Setenv("DEFAULT_MODEL_SONNET", "test-model-sonnet")
	t.Setenv("DEFAULT_MODEL_HAIKU", "test-model-haiku")
	t.Setenv("STRICT_MODELS", "true")
	t.Setenv("INFLIGHT_LIMIT", "32")
	t.Setenv("MAX_ATTEMPTS", "10")
	t.Setenv("ATTEMPT_DEADLINE", "45s")
	t.Setenv("FIRST_EVENT_TIMEOUT", "2m")
	t.Setenv("FIRST_EVENT_MAX_BYTES", "1024")
	t.Setenv("UPSTREAM_HEADER_TIMEOUT", "3m30s")
	t.Setenv("COOLING_DURATION", "90s")
	t.Setenv("EXHAUSTED_FALLBACK", "2h")
	t.Setenv("FAIL_THRESHOLD", "5")
	t.Setenv("AFFINITY_ENABLED", "false")
	t.Setenv("AFFINITY_TTL", "10m")
	t.Setenv("MAX_BODY_BYTES", "1048576")
	t.Setenv("CORS_ORIGINS", " https://a.example.com , https://b.example.com ")
	t.Setenv("LOG_LEVEL", "WARN")

	cfg := mustLoad(t)

	wantKeys := []string{"override-client-key-aaaaaaaaaaaa", "override-client-key-bbbbbbbbbbbb"}
	if len(cfg.ClientAPIKeys) != len(wantKeys) {
		t.Fatalf("ClientAPIKeys = %q, want %q", cfg.ClientAPIKeys, wantKeys)
	}
	for i, want := range wantKeys {
		if cfg.ClientAPIKeys[i] != want {
			t.Errorf("ClientAPIKeys[%d] = %q, want %q (values must be trimmed)", i, cfg.ClientAPIKeys[i], want)
		}
	}
	if cfg.AdminPassword != "override-admin-password-9876543210" {
		t.Errorf("AdminPassword = %q", cfg.AdminPassword)
	}
	if cfg.SecretKey != "override-secret-key-for-aes-gcm" {
		t.Errorf("SecretKey = %q", cfg.SecretKey)
	}
	if cfg.Host != "127.0.0.1" {
		t.Errorf("Host = %q", cfg.Host)
	}
	if cfg.Port != 8443 {
		t.Errorf("Port = %d, want 8443", cfg.Port)
	}
	if cfg.DBPath != "/tmp/z2a-test.db" {
		t.Errorf("DBPath = %q", cfg.DBPath)
	}
	// Channel A overrides.
	if cfg.UpstreamURL != "https://upstream.example.com" {
		t.Errorf("UpstreamURL = %q, want origin with trailing slash trimmed", cfg.UpstreamURL)
	}
	if cfg.ZcodeAppVersion != "9.9.9" {
		t.Errorf("ZcodeAppVersion = %q", cfg.ZcodeAppVersion)
	}
	if cfg.ZcodePlatform != "win-x64" {
		t.Errorf("ZcodePlatform = %q", cfg.ZcodePlatform)
	}
	if cfg.DefaultProxyURL != "socks5://proxy.example.com:1080" {
		t.Errorf("DefaultProxyURL = %q", cfg.DefaultProxyURL)
	}
	// Channel B overrides.
	if cfg.FallbackURL != "https://fallback.example.com" {
		t.Errorf("FallbackURL = %q, want trailing slash trimmed", cfg.FallbackURL)
	}
	if cfg.FallbackAPIKey != "override-fallback-key-0123456789" {
		t.Errorf("FallbackAPIKey = %q", cfg.FallbackAPIKey)
	}
	// Claim overrides.
	if cfg.DailyClaim {
		t.Error("DailyClaim = true, want false")
	}
	if cfg.ClaimTime != "07:30" {
		t.Errorf("ClaimTime = %q, want \"07:30\"", cfg.ClaimTime)
	}
	h, m, err := ParseClaimTime(cfg.ClaimTime)
	if err != nil || h != 7 || m != 30 {
		t.Errorf("ParseClaimTime(%q) = %d:%d, %v; want 7:30, nil", cfg.ClaimTime, h, m, err)
	}
	if cfg.ClaimJitter != 2*time.Minute+30*time.Second {
		t.Errorf("ClaimJitter = %s, want 2m30s", cfg.ClaimJitter)
	}
	if cfg.ClaimSolverURL != "https://solver.example.com/captcha" {
		t.Errorf("ClaimSolverURL = %q", cfg.ClaimSolverURL)
	}
	if cfg.ClaimSolverKey != "override-solver-key-0123456789" {
		t.Errorf("ClaimSolverKey = %q", cfg.ClaimSolverKey)
	}
	if !cfg.ClaimExhausted {
		t.Error("ClaimExhausted = false, want true")
	}
	if cfg.RiskCooldown != 30*time.Minute {
		t.Errorf("RiskCooldown = %s, want 30m", cfg.RiskCooldown)
	}
	if cfg.DefaultModelOpus != "test-model-opus" ||
		cfg.DefaultModelSonnet != "test-model-sonnet" ||
		cfg.DefaultModelHaiku != "test-model-haiku" {
		t.Errorf("default models = %q/%q/%q", cfg.DefaultModelOpus, cfg.DefaultModelSonnet, cfg.DefaultModelHaiku)
	}
	if !cfg.StrictModels {
		t.Error("StrictModels = false, want true")
	}
	if cfg.InflightLimit != 32 {
		t.Errorf("InflightLimit = %d, want 32", cfg.InflightLimit)
	}
	if cfg.MaxAttempts != 10 {
		t.Errorf("MaxAttempts = %d, want 10", cfg.MaxAttempts)
	}
	if cfg.AttemptDeadline != 45*time.Second {
		t.Errorf("AttemptDeadline = %s, want 45s", cfg.AttemptDeadline)
	}
	if cfg.FirstEventTimeout != 2*time.Minute {
		t.Errorf("FirstEventTimeout = %s, want 2m", cfg.FirstEventTimeout)
	}
	if cfg.FirstEventMaxBytes != 1024 {
		t.Errorf("FirstEventMaxBytes = %d, want 1024", cfg.FirstEventMaxBytes)
	}
	if cfg.UpstreamHeaderTimeout != 3*time.Minute+30*time.Second {
		t.Errorf("UpstreamHeaderTimeout = %s, want 3m30s", cfg.UpstreamHeaderTimeout)
	}
	if cfg.CoolingDuration != 90*time.Second {
		t.Errorf("CoolingDuration = %s, want 90s", cfg.CoolingDuration)
	}
	if cfg.ExhaustedFallback != 2*time.Hour {
		t.Errorf("ExhaustedFallback = %s, want 2h", cfg.ExhaustedFallback)
	}
	if cfg.FailThreshold != 5 {
		t.Errorf("FailThreshold = %d, want 5", cfg.FailThreshold)
	}
	if cfg.AffinityEnabled {
		t.Error("AffinityEnabled = true, want false")
	}
	if cfg.AffinityTTL != 10*time.Minute {
		t.Errorf("AffinityTTL = %s, want 10m", cfg.AffinityTTL)
	}
	if cfg.MaxBodyBytes != 1048576 {
		t.Errorf("MaxBodyBytes = %d, want 1048576", cfg.MaxBodyBytes)
	}
	wantCORS := []string{"https://a.example.com", "https://b.example.com"}
	if len(cfg.CORSOrigins) != len(wantCORS) {
		t.Fatalf("CORSOrigins = %q, want %q", cfg.CORSOrigins, wantCORS)
	}
	for i, want := range wantCORS {
		if cfg.CORSOrigins[i] != want {
			t.Errorf("CORSOrigins[%d] = %q, want %q", i, cfg.CORSOrigins[i], want)
		}
	}
	if cfg.LogLevel != "warn" {
		t.Errorf("LogLevel = %q, want normalized \"warn\"", cfg.LogLevel)
	}
}

// TestLoadWeakSecrets covers every way a secret can be rejected: unset, empty,
// too short, an empty entry inside the list, or a placeholder from.env.example.
func TestLoadWeakSecrets(t *testing.T) {
	str := func(s string) *string { return &s }

	cases := []struct {
		name        string
		clientKeys  *string // nil = leave unset
		adminPass   *string // nil = leave unset
		wantErrPart string
	}{
		{"client keys unset", nil, str(validAdminPass), "CLIENT_API_KEYS is required"},
		{"client keys empty", str(""), str(validAdminPass), "CLIENT_API_KEYS is required"},
		{"client key placeholder", str("change-me-client-key-at-least-16-chars"), str(validAdminPass), "placeholder value from .env.example"},
		{"client key too short", str("short"), str(validAdminPass), "must be at least 16 characters"},
		{"one short key in list", str(validClientKey + ", short"), str(validAdminPass), "CLIENT_API_KEYS: entry 2"},
		{"empty entry in list", str(validClientKey + ",   "), str(validAdminPass), "CLIENT_API_KEYS: entry 2 is empty"},
		{"admin password unset", str(validClientKey), nil, "ADMIN_PASSWORD is required"},
		{"admin password empty", str(validClientKey), str(""), "ADMIN_PASSWORD is required"},
		{"admin password placeholder", str(validClientKey), str("change-me-admin-password-min-16-chars"), "placeholder value from .env.example"},
		{"admin password too short", str(validClientKey), str("only-15-chars!!"), "ADMIN_PASSWORD must be at least 16"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			if tc.clientKeys != nil {
				t.Setenv("CLIENT_API_KEYS", *tc.clientKeys)
			}
			if tc.adminPass != nil {
				t.Setenv("ADMIN_PASSWORD", *tc.adminPass)
			}
			wantLoadError(t, tc.wantErrPart)
		})
	}
}

// TestLoadFallbackKeyNotASecretRule pins the deliberate difference between
// client secrets and upstream credentials: FALLBACK_API_KEY may be any value,
// including a short one — it only enables or disables channel B
// (empty disables the fallback).
func TestLoadFallbackKeyNotASecretRule(t *testing.T) {
	clearEnv(t)
	t.Setenv("CLIENT_API_KEYS", validClientKey)
	t.Setenv("ADMIN_PASSWORD", validAdminPass)
	t.Setenv("FALLBACK_API_KEY", "sk-short")
	cfg := mustLoad(t)
	if cfg.FallbackAPIKey != "sk-short" {
		t.Errorf("FallbackAPIKey = %q, want \"sk-short\" (no length rule on upstream credentials)", cfg.FallbackAPIKey)
	}
}

// TestParseClaimTime pins the HH:MM grammar shared by Load and the claim
// scheduler.
func TestParseClaimTime(t *testing.T) {
	valid := map[string][2]int{
		"00:05": {0, 5},
		"00:00": {0, 0},
		"07:30": {7, 30},
		"23:59": {23, 59},
	}
	for in, want := range valid {
		h, m, err := ParseClaimTime(in)
		if err != nil || h != want[0] || m != want[1] {
			t.Errorf("ParseClaimTime(%q) = %d:%d, %v; want %d:%d, nil", in, h, m, err, want[0], want[1])
		}
	}

	invalid := []string{
		"",         // empty
		"0:05",     // single-digit hour
		"00:5",     // single-digit minute
		"25:00",    // hour out of range
		"00:60",    // minute out of range
		"24:00",    // hour out of range
		"7:5",      // both single-digit
		"midnight", // not a time
		"00:05 ",   // trailing space
		"0005",     // missing separator
		"00-05",    // wrong separator
		"00:05:00", // seconds not allowed
		"12:30 PM", // 12-hour notation
	}
	for _, in := range invalid {
		if h, m, err := ParseClaimTime(in); err == nil {
			t.Errorf("ParseClaimTime(%q) = %d:%d, nil; want error", in, h, m)
		}
	}
}

// TestEnvExampleCoversSpec8Table ties.env.example to: the
// file must define every variable of the table, with the documented default
// for the keys that ship one and empty values for the optional credentials.
func TestEnvExampleCoversSpec8Table(t *testing.T) {
	values, err := parseEnvExample(filepath.Join("..", "..", ".env.example"))
	if err != nil {
		t.Fatalf("read .env.example: %v", err)
	}
	for _, key := range spec8Keys {
		if _, ok := values[key]; !ok {
			t.Errorf(".env.example is missing %s from the env table", key)
		}
	}

	wantValues := map[string]string{
		"HOST":               DefaultHost,
		"PORT":               "3101",
		"DB_PATH":            DefaultDBPath,
		"UPSTREAM_URL":       DefaultUpstreamURL,
		"ZCODE_APP_VERSION":  DefaultZcodeAppVersion,
		"ZCODE_PLATFORM":     DefaultZcodePlatform,
		"FALLBACK_URL":       DefaultFallbackURL,
		"FALLBACK_API_KEY":   "", // empty = channel B disabled
		"DAILY_CLAIM":        "true",
		"CLAIM_TIME":         DefaultClaimTime,
		"CLAIM_JITTER":       "10m",
		"CLAIM_SOLVER_URL":   "",
		"CLAIM_SOLVER_KEY":   "",
		"CLAIM_EXHAUSTED":    "false",
		"INFLIGHT_LIMIT":     "3",
		"MAX_ATTEMPTS":       "3",
		"RISK_COOLDOWN":      "6h",
		"EXHAUSTED_FALLBACK": "1h",
		"STRICT_MODELS":      "false",
		"CORS_ORIGINS":       "",
		"LOG_LEVEL":          DefaultLogLevel,
	}
	for key, want := range wantValues {
		if got := values[key]; got != want {
			t.Errorf(".env.example %s = %q, want %q (default)", key, got, want)
		}
	}
}

// TestEnvExamplePlaceholdersRejected ties the validator to.env.example: the
// exact sample values in the file must be refused, and the same values with the
// "change-me" marker replaced must be accepted (so the length is not what
// rejects them).
func TestEnvExamplePlaceholdersRejected(t *testing.T) {
	values, err := parseEnvExample(filepath.Join("..", "..", ".env.example"))
	if err != nil {
		t.Fatalf("read .env.example: %v", err)
	}
	for _, name := range []string{"CLIENT_API_KEYS", "ADMIN_PASSWORD"} {
		if values[name] == "" {
			t.Fatalf(".env.example has no value for %s", name)
		}
		if !isPlaceholder(values[name]) {
			t.Fatalf(".env.example value for %s = %q, want it to contain the %q marker", name, values[name], placeholderMarker)
		}
	}

	clearEnv(t)
	t.Setenv("CLIENT_API_KEYS", values["CLIENT_API_KEYS"])
	t.Setenv("ADMIN_PASSWORD", values["ADMIN_PASSWORD"])
	wantLoadError(t, "placeholder value from .env.example")

	clearEnv(t)
	t.Setenv("CLIENT_API_KEYS", strings.ReplaceAll(values["CLIENT_API_KEYS"], placeholderMarker, "real-value"))
	t.Setenv("ADMIN_PASSWORD", strings.ReplaceAll(values["ADMIN_PASSWORD"], placeholderMarker, "real-value"))
	mustLoad(t) // same length, real marker gone → accepted
}

// parseEnvExample reads KEY=VALUE lines from a.env.example file, ignoring
// comments and blanks.
func parseEnvExample(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	values := make(map[string]string)
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		values[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return values, nil
}

// TestLoadRangeBounds checks that the documented inclusive bounds hold.
func TestLoadRangeBounds(t *testing.T) {
	cases := []struct {
		name   string
		envKey string
		value  string
	}{
		{"inflight lower bound", "INFLIGHT_LIMIT", "1"},
		{"inflight upper bound", "INFLIGHT_LIMIT", "32"},
		{"max attempts lower bound", "MAX_ATTEMPTS", "1"},
		{"max attempts upper bound", "MAX_ATTEMPTS", "10"},
		{"claim jitter zero allowed", "CLAIM_JITTER", "0"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("CLIENT_API_KEYS", validClientKey)
			t.Setenv("ADMIN_PASSWORD", validAdminPass)
			t.Setenv(tc.envKey, tc.value)
			mustLoad(t)
		})
	}
}

// TestLoadInvalidValues covers out-of-range numbers, unparseable values, and
// invalid URLs.
func TestLoadInvalidValues(t *testing.T) {
	cases := []struct {
		name        string
		envKey      string
		value       string
		wantErrPart string
	}{
		{"inflight zero", "INFLIGHT_LIMIT", "0", "INFLIGHT_LIMIT must be between 1 and 32"},
		{"inflight above max", "INFLIGHT_LIMIT", "33", "INFLIGHT_LIMIT must be between 1 and 32"},
		{"inflight not a number", "INFLIGHT_LIMIT", "three", "INFLIGHT_LIMIT: invalid integer"},
		{"attempts zero", "MAX_ATTEMPTS", "0", "MAX_ATTEMPTS must be between 1 and 10"},
		{"attempts above max", "MAX_ATTEMPTS", "11", "MAX_ATTEMPTS must be between 1 and 10"},
		{"attempts not a number", "MAX_ATTEMPTS", "3x", "MAX_ATTEMPTS: invalid integer"},
		{"port zero", "PORT", "0", "PORT must be between 1 and 65535"},
		{"port not a number", "PORT", "31o1", "PORT: invalid integer"},
		{"upstream not a URL", "UPSTREAM_URL", "zcode.z.ai", "UPSTREAM_URL"},
		{"upstream no host", "UPSTREAM_URL", "https://", "UPSTREAM_URL"},
		{"upstream bad scheme", "UPSTREAM_URL", "ftp://zcode.z.ai", "UPSTREAM_URL: unsupported scheme"},
		{"fallback not a URL", "FALLBACK_URL", "api.z.ai", "FALLBACK_URL"},
		{"fallback no host", "FALLBACK_URL", "https://", "FALLBACK_URL"},
		{"fallback bad scheme", "FALLBACK_URL", "ftp://api.z.ai", "FALLBACK_URL: unsupported scheme"},
		{"claim solver not a URL", "CLAIM_SOLVER_URL", "solver.example.com", "CLAIM_SOLVER_URL"},
		{"claim solver bad scheme", "CLAIM_SOLVER_URL", "ftp://solver.example.com", "CLAIM_SOLVER_URL: unsupported scheme"},
		{"claim time bad hour", "CLAIM_TIME", "25:00", "CLAIM_TIME"},
		{"claim time hour 24", "CLAIM_TIME", "24:00", "CLAIM_TIME"},
		{"claim time single-digit hour", "CLAIM_TIME", "7:05", "CLAIM_TIME"},
		{"claim time single-digit minute", "CLAIM_TIME", "00:5", "CLAIM_TIME"},
		{"claim time not a time", "CLAIM_TIME", "midnight", "CLAIM_TIME"},
		{"claim time wrong separator", "CLAIM_TIME", "00-05", "CLAIM_TIME"},
		{"claim time with seconds", "CLAIM_TIME", "00:05:00", "CLAIM_TIME"},
		{"claim jitter negative", "CLAIM_JITTER", "-10m", "CLAIM_JITTER must not be negative"},
		{"claim jitter not a duration", "CLAIM_JITTER", "ten", "CLAIM_JITTER: invalid duration"},
		{"risk cooldown zero", "RISK_COOLDOWN", "0", "RISK_COOLDOWN must be positive"},
		{"risk cooldown negative", "RISK_COOLDOWN", "-6h", "RISK_COOLDOWN must be positive"},
		{"risk cooldown not a duration", "RISK_COOLDOWN", "six", "RISK_COOLDOWN: invalid duration"},
		{"daily claim not a bool", "DAILY_CLAIM", "yes", "DAILY_CLAIM: invalid boolean"},
		{"claim exhausted not a bool", "CLAIM_EXHAUSTED", "maybe", "CLAIM_EXHAUSTED: invalid boolean"},
		{"proxy bad scheme", "DEFAULT_PROXY_URL", "ftp://proxy:1080", "DEFAULT_PROXY_URL: unsupported scheme"},
		{"strict models not a bool", "STRICT_MODELS", "yes", "STRICT_MODELS: invalid boolean"},
		{"affinity not a bool", "AFFINITY_ENABLED", "maybe", "AFFINITY_ENABLED: invalid boolean"},
		{"deadline not a duration", "ATTEMPT_DEADLINE", "five", "ATTEMPT_DEADLINE: invalid duration"},
		{"cooling negative", "COOLING_DURATION", "-30s", "COOLING_DURATION must be positive"},
		{"affinity ttl negative", "AFFINITY_TTL", "-1s", "AFFINITY_TTL must be positive"},
		{"first event max bytes zero", "FIRST_EVENT_MAX_BYTES", "0", "FIRST_EVENT_MAX_BYTES must be positive"},
		{"max body bytes negative", "MAX_BODY_BYTES", "-1", "MAX_BODY_BYTES must be positive"},
		{"fail threshold zero", "FAIL_THRESHOLD", "0", "FAIL_THRESHOLD must be positive"},
		{"unknown log level", "LOG_LEVEL", "verbose", "LOG_LEVEL: unknown level"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("CLIENT_API_KEYS", validClientKey)
			t.Setenv("ADMIN_PASSWORD", validAdminPass)
			t.Setenv(tc.envKey, tc.value)
			wantLoadError(t, tc.wantErrPart)
		})
	}
}
