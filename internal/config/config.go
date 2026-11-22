// Package config loads the Zcode2Api configuration from environment variables.
// The gateway has two upstream channels:
//
// - Channel A (primary): the free JWT account pool, origin UPSTREAM_URL
// (default https://zcode.z.ai), authenticated per account with
// "Authorization: Bearer <JWT>" plus the ZCode source headers.
// - Channel B (optional fallback): one Z.AI API key (FALLBACK_API_KEY,
// origin FALLBACK_URL, default https://api.z.ai) used only when the pool
// has no usable account left.
//
// Load refuses to start the gateway with missing, weak, or placeholder
// secrets so misconfiguration is caught before listening.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Defaults for variables that are optional in Required
// variables (CLIENT_API_KEYS, ADMIN_PASSWORD) have no default.
const (
	// Server.
	DefaultHost   = "0.0.0.0"
	DefaultPort   = 3101
	DefaultDBPath = "data/zcode2api.db"

	// Channel A — free JWT account pool. UPSTREAM_URL is
	// only the origin; the zcode-plan paths (/api/v1/zcode-plan/...) are
	// appended by the upstream client.
	DefaultUpstreamURL     = "https://zcode.z.ai"
	DefaultZcodeAppVersion = "3.16.3" // User-Agent "ZCode/<ver>" and app_version=
	DefaultZcodePlatform   = "darwin-x64"

	// Channel B — optional Z.AI API-key fallback. The key
	// has no default: empty disables the channel.
	DefaultFallbackURL = "https://api.z.ai"

	// Daily claim: schedule in GMT+8 plus a random delay.
	DefaultDailyClaim     = true
	DefaultClaimTime      = "00:05"
	DefaultClaimJitter    = 10 * time.Minute
	DefaultClaimExhausted = false

	// Pool and failover behavior.
	DefaultInflightLimit         = 3
	DefaultMaxAttempts           = 3
	DefaultAttemptDeadline       = 60 * time.Second
	DefaultFirstEventTimeout     = 90 * time.Second
	DefaultFirstEventMaxBytes    = 65536
	DefaultUpstreamHeaderTimeout = 120 * time.Second
	DefaultCoolingDuration       = 60 * time.Second
	DefaultRiskCooldown          = 6 * time.Hour
	DefaultExhaustedFallback     = time.Hour
	DefaultFailThreshold         = 3
	DefaultQuotaRefreshMin       = 10 // background quota refresh (0 = off)
	DefaultAffinityEnabled       = true
	DefaultAffinityTTL           = 30 * time.Minute
	DefaultMaxBodyBytes          = 20 << 20 // 20971520 (20 MiB)
	DefaultLogLevel              = "info"
)

// Validation bounds.
const (
	// MinSecretLen is the minimum length of every client key and of ADMIN_PASSWORD.
	MinSecretLen = 16

	minInflightLimit = 1
	maxInflightLimit = 32
	minMaxAttempts   = 1
	maxMaxAttempts   = 10
	minPort          = 1
	maxPort          = 65535

	// placeholderMarker appears in every sample value of .env.example.
	// Load rejects secrets containing it (case-insensitive) so operators cannot
	// launch with the sample configuration.
	placeholderMarker = "change-me"
)

// Allowed URL schemes per variable. validateURL only reads
// these maps, so sharing them is safe.
var (
	httpURLSchemes  = map[string]bool{"http": true, "https": true}
	proxyURLSchemes = map[string]bool{"http": true, "https": true, "socks5": true}
)

// Config is the resolved configuration. All fields are read-only after Load.
type Config struct {
	// Server.
	Host string
	Port int

	// Secrets and client authentication.
	ClientAPIKeys []string // comma-separated CLIENT_API_KEYS
	AdminPassword string
	SecretKey     string // empty = JWT accounts stored unencrypted

	// Storage.
	DBPath string

	// Channel A — pool of free JWT accounts.
	UpstreamURL     string // origin, trailing slash trimmed
	ZcodeAppVersion string // User-Agent "ZCode/<ver>" and app_version= on balance/claim
	ZcodePlatform   string // X-Platform header value
	DefaultProxyURL string // empty = direct egress

	// Channel B — optional API-key fallback.
	FallbackURL    string // origin, trailing slash trimmed
	FallbackAPIKey string // empty = channel B disabled

	// Daily claim.
	DailyClaim     bool
	ClaimTime      string        // "HH:MM", 24-hour, GMT+8; parse with ParseClaimTime
	ClaimJitter    time.Duration // maximum random delay added to the schedule
	ClaimSolverURL string        // external captcha solver; empty = skip captcha accounts
	ClaimSolverKey string        // key for the solver, if it needs one
	ClaimExhausted bool          // also claim accounts currently EXHAUSTED
	ClaimPlanID    string        // entitlement id posted as {"plan_id":...}; empty = plain claim body

	// Default model mapping targets (DEFAULT_MODEL_*). Model names come only
	// from configuration or the DB — never hardcoded.
	DefaultModelOpus   string
	DefaultModelSonnet string
	DefaultModelHaiku  string

	// Pool and failover behavior.
	StrictModels          bool
	InflightLimit         int
	MaxAttempts           int
	AttemptDeadline       time.Duration
	FirstEventTimeout     time.Duration
	FirstEventMaxBytes    int
	UpstreamHeaderTimeout time.Duration
	CoolingDuration       time.Duration
	RiskCooldown          time.Duration // isolation after upstream code 3012
	ExhaustedFallback     time.Duration
	FailThreshold         int
	QuotaRefreshMin       int // background billing refresh period in minutes, 0 = off
	AffinityEnabled       bool
	AffinityTTL           time.Duration
	MaxBodyBytes          int

	// HTTP and logging.
	CORSOrigins []string
	LogLevel    string // debug | info | warn | error
}

// Load reads every variable of from the process environment,
// applies defaults, and validates the result. An empty value behaves like the
// variable being unset (default applies), except for the required secrets,
// which must be non-empty.

// It returns an error (aggregating every violation found) when:
// - CLIENT_API_KEYS or ADMIN_PASSWORD is empty, shorter than MinSecretLen,
// or still contains a placeholder value from.env.example;
// - INFLIGHT_LIMIT is outside 1–32 or MAX_ATTEMPTS outside 1–10;
// - CLAIM_TIME is not a valid "HH:MM" 24-hour time;
// - CLAIM_JITTER is negative or RISK_COOLDOWN is not positive;
// - a numeric, boolean, or duration variable does not parse;
// - UPSTREAM_URL and FALLBACK_URL (always), or DEFAULT_PROXY_URL and
// CLAIM_SOLVER_URL (when set) are not valid URLs with an allowed scheme;
// - ZCODE_APP_VERSION or ZCODE_PLATFORM is empty (the defaults keep them
// set; this guards the channel A identity required by).
func Load() (*Config, error) {
	cfg := &Config{
		Host:                  DefaultHost,
		Port:                  DefaultPort,
		DBPath:                DefaultDBPath,
		UpstreamURL:           DefaultUpstreamURL,
		FallbackURL:           DefaultFallbackURL,
		ZcodeAppVersion:       DefaultZcodeAppVersion,
		ZcodePlatform:         DefaultZcodePlatform,
		DailyClaim:            DefaultDailyClaim,
		ClaimTime:             DefaultClaimTime,
		ClaimJitter:           DefaultClaimJitter,
		ClaimExhausted:        DefaultClaimExhausted,
		StrictModels:          false,
		InflightLimit:         DefaultInflightLimit,
		MaxAttempts:           DefaultMaxAttempts,
		AttemptDeadline:       DefaultAttemptDeadline,
		FirstEventTimeout:     DefaultFirstEventTimeout,
		FirstEventMaxBytes:    DefaultFirstEventMaxBytes,
		UpstreamHeaderTimeout: DefaultUpstreamHeaderTimeout,
		CoolingDuration:       DefaultCoolingDuration,
		RiskCooldown:          DefaultRiskCooldown,
		ExhaustedFallback:     DefaultExhaustedFallback,
		FailThreshold:         DefaultFailThreshold,
		QuotaRefreshMin:       DefaultQuotaRefreshMin,
		AffinityEnabled:       DefaultAffinityEnabled,
		AffinityTTL:           DefaultAffinityTTL,
		MaxBodyBytes:          DefaultMaxBodyBytes,
		LogLevel:              DefaultLogLevel,
	}

	var errs []error
	add := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	// Required secrets.
	rawKeys := os.Getenv("CLIENT_API_KEYS")
	if err := validateSecret("CLIENT_API_KEYS", rawKeys); err != nil {
		add(err)
	} else {
		keys, err := parseClientKeys(rawKeys)
		add(err)
		cfg.ClientAPIKeys = keys
	}

	cfg.AdminPassword = os.Getenv("ADMIN_PASSWORD")
	add(validateSecret("ADMIN_PASSWORD", cfg.AdminPassword))

	// Plain strings (empty keeps the default). FALLBACK_API_KEY,
	// CLAIM_SOLVER_URL and CLAIM_SOLVER_KEY are optional upstream/solver
	// credentials: empty disables the feature, so no secret-length rule.
	setString(&cfg.Host, "HOST")
	setString(&cfg.DBPath, "DB_PATH")
	setString(&cfg.UpstreamURL, "UPSTREAM_URL")
	setString(&cfg.FallbackURL, "FALLBACK_URL")
	setString(&cfg.FallbackAPIKey, "FALLBACK_API_KEY")
	setTrimmedString(&cfg.ZcodeAppVersion, "ZCODE_APP_VERSION")
	setTrimmedString(&cfg.ZcodePlatform, "ZCODE_PLATFORM")
	setString(&cfg.ClaimSolverURL, "CLAIM_SOLVER_URL")
	setString(&cfg.ClaimPlanID, "CLAIM_PLAN_ID")
	setString(&cfg.ClaimSolverKey, "CLAIM_SOLVER_KEY")
	setString(&cfg.DefaultProxyURL, "DEFAULT_PROXY_URL")
	setString(&cfg.SecretKey, "SECRET_KEY")
	setString(&cfg.DefaultModelOpus, "DEFAULT_MODEL_OPUS")
	setString(&cfg.DefaultModelSonnet, "DEFAULT_MODEL_SONNET")
	setString(&cfg.DefaultModelHaiku, "DEFAULT_MODEL_HAIKU")

	// Numbers, booleans, durations.
	add(setInt(&cfg.Port, "PORT"))
	add(setInt(&cfg.InflightLimit, "INFLIGHT_LIMIT"))
	add(setInt(&cfg.MaxAttempts, "MAX_ATTEMPTS"))
	add(setInt(&cfg.FirstEventMaxBytes, "FIRST_EVENT_MAX_BYTES"))
	add(setInt(&cfg.FailThreshold, "FAIL_THRESHOLD"))
	add(setInt(&cfg.QuotaRefreshMin, "QUOTA_REFRESH_MIN"))
	add(setInt(&cfg.MaxBodyBytes, "MAX_BODY_BYTES"))
	add(setBool(&cfg.StrictModels, "STRICT_MODELS"))
	add(setBool(&cfg.AffinityEnabled, "AFFINITY_ENABLED"))
	add(setBool(&cfg.DailyClaim, "DAILY_CLAIM"))
	add(setBool(&cfg.ClaimExhausted, "CLAIM_EXHAUSTED"))
	add(setDuration(&cfg.AttemptDeadline, "ATTEMPT_DEADLINE"))
	add(setDuration(&cfg.FirstEventTimeout, "FIRST_EVENT_TIMEOUT"))
	add(setDuration(&cfg.UpstreamHeaderTimeout, "UPSTREAM_HEADER_TIMEOUT"))
	add(setDuration(&cfg.CoolingDuration, "COOLING_DURATION"))
	add(setDuration(&cfg.RiskCooldown, "RISK_COOLDOWN"))
	add(setDuration(&cfg.ExhaustedFallback, "EXHAUSTED_FALLBACK"))
	add(setDuration(&cfg.AffinityTTL, "AFFINITY_TTL"))
	add(setDuration(&cfg.ClaimJitter, "CLAIM_JITTER"))

	// CLAIM_TIME keeps the default when unset; whatever remains must be HH:MM.
	if raw := strings.TrimSpace(os.Getenv("CLAIM_TIME")); raw != "" {
		cfg.ClaimTime = raw
	}

	// Comma-separated allowlists and enumerated values.
	add(setList(&cfg.CORSOrigins, "CORS_ORIGINS"))
	add(setLogLevel(&cfg.LogLevel, "LOG_LEVEL"))

	// Range and URL validation.
	add(validateRange("PORT", cfg.Port, minPort, maxPort))
	add(validateRange("INFLIGHT_LIMIT", cfg.InflightLimit, minInflightLimit, maxInflightLimit))
	add(validateRange("MAX_ATTEMPTS", cfg.MaxAttempts, minMaxAttempts, maxMaxAttempts))
	add(validatePositiveInt("FIRST_EVENT_MAX_BYTES", cfg.FirstEventMaxBytes))
	add(validatePositiveInt("FAIL_THRESHOLD", cfg.FailThreshold))
	add(validateRange("QUOTA_REFRESH_MIN", cfg.QuotaRefreshMin, 0, 1440))
	add(validatePositiveInt("MAX_BODY_BYTES", cfg.MaxBodyBytes))
	add(validatePositiveDuration("ATTEMPT_DEADLINE", cfg.AttemptDeadline))
	add(validatePositiveDuration("FIRST_EVENT_TIMEOUT", cfg.FirstEventTimeout))
	add(validatePositiveDuration("UPSTREAM_HEADER_TIMEOUT", cfg.UpstreamHeaderTimeout))
	add(validatePositiveDuration("COOLING_DURATION", cfg.CoolingDuration))
	add(validatePositiveDuration("RISK_COOLDOWN", cfg.RiskCooldown))
	add(validatePositiveDuration("EXHAUSTED_FALLBACK", cfg.ExhaustedFallback))
	add(validatePositiveDuration("AFFINITY_TTL", cfg.AffinityTTL))
	if cfg.ClaimJitter < 0 {
		add(fmt.Errorf("CLAIM_JITTER must not be negative, got %s", cfg.ClaimJitter))
	}
	add(validateClaimTime("CLAIM_TIME", cfg.ClaimTime))
	add(validateNonEmpty("ZCODE_APP_VERSION", cfg.ZcodeAppVersion))
	add(validateNonEmpty("ZCODE_PLATFORM", cfg.ZcodePlatform))

	add(validateURL("UPSTREAM_URL", cfg.UpstreamURL, httpURLSchemes))
	add(validateURL("FALLBACK_URL", cfg.FallbackURL, httpURLSchemes))
	if cfg.ClaimSolverURL != "" {
		add(validateURL("CLAIM_SOLVER_URL", cfg.ClaimSolverURL, httpURLSchemes))
	}
	if cfg.DefaultProxyURL != "" {
		add(validateURL("DEFAULT_PROXY_URL", cfg.DefaultProxyURL, proxyURLSchemes))
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	cfg.UpstreamURL = strings.TrimRight(cfg.UpstreamURL, "/")
	cfg.FallbackURL = strings.TrimRight(cfg.FallbackURL, "/")
	return cfg, nil
}

// ParseClaimTime parses the "HH:MM" 24-hour format of CLAIM_TIME
// and returns the hour and minute in GMT+8. Both Load's
// validation and the daily claim scheduler go through it, so the accepted
// grammar lives in exactly one place.

// The grammar is strict: exactly five characters, a colon separator, two
// digits per field, hour ≤ 23 and minute ≤ 59. time.Parse("15:04") alone is
// not enough — it accepts a single-digit hour such as "0:05".
func ParseClaimTime(s string) (hour, minute int, err error) {
	if !isHHMM(s) {
		return 0, 0, errors.New("want HH:MM (two digits, colon, two digits)")
	}
	h := int(s[0]-'0')*10 + int(s[1]-'0')
	m := int(s[3]-'0')*10 + int(s[4]-'0')
	if h > 23 || m > 59 {
		return 0, 0, fmt.Errorf("hour or minute out of range: %s", s)
	}
	return h, m, nil
}

// isHHMM reports whether s is five characters of the shape "DD:DD" where every
// D is an ASCII digit.
func isHHMM(s string) bool {
	if len(s) != 5 || s[2] != ':' {
		return false
	}
	for _, i := range [4]int{0, 1, 3, 4} {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// validateClaimTime enforces the HH:MM grammar of CLAIM_TIME.
func validateClaimTime(name, v string) error {
	if _, _, err := ParseClaimTime(v); err != nil {
		return fmt.Errorf("%s: %q is not a valid HH:MM 24-hour time (e.g. 00:05)", name, v)
	}
	return nil
}

// validateNonEmpty guards values that the defaults keep non-empty but that
// channel A requires.
func validateNonEmpty(name, v string) error {
	if v == "" {
		return fmt.Errorf("%s must not be empty", name)
	}
	return nil
}

// validateSecret enforces the rules shared by CLIENT_API_KEYS and ADMIN_PASSWORD.
func validateSecret(name, v string) error {
	switch {
	case v == "":
		return fmt.Errorf("%s is required", name)
	case isPlaceholder(v):
		return fmt.Errorf("%s still has the placeholder value from .env.example", name)
	case len(v) < MinSecretLen:
		return fmt.Errorf("%s must be at least %d characters, got %d", name, MinSecretLen, len(v))
	}
	return nil
}

// parseClientKeys splits CLIENT_API_KEYS on commas and validates each key.
func parseClientKeys(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	keys := make([]string, 0, len(parts))
	var errs []error
	for i, part := range parts {
		key := strings.TrimSpace(part)
		if key == "" {
			errs = append(errs, fmt.Errorf("CLIENT_API_KEYS: entry %d is empty", i+1))
			continue
		}
		if isPlaceholder(key) {
			errs = append(errs, fmt.Errorf("CLIENT_API_KEYS: entry %d still has the placeholder value from .env.example", i+1))
			continue
		}
		if len(key) < MinSecretLen {
			errs = append(errs, fmt.Errorf("CLIENT_API_KEYS: entry %d must be at least %d characters, got %d", i+1, MinSecretLen, len(key)))
			continue
		}
		keys = append(keys, key)
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return keys, nil
}

// isPlaceholder reports whether v looks like a sample value from.env.example.
// Every placeholder there embeds the "change-me" marker.
func isPlaceholder(v string) bool {
	return strings.Contains(strings.ToLower(v), placeholderMarker)
}

func setString(dst *string, key string) {
	if v := os.Getenv(key); v != "" {
		*dst = v
	}
}

// setTrimmedString is setString for identity values that must not carry
// accidental whitespace: a whitespace-only value keeps the default.
func setTrimmedString(dst *string, key string) {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		*dst = v
	}
}

func setInt(dst *int, key string) error {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("%s: invalid integer %q", key, raw)
	}
	*dst = v
	return nil
}

func setBool(dst *bool, key string) error {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	v, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("%s: invalid boolean %q (use 1/t/T/TRUE/true/True, 0/f/F/FALSE/false/False)", key, raw)
	}
	*dst = v
	return nil
}

func setDuration(dst *time.Duration, key string) error {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	v, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("%s: invalid duration %q (examples: 60s, 30m, 1h)", key, raw)
	}
	*dst = v
	return nil
}

// setList splits a comma-separated variable and keeps non-empty trimmed entries.
func setList(dst *[]string, key string) error {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*dst = append(*dst, part)
		}
	}
	return nil
}

// setLogLevel validates LOG_LEVEL against the slog level names.
func setLogLevel(dst *string, key string) error {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	switch level := strings.ToLower(raw); level {
	case "debug", "info", "warn", "error":
		*dst = level
		return nil
	default:
		return fmt.Errorf("%s: unknown level %q (use debug, info, warn, or error)", key, raw)
	}
}

func validateRange(name string, v, min, max int) error {
	if v < min || v > max {
		return fmt.Errorf("%s must be between %d and %d, got %d", name, min, max, v)
	}
	return nil
}

func validatePositiveInt(name string, v int) error {
	if v <= 0 {
		return fmt.Errorf("%s must be positive, got %d", name, v)
	}
	return nil
}

func validatePositiveDuration(name string, d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("%s must be positive, got %s", name, d)
	}
	return nil
}

// validateURL requires an absolute URL whose scheme is in schemes.
func validateURL(name, raw string, schemes map[string]bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%s: invalid URL %q: %v", name, raw, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("%s: %q must be an absolute URL with a scheme and host", name, raw)
	}
	if !schemes[u.Scheme] {
		allowed := make([]string, 0, len(schemes))
		for s := range schemes {
			allowed = append(allowed, s+"://")
		}
		return fmt.Errorf("%s: unsupported scheme %q (allowed: %s)", name, u.Scheme, strings.Join(allowed, ", "))
	}
	return nil
}
