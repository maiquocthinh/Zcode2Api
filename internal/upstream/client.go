// The captcha header (X-Aliyun-Captcha-Verify-Param) is NEVER set by the
// request builders: upstream runs chat with skip_model_request=true and a
// captcha header on the chat flow triggers WAF 3012. The
// only call that may carry it is the daily claim (balance.go).
package upstream

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Version is reported in the channel B outbound User-Agent as
// "Zcode2Api/<version>". cmd/zcode2api overrides it at link time via -ldflags
// and assigns it here before serving.
var Version = "3.1.0"

// UserAgent is the channel B identity sent upstream: "Zcode2Api/<version>".
func UserAgent() string {
	return "Zcode2Api/" + Version
}

const (
	defaultAnthropicVersion = "2023-06-01"

	// No Client.Timeout on the data plane: streams may run for minutes.
	DefaultHeaderTimeout = 120 * time.Second
	maxIdleConnsPerHost  = 32
	idleConnTimeout      = 90 * time.Second
	tlsHandshakeTimeout  = 10 * time.Second
)

// Upstream paths, joined onto the channel origins by the builders.
const (
	PathChannelAMessages    = "/api/v1/zcode-plan/anthropic/v1/messages"
	PathChannelACountTokens = PathChannelAMessages + "/count_tokens"
	PathChannelBMessages    = "/api/anthropic/v1/messages"
	PathChannelBCountTokens = PathChannelBMessages + "/count_tokens"
	PathChannelABalance     = "/api/v1/zcode-plan/billing/balance"
	PathChannelAClaim       = "/api/v1/zcode-plan/billing/claim"
)

// Channel A source identity (ZCODE_APP_VERSION / ZCODE_PLATFORM).
const (
	DefaultZcodeAppVersion = "3.16.3"
	DefaultZcodePlatform   = "darwin-x64"
	channelAReferer        = "https://zcode.z.ai"
	channelATitle          = "Z Code@electron"
)

// Captcha headers: claim only — a captcha header on the chat flow triggers
// WAF 3012 (upstream runs chat with skip_model_request=true). The region
// header travels with the verify param's own mint region (WithRegion), never
// a hardcoded constant.
const (
	captchaHeader       = "X-Aliyun-Captcha-Verify-Param"
	captchaRegionHeader = "X-Aliyun-Captcha-Verify-Region"
)

// channelAIdentity carries the per-deployment ZCODE_APP_VERSION /
// ZCODE_PLATFORM values. Guarded by an atomic pointer.
type channelAIdentity struct {
	appVersion string
	platform   string
}

var identityState atomic.Pointer[channelAIdentity]

func init() {
	identityState.Store(&channelAIdentity{appVersion: DefaultZcodeAppVersion, platform: DefaultZcodePlatform})
}

// SetChannelAIdentity applies ZCODE_APP_VERSION / ZCODE_PLATFORM to every
// channel A request built afterwards. Empty values keep the previous setting.
// Safe for concurrent use.
func SetChannelAIdentity(appVersion, platform string) {
	prev := identityState.Load()
	next := &channelAIdentity{appVersion: prev.appVersion, platform: prev.platform}
	if appVersion != "" {
		next.appVersion = appVersion
	}
	if platform != "" {
		next.platform = platform
	}
	identityState.Store(next)
}

// ChannelAIdentity returns the currently applied channel A identity.
func ChannelAIdentity() (appVersion, platform string) {
	id := identityState.Load()
	return id.appVersion, id.platform
}

// DefaultChannelAOrigin is the UPSTREAM_URL default and the initial origin
// used by QueryBalance / ClaimQuota.
const DefaultChannelAOrigin = "https://zcode.z.ai"

var channelAOrigin atomic.Pointer[string]

func init() {
	origin := DefaultChannelAOrigin
	channelAOrigin.Store(&origin)
}

// SetChannelAOrigin applies UPSTREAM_URL as the channel A origin used by
// QueryBalance / ClaimQuota. An empty value keeps the previous setting.
// Safe for concurrent use.
func SetChannelAOrigin(origin string) {
	if origin != "" {
		channelAOrigin.Store(&origin)
	}
}

// ChannelAOrigin returns the currently applied channel A origin.
func ChannelAOrigin() string { return *channelAOrigin.Load() }

var headerTimeoutNs atomic.Int64

func init() {
	headerTimeoutNs.Store(int64(DefaultHeaderTimeout))
}

// SetHeaderTimeout updates the ResponseHeaderTimeout used for transports
// created afterwards. Non-positive values are ignored. Safe for concurrent
// use.
func SetHeaderTimeout(d time.Duration) {
	if d > 0 {
		headerTimeoutNs.Store(int64(d))
	}
}

// TransportFor returns the shared http.Transport for the given egress proxy
// URL ("" = direct). One transport is cached per URL so connection pools are
// reused. Transports are static network configuration: no proxy rotation on
// upstream rejection — a 3012 puts the account into RISK_COOLDOWN instead.
// The returned transport is shared — callers must not mutate it. An
// unparsable proxy URL falls back to direct egress with a WARN log that never
// includes the raw URL (it may carry credentials).
func TransportFor(proxyURL string) *http.Transport {
	transportMu.RLock()
	t, ok := transports[proxyURL]
	transportMu.RUnlock()
	if ok {
		return t
	}

	t = newTransport(proxyURL)

	transportMu.Lock()
	defer transportMu.Unlock()
	if cached, ok := transports[proxyURL]; ok {
		return cached
	}
	transports[proxyURL] = t
	return t
}

var (
	transportMu sync.RWMutex
	transports  = map[string]*http.Transport{}
)

// newTransport builds the shared upstream transport. It is a literal
// transport (no ForceAttemptHTTP2, nil TLSClientConfig) matching the proven-
// passing Go clients of this channel: a Cloned DefaultTransport or a custom
// TLSClientConfig produces a ClientHello/ALPN the upstream WAF answers with
// 3012 while this exact shape passes from the same machine.
func newTransport(proxyURL string) *http.Transport {
	t := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		Proxy:                 nil,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ResponseHeaderTimeout: time.Duration(headerTimeoutNs.Load()),
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		IdleConnTimeout:       idleConnTimeout,
	}

	if proxyURL == "" {
		t.Proxy = nil
	} else if u, err := parseProxyURL(proxyURL); err != nil {
		slog.Warn("upstream: invalid egress proxy URL, falling back to direct egress",
			"scheme", u.Scheme, "host", u.Host)
		t.Proxy = nil
	} else {
		t.Proxy = http.ProxyURL(u)
	}

	return t
}

// parseProxyURL validates an egress proxy URL and returns it parsed. Error
// messages never embed the raw URL because proxy URLs may carry credentials.
func parseProxyURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return &url.URL{}, errors.New("proxy URL does not parse")
	}
	switch u.Scheme {
	case "http", "https", "socks5", "socks5h":
	default:
		return u, fmt.Errorf("unsupported proxy scheme %q (allowed: http, https, socks5)", u.Scheme)
	}
	if u.Host == "" {
		return u, errors.New("proxy URL has no host")
	}
	return u, nil
}

// Client headers the builders may forward (, both channel
// columns). Everything else found in the client request is dropped.
const (
	hdrAnthropicVersion = "Anthropic-Version"
	hdrAnthropicBeta    = "Anthropic-Beta"
	hdrAccept           = "Accept"
	hdrContentType      = "Content-Type"
	hdrUserAgent        = "User-Agent"
)

// BuildChannelARequest creates the channel A chat request with the account
// JWT and the official model-request header set. It never
// sets the captcha header and never forwards client credentials. The request
// carries ctx and is replayable via req.GetBody for failover attempts.
func BuildChannelARequest(ctx context.Context, baseURL, jwt string, attr Attribution, inHeaders http.Header, body []byte) (*http.Request, error) {
	return buildChannelAModel(ctx, joinEndpoint(baseURL, PathChannelAMessages), jwt, attr, inHeaders, body)
}

// BuildChannelARequestAt is the billing/passthrough variant with an explicit
// upstream path: bare UA + X-Device-Mid (the device header belongs to
// non-model endpoints only).
func BuildChannelARequestAt(ctx context.Context, baseURL, path, jwt, deviceMid string, inHeaders http.Header, body []byte) (*http.Request, error) {
	return buildChannelA(ctx, joinEndpoint(baseURL, path), jwt, deviceMid, inHeaders, body)
}

func buildChannelA(ctx context.Context, endpoint, jwt, deviceMid string, inHeaders http.Header, body []byte) (*http.Request, error) {
	req, err := newUpstreamRequest(ctx, endpoint, body)
	if err != nil {
		return nil, err
	}
	applyChannelAHeaders(req.Header, jwt, deviceMid)
	applyPassthroughHeaders(req.Header, inHeaders)
	return req, nil
}

func buildChannelAModel(ctx context.Context, endpoint, jwt string, attr Attribution, inHeaders http.Header, body []byte) (*http.Request, error) {
	req, err := newUpstreamRequest(ctx, endpoint, body)
	if err != nil {
		return nil, err
	}
	applyChannelAModelHeaders(req.Header, jwt, attr)
	applyPassthroughHeaders(req.Header, inHeaders)

	if req.Header.Get("Anthropic-Beta") == "" {
		req.Header.Set("Anthropic-Beta", defaultModelBeta)
	}
	return req, nil
}

const defaultModelBeta = "mid-conversation-system-2026-04-07"

// BuildChannelBRequest creates the channel B fallback request: x-api-key +
// the gateway's own UA, never a ZCode identity header.
func BuildChannelBRequest(ctx context.Context, baseURL, apiKey string, inHeaders http.Header, body []byte) (*http.Request, error) {
	return buildChannelB(ctx, joinEndpoint(baseURL, PathChannelBMessages), apiKey, inHeaders, body)
}

// BuildChannelBRequestAt is BuildChannelBRequest with an explicit upstream
// path, e.g. count_tokens.
func BuildChannelBRequestAt(ctx context.Context, baseURL, path, apiKey string, inHeaders http.Header, body []byte) (*http.Request, error) {
	return buildChannelB(ctx, joinEndpoint(baseURL, path), apiKey, inHeaders, body)
}

func buildChannelB(ctx context.Context, endpoint, apiKey string, inHeaders http.Header, body []byte) (*http.Request, error) {
	req, err := newUpstreamRequest(ctx, endpoint, body)
	if err != nil {
		return nil, err
	}
	h := req.Header
	h.Set(hdrContentType, "application/json")
	h.Set(hdrUserAgent, UserAgent())
	h.Set("X-Api-Key", apiKey)
	applyPassthroughHeaders(h, inHeaders)
	return req, nil
}

// applyChannelAHeaders writes the billing/claim header set: bare ZCode UA +
// X-Device-Mid + the runtime identity headers.
func applyChannelAHeaders(h http.Header, jwt, deviceMid string) {
	appVersion, platform := ChannelAIdentity()
	h.Set("Authorization", "Bearer "+jwt)
	h.Set(hdrUserAgent, "ZCode/"+appVersion)
	h.Set("HTTP-Referer", channelAReferer)
	h.Set("X-Title", channelATitle)
	h.Set("X-Platform", platform)
	h.Set("X-Device-Mid", deviceMid)
	applyChannelARuntimeIdentity(h, appVersion, platform)
}

// applyChannelAModelHeaders writes the model-request header set: ai-sdk
// composite UA, double auth (x-api-key + Authorization carry the same JWT),
// no X-Device-Mid (the fingerprint rides in metadata.user_id), per-request
// attribution headers.
func applyChannelAModelHeaders(h http.Header, jwt string, attr Attribution) {
	appVersion, platform := ChannelAIdentity()
	h.Set("Authorization", "Bearer "+jwt)
	h.Set("X-Api-Key", jwt)
	h.Set(hdrUserAgent, ModelUserAgent(appVersion))
	h.Set("HTTP-Referer", channelAReferer)
	h.Set("X-Title", channelATitle)
	h.Set("X-Platform", platform)
	applyChannelARuntimeIdentity(h, appVersion, platform)
	h.Set("X-Request-Id", attr.RequestID)
	h.Set("X-Session-Id", attr.SessionID)
	h.Set("X-Zcode-Trace-Id", attr.TraceID)
	h.Set("X-Query-Id", attr.QueryID)

	h.Set("Connection", "close")
}

// applyChannelARuntimeIdentity writes the ZCode runtime identity headers
// shared by every channel A call (values captured from the official desktop
// client wire). osIdentityForPlatform derives the OS pair from ZCODE_PLATFORM
// so the whole set stays consistent.
func applyChannelARuntimeIdentity(h http.Header, appVersion, platform string) {
	h.Set("X-Zcode-App-Version", appVersion)
	h.Set("X-Release-Channel", channelAReleaseChannel)
	h.Set("X-Client-Language", channelAClientLanguage)
	h.Set("X-Client-Timezone", channelAClientTimezone)
	h.Set("X-Zcode-Agent", channelAAgent)
	osCategory, osVersion := osIdentityForPlatform(platform)
	h.Set("X-Os-Category", osCategory)
	h.Set("X-Os-Version", osVersion)
	h.Set("X-Zcode-Session-Type", channelASessionType)
	h.Set("X-Zcode-Query-Source", channelAQuerySource)
}

// ZCode client runtime identity constants captured from the official desktop
// client wire. They are client fingerprint values (like the User-Agent), not
// deployment configuration; osIdentityForPlatform derives the OS pair from
// ZCODE_PLATFORM so the whole set stays consistent.
const (
	channelAReleaseChannel  = "production"
	channelAClientLanguage  = "en-US"
	channelAClientTimezone  = "Europe/Bucharest"
	channelAAgent           = "glm"
	channelASessionType     = "main"
	channelAQuerySource     = "main_turn"
	defaultWindowsOsVersion = "10.0.19045"
	defaultMacosOsVersion   = "21.6.0"
	defaultLinuxOsVersion   = "6.8.0"
)

// osIdentityForPlatform maps a ZCODE_PLATFORM value ("win32-x64",
// "darwin-x64", "linux-x64",...) to the X-Os-Category / X-Os-Version pair the
// official client sends for that OS. Unknown platforms fall back to windows.
func osIdentityForPlatform(platform string) (category, version string) {
	switch {
	case strings.HasPrefix(platform, "darwin"), strings.HasPrefix(platform, "macos"):
		return "macos", defaultMacosOsVersion
	case strings.HasPrefix(platform, "linux"):
		return "linux", defaultLinuxOsVersion
	default:
		return "windows", defaultWindowsOsVersion
	}
}

// newRequestUUID generates a random UUIDv4 for the per-request identity
// headers (X-Request-Id, X-Zcode-Trace-Id, X-Query-Id, X-Session-Id). The
// official client issues a fresh value per request; failure falls back to a
// time-derived value so the header is always present.
func newRequestUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		binary.BigEndian.PutUint64(b[:8], uint64(time.Now().UnixNano()))
		binary.BigEndian.PutUint64(b[8:], uint64(time.Now().UnixNano())^0x9e3779b97f4a7c15)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	const hexDigits = "0123456789abcdef"
	out := make([]byte, 36)
	pos := 0
	for i, v := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out[pos] = '-'
			pos++
		}
		out[pos] = hexDigits[v>>4]
		out[pos+1] = hexDigits[v&0x0f]
		pos += 2
	}
	return string(out)
}

// applyPassthroughHeaders copies the passthrough headers from the client
// request: anthropic-version (forwarded, else the default), anthropic-beta
// (forwarded verbatim, repeated lines joined) and accept (only when the client
// sent one). Everything else in inHeaders is ignored.
func applyPassthroughHeaders(h, inHeaders http.Header) {
	if v := firstValue(inHeaders, hdrAnthropicVersion); v != "" {
		h.Set(hdrAnthropicVersion, v)
	} else {
		h.Set(hdrAnthropicVersion, defaultAnthropicVersion)
	}
	if v := joinValues(inHeaders, hdrAnthropicBeta); v != "" {
		h.Set(hdrAnthropicBeta, v)
	}
	if v := firstValue(inHeaders, hdrAccept); v != "" {
		h.Set(hdrAccept, v)
	}
}

// newUpstreamRequest creates the POST request with the fixed content-type and
// a replayable body.
func newUpstreamRequest(ctx context.Context, endpoint string, body []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("upstream: build request: %w", err)
	}
	req.Header.Set(hdrContentType, "application/json")
	return req, nil
}

// joinEndpoint joins an origin and a full upstream path.
func joinEndpoint(baseURL, path string) string {
	return strings.TrimRight(baseURL, "/") + "/" + strings.TrimLeft(path, "/")
}

// firstValue returns the first value of the named header, trimmed; "" when
// the header is absent or blank.
func firstValue(h http.Header, name string) string {
	if h == nil {
		return ""
	}
	return strings.TrimSpace(h.Get(name))
}

// joinValues joins every value of a (possibly repeated) header with "," so
// multi-line client headers reach upstream verbatim in content.
func joinValues(h http.Header, name string) string {
	if h == nil {
		return ""
	}
	parts := make([]string, 0, 2)
	for _, v := range h.Values(name) {
		if v = strings.TrimSpace(v); v != "" {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, ",")
}
