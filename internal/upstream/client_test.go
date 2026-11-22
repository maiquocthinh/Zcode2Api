package upstream

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// Header constants used across the builder tests.
const (
	testJWT       = "eyJhbGciOiJSUzI1NiJ9.e30.sig"
	testDeviceMid = "f2b3d1a4-91c2-4b7e-8a55-1f6e2c9d0b31"
	testAPIKey    = "sk-zai-fallback-cccccccccccccc"
)

// newTestAttr is the fixed attribution used by the builder tests so the
// attribution headers are assertable byte-for-byte.
func newTestAttr() Attribution {
	return Attribution{
		RequestID: "11111111-2222-4333-8444-555555555555",
		SessionID: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
		TraceID:   "99999999-8888-4777-8666-555555555555",
		QueryID:   "dddddddd-cccc-4bbb-8aaa-ffffffffffff",
	}
}

// TestBuildChannelARequestHeaderAllowlist pins the "Gateway → Upstream kênh A"
// column of: the account JWT replaces the client credential,
// all five ZCode source headers are present, anthropic-version/anthropic-beta/
// accept are the only forwarded client headers, and every other client header
// (identity, tracing, cookies — and above all the captcha header) is dropped.
func TestBuildChannelARequestHeaderAllowlist(t *testing.T) {
	SetChannelAIdentity("3.16.3", "darwin-x64")
	defer SetChannelAIdentity(DefaultZcodeAppVersion, DefaultZcodePlatform)
	attr := newTestAttr()

	in := http.Header{}
	in.Set("X-Api-Key", "sk-client-aaaaaaaaaaaaaaaa")
	in.Set("Authorization", "Bearer sk-client-aaaaaaaaaaaaaaaa")
	in.Set("Anthropic-Version", "2023-06-01")
	in.Set("Anthropic-Beta", "context-management-2025-06-27")
	in.Set("Content-Type", "application/json; charset=utf-8")
	in.Set("Accept", "application/json")
	in.Set("User-Agent", "claude-cli/2.0.0 (external, cli)")
	// Client-supplied ZCode source headers must be replaced, not forwarded:
	// the gateway speaks with its own configured identity.
	in.Set("HTTP-Referer", "https://evil.example")
	in.Set("X-Title", "evil-client")
	in.Set("X-Platform", "win32-x64")
	in.Set("X-Device-Mid", "client-chosen-device-id")
	// The captcha header must never reach the chat flow:
	// upstream runs chat with skip_model_request=true — a captcha header there
	// triggers WAF 3012.
	in.Set("X-Aliyun-Captcha-Verify-Param", "captcha-token-from-client")
	// Unrelated client headers never reach upstream.
	in.Set("X-Client-Name", "another-coding-tool")
	in.Set("Request-Id", "req_123")
	in.Set("Cookie", "session=secret")

	req, err := BuildChannelARequest(context.Background(),
		"https://zcode.z.ai", testJWT, attr, in,
		[]byte(`{"model":"claude-sonnet-4-5","max_tokens":1}`))
	if err != nil {
		t.Fatalf("BuildChannelARequest: %v", err)
	}

	// Every header on the outbound request must belong to the channel A
	// allowlist (base identity + the ZCode runtime header set of the official
	// client wire — the business layer answers chat calls without them with
	// 3006 "model not allowed").
	allowed := map[string]bool{
		"Authorization":            true,
		"X-Api-Key":                true,
		"User-Agent":               true,
		"Http-Referer":             true, // canonical form of HTTP-Referer
		"X-Title":                  true,
		"X-Platform":               true,
		"X-Device-Mid":             true,
		"Anthropic-Version":        true,
		"Anthropic-Beta":           true,
		"Content-Type":             true,
		"Accept":                   true,
		"Connection":               true,
		"X-Zcode-App-Version":      true,
		"X-Release-Channel":        true,
		"X-Client-Language":        true,
		"X-Client-Timezone":        true,
		"X-Zcode-Agent":            true,
		"X-Os-Category":            true,
		"X-Os-Version":             true,
		"X-Request-Id":             true,
		"X-Zcode-Session-Type":     true,
		"X-Zcode-Trace-Id":         true,
		"X-Query-Id":               true,
		"X-Zcode-Query-Source":     true,
		"X-Zcode-Session-Features": true,
		"X-Session-Id":             true,
	}
	for name := range req.Header {
		if !allowed[name] {
			t.Errorf("header %q leaked past the channel A allowlist", name)
		}
	}

	if got := req.Header.Get("Authorization"); got != "Bearer "+testJWT {
		t.Errorf("Authorization = %q, want Bearer <account JWT>", got)
	}
	// The five ZCode source headers ( channel A column) in the model
	// request form: the User-Agent carries the ai-sdk suffix and X-Device-Mid
	// is absent (the device fingerprint rides in body.metadata.user_id).
	if got := req.Header.Get("User-Agent"); got != "ZCode/3.16.3 ai-sdk/provider-utils/4.0.27 runtime/node.js/24" {
		t.Errorf("User-Agent = %q, want the ai-sdk composite form", got)
	}
	if got := req.Header.Get("HTTP-Referer"); got != "https://zcode.z.ai" {
		t.Errorf("HTTP-Referer = %q, want %q (client value replaced)", got, "https://zcode.z.ai")
	}
	if got := req.Header.Get("X-Title"); got != "Z Code@electron" {
		t.Errorf("X-Title = %q, want %q", got, "Z Code@electron")
	}
	if got := req.Header.Get("X-Platform"); got != "darwin-x64" {
		t.Errorf("X-Platform = %q, want the configured ZCODE_PLATFORM", got)
	}
	if got := req.Header.Get("X-Device-Mid"); got != "" {
		t.Errorf("X-Device-Mid = %q, want it absent on model requests", got)
	}
	if got := req.Header.Get("X-Request-Id"); got != attr.RequestID {
		t.Errorf("X-Request-Id = %q, want the attribution value", got)
	}
	if got := req.Header.Get("X-Session-Id"); got != attr.SessionID {
		t.Errorf("X-Session-Id = %q, want the attribution session", got)
	}
	if got := req.Header.Get("X-Zcode-Trace-Id"); got != attr.TraceID {
		t.Errorf("X-Zcode-Trace-Id = %q, want the attribution trace", got)
	}
	if got := req.Header.Get("X-Query-Id"); got != attr.QueryID {
		t.Errorf("X-Query-Id = %q, want the attribution query", got)
	}
	if got := req.Header.Get("X-Os-Category"); got != "macos" {
		t.Errorf("X-Os-Category = %q, want the category derived from ZCODE_PLATFORM", got)
	}
	if got := req.Header.Get("Anthropic-Version"); got != "2023-06-01" {
		t.Errorf("Anthropic-Version = %q, want %q", got, "2023-06-01")
	}
	if got := req.Header.Get("Anthropic-Beta"); got != "context-management-2025-06-27" {
		t.Errorf("Anthropic-Beta = %q, want it forwarded verbatim", got)
	}
	if got := req.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want forced %q", got, "application/json")
	}
	// Transport parity of the proven-passing Go client: Connection close, no
	// undici trio (Accept/Accept-Language/Sec-Fetch-Mode are not sent).
	if got := req.Header.Get("Connection"); got != "close" {
		t.Errorf("Connection = %q, want %q", got, "close")
	}
	if got := req.Header.Get("Accept"); got != "application/json" {
		t.Errorf("Accept = %q, want it forwarded from the client", got)
	}
	if got := req.Header.Get("Accept-Language"); got != "" {
		t.Errorf("Accept-Language = %q, want it absent", got)
	}
	if got := req.Header.Get("Sec-Fetch-Mode"); got != "" {
		t.Errorf("Sec-Fetch-Mode = %q, want it absent", got)
	}
	// Dropped client material. X-Api-Key is replaced with the account JWT (not
	// forwarded verbatim), asserted in the round-trip test.
	for _, name := range []string{
		"X-Aliyun-Captcha-Verify-Param", "X-Client-Name",
		"Request-Id", "Cookie",
	} {
		if got := req.Header.Get(name); got != "" {
			t.Errorf("header %q = %q, want it not forwarded", name, got)
		}
	}
	if got := req.Header.Get("X-Api-Key"); got == "sk-client-aaaaaaaaaaaaaaaa" {
		t.Error("client X-Api-Key forwarded verbatim, want it replaced by the account JWT")
	}
}

// TestBuildChannelARequestNeverSetsCaptchaHeader pins the invariant
// directly: even with identity values that look like captcha tokens, the
// builder output never contains the captcha header, and no client captcha
// header sneaks through.
func TestBuildChannelARequestNeverSetsCaptchaHeader(t *testing.T) {
	in := http.Header{}
	in.Set("X-Aliyun-Captcha-Verify-Param", "unsolicited-captcha")
	req, err := BuildChannelARequest(context.Background(), "https://zcode.z.ai",
		testJWT, newTestAttr(), in, nil)
	if err != nil {
		t.Fatalf("BuildChannelARequest: %v", err)
	}
	if got, ok := req.Header[http.CanonicalHeaderKey("X-Aliyun-Captcha-Verify-Param")]; ok {
		t.Errorf("captcha header present on the chat request: %v", got)
	}
}

// TestBuildChannelARequestIdentity pins that ZCODE_APP_VERSION /
// ZCODE_PLATFORM (package identity state, applied by cmd at startup) flow into
// User-Agent and X-Platform.
func TestBuildChannelARequestIdentity(t *testing.T) {
	SetChannelAIdentity("9.9.9-test", "linux-arm64")
	defer SetChannelAIdentity(DefaultZcodeAppVersion, DefaultZcodePlatform)

	req, err := BuildChannelARequest(context.Background(), "https://zcode.z.ai",
		testJWT, newTestAttr(), http.Header{}, nil)
	if err != nil {
		t.Fatalf("BuildChannelARequest: %v", err)
	}
	if got := req.Header.Get("User-Agent"); got != "ZCode/9.9.9-test ai-sdk/provider-utils/4.0.27 runtime/node.js/24" {
		t.Errorf("User-Agent = %q, want %q", got, "ZCode/9.9.9-test ai-sdk/provider-utils/4.0.27 runtime/node.js/24")
	}
	if got := req.Header.Get("X-Platform"); got != "linux-arm64" {
		t.Errorf("X-Platform = %q, want the overridden value", got)
	}
	if got := req.Header.Get("X-Os-Category"); got != "linux" {
		t.Errorf("X-Os-Category = %q, want %q (derived from ZCODE_PLATFORM)", got, "linux")
	}
}

// TestBuildChannelARequestURL pins the endpoint: the origin is joined
// with /api/v1/zcode-plan/anthropic/v1/messages.
func TestBuildChannelARequestURL(t *testing.T) {
	tests := []struct{ baseURL, want string }{
		{"https://zcode.z.ai", "https://zcode.z.ai/api/v1/zcode-plan/anthropic/v1/messages"},
		{"https://zcode.z.ai/", "https://zcode.z.ai/api/v1/zcode-plan/anthropic/v1/messages"},
		{"http://127.0.0.1:9000", "http://127.0.0.1:9000/api/v1/zcode-plan/anthropic/v1/messages"},
	}
	for _, tc := range tests {
		req, err := BuildChannelARequest(context.Background(), tc.baseURL, testJWT, newTestAttr(), http.Header{}, nil)
		if err != nil {
			t.Fatalf("BuildChannelARequest(%q): %v", tc.baseURL, err)
		}
		if got := req.URL.String(); got != tc.want {
			t.Errorf("URL = %q, want %q", got, tc.want)
		}
		if req.Method != http.MethodPost {
			t.Errorf("Method = %q, want POST", req.Method)
		}
	}
}

// TestBuildChannelBRequestHeaderAllowlist pins the "Gateway → Upstream kênh B"
// column of: x-api-key carries the fallback key, User-Agent
// is the gateway's own "Zcode2Api/<version>", and NO ZCode source header
// (Authorization, HTTP-Referer, X-Title, X-Platform, X-Device-Mid) and no
// client header outside the allowlist is sent.
func TestBuildChannelBRequestHeaderAllowlist(t *testing.T) {
	SetChannelAIdentity("3.16.3", "darwin-x64")
	defer SetChannelAIdentity(DefaultZcodeAppVersion, DefaultZcodePlatform)

	in := http.Header{}
	in.Set("X-Api-Key", "sk-client-aaaaaaaaaaaaaaaa")
	in.Set("Authorization", "Bearer sk-client-aaaaaaaaaaaaaaaa")
	in.Set("Anthropic-Version", "2023-06-01")
	in.Set("Anthropic-Beta", "context-management-2025-06-27")
	in.Set("Content-Type", "application/json; charset=utf-8")
	in.Set("Accept", "application/json")
	in.Set("User-Agent", "claude-cli/2.0.0 (external, cli)")
	// ZCode source headers must never leak onto channel B.
	in.Set("HTTP-Referer", "https://zcode.z.ai")
	in.Set("X-Title", "Z Code@electron")
	in.Set("X-Platform", "darwin-x64")
	in.Set("X-Device-Mid", testDeviceMid)
	in.Set("X-Aliyun-Captcha-Verify-Param", "captcha-token-from-client")
	in.Set("X-Client-Name", "another-coding-tool")
	in.Set("Cookie", "session=secret")

	req, err := BuildChannelBRequest(context.Background(),
		"https://api.z.ai", testAPIKey, in,
		[]byte(`{"model":"claude-sonnet-4-5","max_tokens":1}`))
	if err != nil {
		t.Fatalf("BuildChannelBRequest: %v", err)
	}

	allowed := map[string]bool{
		"X-Api-Key":         true,
		"User-Agent":        true,
		"Anthropic-Version": true,
		"Anthropic-Beta":    true,
		"Content-Type":      true,
		"Accept":            true,
	}
	for name := range req.Header {
		if !allowed[name] {
			t.Errorf("header %q leaked past the channel B allowlist", name)
		}
	}

	if got := req.Header.Get("X-Api-Key"); got != testAPIKey {
		t.Errorf("X-Api-Key = %q, want the fallback key", got)
	}
	if got := req.Header.Get("User-Agent"); got != UserAgent() {
		t.Errorf("User-Agent = %q, want %q (gateway identity, never ZCode)", got, UserAgent())
	}
	// The ZCode channel A headers must not leak onto channel B.
	for _, name := range []string{
		"Authorization", "HTTP-Referer", "X-Title", "X-Platform", "X-Device-Mid",
		"X-Aliyun-Captcha-Verify-Param", "X-Client-Name", "Cookie",
	} {
		if got := req.Header.Get(name); got != "" {
			t.Errorf("channel B carries ZCode/client header %q = %q, want none", name, got)
		}
	}
}

// TestBuildChannelBRequestURLAndMethod pins the endpoint.
func TestBuildChannelBRequestURLAndMethod(t *testing.T) {
	req, err := BuildChannelBRequest(context.Background(), "https://api.z.ai/", testAPIKey, http.Header{}, nil)
	if err != nil {
		t.Fatalf("BuildChannelBRequest: %v", err)
	}
	if got := req.URL.String(); got != "https://api.z.ai/api/anthropic/v1/messages" {
		t.Errorf("URL = %q, want https://api.z.ai/api/anthropic/v1/messages", got)
	}
	if req.Method != http.MethodPost {
		t.Errorf("Method = %q, want POST", req.Method)
	}
}

// TestBuildChannelBCountTokensPath pins the count_tokens endpoint used by
// the single-shot passthrough.
func TestBuildChannelBCountTokensPath(t *testing.T) {
	req, err := BuildChannelBRequestAt(context.Background(), "https://api.z.ai",
		PathChannelBCountTokens, testAPIKey, http.Header{}, nil)
	if err != nil {
		t.Fatalf("BuildChannelBRequestAt: %v", err)
	}
	if got := req.URL.String(); got != "https://api.z.ai/api/anthropic/v1/messages/count_tokens" {
		t.Errorf("URL = %q, want https://api.z.ai/api/anthropic/v1/messages/count_tokens", got)
	}
}

// TestBuildChannelBUserAgentVersion pins "Zcode2Api/<version>" with the
// version taken from the Version variable (link-time override).
func TestBuildChannelBUserAgentVersion(t *testing.T) {
	old := Version
	Version = "9.9.9-test"
	defer func() { Version = old }()

	req, err := BuildChannelBRequest(context.Background(), "https://up.example", "k", http.Header{}, nil)
	if err != nil {
		t.Fatalf("BuildChannelBRequest: %v", err)
	}
	if got := req.Header.Get("User-Agent"); got != "Zcode2Api/9.9.9-test" {
		t.Errorf("User-Agent = %q, want %q", got, "Zcode2Api/9.9.9-test")
	}
}

// TestBuildChannelBDefaultAnthropicVersion pins the rule shared by both
// channels: forward anthropic-version when present, else default to
// 2023-06-01.
func TestBuildChannelBDefaultAnthropicVersion(t *testing.T) {
	tests := []struct {
		name  string
		setup func(http.Header)
		want  string
	}{
		{"absent gets the default", func(h http.Header) {}, "2023-06-01"},
		{"blank value gets the default", func(h http.Header) { h.Set("Anthropic-Version", "  ") }, "2023-06-01"},
		{"present value is forwarded", func(h http.Header) { h.Set("anthropic-version", "2099-01-01") }, "2099-01-01"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := http.Header{}
			tc.setup(in)
			req, err := BuildChannelBRequest(context.Background(), "https://up.example", "k", in, nil)
			if err != nil {
				t.Fatalf("BuildChannelBRequest: %v", err)
			}
			if got := req.Header.Get("Anthropic-Version"); got != tc.want {
				t.Errorf("Anthropic-Version = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestBuildChannelRequestAnthropicBetaMultiLine pins that repeated header
// lines are forwarded verbatim in content (channel A path; the passthrough
// helper is shared with channel B).
func TestBuildChannelRequestAnthropicBetaMultiLine(t *testing.T) {
	in := http.Header{}
	in["Anthropic-Beta"] = []string{"prompt-caching-2024-07-31", "context-management-2025-06-27"}

	req, err := BuildChannelARequest(context.Background(), "https://up.example", testJWT, newTestAttr(), in, nil)
	if err != nil {
		t.Fatalf("BuildChannelARequest: %v", err)
	}
	want := "prompt-caching-2024-07-31,context-management-2025-06-27"
	if got := req.Header.Get("Anthropic-Beta"); got != want {
		t.Errorf("Anthropic-Beta = %q, want %q", got, want)
	}
}

// TestBuildChannelAAcceptForwarded pins that the client's accept header is
// forwarded verbatim (the gateway does not rewrite it) and stays absent when
// the client sent none.
func TestBuildChannelAAcceptForwarded(t *testing.T) {
	in := http.Header{}
	in.Set("Accept", "text/event-stream")
	req, err := BuildChannelARequest(context.Background(), "https://up.example", testJWT, newTestAttr(), in, nil)
	if err != nil {
		t.Fatalf("BuildChannelARequest: %v", err)
	}
	if got := req.Header.Get("Accept"); got != "text/event-stream" {
		t.Errorf("Accept = %q, want it forwarded from the client", got)
	}
	req, err = BuildChannelARequest(context.Background(), "https://up.example", testJWT, newTestAttr(), http.Header{}, nil)
	if err != nil {
		t.Fatalf("BuildChannelARequest: %v", err)
	}
	if got := req.Header.Get("Accept"); got != "" {
		t.Errorf("Accept = %q, want it absent without a client accept", got)
	}
}

// TestBuildChannelRequestMethodBodyAndContext pins the request mechanics
// shared by both builders: POST, the body (replayable via GetBody for
// failover), and the caller's context.
func TestBuildChannelRequestMethodBodyAndContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":1}`)
	req, err := BuildChannelARequest(ctx, "https://up.example", testJWT, newTestAttr(), http.Header{}, body)
	if err != nil {
		t.Fatalf("BuildChannelARequest: %v", err)
	}
	if req.Method != http.MethodPost {
		t.Errorf("Method = %q, want POST", req.Method)
	}
	if req.ContentLength != int64(len(body)) {
		t.Errorf("ContentLength = %d, want %d", req.ContentLength, len(body))
	}
	if req.Context() != ctx {
		t.Error("request does not carry the caller's context")
	}
	replay, err := req.GetBody()
	if err != nil {
		t.Fatalf("GetBody: %v", err)
	}
	var got bytes.Buffer
	if _, err := got.ReadFrom(replay); err != nil {
		t.Fatalf("read GetBody: %v", err)
	}
	if !bytes.Equal(got.Bytes(), body) {
		t.Errorf("GetBody = %q, want %q (failover must be able to replay)", got.Bytes(), body)
	}

	// Empty body is allowed on both channels.
	empty, err := BuildChannelBRequest(ctx, "https://up.example", "k", http.Header{}, nil)
	if err != nil {
		t.Fatalf("BuildChannelBRequest(nil body): %v", err)
	}
	if empty.ContentLength != 0 {
		t.Errorf("ContentLength = %d, want 0", empty.ContentLength)
	}
}

// TestTransportForDirectAndCache pins the direct transport and per-URL cache
// identity.
func TestTransportForDirectAndCache(t *testing.T) {
	direct := TransportFor("")
	if direct.Proxy != nil {
		t.Error("direct transport must not use a proxy (env proxy variables are ignored by design)")
	}
	if again := TransportFor(""); again != direct {
		t.Error("TransportFor must return the same cached transport for the same URL")
	}
}

// TestTransportForCachesPerURL pins one transport per distinct proxy URL.
func TestTransportForCachesPerURL(t *testing.T) {
	a := TransportFor("http://10.0.0.1:8080")
	if b := TransportFor("http://10.0.0.1:8080"); b != a {
		t.Error("same proxy URL must yield the same cached transport")
	}
	if c := TransportFor("http://10.0.0.2:8080"); c == a {
		t.Error("different proxy URL must yield a different transport")
	}
}

// TestTransportForConcurrentSameURL exercises the cache from many goroutines
// under -race: everyone must see the same transport.
func TestTransportForConcurrentSameURL(t *testing.T) {
	const url = "http://concurrent-test:8080"
	first := TransportFor(url)

	var wg sync.WaitGroup
	ptrs := make([]*http.Transport, 64)
	for i := range ptrs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ptrs[i] = TransportFor(url)
		}(i)
	}
	wg.Wait()

	for i, p := range ptrs {
		if p != first {
			t.Fatalf("goroutine %d got a different transport for the same URL", i)
		}
	}
}

// TestTransportForProxySchemes pins http/https/socks5 (plus socks5h) support
// through stdlib Transport.Proxy, including credentials (which stay in
// UserInfo, never in the logged host).
func TestTransportForProxySchemes(t *testing.T) {
	tests := []struct {
		raw    string
		scheme string
		host   string
	}{
		{"http://proxy.local:8080", "http", "proxy.local:8080"},
		{"https://proxy.local:8443", "https", "proxy.local:8443"},
		{"socks5://127.0.0.1:1080", "socks5", "127.0.0.1:1080"},
		{"socks5h://127.0.0.1:1080", "socks5h", "127.0.0.1:1080"},
		{"http://user:pw@proxy.local:3128", "http", "proxy.local:3128"},
	}
	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			tr := TransportFor(tc.raw)
			if tr.Proxy == nil {
				t.Fatalf("Proxy is nil, want a %s proxy", tc.scheme)
			}
			req, err := http.NewRequest(http.MethodGet, "https://zcode.z.ai/api/v1/zcode-plan/anthropic/v1/messages", nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			u, err := tr.Proxy(req)
			if err != nil {
				t.Fatalf("Proxy(req): %v", err)
			}
			if u.Scheme != tc.scheme || u.Host != tc.host {
				t.Errorf("proxied as %s://%s, want %s://%s", u.Scheme, u.Host, tc.scheme, tc.host)
			}
			if got := u.User.String(); got != "" && got != "user:pw" {
				t.Errorf("proxy userinfo = %q, unexpected", got)
			}
		})
	}
}

// TestTransportForInvalidProxyFallsBackToDirect pins the documented fallback
// for unparsable/unsupported proxy URLs (direct egress, no panic).
func TestTransportForInvalidProxyFallsBackToDirect(t *testing.T) {
	for _, raw := range []string{"ftp://proxy.local:21", "proxy.local:8080", "http://"} {
		tr := TransportFor(raw)
		if tr.Proxy != nil {
			t.Errorf("%q: want direct fallback, got a proxy", raw)
		}
	}
}

// TestTransportForTimeouts pins the transport settings of
func TestTransportForTimeouts(t *testing.T) {
	tr := TransportFor("")
	if tr.MaxIdleConnsPerHost != 32 {
		t.Errorf("MaxIdleConnsPerHost = %d, want 32", tr.MaxIdleConnsPerHost)
	}
	if tr.IdleConnTimeout != 90*time.Second {
		t.Errorf("IdleConnTimeout = %s, want 90s", tr.IdleConnTimeout)
	}
	if tr.TLSHandshakeTimeout != 10*time.Second {
		t.Errorf("TLSHandshakeTimeout = %s, want 10s", tr.TLSHandshakeTimeout)
	}
	if tr.ResponseHeaderTimeout != DefaultHeaderTimeout {
		t.Errorf("ResponseHeaderTimeout = %s, want %s", tr.ResponseHeaderTimeout, DefaultHeaderTimeout)
	}
}

// TestSetHeaderTimeoutAppliesToNewTransports pins SetHeaderTimeout semantics:
// new transports get the new value, cached ones keep theirs.
func TestSetHeaderTimeoutAppliesToNewTransports(t *testing.T) {
	before := TransportFor("http://header-timeout-before:1")
	if before.ResponseHeaderTimeout != DefaultHeaderTimeout {
		t.Fatalf("pre-change transport = %s, want %s", before.ResponseHeaderTimeout, DefaultHeaderTimeout)
	}

	SetHeaderTimeout(7 * time.Second)
	defer SetHeaderTimeout(DefaultHeaderTimeout)

	after := TransportFor("http://header-timeout-after:1")
	if after.ResponseHeaderTimeout != 7*time.Second {
		t.Errorf("new transport ResponseHeaderTimeout = %s, want 7s", after.ResponseHeaderTimeout)
	}
	if cached := TransportFor("http://header-timeout-before:1"); cached != before {
		t.Error("cached transport must be reused after SetHeaderTimeout")
	}
	if before.ResponseHeaderTimeout != DefaultHeaderTimeout {
		t.Errorf("cached transport changed to %s; it must keep the value it was built with", before.ResponseHeaderTimeout)
	}
}

// TestChannelAOriginAndIdentityDefaults pins the defaults the package
// starts with (UPSTREAM_URL=https://zcode.z.ai, ZCODE_APP_VERSION=3.16.3,
// ZCODE_PLATFORM=darwin-x64) and that the setters only overwrite non-empty
// values.
func TestChannelAOriginAndIdentityDefaults(t *testing.T) {
	if got := ChannelAOrigin(); got != DefaultChannelAOrigin {
		t.Errorf("ChannelAOrigin = %q, want %q", got, DefaultChannelAOrigin)
	}
	appVersion, platform := ChannelAIdentity()
	if appVersion != DefaultZcodeAppVersion {
		t.Errorf("app version = %q, want %q", appVersion, DefaultZcodeAppVersion)
	}
	if platform != DefaultZcodePlatform {
		t.Errorf("platform = %q, want %q", platform, DefaultZcodePlatform)
	}

	SetChannelAIdentity("", "") // empty values must keep the current identity
	appVersion, platform = ChannelAIdentity()
	if appVersion != DefaultZcodeAppVersion || platform != DefaultZcodePlatform {
		t.Errorf("empty SetChannelAIdentity changed the identity to %q/%q", appVersion, platform)
	}
	SetChannelAOrigin("") // empty value must keep the current origin
	if got := ChannelAOrigin(); got != DefaultChannelAOrigin {
		t.Errorf("empty SetChannelAOrigin changed the origin to %q", got)
	}
}

// TestRoundTripChannelAHeadersAndClassify drives BuildChannelARequest +
// TransportFor + Classify against a simulated channel A upstream
// (httptest.Server — no real network): the ZCode source headers arrive
// correctly and an HTTP 200 body with code 1005 classifies as
// quota_exhausted.
func TestRoundTripChannelAHeadersAndClassify(t *testing.T) {
	SetChannelAIdentity("3.16.3", "darwin-x64")
	defer SetChannelAIdentity(DefaultZcodeAppVersion, DefaultZcodePlatform)

	var mu sync.Mutex
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = r.Header.Clone()
		mu.Unlock()
		if r.URL.Path != PathChannelAMessages {
			t.Errorf("upstream path = %q, want %q", r.URL.Path, PathChannelAMessages)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"error":{"code":"1005","message":"quota gone, next_flush_time=1763638800"}}`))
	}))
	defer srv.Close()

	in := http.Header{}
	in.Set("X-Api-Key", "sk-client-aaaaaaaaaaaaaaaa")
	in.Set("Authorization", "Bearer sk-client-aaaaaaaaaaaaaaaa")
	in.Set("User-Agent", "claude-cli/2.0.0")
	in.Set("X-Aliyun-Captcha-Verify-Param", "captcha-token-from-client")
	in.Set("X-Client-Name", "another-coding-tool")

	req, err := BuildChannelARequest(context.Background(), srv.URL, testJWT, newTestAttr(), in,
		[]byte(`{"model":"claude-sonnet-4-5","max_tokens":1}`))
	if err != nil {
		t.Fatalf("BuildChannelARequest: %v", err)
	}

	client := &http.Client{Transport: TransportFor("")}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	mu.Lock()
	hdr := got
	mu.Unlock()
	if hdr.Get("Authorization") != "Bearer "+testJWT {
		t.Errorf("upstream saw Authorization = %q, want the account JWT", hdr.Get("Authorization"))
	}
	// golden parity: model requests carry the ai-sdk composite UA, no
	// X-Device-Mid (the fingerprint rides in metadata.user_id) and the
	// attribution identity headers.
	if hdr.Get("User-Agent") != "ZCode/3.16.3 ai-sdk/provider-utils/4.0.27 runtime/node.js/24" {
		t.Errorf("upstream saw User-Agent = %q, want the ai-sdk composite", hdr.Get("User-Agent"))
	}
	if hdr.Get("HTTP-Referer") != "https://zcode.z.ai" {
		t.Errorf("upstream saw HTTP-Referer = %q", hdr.Get("HTTP-Referer"))
	}
	if hdr.Get("X-Title") != "Z Code@electron" {
		t.Errorf("upstream saw X-Title = %q", hdr.Get("X-Title"))
	}
	if hdr.Get("X-Platform") != "darwin-x64" {
		t.Errorf("upstream saw X-Platform = %q", hdr.Get("X-Platform"))
	}
	if hdr.Get("X-Device-Mid") != "" {
		t.Errorf("upstream saw X-Device-Mid = %q, want it absent on model requests", hdr.Get("X-Device-Mid"))
	}
	if hdr.Get("X-Session-Id") == "" {
		t.Errorf("upstream saw no X-Session-Id, want the attribution session")
	}
	if got := req.Header.Get("Connection"); got != "close" {
		t.Errorf("Connection = %q, want close", got)
	}
	if hdr.Get("X-Api-Key") != testJWT {
		t.Errorf("upstream saw X-Api-Key = %q, want the account JWT (double auth header, golden parity)", hdr.Get("X-Api-Key"))
	}
	if hdr.Get("X-Aliyun-Captcha-Verify-Param") != "" {
		t.Errorf("upstream saw the captcha header %q, want none on the chat flow", hdr.Get("X-Aliyun-Captcha-Verify-Param"))
	}
	if hdr.Get("X-Client-Name") != "" {
		t.Errorf("upstream saw X-Client-Name = %q, want none", hdr.Get("X-Client-Name"))
	}
	if hdr.Get("Anthropic-Version") != "2023-06-01" {
		t.Errorf("upstream saw Anthropic-Version = %q, want the default", hdr.Get("Anthropic-Version"))
	}
	if hdr.Get("Content-Type") != "application/json" {
		t.Errorf("upstream saw Content-Type = %q, want application/json", hdr.Get("Content-Type"))
	}

	out := Classify(resp.StatusCode, body, resp.Header)
	if out.Kind != KindQuotaExhausted || out.AccountAction != AccountExhausted || !out.Retryable {
		t.Fatalf("Classify(200 with 1005) = %+v, want quota_exhausted/EXHAUSTED/retryable", out)
	}
	if !out.Until.Equal(time.Unix(1763638800, 0)) {
		t.Errorf("Until = %s, want next_flush_time %s", out.Until, time.Unix(1763638800, 0))
	}
}

// TestRoundTripChannelBNoLeak drives BuildChannelBRequest over the wire and
// asserts at the receiving end that no ZCode source header and no client
// identity header reaches the fallback upstream.
func TestRoundTripChannelBNoLeak(t *testing.T) {
	var mu sync.Mutex
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = r.Header.Clone()
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "90")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"code":"1302","message":"rate limited"}}`))
	}))
	defer srv.Close()

	in := http.Header{}
	in.Set("X-Api-Key", "sk-client-aaaaaaaaaaaaaaaa")
	in.Set("Authorization", "Bearer sk-client-aaaaaaaaaaaaaaaa")
	in.Set("User-Agent", "claude-cli/2.0.0")
	in.Set("HTTP-Referer", "https://zcode.z.ai")
	in.Set("X-Title", "Z Code@electron")
	in.Set("X-Platform", "darwin-x64")
	in.Set("X-Device-Mid", testDeviceMid)
	in.Set("X-Client-Name", "another-coding-tool")

	req, err := BuildChannelBRequest(context.Background(), srv.URL, testAPIKey, in,
		[]byte(`{"model":"claude-sonnet-4-5","max_tokens":1}`))
	if err != nil {
		t.Fatalf("BuildChannelBRequest: %v", err)
	}

	resp, err := (&http.Client{Transport: TransportFor("")}).Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	mu.Lock()
	hdr := got
	mu.Unlock()
	if hdr.Get("X-Api-Key") != testAPIKey {
		t.Errorf("upstream saw X-Api-Key = %q, want the fallback key", hdr.Get("X-Api-Key"))
	}
	for _, name := range []string{
		"Authorization", "HTTP-Referer", "X-Title", "X-Platform", "X-Device-Mid",
		"X-Client-Name",
	} {
		if hdr.Get(name) != "" {
			t.Errorf("upstream saw %q = %q on channel B, want none", name, hdr.Get(name))
		}
	}
	if ua := hdr.Get("User-Agent"); !startsWith(ua, "Zcode2Api/") {
		t.Errorf("upstream saw User-Agent = %q, want Zcode2Api/<version>", ua)
	}

	out := Classify(resp.StatusCode, body, resp.Header)
	if out.Kind != KindRateLimit || out.AccountAction != AccountCooling || !out.Retryable {
		t.Fatalf("Classify(429 with 1302) = %+v, want rate_limit/COOLING/retryable", out)
	}
	if d := time.Until(out.Until); d < 89*time.Second || d > 95*time.Second {
		t.Errorf("Until is %s from now, want ~90s (Retry-After honored)", d)
	}
}

// TestClassifyOnSSEErrorEventFromUpstreamResponse sends a real HTTP 200 with
// an SSE "event: error" body from a simulated upstream and classifies the raw
// response body — the full-SSE form must parse like the bare data payload.
func TestClassifyOnSSEErrorEventFromUpstreamResponse(t *testing.T) {
	const event = "event: error\n" +
		`data: {"error":{"code":"1308","message":"usage limit reached (next_flush_time=1763638800)"}}` + "\n\n"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(event))
	}))
	defer srv.Close()

	resp, err := (&http.Client{Transport: TransportFor("")}).Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	out := Classify(resp.StatusCode, raw, resp.Header)
	if out.Kind != KindUsageLimit || out.AccountAction != AccountExhausted || !out.Retryable {
		t.Fatalf("Classify(200 SSE 1308) = %+v, want usage_limit/EXHAUSTED/retryable", out)
	}
	if !out.Until.Equal(time.Unix(1763638800, 0)) {
		t.Errorf("Until = %s, want next_flush_time %s", out.Until, time.Unix(1763638800, 0))
	}
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
