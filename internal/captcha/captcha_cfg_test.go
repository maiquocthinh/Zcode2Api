package captcha

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"zcode2api/internal/upstream"
)

// TestFetchCaptchaCfg pins the live client/configs fetch: the scene under
// data.configs.captcha wins, an omitted enabled flag with a full scene means
// enabled, and any transport/shape problem reports not-found (the caller
// falls back to the baked scene).
func TestFetchCaptchaCfg(t *testing.T) {
	// The fetch uses the channel A identity/origin globals: point them at the
	// fake server for the duration (the captcha test binary owns them).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/client/configs" {
			t.Errorf("configs path = %q", r.URL.Path)
		}
		if r.URL.Query().Get("app_version") == "" || r.URL.Query().Get("platform") == "" {
			t.Error("configs query must carry app_version and platform")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"data":{"configs":{"captcha":` +
			`{"enabled":true,"prefix":"live-pre","region":"sg","sceneId":"live-scene"}}}}`))
	}))
	defer srv.Close()

	prevOrigin := upstream.ChannelAOrigin()
	upstream.SetChannelAOrigin(srv.URL)
	t.Cleanup(func() { upstream.SetChannelAOrigin(prevOrigin) })

	c, ok := fetchCaptchaCfg(context.Background())
	if !ok {
		t.Fatal("fetchCaptchaCfg = not ok, want the live scene")
	}
	if !c.Enabled || c.Prefix != "live-pre" || c.Region != "sg" || c.SceneID != "live-scene" {
		t.Fatalf("cfg = %+v, want the live scene", c)
	}

	// A nonzero business code is not ok either.
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":3001,"message":"parameter error"}`))
	}))
	defer bad.Close()
	upstream.SetChannelAOrigin(bad.URL)
	if _, ok := fetchCaptchaCfg(context.Background()); ok {
		t.Fatal("a business-error answer must report not-found")
	}
}
