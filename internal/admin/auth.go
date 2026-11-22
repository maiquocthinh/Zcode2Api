package admin

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Session and CSRF rules: cookie "z2a_session" holding a
// 32-byte random token kept only in RAM with a 12-hour TTL; every write
// request must carry X-Requested-With: zcode2api to defeat CSRF (the cookie
// alone is not enough because EventSource cannot send custom headers — the
// dashboard sends it on every fetch).
const (
	sessionCookieName = "z2a_session"
	sessionTTL        = 12 * time.Hour
	sessionTokenBytes = 32
	csrfHeader        = "X-Requested-With"
	csrfValue         = "zcode2api"

	// Login rate limit: 5 attempts per minute per IP.
	loginMaxPerWindow = 5
	loginWindow       = time.Minute
	// loginFailsSweepAt is the failure-table size that triggers a full sweep:
	// an unauthenticated source can rotate its address (each IPv6 /64 looks
	// like a new IP) and would otherwise grow the map without bound — the
	// per-IP prune only ever touched the calling IP's own rows.
	loginFailsSweepAt = 4096
)

// Auth manages admin sessions in RAM and the per-IP login rate limiter.
type Auth struct {
	mu       sync.Mutex
	sessions map[string]time.Time // token -> expiry

	loginMu    sync.Mutex
	loginFails map[string][]time.Time // ip -> failed attempt timestamps
}

// NewAuth creates the session store.
func NewAuth() *Auth {
	return &Auth{
		sessions:   make(map[string]time.Time),
		loginFails: make(map[string][]time.Time),
	}
}

// Login validates the admin password (constant-time compare) and, on success,
// creates a session and returns the cookie to set. Failed attempts count
// against the per-IP rate limit; when the limit is hit the login is rejected
// before the password is even compared.
func (a *Auth) Login(password, expected string, r *http.Request) (cookie *http.Cookie, ok bool, rateLimited bool) {
	ip := clientIP(r)

	a.loginMu.Lock()
	a.pruneLoginLocked(ip)
	if len(a.loginFails[ip]) >= loginMaxPerWindow {
		a.loginMu.Unlock()
		return nil, false, true
	}
	a.loginMu.Unlock()

	if subtle.ConstantTimeCompare([]byte(password), []byte(expected)) != 1 {
		a.loginMu.Lock()
		a.recordLoginFailLocked(ip)
		a.loginMu.Unlock()
		return nil, false, false
	}

	// Successful login: forget the failures for this IP.
	a.loginMu.Lock()
	delete(a.loginFails, ip)
	a.loginMu.Unlock()

	token, err := newSessionToken()
	if err != nil {
		return nil, false, false
	}
	expiry := time.Now().Add(sessionTTL)
	a.mu.Lock()
	a.pruneSessionsLocked()
	a.sessions[token] = expiry
	a.mu.Unlock()

	return &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		MaxAge:   int(sessionTTL.Seconds()),
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   r.TLS != nil, // Secure when served over HTTPS
	}, true, false
}

// Logout removes the caller's session, if any.
func (a *Auth) Logout(r *http.Request) {
	if c, err := r.Cookie(sessionCookieName); err == nil {
		a.mu.Lock()
		delete(a.sessions, c.Value)
		a.mu.Unlock()
	}
}

// Check reports whether the request carries a valid, unexpired session
// cookie.
func (a *Auth) Check(r *http.Request) bool {
	c, err := r.Cookie(sessionCookieName)
	if err != nil || c.Value == "" {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	expiry, ok := a.sessions[c.Value]
	if !ok {
		return false
	}
	if time.Now().After(expiry) {
		delete(a.sessions, c.Value)
		return false
	}
	return true
}

// Middleware guards /api/* routes: a valid session cookie is required
// everywhere except POST /api/login, and write requests must carry the CSRF
// header.
func (a *Auth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !a.Check(r) {
			writeAdminError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		switch r.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
			if r.Header.Get(csrfHeader) != csrfValue {
				writeAdminError(w, http.StatusForbidden, "missing "+csrfHeader+" header")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// newSessionToken mints a 32-byte random token.
func newSessionToken() (string, error) {
	buf := make([]byte, sessionTokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// pruneSessionsLocked drops expired sessions. Must hold a.mu.
func (a *Auth) pruneSessionsLocked() {
	now := time.Now()
	for token, expiry := range a.sessions {
		if now.After(expiry) {
			delete(a.sessions, token)
		}
	}
}

// pruneLoginLocked drops failed-login timestamps outside the window. Must
// hold a.loginMu.
func (a *Auth) pruneLoginLocked(ip string) {
	cutoff := time.Now().Add(-loginWindow)
	hits := a.loginFails[ip]
	kept := hits[:0]
	for _, t := range hits {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	if len(kept) == 0 {
		delete(a.loginFails, ip)
		return
	}
	a.loginFails[ip] = kept
}

// recordLoginFailLocked appends one failure timestamp, sweeping the whole
// table first when it has grown past the threshold. Must hold a.loginMu.
func (a *Auth) recordLoginFailLocked(ip string) {
	if len(a.loginFails) >= loginFailsSweepAt {
		a.sweepLoginFailsLocked()
	}
	a.loginFails[ip] = append(a.loginFails[ip], time.Now())
}

// sweepLoginFailsLocked drops every failure entry whose timestamps are all
// outside the window, for every IP at once — the table-wide answer to
// unauthenticated sources that rotate their address. Must hold a.loginMu.
func (a *Auth) sweepLoginFailsLocked() {
	cutoff := time.Now().Add(-loginWindow)
	for ip, hits := range a.loginFails {
		kept := hits[:0]
		for _, t := range hits {
			if t.After(cutoff) {
				kept = append(kept, t)
			}
		}
		if len(kept) == 0 {
			delete(a.loginFails, ip)
			continue
		}
		a.loginFails[ip] = kept
	}
}

// clientIP extracts the remote host without the port. Only used for login
// rate limiting; never logged with any other data.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return strings.Trim(r.RemoteAddr, "[]")
	}
	return host
}
