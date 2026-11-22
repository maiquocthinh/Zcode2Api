package pool

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestStatusValid(t *testing.T) {
	for _, s := range []Status{StatusActive, StatusCooling, StatusExhausted, StatusDisabled} {
		if !s.valid() {
			t.Errorf("status %q should be valid", s)
		}
	}
	for _, s := range []Status{"", "FOO", "active"} {
		if s.valid() {
			t.Errorf("status %q should not be valid", s)
		}
	}
}

// TestTransitionAllowedEdges pins the edge set of:
// DISABLED is a one-way door opened only by the admin re-enable path.
func TestTransitionAllowedEdges(t *testing.T) {
	allowed := map[[2]Status]bool{
		{StatusActive, StatusCooling}:     true,
		{StatusActive, StatusExhausted}:   true,
		{StatusActive, StatusDisabled}:    true,
		{StatusCooling, StatusActive}:     true,
		{StatusCooling, StatusExhausted}:  true,
		{StatusCooling, StatusDisabled}:   true,
		{StatusExhausted, StatusActive}:   true,
		{StatusExhausted, StatusCooling}:  true,
		{StatusExhausted, StatusDisabled}: true,
		{StatusDisabled, StatusActive}:    true,
	}
	for from, edges := range transitionAllowed {
		for to := range edges {
			if !allowed[[2]Status{from, to}] {
				t.Errorf("unexpected allowed edge %s -> %s", from, to)
			}
		}
	}
	for from, edges := range transitionAllowed {
		for to := range edges {
			if !transitionAllowed[from][to] {
				t.Errorf("missing edge %s -> %s", from, to)
			}
		}
	}
	// The tricky non-edge: DISABLED only returns to ACTIVE.
	if transitionAllowed[StatusDisabled][StatusCooling] || transitionAllowed[StatusDisabled][StatusExhausted] {
		t.Error("DISABLED must only transition to ACTIVE")
	}
}

func TestParseAccountLines(t *testing.T) {
	jwt := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig"
	raw := "Acc A | " + jwt + "\n" + jwt + "\n\n  \nBare |  \nNo token"
	lines := ParseAccountLines(raw)
	if len(lines) != 4 {
		t.Fatalf("got %d lines, want 4 (blank lines skipped): %+v", len(lines), lines)
	}
	if lines[0].Line != 1 || lines[0].Name != "Acc A" || lines[0].Token != jwt {
		t.Errorf("line 0 = %+v", lines[0])
	}
	if lines[1].Line != 2 || lines[1].Name != "" || lines[1].Token != jwt {
		t.Errorf("line 1 = %+v (bare token)", lines[1])
	}
	// "Bare | " — a name with no token part: invalid, not blank.
	if lines[2].Line != 5 || lines[2].Name != "Bare" || lines[2].Token != "" {
		t.Errorf("line 2 = %+v", lines[2])
	}
	// "No token" has no separator, so the whole line is the token — it is
	// reported and later marked invalid by ImportAccounts (not a JWT shape).
	if lines[3].Line != 6 || lines[3].Token != "No token" {
		t.Errorf("line 3 = %+v (no separator: whole line is the token)", lines[3])
	}
}

func TestJWTLooksValid(t *testing.T) {
	valid := []string{
		"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln",
		"a.b.c",
		"eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJ6Y29kZSJ9.Qw-h_8",
	}
	for _, tok := range valid {
		if !jwtLooksValid(tok) {
			t.Errorf("%q should look like a JWT", tok)
		}
	}
	invalid := []string{
		"", "header.payload", "a.b.c.d", "sk-z2a-api-key-not-a-jwt",
		"..", "a..c", "a.b.c d", "a.b.c=d",
	}
	for _, tok := range invalid {
		if jwtLooksValid(tok) {
			t.Errorf("%q should not look like a JWT", tok)
		}
	}
}

func TestKeyHelpers(t *testing.T) {
	jwt := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln"
	a := &Account{JWT: jwt, TokenHint: hintOf([]byte(jwt))}
	if got := a.MaskedToken(); got != "…c2ln" {
		t.Errorf("MaskedToken = %q, want …c2ln", got)
	}
	if hint := hintOf([]byte("ab")); hint != "ab" {
		t.Errorf("short secret hint = %q", hint)
	}
	fp := fingerprintOf([]byte(jwt))
	if len(fp) != 64 || strings.ToLower(fp) != fp {
		t.Errorf("fingerprint = %q, want 64 lowercase hex chars", fp)
	}
	if fp != fingerprintOf([]byte(jwt)) || fp == fingerprintOf([]byte(jwt+"x")) {
		t.Error("fingerprint is not sha256 of the exact token")
	}
}

// TestNewDeviceMID pins the UUID v4 shape of device_mid (a fixed UUID v4
// generated at import).
func TestNewDeviceMID(t *testing.T) {
	uuidV4 := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		mid := newDeviceMID()
		if !uuidV4.MatchString(mid) {
			t.Fatalf("device_mid = %q, want a UUID v4", mid)
		}
		if seen[mid] {
			t.Fatalf("device_mid %q generated twice", mid)
		}
		seen[mid] = true
	}
}

func TestAccountCanServeAndCooldown(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	a := &Account{Status: StatusActive, Inflight: 1}
	if !a.CanServe(now, 3) {
		t.Error("active account below the limit should serve")
	}
	if a.CanServe(now, 1) {
		t.Error("active account at the limit should not serve")
	}
	a.Status = StatusCooling
	a.CooldownUntil = now.Add(time.Minute)
	if a.CanServe(now, 3) || !a.CooldownExpired(now.Add(2*time.Minute)) {
		t.Error("cooling account must not serve and must expire after its cooldown")
	}
	if a.CooldownExpired(now) {
		t.Error("cooling account must not expire before its cooldown")
	}
	a.Status = StatusDisabled
	a.CooldownUntil = time.Time{}
	if a.CooldownExpired(now) {
		t.Error("disabled accounts never expire (admin re-enable only)")
	}
}
