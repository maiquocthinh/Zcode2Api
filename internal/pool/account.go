// Pool-level invariants: all account mutations happen under Pool.mu; every
// status change goes through Pool.transitionLocked (one DB write, one "state"
// event); inflight/counters live only in RAM and are flushed by the flusher
// (60s) and at shutdown; the wall clock is injected (Config.Now) for tests.
package pool

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Status is the state-machine state of an account.
type Status string

const (
	StatusActive    Status = "ACTIVE"
	StatusCooling   Status = "COOLING"
	StatusExhausted Status = "EXHAUSTED"
	StatusDisabled  Status = "DISABLED"
)

// valid reports whether s is one of the four account states.
func (s Status) valid() bool {
	switch s {
	case StatusActive, StatusCooling, StatusExhausted, StatusDisabled:
		return true
	}
	return false
}

// transitionAllowed is the edge set of the account state machine. An account
// leaves DISABLED only when an operator re-enables it
// (Pool.SetAccountEnabled).
var transitionAllowed = map[Status]map[Status]bool{
	StatusActive: {
		StatusCooling:   true,
		StatusExhausted: true,
		StatusDisabled:  true,
	},
	StatusCooling: {
		StatusActive:    true,
		StatusExhausted: true,
		StatusDisabled:  true,
	},
	StatusExhausted: {
		StatusActive:   true,
		StatusCooling:  true,
		StatusDisabled: true,
	},
	StatusDisabled: {
		StatusActive: true,
	},
}

// Account is one pooled free-account JWT. Field ownership:
//   - ID/Name/JWT/.../CreatedAt are written once at load time, then read-only.
//   - Status/StatusReason/CooldownUntil change only through
//     Pool.transitionLocked; quota fields only through Pool.RefreshBalance;
//     claim fields only through the claim flow.
//   - Inflight/FailStreak/counters are guarded by Pool.mu and never persisted
//     except through Pool.Flush.
type Account struct {
	ID          string
	Name        string
	JWT         string
	TokenHint   string
	Fingerprint string
	DeviceMid   string
	ProxyID     string
	CreatedAt   time.Time

	PlanExpiresAt time.Time

	Status        Status
	StatusReason  string
	CooldownUntil time.Time

	RemainingQuota *int64
	TotalQuota     *int64
	QuotaCheckedAt time.Time

	LastClaimAt   time.Time
	LastClaimKind string

	// RAM-only state.
	Inflight   int
	FailStreak int
	// ExhaustedModels holds per-(account, model) quota exhaustion deadlines
	// (model -> until): a 1005 for one model must not bench the account for
	// the others. The deadlines persist in the exhausted_models table and are
	// dropped only on a confirmed quota reset or an operator re-enable.
	ExhaustedModels map[string]time.Time
	// QuotaStrikes counts consecutive chat 1005 answers on a snapshot that
	// still shows quota — the transient-1005 recheck path. Two strikes bench
	// the account for two minutes and trigger a free billing verification;
	// a successful request resets the count.
	QuotaStrikes int
	// QuotaBenchUntil ends the current strike bench: a balance recheck must
	// not restore the account before it.
	QuotaBenchUntil time.Time

	// Counters accumulated in RAM, flushed every 60s and at shutdown.
	// requests/failures are counted per completed request; tokens come from
	// Lease.AddUsage.
	RequestsTotal int64
	FailuresTotal int64
	InputTokens   int64
	OutputTokens  int64
	LastUsedAt    time.Time
}

// CanServe reports whether the account may take one more request right now
// (selector step 1). The quota ordering happens in selectAccount; this check
// never sorts.
func (a *Account) CanServe(now time.Time, inflightLimit int) bool {
	return a.Status == StatusActive && a.Inflight < inflightLimit
}

// ModelExhausted reports whether the account's quota for model is marked
// exhausted at time now. An empty model (requests that resolve to no concrete
// model) is never filtered.
func (a *Account) ModelExhausted(model string, now time.Time) bool {
	if model == "" || len(a.ExhaustedModels) == 0 {
		return false
	}
	until, ok := a.ExhaustedModels[model]
	return ok && until.After(now)
}

// CooldownExpired reports whether a COOLING/EXHAUSTED account's pause has
// passed and the account may return to ACTIVE: COOLING ends at
// COOLING_DURATION; EXHAUSTED ends at next_flush_time or 00:00 GMT+8 — both
// are stored in CooldownUntil.
func (a *Account) CooldownExpired(now time.Time) bool {
	if a.Status != StatusCooling && a.Status != StatusExhausted {
		return false
	}
	return !a.CooldownUntil.IsZero() && !a.CooldownUntil.After(now)
}

// MaskedToken renders the JWT for dashboard output: only the last 4
// characters are ever shown.
func (a *Account) MaskedToken() string {
	if a.TokenHint == "" {
		return "…"
	}
	return "…" + a.TokenHint
}

// ParsedLine is one line of an admin account-import payload:
// "Tên A | eyJhbGci..." or a bare "eyJhbGci...". Token is empty for a
// non-blank line that carries no token part.
type ParsedLine struct {
	Line  int
	Name  string
	Token string
}

// ParseAccountLines splits an import payload into lines. Blank lines are
// skipped (they carry no line meaning); a non-blank line without a non-empty
// token after the optional "name | " part is reported with an empty Token so
// the caller can return its line number in the "invalid" list. Whether the
// token is a well-formed JWT is checked separately (jwtLooksValid) — a line
// with a non-JWT token is invalid, not empty.
func ParseAccountLines(raw string) []ParsedLine {
	var out []ParsedLine
	for i, line := range strings.Split(raw, "\n") {
		line = strings.TrimSuffix(line, "\r")
		name, token, ok := strings.Cut(line, "|")
		if !ok {
			token = name
			name = ""
		}
		name = strings.TrimSpace(name)
		token = strings.TrimSpace(token)
		if name == "" && token == "" {
			continue
		}
		out = append(out, ParsedLine{Line: i + 1, Name: name, Token: token})
	}
	return out
}

// jwtLooksValid reports whether token has the three-part JWT shape (header.
// payload.signature, base64url, RFC 7519). The signature is never verified —
// only upstream can judge a token's validity (a dead JWT surfaces as 1006 and
// DISABLED); this shape check keeps blank or mistaken lines out of the pool.
func jwtLooksValid(token string) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	for _, p := range parts {
		if p == "" || !isBase64URL(p) {
			return false
		}
	}
	return true
}

// isBase64URL reports whether s consists only of base64url characters
// (RFC 4648, unpadded — JWT segments carry no '=' padding).
func isBase64URL(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

// fingerprintOf returns the sha256 hex fingerprint used to reject duplicate
// imports.
func fingerprintOf(token []byte) string {
	sum := sha256.Sum256(token)
	return hex.EncodeToString(sum[:])
}

// hintOf returns the last 4 characters of the JWT for the dashboard mask.
func hintOf(token []byte) string {
	s := string(token)
	if len(s) <= 4 {
		return s
	}
	return s[len(s)-4:]
}

// newDeviceMID returns a random UUID v4 (RFC 4122), the stable device id
// generated once at import time and sent as X-Device-Mid on every channel A
// request.
func newDeviceMID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		n := time.Now().UnixNano()
		return fmt.Sprintf("%08x-%04x-4%03x-8%03x-%012x",
			uint32(n), uint16(n>>32), uint16(n>>48)&0x0fff, uint16(n)&0x0fff, uint64(n)&0xffffffffffff)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ErrAccountNotFound is returned (wrapped with the id) when an id does not
// match any pooled account or stored row. Admin handlers map it to 404.
var ErrAccountNotFound = errors.New("pool: no such account")

// errDuplicateAccount and errDuplicateProxy are returned by the store when an
// insert violates the fingerprint / proxy-URL UNIQUE constraints. The pool
// counts them as duplicates on import instead of failing.
var (
	errDuplicateAccount = errors.New("pool: an account with this token fingerprint already exists")
	errDuplicateProxy   = errors.New("pool: a proxy with this URL already exists")
)
