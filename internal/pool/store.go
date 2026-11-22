package pool

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base32"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	// Pure-Go SQLite driver, no CGO, static build.
	_ "modernc.org/sqlite"
)

// Store is the SQLite persistence layer. It owns the token
// encryption: JWTs are stored as AES-256-GCM(nonce||ciphertext) when a
// SECRET_KEY is configured, and as plaintext bytes otherwise.

// The pool keeps everything it needs in RAM; the store is written through on
// every state transition (small UPDATE) and by the 60s flusher for counters.
type Store struct {
	db        *sql.DB
	secretKey string // "" = store tokens as plaintext
}

// SQLite PRAGMAs mandated by
const (
	pragmaJournalMode = "PRAGMA journal_mode = WAL"
	pragmaBusyTimeout = "PRAGMA busy_timeout = 5000"
	pragmaForeignKeys = "PRAGMA foreign_keys = ON"
)

// hkdfInfo is the HKDF info string for the per-record AES-256 key derivation.
// The record id (unique per row) is the HKDF salt, so every account token is
// sealed under its own key. Changing either string — or SECRET_KEY —
// invalidates all stored tokens, which matches the documented effect of
// changing SECRET_KEY.
const hkdfInfo = "zcode2api/account-token aes-256-gcm v1"

// hkdfKeyLen is the AES-256 key size in bytes.
const hkdfKeyLen = 32

// migrations holds one entry per schema version; entry i upgrades version i to
// i+1. Every migration runs inside one transaction and ends by bumping
// schema_version.

// Migration 1 is byte-identical to the legacy schema (schema_version = 1,
// table "keys") so a database written by an older build continues cleanly.
// Migration 2 replaces the legacy API-key table with the accounts table: the
// stored API keys are not JWT accounts and cannot be converted, so the legacy
// table is dropped and the accounts schema is created. A fresh database runs
// all of them and ends at the latest version.
var migrations = [][]string{
	{
		`CREATE TABLE IF NOT EXISTS proxies (
		    id              TEXT PRIMARY KEY,
		    name            TEXT NOT NULL DEFAULT '',
		    url             TEXT NOT NULL UNIQUE,
		    latency_ms      INTEGER NOT NULL DEFAULT -1,
		    status          TEXT NOT NULL DEFAULT 'UNKNOWN',
		    last_checked_at INTEGER NOT NULL DEFAULT 0,
		    created_at      INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS keys (
		    id             TEXT PRIMARY KEY,
		    name           TEXT NOT NULL DEFAULT '',
		    secret         BLOB NOT NULL,
		    secret_hint    TEXT NOT NULL,
		    fingerprint    TEXT NOT NULL UNIQUE,
		    proxy_id       TEXT REFERENCES proxies(id) ON DELETE SET NULL,
		    status         TEXT NOT NULL DEFAULT 'ACTIVE',
		    status_reason  TEXT NOT NULL DEFAULT '',
		    cooldown_until INTEGER NOT NULL DEFAULT 0,
		    requests_total INTEGER NOT NULL DEFAULT 0,
		    failures_total INTEGER NOT NULL DEFAULT 0,
		    input_tokens   INTEGER NOT NULL DEFAULT 0,
		    output_tokens  INTEGER NOT NULL DEFAULT 0,
		    last_used_at   INTEGER NOT NULL DEFAULT 0,
		    created_at     INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_keys_status ON keys(status)`,
		`CREATE TABLE IF NOT EXISTS model_mappings (
		    id         TEXT PRIMARY KEY,
		    pattern    TEXT NOT NULL UNIQUE,
		    target     TEXT NOT NULL DEFAULT '',
		    mode       TEXT NOT NULL DEFAULT 'MAPPED',
		    created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS settings (
		    key        TEXT PRIMARY KEY,
		    value      TEXT NOT NULL,
		    updated_at INTEGER NOT NULL
		)`,
	},
	{
		// Migration 2: drop the API-key table (rows are not JWTs and cannot
		// migrate), create the accounts table.
		`DROP TABLE IF EXISTS keys`,
		`CREATE TABLE IF NOT EXISTS accounts (
		    id               TEXT PRIMARY KEY,                -- "acc_" + 12 ký tự
		    name             TEXT NOT NULL DEFAULT '',        -- tên gợi nhớ
		    token            BLOB NOT NULL,                   -- AES-GCM(nonce||JWT) hoặc plaintext nếu không có SECRET_KEY
		    token_hint       TEXT NOT NULL,                   -- 4 ký tự cuối của JWT
		    fingerprint      TEXT NOT NULL UNIQUE,            -- sha256(JWT) hex, chống import trùng
		    device_mid       TEXT NOT NULL,                   -- UUID v4 sinh lúc import (header X-Device-Mid)
		    plan_expires_at  INTEGER NOT NULL DEFAULT 0,      -- hạn gói (unix giây, từ /billing/balance)
		    proxy_id         TEXT REFERENCES proxies(id) ON DELETE SET NULL,
		    status           TEXT NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE | COOLING | EXHAUSTED | DISABLED
		    status_reason    TEXT NOT NULL DEFAULT '',        -- Kind gần nhất
		    cooldown_until   INTEGER NOT NULL DEFAULT 0,      -- unix giây
		    remaining_quota  INTEGER,                         -- NULL = chưa từng kiểm tra
		    total_quota      INTEGER,
		    quota_checked_at INTEGER NOT NULL DEFAULT 0,
		    last_claim_at    INTEGER NOT NULL DEFAULT 0,
		    last_claim_kind  TEXT NOT NULL DEFAULT '',
		    requests_total   INTEGER NOT NULL DEFAULT 0,
		    failures_total   INTEGER NOT NULL DEFAULT 0,
		    input_tokens     INTEGER NOT NULL DEFAULT 0,
		    output_tokens    INTEGER NOT NULL DEFAULT 0,
		    last_used_at     INTEGER NOT NULL DEFAULT 0,
		    created_at       INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status)`,
	},
	{
		// Migration 3: per-(account, model) quota exhaustion — a 1005 for one
		// model must not bench the whole account for the others. Deadlines are
		// unix seconds; a pair stops applying once until passes (readers
		// filter by time; the rows are dropped on a confirmed quota reset or
		// an operator re-enable).
		`CREATE TABLE IF NOT EXISTS exhausted_models (
		    account_id TEXT NOT NULL,
		    model      TEXT NOT NULL,
		    until      INTEGER NOT NULL,
		    PRIMARY KEY (account_id, model)
		)`,
	},
}

// Open opens (creating if needed) the SQLite database at path and runs the
// schema migrations. secretKey is the configured SECRET_KEY; when empty, JWTs
// are stored as plaintext and the dashboard is expected to warn about it.
//
// The pool serializes all access on one connection: with modernc.org/sqlite
// this removes SQLITE_BUSY handling from the callers, and the PRAGMAs below
// are then guaranteed to apply to every query the process runs. WAL mode is
// stored in the database file itself and survives reopenings.
func Open(path, secretKey string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("pool: create db directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", filepath.ToSlash(path))
	if err != nil {
		return nil, fmt.Errorf("pool: open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // one writer connection

	s := &Store{db: db, secretKey: secretKey}
	for _, pragma := range []string{pragmaJournalMode, pragmaBusyTimeout, pragmaForeignKeys} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("pool: apply %q: %w", pragma, err)
		}
	}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the database. The caller is responsible for flushing pool
// counters first (Pool.Stop does it).
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// migrate applies the pending migrations in order, one transaction each, and
// records the new version in schema_version.
func (s *Store) migrate() error {
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return fmt.Errorf("pool: create schema_version: %w", err)
	}
	var version int
	if err := s.db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&version); err != nil {
		return fmt.Errorf("pool: read schema_version: %w", err)
	}
	for v := version; v < len(migrations); v++ {
		if err := s.runMigration(v + 1); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) runMigration(to int) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("pool: begin migration %d: %w", to, err)
	}
	defer tx.Rollback()
	for _, stmt := range migrations[to-1] {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("pool: migration %d: %w", to, err)
		}
	}
	if _, err := tx.Exec(`DELETE FROM schema_version`); err != nil {
		return fmt.Errorf("pool: migration %d: %w", to, err)
	}
	if _, err := tx.Exec(`INSERT INTO schema_version (version) VALUES (?)`, to); err != nil {
		return fmt.Errorf("pool: migration %d: %w", to, err)
	}
	return tx.Commit()
}

// --- token encryption -----------------

// sealToken encrypts one account JWT with AES-256-GCM under a key derived
// from SECRET_KEY via HKDF-SHA256 with the record id as salt and returns
// nonce || ciphertext || tag.
func (s *Store) sealToken(recordID string, plain []byte) ([]byte, error) {
	aead, err := s.aeadFor(recordID)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, plain, nil), nil
}

// openToken decrypts a blob produced by sealToken.
func (s *Store) openToken(recordID string, blob []byte) ([]byte, error) {
	aead, err := s.aeadFor(recordID)
	if err != nil {
		return nil, err
	}
	n := aead.NonceSize()
	if len(blob) < n {
		return nil, errors.New("token blob too short")
	}
	return aead.Open(nil, blob[:n], blob[n:], nil)
}

func (s *Store) aeadFor(recordID string) (cipher.AEAD, error) {
	key, err := hkdf.Key(sha256.New, []byte(s.secretKey), []byte(recordID), hkdfInfo, hkdfKeyLen)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// --- random ids ----
const idRandBytes = 7 // 7 bytes = 56 bits = exactly 12 base32 characters

// rawStd32 is unpadded RFC 4648 base32; this build's stdlib does not expose a
// RawStdEncoding preset, so it is derived here.
var rawStd32 = base32.StdEncoding.WithPadding(base32.NoPadding)

func newID(prefix string) string {
	buf := make([]byte, idRandBytes)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failing is unrecoverable; fall back to a timestamp-based
		// id rather than panicking.
		return fmt.Sprintf("%s%d", prefix, time.Now().UnixNano())
	}
	return prefix + rawStd32.EncodeToString(buf)
}

// --- accounts ---------------------------------------------------------------

// AccountRecord is a stored account row. Token is the plaintext JWT (the
// store encrypts on write and decrypts on read); rows whose blob does not
// decrypt (e.g. the SECRET_KEY changed) are skipped with a WARN, never
// exposed.
type AccountRecord struct {
	ID             string
	Name           string
	Token          []byte
	TokenHint      string
	Fingerprint    string
	DeviceMid      string
	PlanExpiresAt  time.Time
	ProxyID        string
	Status         string
	StatusReason   string
	CooldownUntil  time.Time
	RemainingQuota *int64 // nil = never checked
	TotalQuota     *int64
	QuotaCheckedAt time.Time
	LastClaimAt    time.Time
	LastClaimKind  string
	RequestsTotal  int64
	FailuresTotal  int64
	InputTokens    int64
	OutputTokens   int64
	LastUsedAt     time.Time
	CreatedAt      time.Time
}

// AddAccount inserts one account and returns its id. The device_mid UUID v4
// is generated here, exactly once at import. If an account
// with the same fingerprint already exists it returns errDuplicateAccount,
// which the pool turns into the duplicate counter of the import API.
func (s *Store) AddAccount(name string, token []byte, proxyID string, now time.Time) (string, error) {
	if len(token) == 0 {
		return "", errors.New("pool: AddAccount: empty token")
	}
	hint := hintOf(token)
	fingerprint := fingerprintOf(token)
	for attempt := 0; attempt < 3; attempt++ {
		id := newID("acc_")
		blob := token
		if s.secretKey != "" {
			sealed, err := s.sealToken(id, token)
			if err != nil {
				return "", fmt.Errorf("pool: encrypt token: %w", err)
			}
			blob = sealed
		}
		// proxy_id must be NULL, not '', when unset — it references
		// proxies(id) and an empty string would violate the foreign key.
		var proxyIDCol any
		if proxyID != "" {
			proxyIDCol = proxyID
		}
		_, err := s.db.Exec(`INSERT INTO accounts
			(id, name, token, token_hint, fingerprint, device_mid, proxy_id, status, status_reason, cooldown_until, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', '', 0, ?)`,
			id, name, blob, hint, fingerprint, newDeviceMID(), proxyIDCol, now.Unix())
		if err == nil {
			return id, nil
		}
		if isUniqueViolation(err, "accounts.fingerprint") {
			return "", fmt.Errorf("%w (hint %s)", errDuplicateAccount, hint)
		}
		// Only an (astronomically unlikely) id collision is retried; anything
		// else is a real failure.
		if attempt == 2 || !isUniqueViolation(err, "accounts.id") {
			return "", fmt.Errorf("pool: insert account: %w", err)
		}
	}
	return "", errors.New("pool: insert account: retries exhausted")
}

// ListAccounts returns every stored account ordered by creation. Accounts
// whose token cannot be decrypted (SECRET_KEY changed since they were stored)
// are skipped with a WARN log that does not include the token or the raw
// blob.
func (s *Store) ListAccounts() ([]AccountRecord, error) {
	rows, err := s.db.Query(`SELECT id, name, token, token_hint, fingerprint, device_mid, plan_expires_at,
		COALESCE(proxy_id, ''), status, status_reason, cooldown_until,
		remaining_quota, total_quota, quota_checked_at, last_claim_at, last_claim_kind,
		requests_total, failures_total, input_tokens, output_tokens, last_used_at, created_at
		FROM accounts ORDER BY created_at, id`)
	if err != nil {
		return nil, fmt.Errorf("pool: list accounts: %w", err)
	}
	defer rows.Close()

	var out []AccountRecord
	for rows.Next() {
		var r AccountRecord
		var blob []byte
		var planExpires, cooldown, quotaChecked, lastClaim, lastUsed, created int64
		var remaining, total sql.NullInt64
		if err := rows.Scan(&r.ID, &r.Name, &blob, &r.TokenHint, &r.Fingerprint, &r.DeviceMid, &planExpires,
			&r.ProxyID, &r.Status, &r.StatusReason, &cooldown,
			&remaining, &total, &quotaChecked, &lastClaim, &r.LastClaimKind,
			&r.RequestsTotal, &r.FailuresTotal, &r.InputTokens, &r.OutputTokens, &lastUsed, &created); err != nil {
			return nil, fmt.Errorf("pool: scan account: %w", err)
		}
		r.PlanExpiresAt = unixToTime(planExpires)
		r.CooldownUntil = unixToTime(cooldown)
		if remaining.Valid {
			v := remaining.Int64
			r.RemainingQuota = &v
		}
		if total.Valid {
			v := total.Int64
			r.TotalQuota = &v
		}
		r.QuotaCheckedAt = unixToTime(quotaChecked)
		r.LastClaimAt = unixToTime(lastClaim)
		r.LastUsedAt = unixToTime(lastUsed)
		r.CreatedAt = unixToTime(created)
		token := blob
		if s.secretKey != "" {
			plain, err := s.openToken(r.ID, blob)
			if err != nil {
				slog.Warn("pool: stored account token cannot be decrypted (SECRET_KEY changed?); skipping account",
					"account_id", r.ID)
				continue
			}
			token = plain
		}
		r.Token = token
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpdateAccountState persists a status transition: status, status_reason and
// cooldown_until. Called only by Pool.transitionLocked.
func (s *Store) UpdateAccountState(id, status, reason string, cooldownUntil time.Time) error {
	until := int64(0)
	if !cooldownUntil.IsZero() {
		until = cooldownUntil.Unix()
	}
	_, err := s.db.Exec(`UPDATE accounts SET status = ?, status_reason = ?, cooldown_until = ? WHERE id = ?`,
		status, reason, until, id)
	if err != nil {
		return fmt.Errorf("pool: update account state %s: %w", id, err)
	}
	return nil
}

// UpdateAccountMeta renames an account or changes its dedicated egress proxy.
// proxyID = "" clears the assignment (stored as NULL, same effect as deleting
// the proxy).
func (s *Store) UpdateAccountMeta(id, name, proxyID string) error {
	var proxyIDCol any
	if proxyID != "" {
		proxyIDCol = proxyID
	}
	_, err := s.db.Exec(`UPDATE accounts SET name = ?, proxy_id = ? WHERE id = ?`, name, proxyIDCol, id)
	if err != nil {
		return fmt.Errorf("pool: update account meta %s: %w", id, err)
	}
	return nil
}

// UpdateAccountQuota persists the balance snapshot of /billing/balance:
// remaining_quota, total_quota, plan_expires_at. Nil
// quotas are stored as SQL NULL (= never checked); a zero planExpiresAt keeps
// the stored value.
func (s *Store) UpdateAccountQuota(id string, remaining, total *int64, planExpiresAt, checkedAt time.Time) error {
	var remainingCol, totalCol any
	if remaining != nil {
		remainingCol = *remaining
	}
	if total != nil {
		totalCol = *total
	}
	var expires int64
	if !planExpiresAt.IsZero() {
		expires = planExpiresAt.Unix()
	}
	_, err := s.db.Exec(`UPDATE accounts SET
		remaining_quota = ?, total_quota = ?, quota_checked_at = ?,
		plan_expires_at = CASE WHEN ? = 0 THEN plan_expires_at ELSE ? END
		WHERE id = ?`,
		remainingCol, totalCol, checkedAt.Unix(), expires, expires, id)
	if err != nil {
		return fmt.Errorf("pool: update account quota %s: %w", id, err)
	}
	return nil
}

// UpdateAccountClaim persists the result of a claim attempt.
func (s *Store) UpdateAccountClaim(id string, at time.Time, kind string) error {
	atSec := int64(0)
	if !at.IsZero() {
		atSec = at.Unix()
	}
	_, err := s.db.Exec(`UPDATE accounts SET last_claim_at = ?, last_claim_kind = ? WHERE id = ?`,
		atSec, kind, id)
	if err != nil {
		return fmt.Errorf("pool: update account claim %s: %w", id, err)
	}
	return nil
}

// FlushAccountCounters writes the RAM counters of one account.
func (s *Store) FlushAccountCounters(id string, requests, failures, inTokens, outTokens int64, lastUsed time.Time) error {
	_, err := s.db.Exec(`UPDATE accounts SET
		requests_total = ?, failures_total = ?, input_tokens = ?, output_tokens = ?, last_used_at = ?
		WHERE id = ?`,
		requests, failures, inTokens, outTokens, lastUsed.Unix(), id)
	if err != nil {
		return fmt.Errorf("pool: flush account counters %s: %w", id, err)
	}
	return nil
}

// DeleteAccount removes the account row.
func (s *Store) DeleteAccount(id string) error {
	res, err := s.db.Exec(`DELETE FROM accounts WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("pool: delete account %s: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("%w: %s", ErrAccountNotFound, id)
	}
	return nil
}

// --- proxies ----------------------------------------------------------------

// ProxyRecord is a stored egress proxy row.
type ProxyRecord struct {
	ID            string
	Name          string
	URL           string
	LatencyMS     int64 // -1 = never tested
	Status        string
	LastCheckedAt time.Time
	CreatedAt     time.Time
}

// AddProxy inserts one egress proxy. Static network configuration only —
// the gateway never rotates proxies when upstream rejects an account.
func (s *Store) AddProxy(name, url string, now time.Time) (string, error) {
	if url == "" {
		return "", errors.New("pool: AddProxy: empty url")
	}
	for attempt := 0; attempt < 3; attempt++ {
		id := newID("px_")
		_, err := s.db.Exec(`INSERT INTO proxies (id, name, url, created_at) VALUES (?, ?, ?, ?)`,
			id, name, url, now.Unix())
		if err == nil {
			return id, nil
		}
		if isUniqueViolation(err, "proxies.url") {
			return "", fmt.Errorf("%w (%s)", errDuplicateProxy, maskProxyURL(url))
		}
		if attempt == 2 || !isUniqueViolation(err, "proxies.id") {
			return "", fmt.Errorf("pool: insert proxy: %w", err)
		}
	}
	return "", errors.New("pool: insert proxy: retries exhausted")
}

func (s *Store) ListProxies() ([]ProxyRecord, error) {
	rows, err := s.db.Query(`SELECT id, name, url, latency_ms, status, last_checked_at, created_at
		FROM proxies ORDER BY created_at, id`)
	if err != nil {
		return nil, fmt.Errorf("pool: list proxies: %w", err)
	}
	defer rows.Close()

	var out []ProxyRecord
	for rows.Next() {
		var r ProxyRecord
		var lastChecked, created int64
		if err := rows.Scan(&r.ID, &r.Name, &r.URL, &r.LatencyMS, &r.Status, &lastChecked, &created); err != nil {
			return nil, fmt.Errorf("pool: scan proxy: %w", err)
		}
		r.LastCheckedAt = unixToTime(lastChecked)
		r.CreatedAt = unixToTime(created)
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpdateProxyCheck stores the result of a latency test.
func (s *Store) UpdateProxyCheck(id string, latencyMS int64, status string, now time.Time) error {
	_, err := s.db.Exec(`UPDATE proxies SET latency_ms = ?, status = ?, last_checked_at = ? WHERE id = ?`,
		latencyMS, status, now.Unix(), id)
	if err != nil {
		return fmt.Errorf("pool: update proxy check %s: %w", id, err)
	}
	return nil
}

// DeleteProxy removes the proxy; accounts assigned to it are unassigned via
// the FK ON DELETE SET NULL clause.
func (s *Store) DeleteProxy(id string) error {
	res, err := s.db.Exec(`DELETE FROM proxies WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("pool: delete proxy %s: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("pool: no proxy with id %s", id)
	}
	return nil
}

// --- model mappings ---------------------------------------------------------

// MappingRecord is one stored mapping rule.
type MappingRecord struct {
	ID        string
	Pattern   string
	Target    string
	Mode      string
	CreatedAt time.Time
}

func (s *Store) ListMappings() ([]MappingRecord, error) {
	rows, err := s.db.Query(`SELECT id, pattern, target, mode, created_at
		FROM model_mappings ORDER BY pattern`)
	if err != nil {
		return nil, fmt.Errorf("pool: list mappings: %w", err)
	}
	defer rows.Close()

	var out []MappingRecord
	for rows.Next() {
		var r MappingRecord
		var created int64
		if err := rows.Scan(&r.ID, &r.Pattern, &r.Target, &r.Mode, &created); err != nil {
			return nil, fmt.Errorf("pool: scan mapping: %w", err)
		}
		r.CreatedAt = unixToTime(created)
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpsertMapping inserts or updates a rule identified by its UNIQUE pattern.
func (s *Store) UpsertMapping(pattern, target, mode string, now time.Time) (string, error) {
	_, err := s.db.Exec(`INSERT INTO model_mappings (id, pattern, target, mode, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(pattern) DO UPDATE SET target = excluded.target, mode = excluded.mode`,
		newID("map_"), pattern, target, mode, now.Unix())
	if err != nil {
		return "", fmt.Errorf("pool: upsert mapping %q: %w", pattern, err)
	}
	var id string
	if err := s.db.QueryRow(`SELECT id FROM model_mappings WHERE pattern = ?`, pattern).Scan(&id); err != nil {
		return "", fmt.Errorf("pool: read mapping id %q: %w", pattern, err)
	}
	return id, nil
}

func (s *Store) DeleteMapping(id string) error {
	res, err := s.db.Exec(`DELETE FROM model_mappings WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("pool: delete mapping %s: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("pool: no mapping with id %s", id)
	}
	return nil
}

// ReplaceMappings atomically swaps the whole rule set (used by
// /api/mappings/reset-default).
func (s *Store) ReplaceMappings(recs []MappingRecord, now time.Time) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("pool: begin replace mappings: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM model_mappings`); err != nil {
		return fmt.Errorf("pool: replace mappings: %w", err)
	}
	for _, r := range recs {
		if _, err := tx.Exec(`INSERT INTO model_mappings (id, pattern, target, mode, created_at)
			VALUES (?, ?, ?, ?, ?)`, newID("map_"), r.Pattern, r.Target, r.Mode, now.Unix()); err != nil {
			return fmt.Errorf("pool: replace mappings: %w", err)
		}
	}
	return tx.Commit()
}

// --- settings ---------------------------------------------------------------

// Vacuum compacts the database file and reports its size before and after
// (POST /api/settings/vacuum).
func (s *Store) Vacuum() (before, after int64, err error) {
	before, err = s.dbSize()
	if err != nil {
		return 0, 0, err
	}
	if _, err = s.db.Exec(`VACUUM`); err != nil {
		return before, 0, fmt.Errorf("pool: vacuum: %w", err)
	}
	after, err = s.dbSize()
	if err != nil {
		return before, 0, err
	}
	return before, after, nil
}

// dbSize returns the current database file size (page_count * page_size).
func (s *Store) dbSize() (int64, error) {
	var pages, pageSize int64
	if err := s.db.QueryRow(`PRAGMA page_count`).Scan(&pages); err != nil {
		return 0, fmt.Errorf("pool: read page_count: %w", err)
	}
	if err := s.db.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		return 0, fmt.Errorf("pool: read page_size: %w", err)
	}
	return pages * pageSize, nil
}

// GetSetting returns the stored value of a runtime setting.
func (s *Store) GetSetting(key string) (string, bool, error) {
	var value string
	err := s.db.QueryRow(`SELECT value FROM settings WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("pool: get setting %q: %w", key, err)
	}
	return value, true, nil
}

func (s *Store) SetSetting(key, value string, now time.Time) error {
	_, err := s.db.Exec(`INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
		key, value, now.Unix())
	if err != nil {
		return fmt.Errorf("pool: set setting %q: %w", key, err)
	}
	return nil
}

// SetSettings upserts every key of the map inside ONE transaction: a store
// failure between keys rolls the whole batch back instead of leaving a
// partially applied settings PUT behind.
func (s *Store) SetSettings(values map[string]string, now time.Time) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("pool: begin settings batch: %w", err)
	}
	defer tx.Rollback()
	for key, value := range values {
		if _, err := tx.Exec(`INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
			key, value, now.Unix()); err != nil {
			return fmt.Errorf("pool: set setting %q: %w", key, err)
		}
	}
	return tx.Commit()
}

// AllSettings returns every stored setting.
func (s *Store) AllSettings() (map[string]string, error) {
	rows, err := s.db.Query(`SELECT key, value FROM settings`)
	if err != nil {
		return nil, fmt.Errorf("pool: list settings: %w", err)
	}
	defer rows.Close()

	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("pool: scan setting: %w", err)
		}
		out[k] = v
	}
	return out, rows.Err()
}

// --- helpers ----------------------------------------------------------------
func unixToTime(sec int64) time.Time {
	if sec <= 0 {
		return time.Time{}
	}
	return time.Unix(sec, 0)
}

// isUniqueViolation reports whether err is a UNIQUE constraint failure on the
// named constraint (table.column), as produced by modernc.org/sqlite
// ("constraint failed: UNIQUE constraint failed: accounts.fingerprint (2067)").
func isUniqueViolation(err error, constraint string) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE") && strings.Contains(msg, constraint)
}

// maskProxyURL hides credentials in a proxy URL for logs and duplicate-error
// messages.
func maskProxyURL(raw string) string {
	scheme, rest, ok := strings.Cut(raw, "://")
	if !ok {
		return "(proxy)"
	}
	if i := strings.LastIndex(rest, "@"); i >= 0 {
		return scheme + "://â?▌@" + rest[i+1:]
	}
	return scheme + "://" + rest
}

// --- per-(account, model) quota exhaustion -----------------------------------

// MarkExhaustedModel records that model is exhausted on accountID until the
// given instant (upsert — the newest observation wins).
func (s *Store) MarkExhaustedModel(accountID, model string, until time.Time) error {
	const q = `INSERT INTO exhausted_models (account_id, model, until) VALUES (?, ?, ?)
		ON CONFLICT (account_id, model) DO UPDATE SET until = excluded.until`
	if _, err := s.db.Exec(q, accountID, model, until.Unix()); err != nil {
		return fmt.Errorf("pool: mark exhausted model: %w", err)
	}
	return nil
}

// ClearExhaustedModels drops every per-model exhaustion of one account.
func (s *Store) ClearExhaustedModels(accountID string) error {
	if _, err := s.db.Exec(`DELETE FROM exhausted_models WHERE account_id = ?`, accountID); err != nil {
		return fmt.Errorf("pool: clear exhausted models: %w", err)
	}
	return nil
}

// ListExhaustedModels returns the per-model exhaustion deadlines still in
// force: accountID -> model -> until.
func (s *Store) ListExhaustedModels(now time.Time) (map[string]map[string]time.Time, error) {
	rows, err := s.db.Query(`SELECT account_id, model, until FROM exhausted_models WHERE until > ?`, now.Unix())
	if err != nil {
		return nil, fmt.Errorf("pool: list exhausted models: %w", err)
	}
	defer rows.Close()
	out := make(map[string]map[string]time.Time)
	for rows.Next() {
		var accountID, model string
		var until int64
		if err := rows.Scan(&accountID, &model, &until); err != nil {
			return nil, fmt.Errorf("pool: scan exhausted model: %w", err)
		}
		if out[accountID] == nil {
			out[accountID] = make(map[string]time.Time)
		}
		out[accountID][model] = unixToTime(until)
	}
	return out, rows.Err()
}
