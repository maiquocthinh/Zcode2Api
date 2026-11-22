package pool

import (
	"bytes"
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// openPlain opens a store without token encryption (SECRET_KEY unset).
func openPlain(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path, "")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func addTestAccount(t *testing.T, s *Store, name, jwt string, now time.Time) string {
	t.Helper()
	id, err := s.AddAccount(name, []byte(jwt), "", now)
	if err != nil {
		t.Fatalf("AddAccount(%q): %v", name, err)
	}
	return id
}

// TestOpenMigrationsAndPragmas verifies a fresh database ends at the latest
// schema (version = len(migrations)): the accounts table, no legacy keys
// table, WAL enabled.
func TestOpenMigrationsAndPragmas(t *testing.T) {
	s := openPlain(t, filepath.Join(t.TempDir(), "store.db"))

	var version int
	if err := s.db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&version); err != nil {
		t.Fatalf("read schema_version: %v", err)
	}
	if version != len(migrations) {
		t.Errorf("schema_version = %d, want %d", version, len(migrations))
	}
	var journalMode string
	if err := s.db.QueryRow(`PRAGMA journal_mode`).Scan(&journalMode); err != nil || !strings.EqualFold(journalMode, "wal") {
		t.Errorf("journal_mode = %q (err %v), want wal", journalMode, err)
	}
	var fk int
	if err := s.db.QueryRow(`PRAGMA foreign_keys`).Scan(&fk); err != nil || fk != 1 {
		t.Errorf("foreign_keys = %d (err %v), want 1", fk, err)
	}
	for _, table := range []string{"accounts", "proxies", "model_mappings", "settings"} {
		rows, err := s.db.Query(`SELECT 1 FROM ` + table + ` LIMIT 1`)
		if err != nil {
			t.Errorf("table %s missing: %v", table, err)
			continue
		}
		rows.Close() // release the single connection for the next probe
	}
	if rows, err := s.db.Query(`SELECT 1 FROM keys LIMIT 1`); err == nil {
		rows.Close()
		t.Error("legacy keys table still exists after migration")
	}
}

// TestLegacyV30DatabaseMigratesToAccounts builds a database with the legacy
// schema (schema_version=1, table "keys") and reopens it with the current code:
// the legacy rows cannot become JWTs, so the table is dropped and accounts is
// created.
func TestLegacyV30DatabaseMigratesToAccounts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", filepath.ToSlash(path))
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE schema_version (version INTEGER NOT NULL);
		INSERT INTO schema_version (version) VALUES (1);
		CREATE TABLE proxies (
			id TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '', url TEXT NOT NULL UNIQUE,
			latency_ms INTEGER NOT NULL DEFAULT -1, status TEXT NOT NULL DEFAULT 'UNKNOWN',
			last_checked_at INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL
		);
		CREATE TABLE keys (
			id TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '', secret BLOB NOT NULL,
			secret_hint TEXT NOT NULL, fingerprint TEXT NOT NULL UNIQUE,
			proxy_id TEXT REFERENCES proxies(id) ON DELETE SET NULL,
			status TEXT NOT NULL DEFAULT 'ACTIVE', status_reason TEXT NOT NULL DEFAULT '',
			cooldown_until INTEGER NOT NULL DEFAULT 0,
			requests_total INTEGER NOT NULL DEFAULT 0, failures_total INTEGER NOT NULL DEFAULT 0,
			input_tokens INTEGER NOT NULL DEFAULT 0, output_tokens INTEGER NOT NULL DEFAULT 0,
			last_used_at INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL
		);
		INSERT INTO keys (id, name, secret, secret_hint, fingerprint, created_at)
			VALUES ('key_legacy', 'Old Key', x'736b2d6c6567616379', 'gacy', 'deadbeef', 1700000000);
		CREATE TABLE model_mappings (
			id TEXT PRIMARY KEY, pattern TEXT NOT NULL UNIQUE, target TEXT NOT NULL DEFAULT '',
			mode TEXT NOT NULL DEFAULT 'MAPPED', created_at INTEGER NOT NULL
		);
		CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at INTEGER NOT NULL);
	`)
	db.Close()
	if err != nil {
		t.Fatalf("seed legacy schema: %v", err)
	}

	s := openPlain(t, path)

	var version int
	if err := s.db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&version); err != nil {
		t.Fatalf("read schema_version: %v", err)
	}
	if version != len(migrations) {
		t.Errorf("schema_version = %d, want %d", version, len(migrations))
	}
	if rows, err := s.db.Query(`SELECT 1 FROM keys LIMIT 1`); err == nil {
		rows.Close()
		t.Error("legacy keys table survived the migration")
	}
	if rows, err := s.db.Query(`SELECT 1 FROM accounts LIMIT 1`); err != nil {
		t.Errorf("accounts table missing: %v", err)
	} else {
		rows.Close()
	}
	// The legacy proxy/mapping/settings rows carry over untouched.
	if err := s.SetSetting("kept", "yes", time.Unix(1_700_000_000, 0)); err != nil {
		t.Errorf("settings not usable after migration: %v", err)
	}
}

func TestAccountCRUD(t *testing.T) {
	s := openPlain(t, filepath.Join(t.TempDir(), "crud.db"))
	now := time.Unix(1_700_000_000, 0)
	jwt := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJjcnVkIn0.c2ln"

	id := addTestAccount(t, s, "CRUD Acc", jwt, now)
	if !strings.HasPrefix(id, "acc_") {
		t.Errorf("id = %q, want the acc_ prefix", id)
	}

	recs, err := s.ListAccounts()
	if err != nil || len(recs) != 1 {
		t.Fatalf("ListAccounts = %v (%d records)", err, len(recs))
	}
	r := recs[0]
	if r.ID != id || r.Name != "CRUD Acc" || string(r.Token) != jwt {
		t.Errorf("record = %+v", r)
	}
	if r.TokenHint != "c2ln" {
		t.Errorf("token_hint = %q, want c2ln", r.TokenHint)
	}
	if len(r.Fingerprint) != 64 {
		t.Errorf("fingerprint = %q", r.Fingerprint)
	}
	if !isUUIDv4(r.DeviceMid) {
		t.Errorf("device_mid = %q, want a UUID v4", r.DeviceMid)
	}
	if r.Status != "ACTIVE" || !r.LastClaimAt.IsZero() {
		t.Errorf("fresh account status/claim = %s/%v", r.Status, r.LastClaimAt)
	}
	if r.RemainingQuota != nil || r.TotalQuota != nil {
		t.Error("fresh account must have NULL quota (never checked)")
	}

	// State transition (only transitionLocked's store call shape is checked
	// here — the state machine itself is tested in pool_test.go).
	if err := s.UpdateAccountState(id, "EXHAUSTED", "quota_exhausted", now.Add(time.Hour)); err != nil {
		t.Fatalf("UpdateAccountState: %v", err)
	}
	if err := s.UpdateAccountClaim(id, now, "ok"); err != nil {
		t.Fatalf("UpdateAccountClaim: %v", err)
	}
	remaining, total := int64(300), int64(1000)
	if err := s.UpdateAccountQuota(id, &remaining, &total, now.Add(48*time.Hour), now); err != nil {
		t.Fatalf("UpdateAccountQuota: %v", err)
	}
	if err := s.FlushAccountCounters(id, 42, 3, 1234, 567, now); err != nil {
		t.Fatalf("FlushAccountCounters: %v", err)
	}

	recs, err = s.ListAccounts()
	if err != nil || len(recs) != 1 {
		t.Fatalf("ListAccounts = %v (%d records)", err, len(recs))
	}
	r = recs[0]
	if r.Status != "EXHAUSTED" || r.StatusReason != "quota_exhausted" {
		t.Errorf("state = %s (%s)", r.Status, r.StatusReason)
	}
	if r.CooldownUntil.Unix() != now.Add(time.Hour).Unix() {
		t.Errorf("cooldown_until = %v", r.CooldownUntil)
	}
	if r.LastClaimKind != "ok" || r.LastClaimAt.Unix() != now.Unix() {
		t.Errorf("claim = %q @ %v", r.LastClaimKind, r.LastClaimAt)
	}
	if r.RemainingQuota == nil || *r.RemainingQuota != 300 || r.TotalQuota == nil || *r.TotalQuota != 1000 {
		t.Errorf("quota = %v/%v, want 300/1000", r.RemainingQuota, r.TotalQuota)
	}
	if r.PlanExpiresAt.Unix() != now.Add(48*time.Hour).Unix() {
		t.Errorf("plan_expires_at = %v", r.PlanExpiresAt)
	}
	if r.RequestsTotal != 42 || r.FailuresTotal != 3 || r.InputTokens != 1234 || r.OutputTokens != 567 {
		t.Errorf("counters = %+v", r)
	}

	// Rename and delete.
	if err := s.UpdateAccountMeta(id, "Renamed", ""); err != nil {
		t.Fatalf("UpdateAccountMeta: %v", err)
	}
	if err := s.DeleteAccount(id); err != nil {
		t.Fatalf("DeleteAccount: %v", err)
	}
	if err := s.DeleteAccount(id); err == nil || !strings.Contains(err.Error(), ErrAccountNotFound.Error()) {
		t.Errorf("double delete err = %v, want ErrAccountNotFound", err)
	}
	if recs, _ := s.ListAccounts(); len(recs) != 0 {
		t.Errorf("ListAccounts after delete = %d records", len(recs))
	}
}

// isUUIDv4 checks the UUID v4 shape without importing a dependency.
func isUUIDv4(s string) bool {
	if len(s) != 36 {
		return false
	}
	dashes := [4]int{8, 13, 18, 23}
	for _, d := range dashes {
		if s[d] != '-' {
			return false
		}
	}
	hexOnly := s[:8] + s[9:13] + s[14:18] + s[19:23] + s[24:]
	for i := 0; i < len(hexOnly); i++ {
		c := hexOnly[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return s[14] == '4' && (s[19] == '8' || s[19] == '9' || s[19] == 'a' || s[19] == 'b')
}

func TestAddAccountRejectsEmptyToken(t *testing.T) {
	s := openPlain(t, filepath.Join(t.TempDir(), "empty.db"))
	if _, err := s.AddAccount("empty", nil, "", time.Unix(1_700_000_000, 0)); err == nil {
		t.Error("AddAccount with an empty token must fail")
	}
}

func TestAddAccountDuplicateFingerprint(t *testing.T) {
	s := openPlain(t, filepath.Join(t.TempDir(), "dup.db"))
	jwt := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkdXAifn0.c2ln"
	addTestAccount(t, s, "First", jwt, time.Unix(1_700_000_000, 0))
	_, err := s.AddAccount("Second", []byte(jwt), "", time.Unix(1_700_000_000, 0))
	if err == nil || !strings.Contains(err.Error(), errDuplicateAccount.Error()) {
		t.Fatalf("duplicate import err = %v, want errDuplicateAccount", err)
	}
	if recs, _ := s.ListAccounts(); len(recs) != 1 {
		t.Errorf("duplicate was stored anyway: %d records", len(recs))
	}
}

// TestPlaintextStorageWithoutSecretKey pins the fallback:
// without SECRET_KEY the token column holds the raw JWT bytes.
func TestPlaintextStorageWithoutSecretKey(t *testing.T) {
	s := openPlain(t, filepath.Join(t.TempDir(), "plain.db"))
	jwt := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwbGFpbiJ9.c2ln"
	id := addTestAccount(t, s, "Plain", jwt, time.Unix(1_700_000_000, 0))

	var blob []byte
	if err := s.db.QueryRow(`SELECT token FROM accounts WHERE id = ?`, id).Scan(&blob); err != nil {
		t.Fatalf("read token blob: %v", err)
	}
	if !bytes.Equal(blob, []byte(jwt)) {
		t.Errorf("stored blob = %q, want the plaintext JWT", blob)
	}
}

// TestEncryptedStorageWithSecretKey pins the rule: with
// SECRET_KEY the token column must not contain the JWT, and per-record HKDF
// derivation must yield
// different ciphertexts for different record ids even for the same plaintext.
// The plaintext is recoverable only through the store.
func TestEncryptedStorageWithSecretKey(t *testing.T) {
	secret := "z2a-test-secret-key-0123456789abcdef"
	s, err := Open(filepath.Join(t.TempDir(), "enc.db"), secret)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	now := time.Unix(1_700_000_000, 0)
	jwtA := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJlbmMtYSJ9.c2lnLWE"
	jwtB := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJlbmMtYiJ9.c2lnLWI"

	id1 := addTestAccount(t, s, "Enc A", jwtA, now)
	id2 := addTestAccount(t, s, "Enc B", jwtB, now.Add(time.Minute))

	var blob1, blob2 []byte
	if err := s.db.QueryRow(`SELECT token FROM accounts WHERE id = ?`, id1).Scan(&blob1); err != nil {
		t.Fatalf("read token blob: %v", err)
	}
	if err := s.db.QueryRow(`SELECT token FROM accounts WHERE id = ?`, id2).Scan(&blob2); err != nil {
		t.Fatalf("read token blob: %v", err)
	}
	if bytes.Contains(blob1, []byte(jwtA)) || bytes.Contains(blob2, []byte(jwtB)) {
		t.Error("the raw JWT is readable in the stored blob")
	}

	recs, err := s.ListAccounts()
	if err != nil || len(recs) != 2 {
		t.Fatalf("ListAccounts = %v (%d records)", err, len(recs))
	}
	if string(recs[0].Token) != jwtA || string(recs[1].Token) != jwtB {
		t.Errorf("decrypted tokens = %q/%q, want the original JWTs", recs[0].Token, recs[1].Token)
	}

	// Per-record HKDF: the same plaintext sealed under two record ids produces
	// different ciphertexts, and a blob only opens under its own id.
	plain := []byte(jwtA)
	b1, err := s.sealToken(id1, plain)
	if err != nil {
		t.Fatalf("sealToken: %v", err)
	}
	b2, err := s.sealToken(id2, plain)
	if err != nil {
		t.Fatalf("sealToken: %v", err)
	}
	if bytes.Equal(b1, b2) {
		t.Error("per-record HKDF must derive a different key per record id")
	}
	got, err := s.openToken(id1, b1)
	if err != nil || !bytes.Equal(got, plain) {
		t.Errorf("openToken(id1) = %q (err %v), want the plaintext", got, err)
	}
	if _, err := s.openToken(id2, b1); err == nil {
		t.Error("a blob sealed for one record opened under another record id")
	}
}

// TestWrongSecretKeySkipsAccount: a SECRET_KEY change makes old blobs
// undecryptable — the account is skipped with a WARN, never exposed.
func TestWrongSecretKeySkipsAccount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wrongkey.db")
	s, err := Open(path, "z2a-first-secret-key-0123456789a")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	addTestAccount(t, s, "A", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ3cm9uZyJ9.c2ln", time.Unix(1_700_000_000, 0))
	s.Close()

	s2, err := Open(path, "z2a-second-secret-key-012345678")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	recs, err := s2.ListAccounts()
	if err != nil {
		t.Fatalf("ListAccounts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("got %d accounts after a SECRET_KEY change, want 0 (undecryptable rows skipped)", len(recs))
	}
}

func TestProxyCRUDAndForeignKeySetNull(t *testing.T) {
	s := openPlain(t, filepath.Join(t.TempDir(), "proxies.db"))
	now := time.Unix(1_700_000_000, 0)

	pxID, err := s.AddProxy("EU", "http://user:secret@proxy.example:3128", now)
	if err != nil {
		t.Fatalf("AddProxy: %v", err)
	}
	if _, err := s.AddProxy("dup", "http://user:secret@proxy.example:3128", now); err == nil ||
		!strings.Contains(err.Error(), errDuplicateProxy.Error()) {
		t.Errorf("duplicate proxy err = %v", err)
	}

	accID := addTestAccount(t, s, "Bound", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJweCJ9.c2ln", now)
	if err := s.UpdateAccountMeta(accID, "Bound", pxID); err != nil {
		t.Fatalf("assign proxy: %v", err)
	}
	if err := s.UpdateProxyCheck(pxID, 42, "HEALTHY", now); err != nil {
		t.Fatalf("UpdateProxyCheck: %v", err)
	}

	proxies, err := s.ListProxies()
	if err != nil || len(proxies) != 1 {
		t.Fatalf("ListProxies = %v (%d)", err, len(proxies))
	}
	if proxies[0].LatencyMS != 42 || proxies[0].Status != "HEALTHY" {
		t.Errorf("proxy = %+v", proxies[0])
	}

	// Deleting the proxy unassigns the account (FK ON DELETE SET NULL).
	if err := s.DeleteProxy(pxID); err != nil {
		t.Fatalf("DeleteProxy: %v", err)
	}
	recs, _ := s.ListAccounts()
	if len(recs) != 1 || recs[0].ProxyID != "" {
		t.Errorf("proxy_id after proxy delete = %q, want empty (SET NULL)", recs[0].ProxyID)
	}
}

func TestMappingCRUD(t *testing.T) {
	s := openPlain(t, filepath.Join(t.TempDir(), "mappings.db"))
	now := time.Unix(1_700_000_000, 0)

	id, err := s.UpsertMapping("claude-sonnet-*", "target-model", "MAPPED", now)
	if err != nil {
		t.Fatalf("UpsertMapping: %v", err)
	}
	id2, err := s.UpsertMapping("claude-sonnet-*", "other-model", "MAPPED", now)
	if err != nil || id2 != id {
		t.Errorf("upsert on the same pattern = (%q, %v), want the same id", id2, err)
	}
	recs, _ := s.ListMappings()
	if len(recs) != 1 || recs[0].Target != "other-model" {
		t.Errorf("ListMappings = %+v, want the upserted target", recs)
	}
	if err := s.DeleteMapping(id); err != nil {
		t.Fatalf("DeleteMapping: %v", err)
	}

	recs = []MappingRecord{
		{Pattern: "claude-opus-*", Target: "opus-target", Mode: "MAPPED"},
		{Pattern: "claude-haiku-*", Target: "haiku-target", Mode: "MAPPED"},
	}
	if err := s.ReplaceMappings(recs, now); err != nil {
		t.Fatalf("ReplaceMappings: %v", err)
	}
	if got, _ := s.ListMappings(); len(got) != 2 {
		t.Errorf("after replace: %d rules, want 2", len(got))
	}
}

func TestSettingsCRUD(t *testing.T) {
	s := openPlain(t, filepath.Join(t.TempDir(), "settings.db"))
	now := time.Unix(1_700_000_000, 0)

	if _, ok, _ := s.GetSetting("missing"); ok {
		t.Error("GetSetting on a missing key must report absent")
	}
	if err := s.SetSetting("fallback_api_key", "sk-secret", now); err != nil {
		t.Fatalf("SetSetting: %v", err)
	}
	if err := s.SetSetting("fallback_api_key", "sk-rotated", now.Add(time.Minute)); err != nil {
		t.Fatalf("SetSetting (update): %v", err)
	}
	if v, ok, _ := s.GetSetting("fallback_api_key"); !ok || v != "sk-rotated" {
		t.Errorf("GetSetting = (%q, %v)", v, ok)
	}
	all, _ := s.AllSettings()
	if all["fallback_api_key"] != "sk-rotated" {
		t.Errorf("AllSettings = %v", all)
	}
}

func TestStoreWALPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.db")
	now := time.Unix(1_700_000_000, 0)
	jwt := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ3YWwifn0.c2ln"

	s := openPlain(t, path)
	id := addTestAccount(t, s, "WAL", jwt, now)
	if err := s.UpdateAccountState(id, "COOLING", "rate_limit", now.Add(time.Minute)); err != nil {
		t.Fatalf("UpdateAccountState: %v", err)
	}
	s.Close()

	s2, err := Open(path, "")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	recs, err := s2.ListAccounts()
	if err != nil || len(recs) != 1 {
		t.Fatalf("ListAccounts after reopen = %v (%d)", err, len(recs))
	}
	if recs[0].Status != "COOLING" || recs[0].StatusReason != "rate_limit" {
		t.Errorf("state after reopen = %s (%s), want COOLING (rate_limit)", recs[0].Status, recs[0].StatusReason)
	}
}

// TestConcurrentStoreWrites hammers the single-writer store from many
// goroutines: modernc.org/sqlite plus WAL must never surface SQLITE_BUSY.
func TestConcurrentStoreWrites(t *testing.T) {
	s := openPlain(t, filepath.Join(t.TempDir(), "concurrent.db"))
	now := time.Unix(1_700_000_000, 0)

	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := s.AddAccount("concurrent", []byte("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJjYyJ9.c2ln"), "", now); err != nil {
				if !strings.Contains(err.Error(), errDuplicateAccount.Error()) {
					errs <- err
					return
				}
			}
			if err := s.SetSetting("k", "v", now); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent store write failed: %v", err)
	}
}
