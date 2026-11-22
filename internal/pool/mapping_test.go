package pool

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"zcode2api/internal/config"
)

func mappingStore(t *testing.T) *Store {
	t.Helper()
	return openPlain(t, filepath.Join(t.TempDir(), "mapping.db"))
}

func newTestMapping(t *testing.T) *Mapping {
	t.Helper()
	m, err := NewMapping(mappingStore(t))
	if err != nil {
		t.Fatalf("NewMapping: %v", err)
	}
	return m
}

func defaultTestConfig() *config.Config {
	// GLM model names are configuration, never code (,
	// risk 5) — the tests use neutral placeholder names for exactly that
	// reason.
	return &config.Config{
		DefaultModelOpus:   "example-opus-model",
		DefaultModelSonnet: "example-sonnet-model",
		DefaultModelHaiku:  "example-haiku-model",
	}
}

// TestResetDefaultAndResolveSonnetDate:
// `claude-sonnet-4-5-20250929` resolves to DEFAULT_MODEL_SONNET.
func TestResetDefaultAndResolveSonnetDate(t *testing.T) {
	m := newTestMapping(t)
	cfg := defaultTestConfig()
	if err := m.ResetDefault(cfg); err != nil {
		t.Fatalf("ResetDefault: %v", err)
	}

	out, ruleID := m.Resolve("claude-sonnet-4-5-20250929")
	if out != cfg.DefaultModelSonnet || ruleID == "" {
		t.Errorf("Resolve(sonnet dated) = %q/%q, want %q + rule id", out, ruleID, cfg.DefaultModelSonnet)
	}
	out, _ = m.Resolve("claude-opus-4-6")
	if out != cfg.DefaultModelOpus {
		t.Errorf("Resolve(opus) = %q, want %q", out, cfg.DefaultModelOpus)
	}
	out, _ = m.Resolve("claude-haiku-4-5")
	if out != cfg.DefaultModelHaiku {
		t.Errorf("Resolve(haiku) = %q, want %q", out, cfg.DefaultModelHaiku)
	}
	out, _ = m.Resolve("claude-3-5-sonnet-20241022") // claude-3-* catch-all -> sonnet
	if out != cfg.DefaultModelSonnet {
		t.Errorf("Resolve(claude-3) = %q, want %q", out, cfg.DefaultModelSonnet)
	}

	// Four default rules were installed.
	if rules := m.Rules(); len(rules) != 4 {
		t.Errorf("ResetDefault installed %d rules, want 4", len(rules))
	}
}

// TestExactBeatsPrefix: an exact rule wins over any prefix rule.
func TestExactBeatsPrefix(t *testing.T) {
	m := newTestMapping(t)
	now := time.Unix(1_700_000_000, 0)
	if _, err := m.Add("claude-sonnet-*", "prefix-target", ModeMapped, now); err != nil {
		t.Fatalf("Add prefix: %v", err)
	}
	if _, err := m.Add("claude-sonnet-4-5-20250929", "exact-target", ModeMapped, now); err != nil {
		t.Fatalf("Add exact: %v", err)
	}

	if out, _ := m.Resolve("claude-sonnet-4-5-20250929"); out != "exact-target" {
		t.Errorf("exact rule must win: got %q", out)
	}
	// A different dated snapshot still hits the prefix.
	if out, _ := m.Resolve("claude-sonnet-4-5-20250620"); out != "prefix-target" {
		t.Errorf("other sonnet snapshots follow the prefix: got %q", out)
	}
}

// TestLongestPrefixWins: among matching prefix rules the longest stem wins.
func TestLongestPrefixWins(t *testing.T) {
	m := newTestMapping(t)
	now := time.Unix(1_700_000_000, 0)
	if _, err := m.Add("claude-sonnet-*", "short", ModeMapped, now); err != nil {
		t.Fatalf("Add short prefix: %v", err)
	}
	if _, err := m.Add("claude-sonnet-4-*", "long", ModeMapped, now); err != nil {
		t.Fatalf("Add long prefix: %v", err)
	}
	if out, _ := m.Resolve("claude-sonnet-4-5"); out != "long" {
		t.Errorf("longest prefix must win: got %q", out)
	}
	if out, _ := m.Resolve("claude-sonnet-3-9"); out != "short" {
		t.Errorf("non-matching long prefix falls back to the shorter one: got %q", out)
	}
}

// TestPassthroughWhitelist: PASSTHROUGH rules keep the inbound name and beat
// prefix rules for the same model (whitelist semantics).
func TestPassthroughWhitelist(t *testing.T) {
	m := newTestMapping(t)
	now := time.Unix(1_700_000_000, 0)
	if _, err := m.Add("claude-opus-*", "mapped-opus", ModeMapped, now); err != nil {
		t.Fatalf("Add prefix: %v", err)
	}
	if _, err := m.Add("claude-opus-4-1", "", ModePassthrough, now); err != nil {
		t.Fatalf("Add passthrough: %v", err)
	}
	if out, _ := m.Resolve("claude-opus-4-1"); out != "claude-opus-4-1" {
		t.Errorf("PASSTHROUGH must keep the name: got %q", out)
	}
	if out, _ := m.Resolve("claude-opus-4-6"); out != "mapped-opus" {
		t.Errorf("non-whitelisted model follows the prefix rule: got %q", out)
	}
}

// TestResolveUnknownPassthrough: a model matching no rule passes through with
// no rule id (STRICT_MODELS handling lives in the proxy layer).
func TestResolveUnknownPassthrough(t *testing.T) {
	m := newTestMapping(t)
	out, ruleID := m.Resolve("totally-unknown-model")
	if out != "totally-unknown-model" || ruleID != "" {
		t.Errorf("Resolve(unknown) = %q/%q, want passthrough with empty rule id", out, ruleID)
	}
}

func TestResetDefaultRequiresEnvModels(t *testing.T) {
	m := newTestMapping(t)
	err := m.ResetDefault(&config.Config{})
	if err == nil {
		t.Fatal("ResetDefault without DEFAULT_MODEL_* must fail")
	}
	for _, name := range []string{"DEFAULT_MODEL_OPUS", "DEFAULT_MODEL_SONNET", "DEFAULT_MODEL_HAIKU"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q must name %s", err.Error(), name)
		}
	}
	err = m.ResetDefault(nil)
	if err == nil {
		t.Fatal("ResetDefault(nil) must fail")
	}
}

func TestValidatePattern(t *testing.T) {
	valid := []string{"claude-sonnet-4-5-20250929", "claude-sonnet-*", "gpt-x", "claude-3-*"}
	for _, p := range valid {
		if err := ValidatePattern(p); err != nil {
			t.Errorf("ValidatePattern(%q) = %v, want nil", p, err)
		}
	}
	invalid := []string{"", "  ", "*", "cl*de", "abc*b", "abc**", "claude-sonnet*-4"}
	for _, p := range invalid {
		if err := ValidatePattern(p); err == nil {
			t.Errorf("ValidatePattern(%q) must fail: '*' only at the end", p)
		}
	}
}

func TestMappingAddValidation(t *testing.T) {
	m := newTestMapping(t)
	now := time.Unix(1_700_000_000, 0)
	if _, err := m.Add("claude-sonnet-*", "", ModeMapped, now); err == nil {
		t.Error("MAPPED without target must fail")
	}
	if _, err := m.Add("claude-*x", "t", ModeMapped, now); err == nil {
		t.Error("misplaced '*' must fail")
	}
	if _, err := m.Add("claude-sonnet-*", "t", "REMAP", now); err == nil {
		t.Error("unknown mode must fail")
	}

	id, err := m.Add("keep-name", "", ModePassthrough, now)
	if err != nil {
		t.Fatalf("Add passthrough: %v", err)
	}
	recs, _ := m.store.ListMappings()
	for _, r := range recs {
		if r.ID == id && r.Target != "" {
			t.Errorf("PASSTHROUGH target must be stored empty, got %q", r.Target)
		}
	}

	// Add upserts on the same pattern instead of duplicating it.
	if _, err := m.Add("keep-name", "", ModePassthrough, now); err != nil {
		t.Fatalf("second Add: %v", err)
	}
	if rules := m.Rules(); len(rules) != 1 {
		t.Errorf("Rules = %d, want 1 (pattern is UNIQUE)", len(rules))
	}
}

func TestMappingDeleteAndReload(t *testing.T) {
	m := newTestMapping(t)
	now := time.Unix(1_700_000_000, 0)
	id, err := m.Add("gone-*", "target", ModeMapped, now)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if out, _ := m.Resolve("gone-1"); out != "target" {
		t.Fatalf("pre-delete resolve = %q", out)
	}
	if err := m.Delete(id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if out, ruleID := m.Resolve("gone-1"); out != "gone-1" || ruleID != "" {
		t.Errorf("after delete the rule must be gone from the snapshot: %q/%q", out, ruleID)
	}
	if err := m.Delete("map_doesnotexist"); err == nil {
		t.Error("deleting an unknown rule must fail")
	}
}

// TestMappingRulesOrdering pins the admin-listing order: exact rules first
// (sorted by pattern), then prefix rules, longest stem first.
func TestMappingRulesOrdering(t *testing.T) {
	m := newTestMapping(t)
	now := time.Unix(1_700_000_000, 0)
	for _, r := range []struct{ p, tg string }{
		{"zzz-exact", "t1"},
		{"aaa-exact", "t2"},
		{"claude-sonnet-*", "t3"},
		{"claude-sonnet-4-*", "t4"},
	} {
		if _, err := m.Add(r.p, r.tg, ModeMapped, now); err != nil {
			t.Fatalf("Add %s: %v", r.p, err)
		}
	}
	rules := m.Rules()
	if len(rules) != 4 {
		t.Fatalf("Rules = %d, want 4", len(rules))
	}
	wantOrder := []string{"aaa-exact", "zzz-exact", "claude-sonnet-4-*", "claude-sonnet-*"}
	for i, w := range wantOrder {
		if rules[i].Pattern != w {
			t.Errorf("rules[%d].Pattern = %q, want %q", i, rules[i].Pattern, w)
		}
	}
}

// TestMappingPersistsAcrossReopen: rules live in SQLite; a new Mapping on the
// same store serves the same rules (restart keeps configuration).
func TestMappingPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mapping.db")
	s := openPlain(t, path)
	m, err := NewMapping(s)
	if err != nil {
		t.Fatalf("NewMapping: %v", err)
	}
	now := time.Unix(1_700_000_000, 0)
	if _, err := m.Add("claude-haiku-*", "example-haiku-model", ModeMapped, now); err != nil {
		t.Fatalf("Add: %v", err)
	}
	s.Close()

	s2 := openPlain(t, path)
	m2, err := NewMapping(s2)
	if err != nil {
		t.Fatalf("NewMapping after reopen: %v", err)
	}
	if out, _ := m2.Resolve("claude-haiku-4-5"); out != "example-haiku-model" {
		t.Errorf("rule lost across reopen, resolve = %q", out)
	}
}
