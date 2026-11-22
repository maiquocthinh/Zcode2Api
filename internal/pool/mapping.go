package pool

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"zcode2api/internal/config"
)

// Mapping rule modes.
const (
	ModeMapped      = "MAPPED"      // rename model to target
	ModePassthrough = "PASSTHROUGH" // keep the name — used to whitelist models
)

// MappingRule is one resolved rule: pattern "claude-sonnet-4-5-20250929"
// (exact) or "claude-sonnet-*" (prefix, "*" only at the end). stem is the
// pattern without the trailing "*" and is meaningful only for prefix rules.
type MappingRule struct {
	ID        string
	Pattern   string
	Target    string
	Mode      string
	CreatedAt time.Time

	stem string
}

// mappingSnapshot is an immutable view of the rules. The engine keeps one
// under an atomic.Pointer so a request never queries SQLite and a rule update
// never locks request handling.
type mappingSnapshot struct {
	exact    map[string]MappingRule
	prefixes []MappingRule // ordered by stem length, longest first
}

// Mapping is the model-name engine: inbound Claude-style name → outbound
// model name, in the order exact > longest prefix > passthrough.
// Model names come from configuration/DB only — no
// upstream model name is ever hardcoded.
type Mapping struct {
	store *Store
	snap  atomic.Pointer[mappingSnapshot]
}

// NewMapping loads the rules from the store and serves them from RAM.
func NewMapping(store *Store) (*Mapping, error) {
	m := &Mapping{store: store}
	if err := m.Reload(); err != nil {
		return nil, err
	}
	return m, nil
}

// Reload re-reads the rules from the store and swaps the snapshot atomically.
func (m *Mapping) Reload() error {
	recs, err := m.store.ListMappings()
	if err != nil {
		return err
	}
	m.snap.Store(buildSnapshot(recs))
	return nil
}

func buildSnapshot(recs []MappingRecord) *mappingSnapshot {
	snap := &mappingSnapshot{exact: make(map[string]MappingRule, len(recs))}
	for _, r := range recs {
		rule := MappingRule{ID: r.ID, Pattern: r.Pattern, Target: r.Target, Mode: r.Mode, CreatedAt: r.CreatedAt}
		if isPrefixPattern(rule.Pattern) {
			rule.stem = rule.Pattern[:len(rule.Pattern)-1]
			snap.prefixes = append(snap.prefixes, rule)
			continue
		}
		snap.exact[rule.Pattern] = rule
	}
	// Longest prefix first: two prefix rules matching the same model resolve
	// to the most specific one.
	sort.SliceStable(snap.prefixes, func(i, j int) bool {
		return len(snap.prefixes[i].stem) > len(snap.prefixes[j].stem)
	})
	return snap
}

// Resolve maps an inbound model name to the outbound name and returns the id
// of the rule applied ("" for plain passthrough
// /api/mappings/resolve). Order: exact match, longest matching prefix, then
// passthrough of the input unchanged.
func (m *Mapping) Resolve(model string) (output string, ruleID string) {
	snap := m.snap.Load()
	if snap == nil {
		return model, ""
	}
	if rule, ok := snap.exact[model]; ok {
		return ruleOutput(rule, model), rule.ID
	}
	for _, rule := range snap.prefixes {
		if strings.HasPrefix(model, rule.stem) {
			return ruleOutput(rule, model), rule.ID
		}
	}
	return model, "" // passthrough: no rule matched
}

func ruleOutput(rule MappingRule, inbound string) string {
	if rule.Mode == ModeMapped && rule.Target != "" {
		return rule.Target
	}
	return inbound // PASSTHROUGH keeps the name (whitelist)
}

// Rules returns a copy of the current snapshot for the admin API, exact rules
// first (sorted by pattern), then prefix rules (longest stem first).
func (m *Mapping) Rules() []MappingRule {
	snap := m.snap.Load()
	if snap == nil {
		return nil
	}
	exacts := make([]MappingRule, 0, len(snap.exact))
	for _, r := range snap.exact {
		exacts = append(exacts, r)
	}
	sort.SliceStable(exacts, func(i, j int) bool { return exacts[i].Pattern < exacts[j].Pattern })
	out := make([]MappingRule, 0, len(exacts)+len(snap.prefixes))
	out = append(out, exacts...)
	out = append(out, snap.prefixes...)
	return out
}

// Add validates and upserts one rule, then swaps the snapshot. Pattern
// validation enforces that "*" appears only as the last character
// — the admin API reuses this check.
func (m *Mapping) Add(pattern, target, mode string, now time.Time) (string, error) {
	pattern, target, mode, err := normalizeRule(pattern, target, mode)
	if err != nil {
		return "", err
	}
	id, err := m.store.UpsertMapping(pattern, target, mode, now)
	if err != nil {
		return "", err
	}
	if err := m.Reload(); err != nil {
		return "", err
	}
	return id, nil
}

// Delete removes one rule by id and swaps the snapshot.
func (m *Mapping) Delete(id string) error {
	if err := m.store.DeleteMapping(id); err != nil {
		return err
	}
	return m.Reload()
}

// ResetDefault wipes the rule set and installs the three Claude Code tiers
// plus the claude-3 catch-all, with targets taken from the DEFAULT_MODEL_*
// environment variables. GLM model names are never
// hardcoded here; empty variables make ResetDefault fail.
func (m *Mapping) ResetDefault(cfg *config.Config) error {
	if cfg == nil {
		return errors.New("pool: ResetDefault: nil config")
	}
	opus, sonnet, haiku := cfg.DefaultModelOpus, cfg.DefaultModelSonnet, cfg.DefaultModelHaiku
	var missing []string
	if opus == "" {
		missing = append(missing, "DEFAULT_MODEL_OPUS")
	}
	if sonnet == "" {
		missing = append(missing, "DEFAULT_MODEL_SONNET")
	}
	if haiku == "" {
		missing = append(missing, "DEFAULT_MODEL_HAIKU")
	}
	if len(missing) > 0 {
		return fmt.Errorf("pool: reset default mapping requires %s to be set", strings.Join(missing, ", "))
	}
	recs := []MappingRecord{
		{Pattern: "claude-opus-*", Target: opus, Mode: ModeMapped},
		{Pattern: "claude-sonnet-*", Target: sonnet, Mode: ModeMapped},
		{Pattern: "claude-haiku-*", Target: haiku, Mode: ModeMapped},
		{Pattern: "claude-3-*", Target: sonnet, Mode: ModeMapped},
	}
	if err := m.store.ReplaceMappings(recs, time.Now()); err != nil {
		return err
	}
	return m.Reload()
}

// ValidatePattern checks a rule pattern: non-empty, "*" allowed only as the
// last character, and a prefix rule must keep a non-empty stem.
func ValidatePattern(pattern string) error {
	p := strings.TrimSpace(pattern)
	if p == "" {
		return errors.New("pool: mapping pattern must not be empty")
	}
	if i := strings.IndexByte(p, '*'); i >= 0 {
		if i != len(p)-1 {
			return fmt.Errorf("pool: mapping pattern %q: '*' is only allowed as the last character", p)
		}
		if len(p) == 1 {
			return errors.New("pool: mapping pattern '*': prefix must keep a non-empty stem")
		}
	}
	return nil
}

// isPrefixPattern reports whether the pattern ends with the single trailing
// wildcard. Patterns were validated on insert; malformed ones fall back to
// exact matching here.
func isPrefixPattern(pattern string) bool {
	return len(pattern) > 1 && strings.HasSuffix(pattern, "*") && !strings.Contains(pattern[:len(pattern)-1], "*")
}

// normalizeRule validates and canonicalizes (pattern, target, mode):
// PASSTHROUGH rules always store an empty target.
func normalizeRule(pattern, target, mode string) (string, string, string, error) {
	pattern = strings.TrimSpace(pattern)
	target = strings.TrimSpace(target)
	mode = strings.TrimSpace(strings.ToUpper(mode))
	if err := ValidatePattern(pattern); err != nil {
		return "", "", "", err
	}
	switch mode {
	case ModeMapped:
		if target == "" {
			return "", "", "", fmt.Errorf("pool: mapping %q: MAPPED rules need a non-empty target", pattern)
		}
	case ModePassthrough:
		target = ""
	default:
		return "", "", "", fmt.Errorf("pool: mapping %q: unknown mode %q (use MAPPED or PASSTHROUGH)", pattern, mode)
	}
	return pattern, target, mode, nil
}
