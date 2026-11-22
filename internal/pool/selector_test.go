package pool

import (
	"testing"
	"time"
)

// newSelectorAccount builds an in-RAM account for the selector tests.
func newSelectorAccount(id string, status Status, inflight int, remaining *int64) *Account {
	a := &Account{ID: id, Status: status, Inflight: inflight, RemainingQuota: remaining}
	if status == "" {
		a.Status = StatusActive
	}
	return a
}

func qp(v int64) *int64 { return &v }

// TestSelectorStep1Filter pins selection step 1 of: only
// ACTIVE accounts with inflight < INFLIGHT_LIMIT survive the filter.
func TestSelectorStep1Filter(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	accounts := []*Account{
		newSelectorAccount("acc_cooling", StatusCooling, 0, nil),
		newSelectorAccount("acc_exhausted", StatusExhausted, 0, nil),
		newSelectorAccount("acc_disabled", StatusDisabled, 0, nil),
		newSelectorAccount("acc_full", StatusActive, 3, nil), // at INFLIGHT_LIMIT 3
		newSelectorAccount("acc_ok", StatusActive, 2, nil),
	}
	var cursor int
	got := selectAccount(accounts, "", nil, nil, false, 3, now, &cursor, "")
	if got == nil || got.ID != "acc_ok" {
		t.Fatalf("selected %v, want acc_ok (the only surviving candidate)", got)
	}
}

// TestSelectorNeverReturnsTried pins step 3: an account inside the tried set
// of the current attempt is never returned.
func TestSelectorNeverReturnsTried(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	accounts := []*Account{
		newSelectorAccount("acc_a", StatusActive, 0, qp(500)),
		newSelectorAccount("acc_b", StatusActive, 0, qp(100)),
	}
	var cursor int
	got := selectAccount(accounts, "", map[string]bool{"acc_a": true, "acc_b": true}, nil, false, 3, now, &cursor, "")
	if got != nil {
		t.Fatalf("selected %v, want nil (every candidate tried)", got)
	}
	got = selectAccount(accounts, "", map[string]bool{"acc_a": true}, nil, false, 3, now, &cursor, "")
	if got == nil || got.ID != "acc_b" {
		t.Fatalf("selected %v, want acc_b (the only untried candidate)", got)
	}
}

// TestSelectorStep2AffinityBeatsQuota pins step 2: a session bound to an
// account gets that account back even when another candidate reports a higher
// remaining quota — the quota ordering of step 4 must not run on an affinity
// hit.
func TestSelectorStep2AffinityBeatsQuota(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	accounts := []*Account{
		newSelectorAccount("acc_low", StatusActive, 0, qp(10)),
		newSelectorAccount("acc_high", StatusActive, 0, qp(999)),
	}
	aff := NewAffinity(time.Minute, 0)
	aff.Set("session-1", "acc_low", now)

	var cursor int
	got := selectAccount(accounts, "session-1", nil, aff, true, 3, now, &cursor, "")
	if got == nil || got.ID != "acc_low" {
		t.Fatalf("selected %v, want acc_low (affinity must beat quota)", got)
	}

	// A tried bound account falls through to the quota ranking (step 3 beats
	// step 2 — the same account is never used twice per request).
	got = selectAccount(accounts, "session-1", map[string]bool{"acc_low": true}, aff, true, 3, now, &cursor, "")
	if got == nil || got.ID != "acc_high" {
		t.Fatalf("selected %v, want acc_high (bound account tried)", got)
	}

	// An expired binding must not pin anything.
	aff2 := NewAffinity(time.Minute, 0)
	aff2.Set("session-2", "acc_low", now.Add(-2*time.Minute))
	got = selectAccount(accounts, "session-2", nil, aff2, true, 3, now, &cursor, "")
	if got == nil || got.ID != "acc_high" {
		t.Fatalf("selected %v, want acc_high (binding expired)", got)
	}
}

// TestSelectorStep4QuotaPriority pins step 4: among equal candidates the
// highest known remaining quota wins; an account whose quota was never
// checked ranks above any known value (it must not be starved behind
// accounts that merely happened to be checked).
func TestSelectorStep4QuotaPriority(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)

	// Known quotas: highest first. Each round drains the winner below the
	// next rank (the balance snapshot dropped), so the next one wins.
	accounts := []*Account{
		newSelectorAccount("acc_small", StatusActive, 0, qp(50)),
		newSelectorAccount("acc_large", StatusActive, 0, qp(500)),
		newSelectorAccount("acc_mid", StatusActive, 0, qp(100)),
	}
	var cursor int
	for i, want := range []string{"acc_large", "acc_mid", "acc_small", "acc_large", "acc_mid", "acc_small"} {
		got := selectAccount(accounts, "", nil, nil, false, 3, now, &cursor, "")
		if got == nil || got.ID != want {
			t.Fatalf("selection %d: selected %s, want %s (descending quota)", i+1, got.ID, want)
		}
		*got.RemainingQuota -= 460 // drain the winner below every remaining rank
	}

	// Unknown quota ranks above known values.
	mixed := []*Account{
		newSelectorAccount("acc_checked", StatusActive, 0, qp(500)),
		newSelectorAccount("acc_unchecked", StatusActive, 0, nil),
	}
	cursor = 0
	got := selectAccount(mixed, "", nil, nil, false, 3, now, &cursor, "")
	if got == nil || got.ID != "acc_unchecked" {
		t.Fatalf("selected %s, want acc_unchecked (never-checked quota ranks highest)", got.ID)
	}
}

// TestSelectorStep5RoundRobin pins step 5: accounts with the same quota rank
// rotate through the pool-wide round-robin pointer in creation order.
func TestSelectorStep5RoundRobin(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	accounts := []*Account{
		newSelectorAccount("acc_a", StatusActive, 0, nil),
		newSelectorAccount("acc_b", StatusActive, 0, nil),
		newSelectorAccount("acc_c", StatusActive, 0, nil),
	}
	var cursor int
	order := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		got := selectAccount(accounts, "", nil, nil, false, 3, now, &cursor, "")
		if got == nil {
			t.Fatal("selection failed")
		}
		order = append(order, got.ID)
	}
	for i, want := range []string{"acc_a", "acc_b", "acc_c", "acc_a", "acc_b", "acc_c"} {
		if order[i] != want {
			t.Fatalf("round-robin order = %v, want rotation starting at acc_a (index %d = %s)", order, i, want)
		}
	}
}

// TestSelectorStep5RecordsAffinity pins the binding side effect: a chosen
// account is bound to the session for the next request.
func TestSelectorStep5RecordsAffinity(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	accounts := []*Account{newSelectorAccount("acc_a", StatusActive, 0, nil)}
	aff := NewAffinity(time.Minute, 0)
	var cursor int
	got := selectAccount(accounts, "session-9", nil, aff, true, 3, now, &cursor, "")
	if got == nil {
		t.Fatal("selection failed")
	}
	if id, ok := aff.Get("session-9", now); !ok || id != "acc_a" {
		t.Fatalf("affinity binding = (%q, %v), want (acc_a, true)", id, ok)
	}
}

// TestSelectorIgnoresTokenCounters pins the note of: the
// selector has no "most tokens served" criterion — token usage is counted for
// display only, it must not influence selection.
func TestSelectorIgnoresTokenCounters(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	accounts := []*Account{
		newSelectorAccount("acc_light", StatusActive, 0, nil),
		newSelectorAccount("acc_heavy", StatusActive, 0, nil),
	}
	accounts[1].InputTokens = 10_000_000
	accounts[1].OutputTokens = 900_000
	var cursor int
	first := selectAccount(accounts, "", nil, nil, false, 3, now, &cursor, "")
	second := selectAccount(accounts, "", nil, nil, false, 3, now, &cursor, "")
	if first.ID != "acc_light" || second.ID != "acc_heavy" {
		t.Fatalf("selection order = [%s, %s], want plain round-robin (token counters ignored)", first.ID, second.ID)
	}
}
