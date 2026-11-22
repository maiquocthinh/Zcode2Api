package pool

import (
	"testing"
	"time"
)

var affBase = time.Unix(1_700_000_000, 0)

func TestAffinityGetSet(t *testing.T) {
	a := NewAffinity(30*time.Minute, 100)
	if got, ok := a.Get("s1", affBase); ok || got != "" {
		t.Errorf("empty table returned %q/%v", got, ok)
	}
	a.Set("s1", "key_aaa", affBase)
	if got, ok := a.Get("s1", affBase.Add(time.Second)); !ok || got != "key_aaa" {
		t.Errorf("Get = %q/%v, want key_aaa/true", got, ok)
	}
	if a.Len() != 1 {
		t.Errorf("Len = %d, want 1", a.Len())
	}
}

// TestAffinityTTLExpiry: a binding stops resolving after AFFINITY_TTL
// (default 30 minutes per step 5).
func TestAffinityTTLExpiry(t *testing.T) {
	a := NewAffinity(10*time.Minute, 100)
	a.Set("s1", "key_aaa", affBase)
	if _, ok := a.Get("s1", affBase.Add(9*time.Minute)); !ok {
		t.Error("binding must resolve just before expiry")
	}
	if _, ok := a.Get("s1", affBase.Add(10*time.Minute)); ok {
		t.Error("binding must not resolve at expiry time")
	}
	if a.Len() != 0 {
		t.Errorf("expired binding must be dropped, Len = %d", a.Len())
	}
	// Rebinding after expiry works.
	a.Set("s1", "key_bbb", affBase.Add(11*time.Minute))
	if got, ok := a.Get("s1", affBase.Add(11*time.Minute).Add(time.Minute)); !ok || got != "key_bbb" {
		t.Errorf("rebinding failed: %q/%v", got, ok)
	}
}

// TestAffinityLRUEviction: over the 10,000-entry cap the least recently used
// bindings are evicted. Uses a small max for a
// deterministic test; production uses MaxAffinityEntries.
func TestAffinityLRUEviction(t *testing.T) {
	a := NewAffinity(time.Hour, 3)
	a.Set("s1", "k1", affBase)
	a.Set("s2", "k2", affBase)
	a.Set("s3", "k3", affBase)
	// Touch s1 so s2 becomes the LRU entry.
	if _, ok := a.Get("s1", affBase); !ok {
		t.Fatal("s1 should be present")
	}
	a.Set("s4", "k4", affBase)
	if _, ok := a.Get("s2", affBase); ok {
		t.Error("s2 (least recently used) should have been evicted")
	}
	for _, s := range []string{"s1", "s3", "s4"} {
		if _, ok := a.Get(s, affBase); !ok {
			t.Errorf("%s should still be bound", s)
		}
	}
	if a.Len() != 3 {
		t.Errorf("Len = %d, want 3 (the cap)", a.Len())
	}
}

// TestAffinityExpiredEvictedFirst: when trimming, expired bindings go before
// live ones regardless of recency.
func TestAffinityExpiredEvictedFirst(t *testing.T) {
	a := NewAffinity(10*time.Minute, 3)
	t0 := affBase
	a.Set("old", "k0", t0)                   // expires t0+10m
	a.Set("s1", "k1", t0.Add(time.Minute))   // expires t0+11m
	a.Set("s2", "k2", t0.Add(2*time.Minute)) // expires t0+12m
	now := t0.Add(9 * time.Minute)           // only "old" is expired here
	a.Set("s3", "k3", now)                   // over cap -> trim drops "old"
	if _, ok := a.Get("old", now); ok {
		t.Error("expired binding should have been dropped")
	}
	for _, s := range []string{"s1", "s2", "s3"} {
		if _, ok := a.Get(s, now); !ok {
			t.Errorf("%s should still be bound (live entries survive expired ones)", s)
		}
	}
	if a.Len() > 3 {
		t.Errorf("Len = %d, want <= cap 3", a.Len())
	}
}

func TestAffinitySetRebind(t *testing.T) {
	a := NewAffinity(time.Hour, 10)
	a.Set("s1", "k1", affBase)
	a.Set("s1", "k2", affBase) // rebind refreshes in place
	if got, ok := a.Get("s1", affBase); !ok || got != "k2" {
		t.Errorf("Get = %q/%v, want k2/true", got, ok)
	}
	if a.Len() != 1 {
		t.Errorf("rebinding must not grow the table, Len = %d", a.Len())
	}
}

func TestAffinitySetTTL(t *testing.T) {
	a := NewAffinity(time.Hour, 10)
	a.Set("s1", "k1", affBase)
	a.SetTTL(time.Minute)
	a.Set("s2", "k2", affBase)
	if _, ok := a.Get("s2", affBase.Add(time.Minute)); ok {
		t.Error("new TTL must apply to bindings created afterwards")
	}
	if _, ok := a.Get("s1", affBase.Add(30*time.Minute)); !ok {
		t.Error("existing binding keeps its original TTL")
	}
	a.SetTTL(0) // ignored, no panic
}

// TestAffinityDefaultBounds documents the defaults used by the pool.
func TestAffinityDefaultBounds(t *testing.T) {
	a := NewAffinity(0, 0) // zero values -> defaults
	a.Set("s", "k", affBase)
	if _, ok := a.Get("s", affBase.Add(29*time.Minute)); !ok {
		t.Error("default TTL is 30 minutes")
	}
	if _, ok := a.Get("s", affBase.Add(30*time.Minute)); ok {
		t.Error("default TTL is 30 minutes (expired after)")
	}
}
