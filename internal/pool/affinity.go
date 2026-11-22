package pool

import (
	"container/list"
	"sync"
	"time"
)

// MaxAffinityEntries caps the session→account table (TTL AFFINITY_TTL,
// default 30 minutes, at most 10,000 entries, LRU).
const MaxAffinityEntries = 10000

// affinityEntry is one session binding. Kept in a doubly-linked list ordered
// by recency (front = most recently used) for LRU eviction.
type affinityEntry struct {
	session   string
	accountID string
	expiresAt time.Time
}

// Affinity maps a session (metadata.user_id of the request body) to the
// account that served it last, so consecutive requests of one session reuse
// the same upstream prompt cache. It is its own
// tiny structure guarded by its own mutex; the pool calls it while holding
// Pool.mu, and Affinity never calls back into the pool, so no lock cycle
// exists.
type Affinity struct {
	mu      sync.Mutex
	ttl     time.Duration
	max     int
	entries map[string]*list.Element
	lru     *list.List // element values are *affinityEntry
}

// NewAffinity builds an affinity table. ttl <= 0 defaults to 30 minutes and
// max <= 0 to MaxAffinityEntries.
func NewAffinity(ttl time.Duration, max int) *Affinity {
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	if max <= 0 {
		max = MaxAffinityEntries
	}
	return &Affinity{
		ttl:     ttl,
		max:     max,
		entries: make(map[string]*list.Element),
		lru:     list.New(),
	}
}

// Get returns the account bound to the session, if the binding exists and has
// not expired. A hit refreshes recency; an expired binding is dropped.
func (a *Affinity) Get(session string, now time.Time) (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	el, ok := a.entries[session]
	if !ok {
		return "", false
	}
	e := el.Value.(*affinityEntry)
	if !e.expiresAt.After(now) {
		a.lru.Remove(el)
		delete(a.entries, session)
		return "", false
	}
	a.lru.MoveToFront(el)
	return e.accountID, true
}

// Set binds session to accountID with the current TTL. An existing binding is
// refreshed in place; when the table is full, expired entries are dropped
// first and then the least recently used ones.
func (a *Affinity) Set(session, accountID string, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	expiresAt := now.Add(a.ttl)
	if el, ok := a.entries[session]; ok {
		e := el.Value.(*affinityEntry)
		e.accountID = accountID
		e.expiresAt = expiresAt
		a.lru.MoveToFront(el)
		return
	}
	el := a.lru.PushFront(&affinityEntry{session: session, accountID: accountID, expiresAt: expiresAt})
	a.entries[session] = el
	a.trimLocked(now)
}

// SetTTL changes the binding TTL; it applies to bindings created afterwards.
func (a *Affinity) SetTTL(ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	a.mu.Lock()
	a.ttl = ttl
	a.mu.Unlock()
}

// Len returns the number of live bindings (expired ones included — they are
// dropped lazily on access).
func (a *Affinity) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.entries)
}

// trimLocked evicts until the table is within its cap: expired bindings go
// first (anywhere in the LRU order), then least-recently-used ones from the
// tail.
func (a *Affinity) trimLocked(now time.Time) {
	for len(a.entries) > a.max && a.dropExpiredLocked(now) {
	}
	for len(a.entries) > a.max {
		back := a.lru.Back()
		if back == nil {
			break
		}
		e := back.Value.(*affinityEntry)
		a.lru.Remove(back)
		delete(a.entries, e.session)
	}
}

// dropExpiredLocked removes one expired binding, tail (oldest) first, and
// reports whether it found one.
func (a *Affinity) dropExpiredLocked(now time.Time) bool {
	for el := a.lru.Back(); el != nil; el = el.Prev() {
		e := el.Value.(*affinityEntry)
		if !e.expiresAt.After(now) {
			a.lru.Remove(el)
			delete(a.entries, e.session)
			return true
		}
	}
	return false
}
