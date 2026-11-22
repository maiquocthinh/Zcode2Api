package admin

import (
	"encoding/json"
	"testing"
	"time"
)

// TestLogsSlowSubscriberDisconnected: a subscriber that stops reading is
// disconnected once its 256-entry buffer overflows, and it can never block
// the data plane.
func TestLogsSlowSubscriberDisconnected(t *testing.T) {
	l := NewLogs()
	ch, cancel := l.Subscribe()
	defer cancel()

	for i := 0; i < subBuffer+1; i++ {
		l.Request(RequestEntry{Ts: int64(i)})
	}

	done := make(chan struct{})
	go func() {
		for range ch { // drains the buffered entries, then observes the close
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("slow subscriber was not disconnected after the 256-entry buffer overflowed")
	}

	// Publishing must not block after the disconnect.
	start := time.Now()
	for i := 0; i < 100; i++ {
		l.Request(RequestEntry{Ts: int64(i)})
	}
	if time.Since(start) > time.Second {
		t.Fatal("publish blocked while a disconnected subscriber existed")
	}
}

// TestLogsRingKeepsLastThousand covers the ring buffer: it holds at most 1000
// entries and evicts the oldest ones.
func TestLogsRingKeepsLastThousand(t *testing.T) {
	l := NewLogs()
	for i := 0; i < ringSize+5; i++ {
		l.Request(RequestEntry{Ts: int64(i)})
	}
	snap := l.Snapshot(ringSize + 100) // oversized limit is clamped
	if len(snap) != ringSize {
		t.Fatalf("ring holds %d entries, want %d", len(snap), ringSize)
	}
	var first RequestEntry
	if err := json.Unmarshal(snap[0].Data, &first); err != nil {
		t.Fatalf("ring entry is not request JSON: %v", err)
	}
	if first.Ts != 5 {
		t.Errorf("oldest kept entry ts = %d, want 5 (five oldest were evicted)", first.Ts)
	}

	// A smaller limit returns only the newest entries.
	short := l.Snapshot(10)
	if len(short) != 10 {
		t.Fatalf("snapshot(10) returned %d entries", len(short))
	}
	var newest RequestEntry
	if err := json.Unmarshal(short[len(short)-1].Data, &newest); err != nil {
		t.Fatalf("snapshot entry is not request JSON: %v", err)
	}
	if newest.Ts != ringSize+4 {
		t.Errorf("newest entry ts = %d, want %d", newest.Ts, ringSize+4)
	}

	// DELETE /api/logs clears the ring.
	l.Reset()
	if got := l.Snapshot(ringSize); len(got) != 0 {
		t.Errorf("ring after Reset holds %d entries, want 0", len(got))
	}
}

// TestLogsFailoverCounter covers the failovers_total stat.
func TestLogsFailoverCounter(t *testing.T) {
	l := NewLogs()
	if got := l.FailoversTotal(); got != 0 {
		t.Fatalf("failovers = %d, want 0", got)
	}
	l.Failover(FailoverEntry{RID: "r_1", From: "key_a", To: "key_b", Kind: "rate_limit"})
	l.Failover(FailoverEntry{RID: "r_1", From: "key_b", To: "key_c", Kind: "usage_limit"})
	if got := l.FailoversTotal(); got != 2 {
		t.Errorf("failovers = %d, want 2", got)
	}
}
