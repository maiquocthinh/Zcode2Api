package pool

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestStoreVacuumReportsSizes backs POST /api/settings/vacuum: VACUUM runs
// and the file size is reported on both sides.
func TestStoreVacuumReportsSizes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vacuum.db")
	store, err := Open(path, "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	now := time.Now()
	for i := 0; i < 50; i++ {
		token := []byte(fmt.Sprintf("eyJhbGciOiJIUzI1NiJ9.eyJ2IjoidmFjdXVtLTAwMDAiLmsiOX0.c2lnLTAwMA==%04d", i))
		if _, err := store.AddAccount("acc", token, "", now); err != nil {
			t.Fatalf("add account: %v", err)
		}
	}

	before, after, err := store.Vacuum()
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if before <= 0 {
		t.Errorf("before_bytes = %d, want > 0", before)
	}
	if after <= 0 {
		t.Errorf("after_bytes = %d, want > 0", after)
	}
}
