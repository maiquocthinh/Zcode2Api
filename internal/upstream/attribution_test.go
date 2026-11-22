package upstream

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestInjectModelIdentity pins the golden body parity: metadata.user_id
// becomes the official JSON string (fixed key order, session_id equal to the
// attribution session) and the official identity blocks precede the client's
// system blocks.
func TestInjectModelIdentity(t *testing.T) {
	attr := newTestAttr()
	body := []byte(`{"model":"GLM-5.3-Flash","max_tokens":64,"system":[{"type":"text","text":"You are Claude Code"}],"messages":[{"role":"user","content":"hi"}]}`)

	out := InjectModelIdentity(body, testDeviceMid, attr)
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("output not JSON: %v", err)
	}

	var metadata struct {
		UserID string `json:"user_id"`
	}
	if err := json.Unmarshal(doc["metadata"], &metadata); err != nil {
		t.Fatalf("metadata: %v", err)
	}
	want := `{"device_id":"` + testDeviceMid + `","account_uuid":"","session_id":"` + attr.SessionID + `"}`
	if metadata.UserID != want {
		t.Errorf("metadata.user_id = %q, want %q", metadata.UserID, want)
	}

	var system []map[string]any
	if err := json.Unmarshal(doc["system"], &system); err != nil {
		t.Fatalf("system: %v", err)
	}
	if len(system) != 3 {
		t.Fatalf("system blocks = %d, want 3 (official shape, caller text merged)", len(system))
	}
	if got := system[0]["text"]; got != "You are ZCode, an interactive coding agent" {
		t.Errorf("system[0] = %v, want the official identity line", got)
	}
	if _, has := system[0]["cache_control"]; has {
		t.Errorf("system[0] carries cache_control, want none (official wire shape)")
	}
	if got, ok := system[1]["cache_control"].(map[string]any)["type"]; !ok || got != "ephemeral" {
		t.Errorf("system[1].cache_control = %v, want ephemeral", system[1]["cache_control"])
	}
	// The caller's text merges into the LAST official block ("at most three
	// blocks" constraint of the official wire shape).
	if text, _ := system[2]["text"].(string); !strings.HasSuffix(text, "You are Claude Code") {
		t.Errorf("system[2] does not end with the caller text (len %d)", len(text))
	}
}

// TestInjectModelIdentityStringSystem pins the string-system normalization:
// a bare string system becomes one block after the official pair.
func TestInjectModelIdentityStringSystem(t *testing.T) {
	out := InjectModelIdentity([]byte(`{"model":"m","system":"be brief","messages":[]}`), "d1", newTestAttr())
	var doc struct {
		System []map[string]any `json:"system"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("output not JSON: %v", err)
	}
	if len(doc.System) != 3 {
		t.Fatalf("system blocks = %d, want 3", len(doc.System))
	}
	if text, _ := doc.System[2]["text"].(string); !strings.HasSuffix(text, "be brief") {
		t.Errorf("caller text not merged into block2: %q", text)
	}
}

// TestInjectModelIdentityBrokenBody pins the best-effort contract: a body the
// rewrite cannot understand is never failed — non-object bodies come back
// untouched, and an undecorable system keeps its original value while the
// metadata identity is still applied.
func TestInjectModelIdentityBrokenBody(t *testing.T) {
	for _, body := range []string{`not json at all`, `[1,2,3]`} {
		if got := InjectModelIdentity([]byte(body), "d", newTestAttr()); string(got) != body {
			t.Errorf("InjectModelIdentity(%q) = %q, want it untouched", body, got)
		}
	}
	weird := `{"system":{"weird":"object"}}`
	out := string(InjectModelIdentity([]byte(weird), "d", newTestAttr()))
	if !strings.Contains(out, `"system":{"weird":"object"}`) {
		t.Errorf("weird system not preserved: %s", out)
	}
	if !strings.Contains(out, `"user_id"`) {
		t.Errorf("metadata identity not applied: %s", out)
	}
}

// TestOfficialSystemBlocks pins the embedded profile: the identity line, the
// harness block and the third official block are all present.
func TestOfficialSystemBlocks(t *testing.T) {
	blocks := OfficialSystemBlocks()
	if len(blocks) != 3 {
		t.Fatalf("official blocks = %d, want 3", len(blocks))
	}
	b0, _ := blocks[0].(map[string]any)
	if got := b0["text"]; got != "You are ZCode, an interactive coding agent" {
		t.Errorf("block0 text = %v, want the official identity line", got)
	}
	b1, _ := blocks[1].(map[string]any)
	if text, _ := b1["text"].(string); !strings.Contains(text, "# Harness") {
		t.Errorf("block1 text missing # Harness section")
	}
}

// TestInjectModelIdentityClampsMaxTokens pins the upstream ceiling: a 200000
// budget is clamped to 131072 in place, 64 stays 64, and a body without
// max_tokens stays without one.
func TestInjectModelIdentityClampsMaxTokens(t *testing.T) {
	out := InjectModelIdentity([]byte(`{"model":"m","max_tokens":200000,"messages":[]}`), "d1", newTestAttr())
	if !strings.Contains(string(out), `"max_tokens":131072`) {
		t.Errorf("max_tokens not clamped: %s", out)
	}
	out = InjectModelIdentity([]byte(`{"model":"m","max_tokens":64,"messages":[]}`), "d1", newTestAttr())
	if !strings.Contains(string(out), `"max_tokens":64`) || strings.Contains(string(out), "131072") {
		t.Errorf("small max_tokens changed: %s", out)
	}
	out = InjectModelIdentity([]byte(`{"model":"m","messages":[]}`), "d1", newTestAttr())
	if strings.Contains(string(out), "max_tokens") {
		t.Errorf("max_tokens invented: %s", out)
	}
}

// TestInjectModelIdentityPreservesOfficialSystem pins the dedup: a caller
// system that already carries the official identity blocks (array form, the
// first two blocks byte-match the profile) is kept verbatim — no rebuild, no
// duplication.
func TestInjectModelIdentityPreservesOfficialSystem(t *testing.T) {
	raw, err := json.Marshal(OfficialSystemBlocks())
	if err != nil {
		t.Fatalf("marshal official blocks: %v", err)
	}
	in := `{"model":"m","system":` + string(raw) + `,"messages":[]}`
	out := InjectModelIdentity([]byte(in), "d1", newTestAttr())
	var docIn, docOut struct {
		System json.RawMessage `json:"system"`
	}
	_ = json.Unmarshal([]byte(in), &docIn)
	_ = json.Unmarshal(out, &docOut)
	if string(docIn.System) != string(docOut.System) {
		t.Errorf("official caller system was rebuilt:\n in: %s\nout: %s", docIn.System, docOut.System)
	}
}

// TestInjectModelIdentityDropsStaleOfficialBlocks pins the merge hygiene: a
// caller system carrying a stale official identity block plus its own text
// contributes only its own text (the stale identity must not drift into the
// rebuilt prompt), and the canned opener is stripped from the merged block.
func TestInjectModelIdentityDropsStaleOfficialBlocks(t *testing.T) {
	blocks := OfficialSystemBlocks()
	staleID, _ := blocks[0].(map[string]any)["text"].(string)
	in := `{"model":"m","system":[{"type":"text","text":` + quoteJSON(staleID) +
		`},{"type":"text","text":"be brief"}],"messages":[]}`
	out := InjectModelIdentity([]byte(in), "d1", newTestAttr())
	var doc struct {
		System []map[string]any `json:"system"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("output not JSON: %v", err)
	}
	if len(doc.System) != 3 {
		t.Fatalf("system blocks = %d, want 3", len(doc.System))
	}
	text, _ := doc.System[2]["text"].(string)
	if strings.Contains(text, staleID) {
		t.Error("stale official identity drifted into the merged block")
	}
	if !strings.HasSuffix(text, "be brief") {
		t.Errorf("caller text not merged: %q", text)
	}
	if strings.Contains(text, "Before your first tool call") {
		t.Error("canned opener still present in the merged block")
	}
}
