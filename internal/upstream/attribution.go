package upstream

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
)

// Attribution is the per-request identity set carried by the X-Request-Id /
// X-Session-Id / X-Zcode-Trace-Id / X-Query-Id headers and echoed inside
// metadata.user_id.
type Attribution struct {
	RequestID string
	SessionID string
	TraceID   string
	QueryID   string
}

// model UA suffix constants of the official client's composite model-request
// User-Agent. The billing UA stays the bare "ZCode/<ver>" string.
const (
	modelAISDKVersion = "4.0.27"
	modelNodeMajor    = "24"
)

// ModelUserAgent is the model-request User-Agent: the ai-sdk composite form.
func ModelUserAgent(appVersion string) string {
	return "ZCode/" + appVersion + " ai-sdk/provider-utils/" + modelAISDKVersion +
		" runtime/node.js/" + modelNodeMajor
}

// NewAttribution synthesizes a fresh identity set for one client request.
func NewAttribution() Attribution {
	return Attribution{
		RequestID: newRequestUUID(),
		SessionID: newRequestUUID(),
		TraceID:   newRequestUUID(),
		QueryID:   newRequestUUID(),
	}
}

// WithSessionID returns a copy of the attribution bound to one account's
// device: the official client reuses one session id per device across turns
// ("session_" + 32 hex), so every attempt of a
// request must carry the session of the account actually being used.
func (a Attribution) WithSessionID(deviceMid string) Attribution {
	a.SessionID = SessionIDFor(deviceMid)
	return a
}

// WithFreshRequestID returns a copy with a new request id and every other
// value kept: the official client issues a fresh request id per upstream HTTP
// attempt (4 retries = 4 request ids, session/trace/query unchanged), so
// failover attempts and same-account replays each look like their own
// outbound call instead of one reused id.
func (a Attribution) WithFreshRequestID() Attribution {
	a.RequestID = newRequestUUID()
	return a
}

// SessionIDFor derives the official per-device session id: "session_" + 32
// lowercase hex, stable per device mid (x-session-id and
// metadata.user_id.session_id carry the same value).
func SessionIDFor(deviceMid string) string {
	if deviceMid == "" {
		deviceMid = "anon"
	}
	sum := sha256.Sum256([]byte("zcode-session." + deviceMid))
	return "session_" + hex.EncodeToString(sum[:16])
}

// zcode_system.json holds the static system blocks captured from the official
// client. Blocks 0 (identity line) and 1 (# Harness) are the identity section
// the upstream validates byte-by-byte; block 2 completes the official
// three-block shape, with caller text merged into the last one.
//
//go:embed zcode_system.json
var zcodeSystemJSON []byte

// OfficialSystemBlocks returns the three official system blocks (with their
// cache_control), ready to replace a chat body's system field. The embedded
// profile is a bare JSON array holding the identity text the upstream content
// inspector currently accepts. Block 0 carries no cache_control on the wire.
// The parse never fails the request path: a broken asset yields nil (the
// gateway then forwards the client system untouched). The asset is parsed
// once; each call returns a fresh slice over shared block maps, which callers
// must clone before mutating.
func OfficialSystemBlocks() []any {
	officialBlocksOnce.Do(func() { officialBlocks = parseOfficialSystemBlocks() })
	if officialBlocks == nil {
		return nil
	}
	return append([]any(nil), officialBlocks...)
}

var (
	officialBlocksOnce sync.Once
	officialBlocks     []any
)

func parseOfficialSystemBlocks() []any {
	var arr []json.RawMessage
	if err := json.Unmarshal(zcodeSystemJSON, &arr); err != nil {
		// tolerate the {blocks: [...]} wrapper shape as well
		var doc struct {
			Blocks []json.RawMessage `json:"blocks"`
		}
		if err := json.Unmarshal(zcodeSystemJSON, &doc); err != nil || len(doc.Blocks) < 3 {
			return nil
		}
		arr = doc.Blocks
	}
	if len(arr) < 3 {
		return nil
	}
	blocks := make([]any, 0, len(arr))
	for i, raw := range arr {
		var b map[string]any
		if err := json.Unmarshal(raw, &b); err != nil {
			blocks = append(blocks, json.RawMessage(raw))
			continue
		}
		if i == 0 {
			delete(b, "cache_control")
		}
		blocks = append(blocks, b)
	}
	return blocks
}

// maxTokensCap is the zcode-plan upstream's max_tokens ceiling: bigger
// budgets from 128k+ harnesses answer 400 code 1210 and, as a client error,
// are relayed verbatim with no retry chance — clamp instead.
const maxTokensCap = 131072

// Canned sentences of the official client, stripped from the merged system
// block (byte-exact against the embedded profile — note the plain hyphen in
// the second one): without the strip every reply opens with the same canned
// line.
const (
	cannedOpenerSentence  = `Before your first tool call, say in a sentence what you're about to do; while working, give brief updates when you find something load-bearing or change direction.`
	cannedOutcomeSentence = `Lead with the outcome. Your first sentence after finishing should answer "what happened" or "what did you find" - the thing the user would ask for if they said "just give me the TLDR." Supporting detail and reasoning come after, for readers who want them.`
)

// The rewrite is best-effort: any shape it cannot understand (body not an
// object, system not string/array, metadata not an object) is left untouched
// rather than failing the request — the gateway must not break on bodies it
// merely cannot decorate.
func InjectModelIdentity(body []byte, deviceID string, attr Attribution) []byte {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(body, &doc); err != nil {
		return body
	}
	keyOrder := topLevelKeyOrder(body)

	metadata := map[string]json.RawMessage{}
	if raw, ok := doc["metadata"]; ok {
		_ = json.Unmarshal(raw, &metadata)
	}
	metadata["user_id"] = json.RawMessage(quoteJSON(deviceUserID(deviceID, attr.SessionID)))
	if raw, err := json.Marshal(metadata); err == nil {
		doc["metadata"] = raw
	}

	// max_tokens above the upstream ceiling is clamped in place — the field
	// already sits in the client's key order, so the clamp keeps its position.
	if raw, ok := doc["max_tokens"]; ok {
		var n json.Number
		if err := json.Unmarshal(bytes.TrimSpace(raw), &n); err == nil {
			if v, err2 := n.Int64(); err2 == nil && v > maxTokensCap {
				doc["max_tokens"] = json.RawMessage(strconv.FormatInt(maxTokensCap, 10))
			}
		}
	}

	if blocks := OfficialSystemBlocks(); len(blocks) == 3 {
		merge := true
		extra := ""
		if raw, ok := doc["system"]; ok {
			if callerSystemMatchesOfficial(raw) {
				// The caller already carries the official system (its first two
				// blocks byte-match the current profile): keep it verbatim —
				// rebuilding would duplicate the identity text, and only the
				// array form is trusted (a rebuilt string form is what drifted
				// into 3012 in the identity incident).
				merge = false
			} else {
				var replace bool
				extra, replace = callerExtraSystemText(raw)
				if !replace {
					merge = false
				}
			}
		}
		if merge {
			if extra != "" {
				if last, ok := blocks[2].(map[string]any); ok {
					clone := make(map[string]any, len(last)+1)
					for k, v := range last {
						clone[k] = v
					}
					text, _ := clone["text"].(string)
					text = strings.ReplaceAll(text, cannedOpenerSentence, "")
					text = strings.ReplaceAll(text, cannedOutcomeSentence, "")
					clone["text"] = strings.TrimSpace(text) + "\n\n" + extra
					blocks[2] = clone
				}
			}
			if out, err := json.Marshal(blocks); err == nil {
				doc["system"] = out
			}
		}
	}

	if raw, ok := doc["messages"]; ok {
		if bridged, ok2 := bridgeMessageContents(raw); ok2 {
			applyLastMessageCacheControl(bridged)
			if out, err := json.Marshal(bridged); err == nil {
				doc["messages"] = out
			}
		}
	}

	remapClaudeThinking(doc)

	if len(keyOrder) == 0 {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(doc); err != nil {
			return body
		}
		return bytes.TrimRight(buf.Bytes(), "\n")
	}
	seen := make(map[string]bool, len(keyOrder))
	var buf bytes.Buffer
	buf.WriteByte('{')
	first := true
	writePair := func(key string) {
		raw, ok := doc[key]
		if !ok {
			return
		}
		if !first {
			buf.WriteByte(',')
		}
		first = false
		kb, _ := json.Marshal(key)
		buf.Write(kb)
		buf.WriteByte(':')
		buf.Write(bytes.TrimSpace(raw))
		seen[key] = true
	}
	for _, key := range keyOrder {
		writePair(key)
	}
	for _, key := range []string{"metadata", "system"} {
		if !seen[key] {
			writePair(key)
		}
	}
	buf.WriteByte('}')
	return buf.Bytes()
}

// topLevelKeyOrder records the client body's top-level key order.
func topLevelKeyOrder(body []byte) []string {
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return nil
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil
	}
	var order []string
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return order
		}
		key, ok := kt.(string)
		if !ok {
			return order
		}
		order = append(order, key)
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			return order
		}
	}
	return order
}

// applyLastMessageCacheControl sets cache_control {type: ephemeral} on the
// last content block of the last non-system message (the official client
// marks its turn boundary this way).
func applyLastMessageCacheControl(messages []any) {
	for i := len(messages) - 1; i >= 0; i-- {
		msg, ok := messages[i].(map[string]any)
		if !ok {
			continue
		}
		if role, _ := msg["role"].(string); role == "system" {
			continue
		}
		switch content := msg["content"].(type) {
		case string:
			msg["content"] = []map[string]any{{
				"type": "text", "text": content,
				"cache_control": map[string]any{"type": "ephemeral"},
			}}
		case []any:
			if len(content) == 0 {
				return
			}
			if last, ok := content[len(content)-1].(map[string]any); ok {
				last["cache_control"] = map[string]any{"type": "ephemeral"}
			}
		}
		return
	}
}

// remapClaudeThinking maps the Claude thinking forms onto the GLM forms:
// thinking.type "adaptive" becomes "enabled"; output_config.effort is
// remapped through the effort dictionary (medium -> high, high -> high).
func remapClaudeThinking(doc map[string]json.RawMessage) {
	if raw, ok := doc["thinking"]; ok {
		var t struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(raw, &t) == nil && strings.EqualFold(t.Type, "adaptive") {
			doc["thinking"] = json.RawMessage(`{"type":"enabled"}`)
		}
	}
	raw, ok := doc["output_config"]
	if !ok {
		return
	}
	var oc struct {
		Effort string `json:"effort"`
	}
	if json.Unmarshal(raw, &oc) != nil || oc.Effort == "" {
		return
	}
	if mapped := mapGLMEffort(oc.Effort); mapped != oc.Effort {
		var full map[string]any
		if json.Unmarshal(raw, &full) == nil {
			full["effort"] = mapped
			if out, err := json.Marshal(full); err == nil {
				doc["output_config"] = out
			}
		}
	}
}

// mapGLMEffort is the Claude -> GLM effort dictionary: medium collapses to
// high; unknown values pass unchanged.
func mapGLMEffort(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "low":
		return "low"
	case "medium":
		return "high"
	case "high":
		return "high"
	}
	return v
}

// bridgeMessageContents converts every message whose content is a bare string
// into the block-array form (content: [{type:"text",text:...}]). Messages with
// non-string content pass through untouched.
func bridgeMessageContents(raw json.RawMessage) ([]any, bool) {
	var messages []map[string]any
	if err := json.Unmarshal(raw, &messages); err != nil {
		return nil, false
	}
	out := make([]any, len(messages))
	for i, msg := range messages {
		if text, ok := msg["content"].(string); ok {
			msg["content"] = []map[string]any{{"type": "text", "text": text}}
		}
		out[i] = msg
	}
	return out, true
}

// systemText flattens a system field into one text plus a replaceable flag:
// a string becomes that string; a text-block array joins the texts with blank
// lines (mirroring how the official client concatenates multi-block prompts);
// anything else reports replace=false so the caller keeps the original value.
func systemText(raw json.RawMessage) (text string, replace bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", true
	}
	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return "", false
		}
		return s, true
	}
	if trimmed[0] != '[' {
		return "", false
	}
	var arr []map[string]any
	if err := json.Unmarshal(trimmed, &arr); err != nil {
		return "", false
	}
	var parts []string
	for _, b := range arr {
		if t, ok := b["text"].(string); ok {
			parts = append(parts, t)
		}
	}
	return strings.Join(parts, "\n\n"), true
}

// callerSystemMatchesOfficial reports whether a caller system field is the
// official prompt itself: an array whose first two blocks carry the current
// profile's identity texts byte-for-byte (blocks 0 and 1 are the identity
// section the upstream validates). String form never matches — a rebuilt
// string form is what drifted into 3012 in the identity incident, so the
// caller's array is kept verbatim instead of being rebuilt. (Array form is
// also what the golden capture shows on the wire.)
func callerSystemMatchesOfficial(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return false
	}
	var arr []map[string]any
	if json.Unmarshal(trimmed, &arr) != nil || len(arr) < 2 {
		return false
	}
	blocks := OfficialSystemBlocks()
	if len(blocks) != 3 {
		return false
	}
	b0, _ := blocks[0].(map[string]any)
	b1, _ := blocks[1].(map[string]any)
	want0, _ := b0["text"].(string)
	want1, _ := b1["text"].(string)
	got0, _ := arr[0]["text"].(string)
	got1, _ := arr[1]["text"].(string)
	return got0 != "" && got0 == want0 && got1 != "" && got1 == want1
}

// callerExtraSystemText flattens a caller system field into the text to merge
// into the official block 2: caller blocks whose text byte-matches an
// official profile block are dropped (a stale identity must not drift into
// the rebuilt prompt), and what remains keeps the replaceable contract of
// systemText (string and text-block array forms; anything else reports
// replace=false so the original field passes through untouched).
func callerExtraSystemText(raw json.RawMessage) (string, bool) {
	official := map[string]bool{}
	for _, b := range OfficialSystemBlocks() {
		if m, ok := b.(map[string]any); ok {
			if t, _ := m["text"].(string); t != "" {
				official[t] = true
			}
		}
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", true
	}
	var parts []string
	switch {
	case trimmed[0] == '"':
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return "", false
		}
		if !official[s] {
			parts = append(parts, s)
		}
	case trimmed[0] == '[':
		var arr []map[string]any
		if json.Unmarshal(trimmed, &arr) != nil {
			return "", false
		}
		for _, b := range arr {
			t, ok := b["text"].(string)
			if !ok || official[t] {
				continue
			}
			parts = append(parts, t)
		}
	default:
		return "", false
	}
	return strings.TrimSpace(strings.Join(parts, "\n\n")), true
}

// deviceUserID builds the official metadata.user_id string (fixed key order
// device_id / account_uuid / session_id, as captured from the official client).
func deviceUserID(deviceID, sessionID string) string {
	return `{"device_id":` + quoteJSON(deviceID) +
		`,"account_uuid":"","session_id":` + quoteJSON(sessionID) + "}"
}

// quoteJSON wraps a string with JSON escaping (including the quotes).
// Deliberately not strconv.Quote: the escape forms differ (\x00 vs \u0000).
func quoteJSON(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		return `""`
	}
	return string(b)
}
