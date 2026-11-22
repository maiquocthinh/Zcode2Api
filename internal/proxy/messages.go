package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"zcode2api/internal/admin"
	"zcode2api/internal/config"
	"zcode2api/internal/pool"
	"zcode2api/internal/upstream"
)

// statusClientClosed is the de-facto status for "the client hung up"
// (nginx-style 499) used only in the request log entry.
const statusClientClosed = 499

// flushThreshold flushes an over-long SSE line to the client before its event
// boundary, bounding gateway memory on pathological upstreams.
const flushThreshold = 64 << 10

// runtimeKnobs are the settings PUT /api/settings may change without a
// restart. Guarded by an atomic pointer. FallbackAPIKey
// is write-only in the admin API but reaches this handler to arm channel B.
type runtimeKnobs struct {
	MaxAttempts    int
	StrictModels   bool
	FallbackAPIKey string
}

// Handler serves the data-plane endpoints.
type Handler struct {
	pool    *pool.Pool
	mapping *pool.Mapping
	logs    *admin.Logs

	// upstreamURL is the channel A origin (UPSTREAM_URL, default
	// https://zcode.z.ai): the pool JWT accounts go to
	// /api/v1/zcode-plan/anthropic/v1/messages.
	upstreamURL string
	// fallbackURL is the channel B origin (FALLBACK_URL), used only when the
	// pool returns ErrPoolExhausted.
	fallbackURL string

	attemptDeadline    time.Duration
	firstEventTimeout  time.Duration
	firstEventMaxBytes int
	maxBodyBytes       int

	rt atomic.Pointer[runtimeKnobs]
}

// NewHandler builds the data-plane handler from the configuration. Upstream
// authentication uses the pooled account JWT per request — never a client
// key.
func NewHandler(cfg *config.Config, pl *pool.Pool, mp *pool.Mapping, lg *admin.Logs) *Handler {
	h := &Handler{
		pool:               pl,
		mapping:            mp,
		logs:               lg,
		upstreamURL:        cfg.UpstreamURL,
		fallbackURL:        cfg.FallbackURL,
		attemptDeadline:    cfg.AttemptDeadline,
		firstEventTimeout:  cfg.FirstEventTimeout,
		firstEventMaxBytes: cfg.FirstEventMaxBytes,
		maxBodyBytes:       cfg.MaxBodyBytes,
	}
	h.ApplyRuntime(cfg.MaxAttempts, cfg.StrictModels, cfg.FallbackAPIKey)
	return h
}

// ApplyRuntime applies the runtime settings without a restart. Out-of-range values keep the previous setting; an empty
// fallbackAPIKey keeps the previous key ("" disables channel B — use a single
// space to clear it, though the admin API never writes empty values).
func (h *Handler) ApplyRuntime(maxAttempts int, strictModels bool, fallbackAPIKey string) {
	prev := h.rt.Load()
	next := &runtimeKnobs{MaxAttempts: config.DefaultMaxAttempts, StrictModels: false, FallbackAPIKey: ""}
	if prev != nil {
		next = &runtimeKnobs{MaxAttempts: prev.MaxAttempts, StrictModels: prev.StrictModels, FallbackAPIKey: prev.FallbackAPIKey}
	}
	if maxAttempts >= 1 {
		next.MaxAttempts = maxAttempts
	}
	next.StrictModels = strictModels
	if fallbackAPIKey != "" {
		next.FallbackAPIKey = fallbackAPIKey
	}
	h.rt.Store(next)
}

// outcomeResult aggregates one upstream attempt for the attempt loop.
type outcomeResult struct {
	outcome    upstream.Outcome
	committed  bool // COMMIT happened: bytes/headers reached the client
	clientGone bool // the client disconnected; stop quietly
	status     int  // status written to the client (0 = nothing written)
	inTok      int64
	outTok     int64
	// cacheRead/cacheCreate are the Anthropic cache token counters — the
	// injected cache_control makes them non-zero, so accounting adds them to
	// the input side or the dashboard under-reports by most of the request.
	cacheRead   int64
	cacheCreate int64
}

// outcomeClientGone is released for attempts aborted by a client disconnect:
// the key is not at fault, so no state change and no
// failure counter.
var outcomeClientGone = upstream.Outcome{Kind: upstream.KindOK, AccountAction: upstream.AccountNone}

// msgRequest is the minimal view of the /v1/messages body: only the fields
// lists are interpreted; the rest is forwarded untouched.
type msgRequest struct {
	Model    string `json:"model"`
	Stream   bool   `json:"stream"`
	Metadata struct {
		UserID string `json:"user_id"`
	} `json:"metadata"`
}

// ServeMessages implements POST /v1/messages with the failover attempt loop.
//
// COMMIT invariant: failover happens only before the first byte reaches the
// client. Once committed, a mid-stream upstream error is forwarded verbatim
// as an SSE "event: error" and never retried.
func (h *Handler) ServeMessages(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rid := admin.RequestIDFrom(r.Context())

	body, err := readBody(w, r, h.maxBodyBytes)
	if err != nil {
		return // response already written
	}

	var parsed msgRequest
	if err := json.Unmarshal(body, &parsed); err != nil {
		WriteError(w, http.StatusBadRequest, errTypeInvalidRequest,
			"request body must be a JSON object", 0, 0)
		return
	}

	modelOut, ruleID := h.mapping.Resolve(parsed.Model)
	if ruleID == "" && h.rt.Load().StrictModels {
		WriteError(w, http.StatusNotFound, errTypeNotFound,
			fmt.Sprintf("model %q is not allowed (STRICT_MODELS)", parsed.Model), 0, 0)
		return
	}
	if modelOut != parsed.Model {
		if body, err = rewriteModel(body, modelOut); err != nil {
			WriteError(w, http.StatusBadRequest, errTypeInvalidRequest,
				"could not rewrite the model field", 0, 0)
			return
		}
	}

	// Channel A parity: one attribution per CLIENT request — session and
	// trace/query ids stay fixed for the turn, while every outbound attempt
	// (failover or same-account replay) mints its own request id, matching
	// the official client's one-request-id-per-HTTP-attempt rhythm.
	attr := upstream.NewAttribution()
	var (
		tried        = make(map[string]bool)
		attempts     int
		last         outcomeResult
		prevID       string
		prevName     string
		deadline     = time.Now().Add(h.attemptDeadline)
		sessionKey   = parsed.Metadata.UserID // "" when absent; never guessed
		fallbackUsed bool
	)

	for attempt := 1; attempt <= h.rt.Load().MaxAttempts; attempt++ {
		// ATTEMPT_DEADLINE bounds the total time spent before COMMIT.
		// A committed response never returns here.
		if attempts > 0 && !time.Now().Before(deadline) {
			break
		}

		lease, aerr := h.pool.Acquire(sessionKey, tried, modelOut)
		if aerr != nil {
			if errors.Is(aerr, pool.ErrPoolExhausted) && !fallbackUsed {
				if fb := h.rt.Load().FallbackAPIKey; fb != "" {
					fallbackUsed = true
					attempts++
					w.Header().Set(headerAttempts, strconv.Itoa(attempts))
					w.Header().Set(headerChannel, channelB)
					if prevID != "" {
						h.logs.Failover(admin.FailoverEntry{
							Ts: time.Now().Unix(), RID: rid,
							From: prevID, To: "(fallback)", Kind: string(last.outcome.Kind),
						})
					}
					res := h.runFallbackAttempt(w, r, fb, body, parsed.Stream)
					last = res
					if res.clientGone {
						h.logRequest(rid, "", "", channelB, parsed, statusClientClosed, attempts, start, res.inTok, res.outTok)
						return
					}
					if res.committed {
						h.logRequest(rid, "", "(fallback)", channelB, parsed, res.status, attempts, start, res.inTok, res.outTok)
						return
					}
					break // channel B also failed before COMMIT: attempts over
				}
			}
			var ae *pool.AcquireError
			if errors.As(aerr, &ae) {
				if ae.Failure == pool.AcquireTriedAll && attempts > 0 {
					// Every account was tried: the last upstream outcome (quota,
					// model-busy, overload) says more than "all tried".
					break
				}
				status, el, retryAfter := acquireErrorToAnthropic(ae)
				h.logRequest(rid, prevID, prevName, "", parsed, status, attempts, start, last.inTok, last.outTok)
				WriteError(w, status, el.Type, el.Message, retryAfter, attempts)
				return
			}
			h.logRequest(rid, prevID, prevName, "", parsed, statusOverloaded, attempts, start, last.inTok, last.outTok)
			WriteError(w, statusOverloaded, errTypeOverloaded,
				"gateway is shutting down", 0, attempts)
			return
		}
		attempts++
		// Visible to the client as soon as any status is committed; a later
		// attempt overwrites it before that commit.
		w.Header().Set(headerAttempts, strconv.Itoa(attempts))
		w.Header().Set(headerChannel, channelA)

		if prevID != "" {
			h.logs.Failover(admin.FailoverEntry{
				Ts: time.Now().Unix(), RID: rid,
				From: prevID, To: lease.AccountID(), Kind: string(last.outcome.Kind),
			})
		}

		res := h.runAttempt(w, r, lease, body, attr, parsed.Stream)
		tried[lease.AccountID()] = true // one attempt per account per request
		prevID, prevName = lease.AccountID(), lease.AccountName()
		last = res

		if res.clientGone {
			h.logRequest(rid, prevID, prevName, channelA, parsed, statusClientClosed, attempts, start, res.inTok, res.outTok)
			return
		}
		if res.committed {
			h.logRequest(rid, prevID, prevName, channelA, parsed, res.status, attempts, start, res.inTok, res.outTok)
			return
		}
		if !res.outcome.Retryable {
			break // defensive: non-retryable outcome that did not commit
		}
		if res.outcome.Kind == upstream.KindUpstreamOverloaded {
			break // overload replay budget spent: another account only adds load
		}
	}

	status, el, retryAfter := exhaustedOutcomeToAnthropic(last)
	h.logRequest(rid, prevID, prevName, "", parsed, status, attempts, start, last.inTok, last.outTok)
	WriteError(w, status, el.Type, el.Message, retryAfter, attempts)
}

// channelA/channelB are the `channel` values of the `event: request` log
// entry.
const (
	channelA = "A"
	channelB = "B"
)

// Same-account replay pauses: the model-busy family follows the official
// admission rhythm (1s then 2s); the platform-overload family uses a longer
// backoff (1s then 3s) and, once spent, ends the request instead of failing
// over — switching accounts during a platform-wide overload only amplifies
// the traffic.
var (
	busyRetryDelays     = [2]time.Duration{time.Second, 2 * time.Second}
	overloadRetryDelays = [2]time.Duration{time.Second, 3 * time.Second}
)

// sameAccountReplayDelay reports the pause before replaying the same lease
// for a replayable-in-place outcome kind, and whether the two-replay budget
// still allows it.
func sameAccountReplayDelay(kind upstream.Kind, try int) (time.Duration, bool) {
	var delays [2]time.Duration
	switch kind {
	case upstream.KindModelBusy:
		delays = busyRetryDelays
	case upstream.KindUpstreamOverloaded:
		delays = overloadRetryDelays
	default:
		return 0, false
	}
	if try >= len(delays) {
		return 0, false
	}
	return delays[try], true
}

// runAttempt performs one channel A attempt with the leased account:
// POST {UPSTREAM_URL}/api/v1/zcode-plan/anthropic/v1/messages
// with Authorization: Bearer <JWT> and the model-request parity set.
//
// An outcome that is not the account's fault (model-busy 3010/3008/3009, or
// the platform-wide overload 1305/529) is replayed on the SAME lease — up to
// two replays inside this function, per the family's pause table: the
// account takes no state change and the outer failover loop never
// re-acquires a tried account. The lease is released when the attempt,
// replays included, ends; nothing panics past the COMMIT (stream errors
// become outcomes).
func (h *Handler) runAttempt(w http.ResponseWriter, r *http.Request, lease *pool.Lease, body []byte, attr upstream.Attribution, wantStream bool) (res outcomeResult) {
	defer func() {
		lease.AddUsage(res.inTok+res.cacheRead+res.cacheCreate, res.outTok)
		lease.Release(res.outcome)
	}()
	for try := 0; ; try++ {
		res = h.oneUpstreamAttempt(w, r, lease, body, attr, wantStream)
		if res.committed || res.clientGone {
			return
		}
		delay, ok := sameAccountReplayDelay(res.outcome.Kind, try)
		if !ok {
			return
		}
		// Replay pause, cut short when the client hangs up meanwhile.
		timer := time.NewTimer(delay)
		select {
		case <-r.Context().Done():
			timer.Stop()
			res.clientGone = true
			res.outcome = outcomeClientGone
			return
		case <-timer.C:
		}
	}
}

// oneUpstreamAttempt performs exactly one upstream call with the lease. It
// never releases the lease (runAttempt owns it) and never panics past the
// COMMIT.
func (h *Handler) oneUpstreamAttempt(w http.ResponseWriter, r *http.Request, lease *pool.Lease, body []byte, attr upstream.Attribution, wantStream bool) (res outcomeResult) {
	// The captcha header is never set here: upstream runs chat with
	// skip_model_request=true and a captcha header on chat triggers WAF 3012.
	// The body identity is per-attempt: metadata.user_id carries the leased
	// account's device fingerprint AND its stable per-device session id (the
	// official client reuses one session per device across turns), while the
	// request id is minted fresh for this outbound call — the official client
	// issues one per upstream HTTP attempt (session/trace/query unchanged).
	attemptAttr := attr.WithSessionID(lease.DeviceMid()).WithFreshRequestID()
	attemptBody := upstream.InjectModelIdentity(body, lease.DeviceMid(), attemptAttr)
	req, err := upstream.BuildChannelARequest(r.Context(), h.upstreamURL,
		lease.JWT(), attemptAttr, r.Header, attemptBody)
	if err != nil {
		res.outcome = upstream.ClassifyTransportError(err)
		return
	}
	if os.Getenv("ZCODE_DEBUG_DUMP") != "" {
		// Ground-truth dump of the exact bytes Go writes upstream (WAF 3012
		// forensics): headers in wire order + the unescaped body.
		if dump, derr := httputil.DumpRequestOut(req, true); derr == nil {
			dumpPath := os.Getenv("ZCODE_DEBUG_DUMP")
			os.WriteFile(dumpPath, dump, 0o600)
		}
	}

	// No Client.Timeout anywhere: streams may run for minutes. Cancellation
	// rides on r.Context: when the client goes, the upstream request is
	// canceled too.
	client := &http.Client{Transport: upstream.TransportFor(lease.ProxyURL())}
	resp, err := client.Do(req)
	if err != nil {
		if r.Context().Err() != nil {
			res.clientGone = true
			res.outcome = outcomeClientGone
		} else {
			res.outcome = upstream.ClassifyTransportError(err)
		}
		return
	}

	// SSE first-event lookahead only applies to HTTP 200 streaming responses:
	// an HTTP error — even for a
	// stream:true request — carries a JSON body without SSE framing and is
	// classified on the plain path.
	if wantStream && resp.StatusCode == http.StatusOK && isSSEContent(resp.Header) {
		return h.runStreamAttempt(w, r, lease, resp)
	}
	return h.runPlainAttempt(w, r, lease, resp)
}

// runFallbackAttempt performs the single channel B attempt with the fallback
// API key: POST {FALLBACK_URL}/api/anthropic with
// x-api-key, reached only after the pool returned ErrPoolExhausted. There is
// no lease: the fallback key's own counters are out of scope of the pool.
func (h *Handler) runFallbackAttempt(w http.ResponseWriter, r *http.Request, apiKey string, body []byte, wantStream bool) (res outcomeResult) {
	req, err := upstream.BuildChannelBRequest(r.Context(), h.fallbackURL, apiKey, r.Header, body)
	if err != nil {
		res.outcome = upstream.ClassifyTransportError(err)
		return
	}

	client := &http.Client{Transport: upstream.TransportFor("")}
	resp, err := client.Do(req)
	if err != nil {
		if r.Context().Err() != nil {
			res.clientGone = true
			res.outcome = outcomeClientGone
		} else {
			res.outcome = upstream.ClassifyTransportError(err)
		}
		return
	}

	if wantStream && resp.StatusCode == http.StatusOK && isSSEContent(resp.Header) {
		return h.runStreamAttempt(w, r, nil, resp)
	}
	return h.runPlainAttempt(w, r, nil, resp)
}

// isSSEContent reports whether the response head announces an SSE body. An
// absent Content-Type is treated as SSE for a streaming request, since that
// is what the upstream was asked for.
func isSSEContent(h http.Header) bool {
	ct := strings.TrimSpace(h.Get("Content-Type"))
	return ct == "" || strings.Contains(strings.ToLower(ct), "text/event-stream")
}

// runStreamAttempt handles a streaming response up to and past the COMMIT.
// It closes the upstream body but never releases the lease: the caller
// (runAttempt, or the fallback path with a nil lease) owns it.
func (h *Handler) runStreamAttempt(w http.ResponseWriter, r *http.Request, lease *pool.Lease, resp *http.Response) (res outcomeResult) {
	defer func() {
		resp.Body.Close() // also unblocks an abandoned first-event read
	}()

	br := bufio.NewReader(resp.Body)
	buf, event, data, ferr := ReadFirstEvent(r.Context(), br, h.firstEventMaxBytes, h.firstEventTimeout)
	if ferr != nil {
		switch {
		case errors.Is(ferr, context.Canceled) || r.Context().Err() != nil:
			res.clientGone = true
			res.outcome = outcomeClientGone
		default:
			// First-event timeout or transport failure: nothing committed,
			// so the attempt may fail over.
			res.outcome = upstream.ClassifyTransportError(ferr)
		}
		return
	}

	if event == "error" {
		// HTTP 200 with an SSE error as the first event: classify exactly
		// like a non-200 body.
		res.outcome = upstream.Classify(http.StatusOK, data, resp.Header)
		if res.outcome.Retryable {
			return // failover before COMMIT
		}
		// Non-retryable (client_error/unknown): commit and forward the event
		// in SSE form — the client asked for a stream.
		h.commitStream(w, resp)
		if _, werr := w.Write(buf); werr != nil {
			res.clientGone = true
			res.outcome = outcomeClientGone
			return
		}
		flushWriter(w)
		res.committed = true
		res.status = http.StatusOK
		return
	}

	// COMMIT — the point of no return. From here on
	// the response status is fixed at 200 and no retry is possible.
	h.commitStream(w, resp)
	if _, werr := w.Write(buf); werr != nil {
		res.clientGone = true
		res.outcome = outcomeClientGone
		return
	}
	flushWriter(w)
	res.committed = true
	res.status = http.StatusOK

	h.streamRest(w, r, br, resp, &res)
	return
}

// commitStream writes the response head: forwarded upstream headers plus the
// gateway attempts header, then the 200 status.
func (h *Handler) commitStream(w http.ResponseWriter, resp *http.Response) {
	h.forwardHeaders(w.Header(), resp.Header)
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "text/event-stream")
	}
	w.WriteHeader(http.StatusOK)
}

// streamTerminalFrame is the SSE error frame appended when the upstream
// stream ends abnormally after COMMIT (EOF before message_stop, or a
// transport failure). The message prefix "[1312][" matches the client-side
// failure classifier's code extraction (^\[(\d{4})\](?=\[), so agent clients
// recognize the frame as retryable instead of seeing a silently truncated
// stream.
const streamTerminalFrame = "\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\"," +
	"\"code\":\"overloaded_error\",\"message\":\"[1312][upstream stream interrupted before message_stop]\"}}\n\n"

// writeStreamTerminal appends the terminal error frame after an abnormal
// stream end. Best-effort: the client may already be gone.
func (h *Handler) writeStreamTerminal(w http.ResponseWriter) {
	_, _ = io.WriteString(w, streamTerminalFrame)
	flushWriter(w)
}

// streamRest copies the remainder of an SSE stream to the client, flushing
// per event. It parses passing events to (a) count usage tokens from
// message_start/message_delta without buffering the whole stream
// and (b) detect mid-stream error events,
// which are forwarded verbatim and never retried.
func (h *Handler) streamRest(w http.ResponseWriter, r *http.Request, br *bufio.Reader, resp *http.Response, res *outcomeResult) {
	var (
		pending []byte
		evName  string
		data    bytes.Buffer
		// sawMessageStop distinguishes a complete stream from one the
		// upstream cut short: a clean EOF without message_stop is still an
		// interrupted stream.
		sawMessageStop bool
	)

	writePending := func() error {
		if len(pending) == 0 {
			return nil
		}
		_, err := w.Write(pending)
		if err == nil {
			flushWriter(w)
		}
		pending = pending[:0]
		return err
	}

	handleEvent := func() bool { // false = stop streaming
		switch evName {
		case "message_start":
			var m struct {
				Message struct {
					Usage struct {
						InputTokens              int64 `json:"input_tokens"`
						OutputTokens             int64 `json:"output_tokens"`
						CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
						CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
					} `json:"usage"`
				} `json:"message"`
			}
			if json.Unmarshal(bytes.TrimSpace(data.Bytes()), &m) == nil {
				res.inTok = m.Message.Usage.InputTokens
				if m.Message.Usage.CacheCreationInputTokens > res.cacheCreate {
					res.cacheCreate = m.Message.Usage.CacheCreationInputTokens
				}
				if m.Message.Usage.CacheReadInputTokens > res.cacheRead {
					res.cacheRead = m.Message.Usage.CacheReadInputTokens
				}
			}
		case "message_stop":
			sawMessageStop = true
		case "message_delta":
			var m struct {
				Usage struct {
					InputTokens              int64 `json:"input_tokens"`
					OutputTokens             int64 `json:"output_tokens"`
					CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
					CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
				} `json:"usage"`
			}
			if json.Unmarshal(bytes.TrimSpace(data.Bytes()), &m) == nil {
				if m.Usage.OutputTokens > 0 {
					res.outTok = m.Usage.OutputTokens // cumulative per Anthropic
				}
				// Some upstreams answer message_start with input_tokens 0 and
				// only report the real count in message_delta (observed live on
				// the zcode-plan channel): prefer a positive delta count.
				if m.Usage.InputTokens > 0 {
					res.inTok = m.Usage.InputTokens
				}
				// Usage fields are cumulative snapshots, so merging across
				// events must take the max — adding would double-count.
				if m.Usage.CacheCreationInputTokens > res.cacheCreate {
					res.cacheCreate = m.Usage.CacheCreationInputTokens
				}
				if m.Usage.CacheReadInputTokens > res.cacheRead {
					res.cacheRead = m.Usage.CacheReadInputTokens
				}
			}
		case "error":
			// Mid-stream error after COMMIT: forward (already written above)
			// and stop. The key state still follows the outcome.
			res.outcome = upstream.Classify(http.StatusOK, data.Bytes(), resp.Header)
			return false
		}
		evName = ""
		data.Reset()
		return true
	}

	for {
		frag, ferr := br.ReadSlice('\n')
		switch {
		case errors.Is(ferr, bufio.ErrBufferFull):
			pending = append(pending, frag...)
			if len(pending) >= flushThreshold {
				if err := writePending(); err != nil {
					res.clientGone = true
					res.outcome = outcomeClientGone
					return
				}
			}
			continue

		case errors.Is(ferr, io.EOF):
			pending = append(pending, frag...)
			_ = writePending()
			if sawMessageStop {
				res.outcome = upstream.Outcome{Kind: upstream.KindOK, AccountAction: upstream.AccountResetStreak}
				return
			}
			// EOF before message_stop: the upstream cut the stream short
			// after COMMIT — the account takes a failure-streak hit and the
			// client still gets a terminal error frame instead of a silently
			// truncated stream.
			res.outcome = upstream.Outcome{Kind: upstream.KindUpstreamUnavailable, AccountAction: upstream.AccountFailStreak, Retryable: true}
			h.writeStreamTerminal(w)
			return

		case ferr != nil:
			_ = writePending()
			if r.Context().Err() != nil {
				res.clientGone = true
				res.outcome = outcomeClientGone
				return
			}
			// Upstream died mid-stream after COMMIT: no retry, the account
			// only takes a failure-streak hit — but the client still gets a
			// terminal error frame instead of a truncated stream.
			res.outcome = upstream.ClassifyTransportError(ferr)
			h.writeStreamTerminal(w)
			return
		}

		// Complete line: buffer it for the client and parse it.
		pending = append(pending, frag...)
		line := bytes.TrimSuffix(frag, []byte{'\n'})
		line = bytes.TrimSuffix(line, []byte{'\r'})

		if isBlankLine(line) {
			// Event boundary: flush the event to the client first, then
			// interpret it.
			if err := writePending(); err != nil {
				res.clientGone = true
				res.outcome = outcomeClientGone
				return
			}
			if !handleEvent() {
				return
			}
			continue
		}

		field, value, _ := bytes.Cut(line, []byte{':'})
		switch string(field) {
		case "event":
			evName = strings.TrimSpace(string(value))
		case "data":
			data.WriteString(strings.TrimPrefix(string(value), " "))
			data.WriteByte('\n')
		}
	}
}

// runPlainAttempt handles a non-streaming response: read the (bounded) body,
// classify it, and either fail over or COMMIT. It closes the upstream body
// but never releases the lease (nil for a fallback attempt): the caller owns
// it.
func (h *Handler) runPlainAttempt(w http.ResponseWriter, r *http.Request, lease *pool.Lease, resp *http.Response) (res outcomeResult) {
	defer func() {
		resp.Body.Close()
	}()

	respBody, rerr := io.ReadAll(io.LimitReader(resp.Body, int64(h.firstEventMaxBytes)+1))
	if rerr != nil {
		if r.Context().Err() != nil {
			res.clientGone = true
			res.outcome = outcomeClientGone
		} else {
			res.outcome = upstream.ClassifyTransportError(rerr)
		}
		return
	}

	// Body over the pre-COMMIT budget: cannot be a short JSON error — commit
	// and stream the rest verbatim.
	if len(respBody) > h.firstEventMaxBytes {
		h.forwardHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		if _, werr := w.Write(respBody); werr != nil {
			res.clientGone = true
			res.outcome = outcomeClientGone
			return
		}
		if _, cerr := io.Copy(w, resp.Body); cerr != nil {
			res.clientGone = true
			res.outcome = outcomeClientGone
			return
		}
		flushWriter(w)
		res.committed = true
		res.status = resp.StatusCode
		res.outcome = upstream.Outcome{Kind: upstream.KindOK, AccountAction: upstream.AccountResetStreak}
		return
	}

	res.outcome = upstream.Classify(resp.StatusCode, respBody, resp.Header)
	switch res.outcome.Kind {
	case upstream.KindOK:
		// Token usage from the response body.
		var u struct {
			Usage struct {
				InputTokens              int64 `json:"input_tokens"`
				OutputTokens             int64 `json:"output_tokens"`
				CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
				CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(respBody, &u) == nil {
			res.inTok = u.Usage.InputTokens
			res.outTok = u.Usage.OutputTokens
			res.cacheCreate = u.Usage.CacheCreationInputTokens
			res.cacheRead = u.Usage.CacheReadInputTokens
		}
		h.forwardHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		if _, werr := w.Write(respBody); werr != nil {
			res.clientGone = true
			res.outcome = outcomeClientGone
			return
		}
		flushWriter(w)
		res.committed = true
		res.status = resp.StatusCode

	case upstream.KindClientError, upstream.KindUnknown:
		// No retry, no key-state change: forward the
		// upstream status and body verbatim.
		h.forwardHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		if _, werr := w.Write(respBody); werr != nil {
			res.clientGone = true
			res.outcome = outcomeClientGone
			return
		}
		flushWriter(w)
		res.committed = true
		res.status = resp.StatusCode

	default:
		// Retryable (rate_limit / usage_limit / auth / no_balance /
		// upstream_unavailable): the loop fails over to the next key.
	}
	return
}

// forwardHeaders copies the client-facing response headers: content-type,
// request-id, retry-after and every anthropic-ratelimit-* header. Anything
// else stays out.
func (h *Handler) forwardHeaders(dst, src http.Header) {
	for _, name := range []string{"Content-Type", "Request-Id", headerRetryAfter} {
		if v := src.Get(name); v != "" {
			dst.Set(name, v)
		}
	}
	for name, vals := range src {
		if strings.HasPrefix(strings.ToLower(name), "anthropic-ratelimit-") {
			for _, v := range vals {
				dst.Add(name, v)
			}
		}
	}
}

// logRequest emits the request log entry. It never
// carries prompt, response, or secret material.
func (h *Handler) logRequest(rid, accID, accName, channel string, parsed msgRequest, status, attempts int, start time.Time, inTok, outTok int64) {
	h.logs.Request(admin.RequestEntry{
		Ts:       time.Now().Unix(),
		RID:      rid,
		Acc:      accID,
		Name:     accName,
		Channel:  channel,
		ModelIn:  parsed.Model,
		ModelOut: h.resolveOut(parsed.Model),
		Stream:   parsed.Stream,
		Status:   status,
		Attempts: attempts,
		MS:       time.Since(start).Milliseconds(),
		InTok:    inTok,
		OutTok:   outTok,
	})
}

func (h *Handler) resolveOut(modelIn string) string {
	out, _ := h.mapping.Resolve(modelIn)
	return out
}

// readBody reads the request body under the MAX_BODY_BYTES limit and answers
// 413 request_too_large on overflow.
func readBody(w http.ResponseWriter, r *http.Request, maxBytes int) ([]byte, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(maxBytes)))
	if err == nil {
		return body, nil
	}
	var mbe *http.MaxBytesError
	if errors.As(err, &mbe) {
		WriteError(w, http.StatusRequestEntityTooLarge, errTypeRequestTooLarge,
			"request body exceeds the size limit", 0, 0)
	} else {
		WriteError(w, http.StatusBadRequest, errTypeInvalidRequest,
			"could not read the request body", 0, 0)
	}
	return nil, err
}

// rewriteModel swaps the model field while keeping every other JSON field
// byte-for-byte (json.RawMessage).
func rewriteModel(body []byte, model string) ([]byte, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("proxy: rewrite model: %w", err)
	}
	if doc == nil {
		doc = make(map[string]json.RawMessage, 1)
	}
	raw, err := json.Marshal(model)
	if err != nil {
		return nil, fmt.Errorf("proxy: rewrite model: %w", err)
	}
	doc["model"] = raw
	out, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("proxy: rewrite model: %w", err)
	}
	return out, nil
}

func flushWriter(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}
