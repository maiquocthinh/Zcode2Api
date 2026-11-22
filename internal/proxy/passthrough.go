package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"zcode2api/internal/admin"
	"zcode2api/internal/pool"
	"zcode2api/internal/upstream"
)

// ServeCountTokens implements POST /v1/messages/count_tokens:
// the inbound model is mapped, then the request is forwarded exactly
// once — no failover. A 404 from upstream is passed through verbatim so the
// client falls back to estimating the count itself.
func (h *Handler) ServeCountTokens(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rid := admin.RequestIDFrom(r.Context())

	body, err := readBody(w, r, h.maxBodyBytes)
	if err != nil {
		return // response already written
	}

	var parsed struct {
		Model string `json:"model"`
	}
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

	lease, aerr := h.pool.Acquire("", nil, modelOut)
	if aerr != nil {
		if errors.Is(aerr, pool.ErrPoolExhausted) {
			if fb := h.rt.Load().FallbackAPIKey; fb != "" {
				h.countTokensFallback(w, r, fb, body, parsed.Model, start, rid)
				return
			}
		}
		status, el, retryAfter := statusOverloaded,
			anthropicErrorEl{Type: errTypeOverloaded, Message: "gateway is shutting down"}, time.Duration(0)
		var ae *pool.AcquireError
		if errors.As(aerr, &ae) {
			status, el, retryAfter = acquireErrorToAnthropic(ae)
		}
		h.logCountTokens(rid, nil, parsed.Model, status, 0, start)
		WriteError(w, status, el.Type, el.Message, retryAfter, 0)
		return
	}
	h.countTokensUpstream(w, r, lease, body, parsed.Model, start, rid)
}

// countTokensUpstream performs the single forwarded call; every path releases
// the lease exactly once.
func (h *Handler) countTokensUpstream(w http.ResponseWriter, r *http.Request, lease *pool.Lease, body []byte, modelIn string, start time.Time, rid string) {
	const attempts = 1
	w.Header().Set(headerAttempts, strconv.Itoa(attempts))
	w.Header().Set(headerChannel, channelA)

	// Channel A: the same ZCode headers and JWT as the
	// chat flow — and never a captcha header.
	req, berr := upstream.BuildChannelARequestAt(r.Context(), h.upstreamURL,
		upstream.PathChannelACountTokens, lease.JWT(), lease.DeviceMid(), r.Header, body)
	if berr != nil {
		out := upstream.ClassifyTransportError(berr)
		lease.Release(out)
		h.logCountTokens(rid, lease, modelIn, statusOverloaded, attempts, start)
		WriteError(w, statusOverloaded, errTypeOverloaded,
			"could not build the upstream request", 0, attempts)
		return
	}

	// No Client.Timeout: same upstream rules as /v1/messages.
	client := &http.Client{Transport: upstream.TransportFor(lease.ProxyURL())}
	resp, derr := client.Do(req)
	if derr != nil {
		lease.Release(upstream.ClassifyTransportError(derr))
		h.logCountTokens(rid, lease, modelIn, statusOverloaded, attempts, start)
		WriteError(w, statusOverloaded, errTypeOverloaded,
			"upstream is unavailable", 0, attempts)
		return
	}

	respBody, rerr := io.ReadAll(io.LimitReader(resp.Body, int64(h.firstEventMaxBytes)+1))
	resp.Body.Close()
	if rerr != nil {
		if r.Context().Err() != nil {
			lease.Release(outcomeClientGone)
			return // client is gone; nothing else to write
		}
		lease.Release(upstream.ClassifyTransportError(rerr))
		h.logCountTokens(rid, lease, modelIn, statusOverloaded, attempts, start)
		WriteError(w, statusOverloaded, errTypeOverloaded,
			"upstream is unavailable", 0, attempts)
		return
	}

	out := upstream.Classify(resp.StatusCode, respBody, resp.Header)
	lease.Release(out)

	h.forwardHeaders(w.Header(), resp.Header)
	switch out.Kind {
	case upstream.KindOK, upstream.KindClientError, upstream.KindUnknown:
		// Verbatim passthrough, including the documented 404 case.
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(respBody)
		flushWriter(w)
		h.logCountTokens(rid, lease, modelIn, resp.StatusCode, attempts, start)

	default:
		// Retryable upstream errors are not failed over here (single-shot),
		// but the client still gets an Anthropic-shaped error.
		status, el := anthropicForKind(out)
		h.logCountTokens(rid, lease, modelIn, status, attempts, start)
		WriteError(w, status, el.Type, el.Message, 0, attempts)
	}
}

// countTokensFallback performs the single-shot count_tokens on channel B
// (pool exhausted + FALLBACK_API_KEY configured). There is
// no lease behind a fallback attempt.
func (h *Handler) countTokensFallback(w http.ResponseWriter, r *http.Request, apiKey string, body []byte, modelIn string, start time.Time, rid string) {
	const attempts = 1
	w.Header().Set(headerAttempts, strconv.Itoa(attempts))
	w.Header().Set(headerChannel, channelB)

	req, berr := upstream.BuildChannelBRequestAt(r.Context(), h.fallbackURL,
		upstream.PathChannelBCountTokens, apiKey, r.Header, body)
	if berr != nil {
		h.logCountTokens(rid, nil, modelIn, statusOverloaded, attempts, start)
		WriteError(w, statusOverloaded, errTypeOverloaded,
			"could not build the upstream request", 0, attempts)
		return
	}
	client := &http.Client{Transport: upstream.TransportFor("")}
	resp, derr := client.Do(req)
	if derr != nil {
		h.logCountTokens(rid, nil, modelIn, statusOverloaded, attempts, start)
		WriteError(w, statusOverloaded, errTypeOverloaded,
			"upstream is unavailable", 0, attempts)
		return
	}
	respBody, rerr := io.ReadAll(io.LimitReader(resp.Body, int64(h.firstEventMaxBytes)+1))
	resp.Body.Close()
	if rerr != nil && r.Context().Err() == nil {
		h.logCountTokens(rid, nil, modelIn, statusOverloaded, attempts, start)
		WriteError(w, statusOverloaded, errTypeOverloaded,
			"upstream is unavailable", 0, attempts)
		return
	}
	if rerr != nil {
		return // client is gone; nothing else to write
	}

	out := upstream.Classify(resp.StatusCode, respBody, resp.Header)
	h.forwardHeaders(w.Header(), resp.Header)
	switch out.Kind {
	case upstream.KindOK, upstream.KindClientError, upstream.KindUnknown:
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(respBody)
		flushWriter(w)
		h.logCountTokens(rid, nil, modelIn, resp.StatusCode, attempts, start)
	default:
		status, el := anthropicForKind(out)
		h.logCountTokens(rid, nil, modelIn, status, attempts, start)
		WriteError(w, status, el.Type, el.Message, 0, attempts)
	}
}

// anthropicForKind maps a retryable upstream kind to the client-facing error,
// used where failover is not available.
func anthropicForKind(out upstream.Outcome) (int, anthropicErrorEl) {
	switch out.Kind {
	case upstream.KindRateLimit, upstream.KindUsageLimit, upstream.KindQuotaExhausted, upstream.KindModelQuotaExhausted, upstream.KindModelBusy:
		return http.StatusTooManyRequests,
			anthropicErrorEl{Type: errTypeRateLimit,
				Message: "upstream account is rate limited or exhausted"}
	case upstream.KindAuth, upstream.KindNoBalance:
		return http.StatusServiceUnavailable,
			anthropicErrorEl{Type: errTypeAPI, Message: "no usable upstream account"}
	default:
		return statusOverloaded,
			anthropicErrorEl{Type: errTypeOverloaded, Message: "upstream is unavailable"}
	}
}

func (h *Handler) logCountTokens(rid string, lease *pool.Lease, modelIn string, status, attempts int, start time.Time) {
	channel := channelA
	if lease == nil {
		channel = channelB // a fallback attempt (no lease)
	}
	h.logs.Request(admin.RequestEntry{
		Ts: time.Now().Unix(), RID: rid,
		Acc: leaseID(lease), Name: leaseName(lease), Channel: channel,
		ModelIn:  modelIn,
		ModelOut: h.resolveOut(modelIn),
		Status:   status,
		Attempts: attempts,
		MS:       time.Since(start).Milliseconds(),
	})
}

func leaseID(l *pool.Lease) string {
	if l == nil {
		return ""
	}
	return l.AccountID()
}

func leaseName(l *pool.Lease) string {
	if l == nil {
		return ""
	}
	return l.AccountName()
}

// ServeModels implements GET /v1/models: the model list
// is generated from the mapping — the inbound names of
// exact rules plus the outbound models of the rules — never hardcoded.
func (h *Handler) ServeModels(w http.ResponseWriter, r *http.Request) {
	ids := make(map[string]bool)
	for _, rule := range h.mapping.Rules() {
		if rule.Target != "" {
			ids[rule.Target] = true // outbound model of the rule
		}
		if strings.HasSuffix(rule.Pattern, "*") {
			continue // prefix patterns are not concrete model ids
		}
		ids[rule.Pattern] = true // inbound name of exact rules (PASSTHROUGH keeps it)
	}
	list := make([]string, 0, len(ids))
	for id := range ids {
		list = append(list, id)
	}
	sort.Strings(list)

	type modelEntry struct {
		Type        string `json:"type"`
		ID          string `json:"id"`
		DisplayName string `json:"display_name"`
	}
	data := make([]modelEntry, 0, len(list))
	for _, id := range list {
		data = append(data, modelEntry{Type: "model", ID: id, DisplayName: id})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(struct {
		Data    []modelEntry `json:"data"`
		HasMore bool         `json:"has_more"`
	}{Data: data, HasMore: false})
}
