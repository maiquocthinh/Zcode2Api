package upstream

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

// Claim-side discovery endpoints (channel A origin, same billing egress):
// the claimable-plan delivery now lives in marketing/touch — billing/preview
// no longer returns plans — and the campaign confirm receipt is posted after
// a successful claim. The activation telemetry keys the campaign off the
// account's daily activity; the endpoint does not check login state, so the
// events carry no Authorization.
const (
	pathChannelAEventReport     = "/api/v1/event/report"
	pathChannelAMarketingTouch  = "/api/v1/marketing/touch"
	pathChannelAMarketingAction = "/api/v1/marketing/touch/action"
)

// Impersonated-client constants of the activation/marketing bodies. They must
// match the identity the request HEADERS claim (the upstream cross-checks),
// never the runtime — the same reasoning as the channel A identity.
const (
	zcodeClientLanguage = "zh-CN"
	zcodeClientTimezone = "Asia/Shanghai"
	activationScreen    = "2560x1440"
	// The delivery action type that carries a claimable plan id.
	claimPlanActionType = "claim_zcode_plan"
)

// activationElements are the events reported before claiming, in order: the
// upstream consumes them sequentially and the daily-active key is deduped per
// device+date upstream, so a failed round must abort instead of retrying
// blindly (the third element was added against the official client's rounds).
var activationElements = []string{"app_launch", "app_daily_active", "app_login_success"}

// marketingSeq is the process-wide touch query counter: the official client
// post-increments (first call seq=0) and upstream uses it for dedup/attribution
// — skipping numbers or pinning a constant is a recognizable difference.
var marketingSeq atomic.Uint64

// OfferedPlan is one claimable plan from a marketing delivery, with the
// campaign id for the post-claim confirm receipt (the delivery is withdrawn
// the moment the claim succeeds, so the id must be captured BEFORE claiming).
type OfferedPlan struct {
	PlanID     string
	Name       string
	Priority   float64
	CampaignID string
}

// jwtUserID extracts the account user id from the JWT payload — the user_id
// claim, falling back to sub (what the activation telemetry reports).
func jwtUserID(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		payload, err = base64.URLEncoding.DecodeString(parts[1])
		if err != nil {
			return ""
		}
	}
	var claims struct {
		UserID string `json:"user_id"`
		Sub    string `json:"sub"`
	}
	if json.Unmarshal(payload, &claims) != nil {
		return ""
	}
	if claims.UserID != "" {
		return claims.UserID
	}
	return claims.Sub
}

// activationBody builds the fixed 16-field event/report body. Device fields
// derive from the channel A identity (the headers' claimed client), so the
// body cannot contradict the headers.
func activationBody(element, userID, deviceMid, appVersion, platform string) map[string]any {
	region := "app"
	detail := map[string]any{}
	if element == "app_login_success" {
		region = "app_profile"
		detail = map[string]any{"login_provider": "zai"}
	}
	category, osVersion := osIdentityForPlatform(platform)
	return map[string]any{
		"event_id":           newRequestUUID(),
		"client_timezone":    zcodeClientTimezone,
		"client_language":    zcodeClientLanguage,
		"element_name":       element,
		"event_region":       region,
		"event_type":         "view",
		"event_text":         "",
		"event_extra_detail": detail,
		"user_id":            userID,
		"screen_resolution":  activationScreen,
		"app_version":        appVersion,
		"device_os_category": category,
		"device_os_version":  osVersion,
		"device_mid":         deviceMid,
		"mac_id":             "",
		"marketing_params":   "{}",
	}
}

// businessCode reads the Z.AI business code of a claim-side body with the
// same extraction order the Classify table uses (error.code > root code). A
// missing or non-numeric code counts as a failure — an error-shaped body
// must never pass as code 0.
func businessCode(body []byte) (int, bool) {
	code := extractCode(body)
	if code == "" {
		return -1, false
	}
	v, err := strconv.Atoi(code)
	if err != nil {
		return -1, false
	}
	return v, true
}

// ReportActivationEvents posts app_launch → app_daily_active →
// app_login_success with the account's device identity and NO Authorization
// (the endpoint does not check login state). First failure aborts the round:
// the daily-active key is deduped upstream per device+date, so a blind retry
// of the whole round is wasted calls.
func ReportActivationEvents(ctx context.Context, acc ChannelAAccount, opts ...CallOption) error {
	cfg := callConfigFor(opts)
	userID := jwtUserID(acc.JWT)
	if userID == "" {
		return fmt.Errorf("upstream: activation events skipped: the JWT carries no user id")
	}
	appVersion, platform := ChannelAIdentity()
	for _, element := range activationElements {
		body, err := json.Marshal(activationBody(element, userID, acc.DeviceMid, appVersion, platform))
		if err != nil {
			return fmt.Errorf("upstream: encode activation event: %w", err)
		}
		req, err := newUpstreamRequest(ctx, joinEndpoint(cfg.origin, pathChannelAEventReport), body)
		if err != nil {
			return fmt.Errorf("upstream: build activation request: %w", err)
		}
		req.Header.Set(hdrUserAgent, "ZCode/"+appVersion)
		b, resp, err := doBillingCall(ctx, cfg, req)
		if err != nil {
			return fmt.Errorf("upstream: activation event %s: %w", element, err)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("upstream: activation event %s answered %s", element, resp.Status)
		}
		if code, ok := businessCode(b); ok && code != 0 {
			return fmt.Errorf("upstream: activation event %s answered code %d", element, code)
		}
	}
	return nil
}

// parseOfferedPlans parses data.deliveries[] of a marketing/touch answer: every
// claim_zcode_plan button action (banner and popup shapes both) becomes one
// plan, carrying its delivery's campaign id and plan metadata.
func parseOfferedPlans(body []byte) ([]OfferedPlan, bool) {
	var doc struct {
		Data struct {
			Deliveries []map[string]any `json:"deliveries"`
		} `json:"data"`
	}
	payload := decodeBody(body)
	if len(payload) == 0 || json.Unmarshal(payload, &doc) != nil {
		return nil, false
	}
	out := make([]OfferedPlan, 0, len(doc.Data.Deliveries))
	for _, delivery := range doc.Data.Deliveries {
		campaignID, _ := delivery["campaign_id"].(string)
		meta := deliveryPlanMeta(delivery)
		for _, args := range deliveryClaimActions(delivery) {
			planID, _ := args["plan_id"].(string)
			if strings.TrimSpace(planID) == "" {
				continue
			}
			plan := OfferedPlan{PlanID: strings.TrimSpace(planID), CampaignID: campaignID}
			if meta != nil {
				if name, ok := meta["name"].(string); ok {
					plan.Name = name
				}
				if p, ok := meta["priority"].(float64); ok {
					plan.Priority = p
				}
			}
			out = append(out, plan)
		}
	}
	return out, true
}

// deliveryClaimActions collects the claim actions of one delivery: the
// delivery is a union discriminated by resource_position, so buttons hang off
// banner.buttons and popup.buttons — both must be read.
func deliveryClaimActions(delivery map[string]any) []map[string]any {
	var out []map[string]any
	for _, holder := range []string{"banner", "popup"} {
		node, ok := delivery[holder].(map[string]any)
		if !ok {
			continue
		}
		buttons, _ := node["buttons"].([]any)
		for _, raw := range buttons {
			button, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			action, _ := button["action"].(map[string]any)
			if action == nil {
				continue
			}
			if kind, _ := action["type"].(string); kind != claimPlanActionType {
				continue
			}
			if args, ok := action["args"].(map[string]any); ok {
				out = append(out, args)
			}
		}
	}
	return out
}

// deliveryPlanMeta reads the plan metadata of one delivery (name/priority):
// one delivery carries one campaign and one plan, so the first hit wins.
func deliveryPlanMeta(delivery map[string]any) map[string]any {
	for _, node := range []any{
		delivery["banner"],
		delivery["popup"],
	} {
		switch n := node.(type) {
		case map[string]any:
			if background, ok := n["background"].(map[string]any); ok {
				if meta := zcodePlanArg(background); meta != nil {
					return meta
				}
			}
			if popup, ok := n["success_popup"].(map[string]any); ok {
				if hero, ok := popup["hero"].(map[string]any); ok {
					if meta := zcodePlanArg(hero); meta != nil {
						return meta
					}
				}
			}
			if hero, ok := n["hero"].(map[string]any); ok {
				if meta := zcodePlanArg(hero); meta != nil {
					return meta
				}
			}
		}
	}
	return nil
}

func zcodePlanArg(node map[string]any) map[string]any {
	args, _ := node["args"].(map[string]any)
	if args == nil {
		return nil
	}
	plan, _ := args["zcode_plan"].(map[string]any)
	return plan
}

// FetchOfferedPlans returns the plans the upstream currently offers the
// account (marketing/touch), sorted by priority (desc, then plan id). An
// empty delivery list is a valid answer — "no campaign right now" is not a
// failure — and only a transport problem or a nonzero business code errors.
func FetchOfferedPlans(ctx context.Context, acc ChannelAAccount, opts ...CallOption) ([]OfferedPlan, error) {
	cfg := callConfigFor(opts)
	appVersion, _ := ChannelAIdentity()
	seq := marketingSeq.Add(1) - 1
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		joinEndpoint(cfg.origin, pathChannelAMarketingTouch)+"?seq="+strconv.FormatUint(seq, 10), nil)
	if err != nil {
		return nil, fmt.Errorf("upstream: build marketing request: %w", err)
	}
	// The official marketing path carries Authorization plus
	// device/language/version and the bare ZCode UA (set explicitly, or Go
	// sends its own default), and deliberately none of the billing source
	// headers (X-Title/X-Platform/X-Os-*) nor a Content-Type.
	h := req.Header
	h.Set(hdrUserAgent, "ZCode/"+appVersion)
	h.Set("Authorization", "Bearer "+acc.JWT)
	h.Set("X-Device-Mid", acc.DeviceMid)
	h.Set("X-Client-Language", zcodeClientLanguage)
	h.Set("X-ZCode-App-Version", appVersion)

	b, resp, err := doBillingCall(ctx, cfg, req)
	if err != nil {
		return nil, fmt.Errorf("upstream: marketing touch: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("upstream: marketing touch answered %s", resp.Status)
	}
	if code, ok := businessCode(b); ok && code != 0 {
		return nil, fmt.Errorf("upstream: marketing touch answered code %d", code)
	}
	plans, ok := parseOfferedPlans(b)
	if !ok {
		return nil, fmt.Errorf("upstream: marketing touch body is not a delivery list")
	}
	sort.SliceStable(plans, func(i, j int) bool {
		if plans[i].Priority != plans[j].Priority {
			return plans[i].Priority > plans[j].Priority
		}
		return plans[i].PlanID < plans[j].PlanID
	})
	return plans, nil
}

// ConfirmCampaign posts the marketing confirm receipt after a successful
// claim. A failed receipt must never fail the claim — the caller only warns.
func ConfirmCampaign(ctx context.Context, acc ChannelAAccount, campaignID string, opts ...CallOption) error {
	if campaignID == "" {
		return nil
	}
	cfg := callConfigFor(opts)
	appVersion, _ := ChannelAIdentity()
	body, err := json.Marshal(map[string]string{"campaign_id": campaignID, "action_type": "confirm"})
	if err != nil {
		return fmt.Errorf("upstream: encode campaign confirm: %w", err)
	}
	req, err := newUpstreamRequest(ctx, joinEndpoint(cfg.origin, pathChannelAMarketingAction), body)
	if err != nil {
		return fmt.Errorf("upstream: build campaign confirm: %w", err)
	}
	h := req.Header
	h.Set(hdrUserAgent, "ZCode/"+appVersion)
	h.Set("Authorization", "Bearer "+acc.JWT)
	h.Set("X-Device-Mid", acc.DeviceMid)
	h.Set("X-Client-Language", zcodeClientLanguage)
	h.Set("X-ZCode-App-Version", appVersion)
	b, resp, err := doBillingCall(ctx, cfg, req)
	if err != nil {
		return fmt.Errorf("upstream: campaign confirm: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("upstream: campaign confirm answered %s", resp.Status)
	}
	if code, ok := businessCode(b); ok && code != 0 {
		return fmt.Errorf("upstream: campaign confirm answered code %d", code)
	}
	return nil
}
