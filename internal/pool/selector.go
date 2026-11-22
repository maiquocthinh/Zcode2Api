package pool

import (
	"sort"
	"time"
)

// selectAccount picks the next account for a request: ACTIVE with spare
// inflight, not already tried, and (when model is set) not marked
// model-exhausted → session affinity → highest remaining quota (unchecked
// counts as full) → round-robin among ties. It must be called with Pool.mu
// held. Returns nil when no account passes.
func selectAccount(
	accounts []*Account,
	sessionKey string,
	tried map[string]bool,
	aff *Affinity,
	affinityEnabled bool,
	inflightLimit int,
	now time.Time,
	cursor *int,
	model string,
) *Account {
	candidates := make([]*Account, 0, len(accounts))
	for _, a := range accounts {
		if a.CanServe(now, inflightLimit) && !tried[a.ID] && !a.ModelExhausted(model, now) {
			candidates = append(candidates, a)
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	if affinityEnabled && sessionKey != "" && aff != nil {
		if id, ok := aff.Get(sessionKey, now); ok {
			for _, a := range candidates {
				if a.ID == id {
					aff.Set(sessionKey, a.ID, now)
					return a
				}
			}
		}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return quotaRank(candidates[i]) > quotaRank(candidates[j])
	})

	top := quotaRank(candidates[0])
	tied := candidates[:1]
	for i := 1; i < len(candidates); i++ {
		if quotaRank(candidates[i]) != top {
			break
		}
		tied = candidates[:i+1]
	}
	chosen := tied[*cursor%len(tied)]
	*cursor++

	if affinityEnabled && sessionKey != "" && aff != nil {
		aff.Set(sessionKey, chosen.ID, now)
	}
	return chosen
}

// quotaRank is the sort key: the known remaining quota, with "never checked"
// ranked as the maximum possible value so unchecked accounts stay ahead of any
// checked number.
func quotaRank(a *Account) int64 {
	if a.RemainingQuota == nil {
		return int64(^uint64(0) >> 1)
	}
	return *a.RemainingQuota
}
