package cache

import (
	"time"

	"github.com/shayanHadad/cachepilot/internal/mlclient/decisionpb"
	"github.com/shayanHadad/cachepilot/internal/types"
)

// ToDecisionRequest converts this package's Features
// into the wire request shape the ML service expects.
func ToDecisionRequest(key string, f Features) *decisionpb.DecisionRequest {
	return &decisionpb.DecisionRequest{
		Key:             key,
		Frequency_1Min:  int32(f.Frequency1Min),
		Frequency_5Min:  int32(f.Frequency5Min),
		RecencySec:      f.RecencySec,
		InterArrivalAvg: f.InterArrivalAvg,
		PayloadSizeKb:   f.PayloadSizeKB,
		QueryType:       f.QueryType,
	}
}

// FromDecisionResponse converts the ML service's wire response into
// this package's own types.CacheDecision.
func FromDecisionResponse(resp *decisionpb.DecisionResponse) types.CacheDecision {
	return types.CacheDecision{
		Admit:  resp.Admit,
		TTL:    time.Duration(resp.TtlMs) * time.Millisecond,
		Source: resp.Source,
	}
}
