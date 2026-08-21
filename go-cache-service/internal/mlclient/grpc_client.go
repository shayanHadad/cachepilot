package mlclient

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/shayanHadad/cachepilot/internal/cache"
	"github.com/shayanHadad/cachepilot/internal/mlclient/decisionpb"
	"github.com/shayanHadad/cachepilot/internal/types"
)

// Client implements cache.Decider by calling the ML service's
// DecisionService RPC.
type Client struct {
	conn *grpc.ClientConn
	rpc  decisionpb.DecisionServiceClient
}

var _ cache.Decider = (*Client)(nil)

// NewClient dials the ML service at addr (e.g. "localhost:50051" or
// a docker-compose service name like "ml-service:50051") and returns
// a ready-to-use Client.
func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("mlclient: failed to create client for %s: %w", addr, err)
	}

	return &Client{
		conn: conn,
		rpc:  decisionpb.NewDecisionServiceClient(conn),
	}, nil
}

// Decide implements cache.Decider.
func (c *Client) Decide(ctx context.Context, key string, features cache.Features) (types.CacheDecision, error) {
	req := toDecisionRequest(key, features)

	resp, err := c.rpc.Decide(ctx, req)
	if err != nil {
		return types.CacheDecision{}, fmt.Errorf("mlclient: Decide RPC failed: %w", err)
	}

	return fromDecisionResponse(resp), nil
}

// toDecisionRequest turns our own Features struct into the protobuf
// message the ML service expects on the wire.
func toDecisionRequest(key string, f cache.Features) *decisionpb.DecisionRequest {
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

// fromDecisionResponse does the reverse: takes what came back over
// the wire and turns it into the CacheDecision the rest of the
// service actually works with.
func fromDecisionResponse(resp *decisionpb.DecisionResponse) types.CacheDecision {
	return types.CacheDecision{
		Admit:  resp.Admit,
		TTL:    time.Duration(resp.TtlMs) * time.Millisecond,
		Source: resp.Source,
	}
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
