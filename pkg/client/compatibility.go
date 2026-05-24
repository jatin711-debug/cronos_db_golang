package client

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Capability identifies an optional server feature.
type Capability string

const (
	CapabilityMetadataAPI Capability = "metadata_api"
	CapabilityReplayAPI   Capability = "replay_api"
	CapabilitySubscribe   Capability = "subscribe_api"
	CapabilityAck         Capability = "ack_api"
)

// Capabilities reports detected server support.
type Capabilities struct {
	MetadataAPI bool
	ReplayAPI   bool
	Subscribe   bool
	Ack         bool
}

// Supports reports if a capability is available.
func (c Capabilities) Supports(capability Capability) bool {
	switch capability {
	case CapabilityMetadataAPI:
		return c.MetadataAPI
	case CapabilityReplayAPI:
		return c.ReplayAPI
	case CapabilitySubscribe:
		return c.Subscribe
	case CapabilityAck:
		return c.Ack
	default:
		return false
	}
}

// DetectCapabilities probes server APIs and returns compatibility signals.
func (c *Client) DetectCapabilities(ctx context.Context) (Capabilities, error) {
	caps := Capabilities{}
	addresses := c.pool.Addresses()
	if len(addresses) == 0 {
		return caps, wrapError("client.capabilities", ErrorKindUnavailable, fmt.Errorf("no node addresses available"))
	}

	addr := addresses[0]
	partitionClient, err := c.partitionClientForAddress(addr)
	if err == nil {
		reqCtx, cancel := c.requestContext(ctx)
		start := time.Now()
		resp, listErr := partitionClient.ListPartitions(reqCtx, &types.ListPartitionsRequest{})
		cancel()
		c.observeRequest("partition.list", addr, start, listErr)
		if listErr == nil {
			caps.MetadataAPI = true
			if len(resp.GetPartitions()) > 0 {
				replayOK := c.probeReplay(ctx, addr, resp.GetPartitions()[0])
				caps.ReplayAPI = replayOK
			}
		} else if st, ok := status.FromError(listErr); ok && st.Code() == codes.Unimplemented {
			caps.MetadataAPI = false
		}
	}

	eventClient, eventErr := c.eventClientForAddress(addr)
	if eventErr != nil {
		if err != nil {
			return caps, wrapError("client.capabilities", ErrorKindTransport, err)
		}
		return caps, nil
	}

	openCtx, cancel := context.WithCancel(ctx)
	start := time.Now()
	subStream, subErr := eventClient.Subscribe(openCtx)
	c.observeRequest("event.subscribe.open", addr, start, subErr)
	if subErr == nil && subStream != nil {
		caps.Subscribe = true
	}
	cancel()

	openCtx, cancel = context.WithCancel(ctx)
	start = time.Now()
	ackStream, ackErr := eventClient.Ack(openCtx)
	c.observeRequest("event.ack.open", addr, start, ackErr)
	if ackErr == nil && ackStream != nil {
		caps.Ack = true
	}
	cancel()

	return caps, nil
}

// RequireCapabilities validates server support and returns graceful fallback errors.
func (c *Client) RequireCapabilities(ctx context.Context, required ...Capability) error {
	caps, err := c.DetectCapabilities(ctx)
	if err != nil {
		return err
	}
	for _, capability := range required {
		if caps.Supports(capability) {
			continue
		}
		return wrapError("client.capabilities", ErrorKindValidation, fmt.Errorf("server does not support required capability: %s", capability))
	}
	return nil
}

func (c *Client) probeReplay(ctx context.Context, addr string, info *types.PartitionInfo) bool {
	if info == nil {
		return false
	}

	eventClient, err := c.eventClientForAddress(addr)
	if err != nil {
		return false
	}
	streamCtx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()

	start := time.Now()
	stream, replayErr := eventClient.Replay(streamCtx, &types.ReplayRequest{
		Topic:       info.GetTopic(),
		PartitionId: info.GetPartitionId(),
		StartOffset: 0,
		Count:       1,
	})
	c.observeRequest("event.replay.open", addr, start, replayErr)
	if replayErr != nil {
		if st, ok := status.FromError(replayErr); ok && st.Code() == codes.Unimplemented {
			return false
		}
		return true
	}

	start = time.Now()
	_, recvErr := stream.Recv()
	c.observeRequest("event.replay.recv", addr, start, recvErr)
	return recvErr == nil || recvErr == io.EOF
}
