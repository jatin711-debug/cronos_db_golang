package api

import (
	"context"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// WireVersion is the current protocol version for zero-downtime upgrades.
const WireVersion = 1

// VersionGate controls feature availability across protocol versions.
type VersionGate struct {
	minClientVersion atomic.Int32
	maxClientVersion atomic.Int32
}

// NewVersionGate creates a version gate.
func NewVersionGate() *VersionGate {
	vg := &VersionGate{}
	vg.minClientVersion.Store(1)
	vg.maxClientVersion.Store(WireVersion)
	return vg
}

// IsCompatible returns true if the client version can talk to this server.
func (vg *VersionGate) IsCompatible(clientVersion int32) bool {
	return clientVersion >= vg.minClientVersion.Load() && clientVersion <= vg.maxClientVersion.Load()
}

// SetMinVersion updates the minimum supported client version (for deprecations).
func (vg *VersionGate) SetMinVersion(v int32) {
	vg.minClientVersion.Store(v)
}

// VersionInterceptor adds wire version compatibility to gRPC metadata.
// Clients send "x-cronos-version" header; servers validate.
func VersionInterceptor(vg *VersionGate) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			vals := md.Get("x-cronos-version")
			if len(vals) > 0 {
				var clientVer int32
				if _, err := fmt.Sscanf(vals[0], "%d", &clientVer); err == nil {
					if !vg.IsCompatible(clientVer) {
						return nil, status.Errorf(codes.FailedPrecondition,
							"client version %d incompatible with server (min=%d max=%d)",
							clientVer, vg.minClientVersion.Load(), vg.maxClientVersion.Load())
					}
				}
			}
		}
		return handler(ctx, req)
	}
}

// Wire protocol compatibility notes:
// - Version 1: Original protocol
// - Future versions add new optional fields; never remove old fields
// - Servers ignore unknown fields (protobuf forward compat)
// - New features gated behind version check

// UpgradePolicy documents zero-downtime rolling upgrade strategy.
//
// 1. Deploy new binary to follower nodes first
// 2. Verify followers catch up and pass health checks
// 3. Trigger leader transfer away from old binary nodes
// 4. Deploy new binary to former leaders
// 5. All nodes running new binary → enable new features
//
// During rolling upgrade, cluster runs in mixed-version mode.
// New features are disabled until all nodes report compatible version.

type UpgradePolicy struct{}

func (UpgradePolicy) ValidateRollingUpgrade(currentVersion, targetVersion int) error {
	if targetVersion < currentVersion {
		return fmt.Errorf("downgrades not supported: %d -> %d", currentVersion, targetVersion)
	}
	if targetVersion-currentVersion > 1 {
		return fmt.Errorf("can only upgrade one major version at a time: %d -> %d", currentVersion, targetVersion)
	}
	return nil
}
