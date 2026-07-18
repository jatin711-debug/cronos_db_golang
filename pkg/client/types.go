package client

import "github.com/jatin711-debug/cronos_db_golang/pkg/types"

// Route describes candidate addresses for a partition-targeted request.
// PreferredAddress is tried first; CandidateAddresses are failover targets.
type Route struct {
	// PartitionID is the target partition for the request.
	PartitionID int32
	// LeaderID is the cluster node ID believed to lead this partition, if known.
	LeaderID string
	// PreferredAddress is the best gRPC address to dial first (usually the leader).
	PreferredAddress string
	// CandidateAddresses lists alternate addresses if PreferredAddress fails or leadership moves.
	CandidateAddresses []string
}

// PartitionMetadata is a lightweight client-facing view of partition placement.
type PartitionMetadata struct {
	// PartitionID is the numeric partition identifier.
	PartitionID int32
	// Topic is the topic associated with the partition, when known.
	Topic string
	// LeaderID is the node ID of the current leader, if advertised.
	LeaderID string
	// ReplicaIDs lists replica node IDs for the partition, if advertised.
	ReplicaIDs []string
}

// partitionMetadataFromProto converts a wire PartitionInfo into PartitionMetadata.
func partitionMetadataFromProto(info *types.PartitionInfo) PartitionMetadata {
	if info == nil {
		return PartitionMetadata{}
	}
	return PartitionMetadata{
		PartitionID: info.GetPartitionId(),
		Topic:       info.GetTopic(),
		LeaderID:    info.GetLeaderId(),
		ReplicaIDs:  append([]string(nil), info.GetReplicaIds()...),
	}
}
