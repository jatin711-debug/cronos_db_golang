package client

import "cronos_db/pkg/types"

// Route describes candidate addresses for a partition request.
type Route struct {
	PartitionID        int32
	LeaderID           string
	PreferredAddress   string
	CandidateAddresses []string
}

// PartitionMetadata is a lightweight client-facing partition metadata view.
type PartitionMetadata struct {
	PartitionID int32
	Topic       string
	LeaderID    string
	ReplicaIDs  []string
}

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
