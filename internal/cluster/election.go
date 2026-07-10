package cluster

import "sort"

// ChooseFailoverLeader selects the best new leader for a partition after the
// current leader has failed. It prefers alive ISR members with the highest known
// offset to minimize data loss and replay; if no ISR member is available, it
// falls back to alive replicas. The old leader is excluded from consideration.
// The returned offset is the highest known watermark among the candidates.
func ChooseFailoverLeader(info *PartitionInfo, aliveNodeIDs map[string]bool) (leaderID string, bestOffset int64) {
	if info == nil || len(aliveNodeIDs) == 0 {
		return "", 0
	}

	oldLeader := info.LeaderID

	// Build candidate list from alive ISR members, excluding the failed leader.
	candidates := make([]string, 0, len(info.ISR))
	for _, nodeID := range info.ISR {
		if aliveNodeIDs[nodeID] && nodeID != oldLeader {
			candidates = append(candidates, nodeID)
		}
	}

	// Fall back to replicas if no ISR member is available.
	if len(candidates) == 0 {
		for _, nodeID := range info.Replicas {
			if aliveNodeIDs[nodeID] && nodeID != oldLeader {
				candidates = append(candidates, nodeID)
			}
		}
	}

	if len(candidates) == 0 {
		return "", 0
	}

	// Sort by node ID for deterministic tie-breaking, then choose the highest
	// offset. We sort first so that equal offsets always pick the same replica.
	sort.Strings(candidates)

	bestLeader := candidates[0]
	bestOffset = 0
	if info.ReplicaOffsets != nil {
		bestOffset = info.ReplicaOffsets[bestLeader]
	}

	for _, nodeID := range candidates[1:] {
		offset := int64(0)
		if info.ReplicaOffsets != nil {
			offset = info.ReplicaOffsets[nodeID]
		}
		if offset > bestOffset {
			bestLeader = nodeID
			bestOffset = offset
		}
	}

	return bestLeader, bestOffset
}
