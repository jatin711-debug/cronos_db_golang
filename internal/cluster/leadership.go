package cluster

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// Partition leadership has one authority: the assignment committed through
// Raft. The hash ring only says where replicas should live and which node
// would ideally lead; it never makes a node the leader by itself. Leadership
// moves in exactly two ways, both committed by the Raft leader:
//
//   - failover, when the committed leader is dead: the reachable replica with
//     the most complete log is elected;
//   - handoff, when the ring prefers another live replica: the current leader
//     first stops accepting publishes, and the target takes over only once its
//     log is shown to equal the leader's.
//
// Every node then follows the committed assignment: it promotes itself when it
// is named leader and steps down when another node is.

// ReplicaPosition describes where one replica's log ends.
type ReplicaPosition struct {
	// Found is false when the node holds no data for the partition.
	Found bool
	// LastOffset and LastTerm identify the last log entry (-1, 0 when empty).
	LastOffset int64
	LastTerm   int64
	// Epoch is the highest leadership epoch the replica has accepted.
	Epoch int64
	// AcceptingWrites is true while the node serves publishes as leader.
	AcceptingWrites bool
}

// ReplicaPositioner reports replica log positions. It is implemented by the
// partition accessor; an accessor without it falls back to the offsets leaders
// last reported, which may be stale.
type ReplicaPositioner interface {
	// ReplicaLogPosition returns the log position of the replica on the node
	// whose internal address is addr. An empty addr means this node.
	ReplicaLogPosition(ctx context.Context, addr string, partitionID int32) (found bool, lastOffset, lastTerm, epoch int64, acceptingWrites bool, err error)
}

// assignmentStore is the committed partition metadata and the way to change it.
type assignmentStore interface {
	IsLeader() bool
	Partition(id int32) (PartitionInfo, bool)
	Partitions() map[int32]PartitionInfo
	ProposePartition(info *PartitionInfo, assign bool) error
}

const (
	// positionQueryTimeout bounds one round of replica position queries.
	positionQueryTimeout = 2 * time.Second
	// transferStartLag is how far behind (in events) a handoff target may be
	// when the handoff starts. Publishes are refused for its duration, so it
	// only starts once the remaining catch-up is short.
	transferStartLag = 50_000
	// transferTimeout and transferTimeoutUnreplicated abandon a handoff that
	// has not completed. Without replication the target copies the whole
	// partition, which takes longer.
	transferTimeout             = 30 * time.Second
	transferTimeoutUnreplicated = 5 * time.Minute
	// transferRetryDelay keeps an abandoned handoff from restarting at once
	// and refusing publishes again.
	transferRetryDelay = time.Minute
)

// committed returns the Raft-committed assignment of a partition.
func (m *Manager) committed(partitionID int32) (PartitionInfo, bool) {
	m.mu.RLock()
	store := m.assignments
	m.mu.RUnlock()
	if store == nil {
		return PartitionInfo{}, false
	}
	info, ok := store.Partition(partitionID)
	if !ok || info.LeaderID == "" {
		return PartitionInfo{}, false
	}
	return info, true
}

// IsPartitionWritable reports whether this node may accept publishes for a
// partition right now: it is the committed leader and no handoff is under way.
func (m *Manager) IsPartitionWritable(partitionID int32) bool {
	if info, ok := m.committed(partitionID); ok {
		return info.LeaderID == m.config.NodeID && info.TransferTo == ""
	}
	if m.leadershipIsCommitted() {
		return false // no leader has been committed for it yet
	}
	return m.router.IsPartitionLeader(partitionID)
}

// leadershipIsCommitted reports whether committed assignments are this node's
// only source of leadership, which they are whenever it has an assignment
// store. A partition without a committed assignment then has no leader here,
// whatever the hash ring would pick: a node that has just joined does not know
// yet what the cluster committed before it arrived, and its ring would have it
// take over partitions that another node is leading.
func (m *Manager) leadershipIsCommitted() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.assignments != nil
}

// localEpochs is implemented by a partition accessor that can say at which
// epoch this node has taken a partition up (0 when it has not).
type localEpochs interface {
	GetPartitionEpoch(partitionID int32) int64
}

// LeadershipReady reports whether this node can serve publishes for every
// partition it is supposed to: each partition has a committed leader, and the
// ones committed to this node have been taken up here and are not being handed
// over. While it is false, publishes for the partitions named in the detail
// are refused with a retryable error; that is the case while a cluster forms,
// after a failover until the new leader has loaded the partition, and during
// a handoff.
func (m *Manager) LeadershipReady() (bool, string) {
	m.mu.RLock()
	pa, rt, nodeID := m.partitionAccessor, m.router, m.config.NodeID
	m.mu.RUnlock()
	if rt == nil || !m.leadershipIsCommitted() {
		return true, "leadership follows the hash ring"
	}
	epochs, _ := pa.(localEpochs)

	total, led, unassigned, loading, moving := 0, 0, 0, 0, 0
	for partitionID := range rt.DesiredAssignments() {
		total++
		info, ok := m.committed(partitionID)
		switch {
		case !ok:
			unassigned++
		case info.LeaderID != nodeID:
		case info.TransferTo != "":
			moving++
		case epochs != nil && epochs.GetPartitionEpoch(partitionID) < info.Epoch:
			loading++
		default:
			led++
		}
	}
	if unassigned+loading+moving == 0 {
		return true, fmt.Sprintf("leading %d of %d partitions", led, total)
	}
	return false, fmt.Sprintf("%d of %d partitions have no committed leader, %d are being loaded here, %d are being handed over",
		unassigned, total, loading, moving)
}

// noteMembershipChange restarts the settle period for first assignments.
func (m *Manager) noteMembershipChange() {
	m.membershipChangedAt.Store(time.Now().UnixNano())
}

// membershipSettled reports whether partitions that never had a leader may
// be given one now.
//
// Nodes of a new cluster start seconds apart. Assigning as soon as the first
// is up would give it every partition, only to hand most of them over, with
// publishes refused meanwhile, as each of the others arrives. So the first
// assignment waits until the cluster is known to be complete: the configured
// number of nodes is alive, or, failing that, membership has not changed for
// the formation wait.
func (m *Manager) membershipSettled() bool {
	if expected := m.config.ExpectedNodes; expected > 0 && len(m.aliveNodes()) >= expected {
		return true
	}
	changed := m.membershipChangedAt.Load()
	return changed == 0 || time.Since(time.Unix(0, changed)) >= m.config.FormationWait
}

// PartitionHasLeader reports whether a leader is known for the partition.
// It is false while a new cluster is still forming.
func (m *Manager) PartitionHasLeader(partitionID int32) bool {
	_, err := m.router.GetPartitionLeader(partitionID)
	return err == nil
}

// kickReconcile asks the background loop to reconcile soon. It never blocks.
func (m *Manager) kickReconcile() {
	select {
	case m.reconcileCh <- struct{}{}:
	default:
	}
}

// aliveNodes returns the addresses of the nodes membership considers alive.
func (m *Manager) aliveNodes() map[string]string {
	alive := make(map[string]string)
	for _, node := range m.membership.GetAliveNodes() {
		alive[node.ID] = node.Address
	}
	return alive
}

// replicaPositions asks each node for its log position, in parallel. Nodes
// that do not answer are left out of the result.
func (m *Manager) replicaPositions(partitionID int32, nodes map[string]string) map[string]ReplicaPosition {
	m.mu.RLock()
	positioner := m.positions
	m.mu.RUnlock()
	if positioner == nil || len(nodes) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(m.ctx, positionQueryTimeout)
	defer cancel()

	var mu sync.Mutex
	var wg sync.WaitGroup
	result := make(map[string]ReplicaPosition, len(nodes))
	for id, addr := range nodes {
		if id == m.config.NodeID {
			addr = ""
		} else if addr == "" {
			continue
		}
		wg.Add(1)
		go func(id, addr string) {
			defer wg.Done()
			found, lastOffset, lastTerm, epoch, accepting, err := positioner.ReplicaLogPosition(ctx, addr, partitionID)
			if err != nil {
				return
			}
			if !found {
				lastOffset, lastTerm = -1, 0
			}
			mu.Lock()
			result[id] = ReplicaPosition{Found: found, LastOffset: lastOffset, LastTerm: lastTerm, Epoch: epoch, AcceptingWrites: accepting}
			mu.Unlock()
		}(id, addr)
	}
	wg.Wait()
	return result
}

// cleanElectionQuorum is how many replicas an election must hear from to be
// sure one of them holds every acknowledged write. Each acknowledged write is
// on at least minISR of the replicas, so any replicas-minISR+1 of them include
// one that has it. The failed leader cannot answer; when the requirement
// exceeds the replicas that could (minISR of 1 keeps writes only the leader
// has), no election can give that guarantee and ok is false.
func cleanElectionQuorum(replicas, minISR int) (need int, ok bool) {
	if minISR < 1 {
		minISR = 1
	}
	need = replicas - minISR + 1
	if need < 1 {
		need = 1
	}
	return need, need <= replicas-1
}

// mostCompleteReplica picks the replica whose log ends last: highest term of
// the last entry, then highest offset, then lowest node ID so that every node
// would make the same choice.
func mostCompleteReplica(positions map[string]ReplicaPosition) string {
	ids := make([]string, 0, len(positions))
	for id := range positions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	best := ""
	for _, id := range ids {
		if best == "" {
			best = id
			continue
		}
		p, b := positions[id], positions[best]
		if p.LastTerm > b.LastTerm || (p.LastTerm == b.LastTerm && p.LastOffset > b.LastOffset) {
			best = id
		}
	}
	return best
}

// electNewLeader replaces a dead partition leader. It asks the surviving
// replicas where their logs end and commits the most complete one. When too
// few replicas answer to be sure none of them is missing acknowledged writes,
// it elects nobody and the partition stays unavailable until more return.
// The caller holds leadershipMu.
func (m *Manager) electNewLeader(partitionID int32, info *PartitionInfo) {
	m.mu.RLock()
	store, rt, hasPositions := m.assignments, m.router, m.positions != nil
	m.mu.RUnlock()

	alive := m.aliveNodes()
	oldLeader := info.LeaderID
	candidates := make(map[string]string)
	for _, id := range info.Replicas {
		if addr, ok := alive[id]; ok && id != oldLeader {
			candidates[id] = addr
		}
	}
	if len(candidates) == 0 {
		log.Printf("[CLUSTER] No available replica for partition %d", partitionID)
		return
	}

	newLeader := ""
	if hasPositions {
		positions := m.replicaPositions(partitionID, candidates)
		need, guaranteed := cleanElectionQuorum(len(info.Replicas), m.config.MinInSyncReplicas)
		if guaranteed && len(positions) < need {
			log.Printf("[CLUSTER] Partition %d: not electing a leader, only %d of the %d replicas needed to rule out losing acknowledged writes answered",
				partitionID, len(positions), need)
			return
		}
		newLeader = mostCompleteReplica(positions)
	} else {
		aliveIDs := make(map[string]bool, len(alive))
		for id := range alive {
			aliveIDs[id] = true
		}
		newLeader, _ = ChooseFailoverLeader(info, aliveIDs)
	}
	if newLeader == "" {
		log.Printf("[CLUSTER] No reachable replica for partition %d", partitionID)
		return
	}

	updated := &PartitionInfo{
		ID:             partitionID,
		Topic:          info.Topic,
		LeaderID:       newLeader,
		Replicas:       info.Replicas,
		ISR:            info.ISR,
		Epoch:          info.Epoch + 1,
		State:          PartitionStateOnline,
		ReplicaOffsets: info.ReplicaOffsets,
	}
	if store != nil {
		if err := store.ProposePartition(updated, false); err != nil {
			log.Printf("[CLUSTER] Failed to update partition leader: %v", err)
			return
		}
	}
	if rt != nil {
		rt.UpdatePartitionAssignment(partitionID, newLeader, updated.Replicas, updated.ISR)
	}
	log.Printf("[CLUSTER] Partition %d new leader: %s (was %s)", partitionID, newLeader, oldLeader)
	// Promotion and demotion follow from the committed assignment.
	m.kickReconcile()
}

// reconcileLocalLeadership makes this node's partitions match the committed
// assignment: it leads the partitions it is named leader of, replicating to
// their other replicas, and stops leading every other partition. Until an
// assignment is committed (cluster bootstrap) the ring's view is used.
func (m *Manager) reconcileLocalLeadership() {
	m.mu.RLock()
	pa := m.partitionAccessor
	rt := m.router
	mem := m.membership
	nodeID := m.config.NodeID
	replicated := m.config.ReplicationFactor > 1
	m.mu.RUnlock()
	if pa == nil || rt == nil || mem == nil {
		return
	}

	alive := m.aliveNodes()
	committedOnly := m.leadershipIsCommitted()
	for partitionID, ring := range rt.DesiredAssignments() {
		leader, epoch, followers, transferTo := ring.LeaderID, ring.Epoch, ring.Replicas, ""
		committed, isCommitted := m.committed(partitionID)
		if isCommitted {
			leader, epoch, followers, transferTo = committed.LeaderID, committed.Epoch, committed.Replicas, committed.TransferTo
		} else if committedOnly {
			continue // nothing committed for it yet: the ring alone makes no leader
		}

		if leader != nodeID {
			if isCommitted {
				// Another node is the committed leader: this one must not lead.
				// (An error only means the partition is not held here.)
				_ = pa.DemoteFromLeader(partitionID)
				switch {
				case transferTo == nodeID:
					if !replicated {
						m.pullForHandoff(partitionID, alive[leader])
					}
				case !holdsReplica(committed, nodeID):
					// Neither leader, replica nor handoff target: a node that
					// led this partition before the others joined would keep
					// it loaded, and its timers running, for good.
					if releaser, ok := pa.(partitionReleaser); ok {
						if err := releaser.ReleasePartition(partitionID); err != nil {
							log.Printf("[CLUSTER] reconcile: release partition %d failed: %v", partitionID, err)
						}
					}
				}
			}
			continue
		}
		if epoch <= 0 {
			log.Printf("[CLUSTER] reconcile: skip partition %d with no positive leadership epoch", partitionID)
			continue
		}
		// Idempotent: once the local replication leader exists this only
		// refreshes its epoch.
		if err := pa.PromoteToLeader(partitionID, epoch); err != nil {
			log.Printf("[CLUSTER] reconcile: promote partition %d failed: %v", partitionID, err)
			continue
		}
		if !replicated {
			continue
		}
		if transferTo != "" {
			followers = append(append([]string(nil), followers...), transferTo)
		}
		added := make(map[string]bool, len(followers))
		for _, replicaID := range followers {
			addr := alive[replicaID]
			if replicaID == nodeID || addr == "" || added[replicaID] {
				continue // itself, not currently alive, or already handled
			}
			added[replicaID] = true
			if err := pa.AddFollower(partitionID, replicaID, addr); err != nil {
				log.Printf("[CLUSTER] reconcile: add follower %s to partition %d failed: %v", replicaID, partitionID, err)
			}
		}
	}
}

// partitionReleaser is implemented by a partition accessor that can unload a
// partition this node no longer holds.
type partitionReleaser interface {
	ReleasePartition(partitionID int32) error
}

// holdsReplica reports whether nodeID is the leader, a replica or the handoff
// target of a committed assignment.
func holdsReplica(info PartitionInfo, nodeID string) bool {
	if info.LeaderID == nodeID || info.TransferTo == nodeID {
		return true
	}
	for _, replica := range info.Replicas {
		if replica == nodeID {
			return true
		}
	}
	return false
}

// pullForHandoff copies a partition from its leader to this node, which is
// about to take it over. It is used when there is no replication stream to
// bring the target up to date (replication factor 1). One copy runs at a time
// per partition; the handoff completes once the logs are equal.
func (m *Manager) pullForHandoff(partitionID int32, leaderAddr string) {
	if leaderAddr == "" {
		return
	}
	m.mu.Lock()
	if m.pulling == nil {
		m.pulling = make(map[int32]bool)
	}
	if m.pulling[partitionID] {
		m.mu.Unlock()
		return
	}
	m.pulling[partitionID] = true
	pa := m.partitionAccessor
	m.mu.Unlock()

	go func() {
		defer func() {
			m.mu.Lock()
			delete(m.pulling, partitionID)
			m.mu.Unlock()
		}()
		positions := m.replicaPositions(partitionID, map[string]string{m.config.NodeID: "", "leader": leaderAddr})
		local, okLocal := positions[m.config.NodeID]
		remote, okRemote := positions["leader"]
		if !okLocal || !okRemote || remote.AcceptingWrites {
			return // the leader has not stopped yet; a copy now could miss writes
		}
		if local.LastOffset == remote.LastOffset && local.LastTerm == remote.LastTerm {
			return
		}
		if err := pa.GetOrCreatePartition(partitionID); err != nil {
			log.Printf("[CLUSTER] handoff: create partition %d: %v", partitionID, err)
			return
		}
		if err := pa.SyncPartitionFromLeader(partitionID, leaderAddr); err != nil {
			log.Printf("[CLUSTER] handoff: copy partition %d from %s: %v", partitionID, leaderAddr, err)
		}
	}()
}

// checkPartitionHealth elects a new leader for every partition whose committed
// leader is no longer alive.
func (m *Manager) checkPartitionHealth() {
	m.leadershipMu.Lock()
	defer m.leadershipMu.Unlock()
	m.mu.RLock()
	store, rt := m.assignments, m.router
	m.mu.RUnlock()
	alive := m.aliveNodes()

	partitions := map[int32]PartitionInfo{}
	if store != nil {
		partitions = store.Partitions()
	} else if rt != nil {
		partitions = rt.DesiredAssignments()
	}
	for partitionID, info := range partitions {
		if info.LeaderID == "" {
			continue
		}
		if _, ok := alive[info.LeaderID]; !ok {
			log.Printf("[CLUSTER] Partition %d leader %s is dead, triggering leader election", partitionID, info.LeaderID)
			info := info
			m.electNewLeader(partitionID, &info)
		}
		inSync := 0
		for _, nodeID := range info.ISR {
			if _, ok := alive[nodeID]; ok {
				inSync++
			}
		}
		if inSync < m.config.ReplicationFactor {
			log.Printf("[CLUSTER] Partition %d under-replicated (ISR=%d, RF=%d)", partitionID, inSync, m.config.ReplicationFactor)
		}
	}
}

// syncClusterState commits the ring's replica placement and moves leadership
// toward the ring's preferred leader. It never changes the leader of a
// partition directly: a dead leader is replaced by election, and a live one is
// replaced only through a completed handoff.
func (m *Manager) syncClusterState() {
	m.leadershipMu.Lock()
	defer m.leadershipMu.Unlock()
	m.mu.RLock()
	store, rt, mem := m.assignments, m.router, m.membership
	hasPositions := m.positions != nil
	m.mu.RUnlock()
	if store == nil || rt == nil || mem == nil || !store.IsLeader() {
		return
	}

	alive := m.aliveNodes()
	committedAll := store.Partitions()
	settled := m.membershipSettled()
	for partitionID, ring := range rt.DesiredAssignments() {
		if ring.LeaderID == "" {
			continue
		}
		if _, ok := alive[ring.LeaderID]; !ok {
			continue
		}
		existing, exists := committedAll[partitionID]
		if !exists {
			if !settled {
				continue // the cluster is still forming; see initialAssignmentSettle
			}
			assigned := ring.clone()
			if err := store.ProposePartition(&assigned, true); err != nil {
				log.Printf("[CLUSTER] Failed to sync partition %d metadata to Raft: %v", partitionID, err)
			}
			continue
		}
		if existing.TransferTo != "" {
			continue // advanceTransfers owns a handoff in progress
		}
		if _, ok := alive[existing.LeaderID]; !ok {
			continue // checkPartitionHealth elects a replacement
		}

		// Placement follows the ring, but the current leader stays a replica
		// until a handoff has moved leadership away from it. The in-sync set
		// is carried along but is not a reason to commit: every commit raises
		// the epoch, and followers drop in and out of sync under load.
		replicas := withNode(ring.Replicas, existing.LeaderID)
		updated := existing.clone()
		updated.Replicas, updated.ISR = replicas, withNode(ring.ISR, existing.LeaderID)
		if updated.Topic == "" {
			updated.Topic = ring.Topic
		}
		updated.State = PartitionStateOnline
		placementChanged := !stringSliceSetEqual(existing.Replicas, replicas) ||
			existing.State != PartitionStateOnline || existing.Epoch <= 0

		if ring.LeaderID != existing.LeaderID {
			if !hasPositions {
				// No way to compare logs: move leadership as older builds did.
				updated.LeaderID = ring.LeaderID
				updated.Replicas, updated.ISR = ring.Replicas, ring.ISR
				placementChanged = true
			} else if m.handoffMayStart(partitionID, existing.LeaderID, ring.LeaderID, alive) {
				updated.State = PartitionStateRebalancing
				updated.TransferTo = ring.LeaderID
				updated.TransferStartedMs = time.Now().UnixMilli()
				placementChanged = true
				log.Printf("[CLUSTER] Partition %d: starting leadership handoff %s -> %s", partitionID, existing.LeaderID, ring.LeaderID)
			}
		}
		if !placementChanged {
			continue
		}
		if err := store.ProposePartition(&updated, false); err != nil {
			log.Printf("[CLUSTER] Failed to update partition %d metadata in Raft: %v", partitionID, err)
		}
	}
}

// handoffMayStart decides whether a leadership handoff can begin now. The
// leader refuses publishes for the whole handoff, so with replication it
// waits until the target is nearly caught up. Without replication the target
// can only copy the partition after the leader has stopped, so it starts
// straight away.
func (m *Manager) handoffMayStart(partitionID int32, leader, target string, alive map[string]string) bool {
	m.mu.Lock()
	retryAt := m.transferRetryAt[partitionID]
	m.mu.Unlock()
	if time.Now().Before(retryAt) {
		return false
	}
	positions := m.replicaPositions(partitionID, map[string]string{leader: alive[leader], target: alive[target]})
	leaderPos, okLeader := positions[leader]
	targetPos, okTarget := positions[target]
	if !okLeader || !okTarget {
		return false // one of them is not reachable yet
	}
	if m.config.ReplicationFactor <= 1 {
		return true
	}
	return leaderPos.LastOffset-targetPos.LastOffset <= transferStartLag
}

// advanceTransfers completes or abandons the handoffs in progress. A handoff
// completes when the leader reports that it has stopped accepting publishes
// and the target's log ends exactly where the leader's does: nothing can have
// been acknowledged that the target lacks. It is abandoned, leaving the leader
// in place, when the target disappears, the ring no longer wants it, or it
// takes too long.
func (m *Manager) advanceTransfers() {
	m.leadershipMu.Lock()
	defer m.leadershipMu.Unlock()
	m.mu.RLock()
	store, rt := m.assignments, m.router
	m.mu.RUnlock()
	if store == nil || rt == nil || !store.IsLeader() {
		return
	}
	var alive map[string]string
	var ring map[int32]PartitionInfo
	for partitionID, info := range store.Partitions() {
		if info.TransferTo == "" {
			continue
		}
		if alive == nil {
			alive, ring = m.aliveNodes(), rt.DesiredAssignments()
		}
		leader, target := info.LeaderID, info.TransferTo
		if _, ok := alive[leader]; !ok {
			continue // checkPartitionHealth elects; that also ends the handoff
		}

		limit := transferTimeout
		if m.config.ReplicationFactor <= 1 {
			limit = transferTimeoutUnreplicated
		}
		_, targetAlive := alive[target]
		expired := time.Since(time.UnixMilli(info.TransferStartedMs)) > limit
		if !targetAlive || ring[partitionID].LeaderID != target || expired {
			aborted := info.clone()
			aborted.State, aborted.TransferTo, aborted.TransferStartedMs = PartitionStateOnline, "", 0
			if err := store.ProposePartition(&aborted, false); err != nil {
				log.Printf("[CLUSTER] Partition %d: failed to abandon handoff to %s: %v", partitionID, target, err)
				continue
			}
			m.mu.Lock()
			if m.transferRetryAt == nil {
				m.transferRetryAt = make(map[int32]time.Time)
			}
			m.transferRetryAt[partitionID] = time.Now().Add(transferRetryDelay)
			m.mu.Unlock()
			log.Printf("[CLUSTER] Partition %d: abandoned handoff %s -> %s (target alive=%v, expired=%v)", partitionID, leader, target, targetAlive, expired)
			continue
		}

		positions := m.replicaPositions(partitionID, map[string]string{leader: alive[leader], target: alive[target]})
		leaderPos, okLeader := positions[leader]
		targetPos, okTarget := positions[target]
		if !okLeader || !okTarget || leaderPos.AcceptingWrites {
			continue
		}
		if targetPos.LastOffset != leaderPos.LastOffset || targetPos.LastTerm != leaderPos.LastTerm {
			continue
		}

		done := info.clone()
		done.LeaderID = target
		done.Replicas, done.ISR = ring[partitionID].Replicas, ring[partitionID].ISR
		done.State, done.TransferTo, done.TransferStartedMs = PartitionStateOnline, "", 0
		if err := store.ProposePartition(&done, false); err != nil {
			log.Printf("[CLUSTER] Partition %d: failed to complete handoff to %s: %v", partitionID, target, err)
			continue
		}
		rt.UpdatePartitionAssignment(partitionID, target, done.Replicas, done.ISR)
		log.Printf("[CLUSTER] Partition %d: leadership handed off %s -> %s at offset %d", partitionID, leader, target, leaderPos.LastOffset)
	}
}

// withNode returns nodes with id included.
func withNode(nodes []string, id string) []string {
	for _, existing := range nodes {
		if existing == id {
			return append([]string(nil), nodes...)
		}
	}
	return append(append([]string(nil), nodes...), id)
}
