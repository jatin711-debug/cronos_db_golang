package cluster

import (
	"fmt"
	"log"
	"slices"
	"sort"
	"strings"
)

// A capability names something a build can do that other nodes may depend
// on: a record format it can read, a protocol it speaks. Nodes advertise the
// capabilities of their build in membership messages, and a partition can
// require some of them (PartitionInfo.Requires). From then on the partition
// is placed on, led by and handed over to only nodes that advertise every
// one of them.
//
// A node that sends no capabilities has none. Builds before capabilities
// existed send none, and they support none of the formats that need them.
const (
	// CapabilityLifecycleRecords: the build reads lifecycle records (cancel,
	// reschedule, relocate) of version 1 in a WAL, and refuses to store or
	// deliver them as ordinary events.
	CapabilityLifecycleRecords = "lifecycle-records/1"
	// CapabilityLifecycleReplication: the build replicates lifecycle records
	// through the versioned append, catch-up and snapshot protocol. No build
	// advertises it yet; until one does, no partition can require it.
	CapabilityLifecycleReplication = "lifecycle-replication/1"
	// The build reads term-start records and uses their versioned append and
	// snapshot protocol, committing old entries only through its current term.
	CapabilityTermStart = "term-start/1"
)

// LifecycleRequirement is what a replicated partition requires before
// lifecycle records may be written to it.
var LifecycleRequirement = []string{CapabilityLifecycleRecords, CapabilityLifecycleReplication, CapabilityTermStart}

// LocalCapabilities returns the capabilities this build implements.
func LocalCapabilities() []string {
	return []string{CapabilityLifecycleRecords, CapabilityTermStart}
}

func (m *Manager) PartitionRequiresTermStart(partitionID int32) bool {
	m.mu.RLock()
	store := m.assignments
	m.mu.RUnlock()
	if store == nil {
		return false
	}
	info, ok := store.Partition(partitionID)
	return ok && slices.Contains(info.Requires, CapabilityTermStart)
}

// Upgrade only after every assigned node and voter supports the format. A
// durable requirement also protects replacement placement and later elections.
func (m *Manager) enableTermStartRequirements() {
	m.mu.RLock()
	store := m.assignments
	replicated := m.config.ReplicationFactor > 1
	m.mu.RUnlock()
	if !replicated || store == nil || !store.IsLeader() {
		return
	}
	for id, info := range store.Partitions() {
		if !slices.Contains(info.Requires, CapabilityTermStart) {
			// A mixed or temporarily unavailable cluster retains its legacy
			// format. No new control record is sent to an unsupported replica.
			_ = m.RequirePartitionCapabilities(id, []string{CapabilityTermStart})
		}
	}
}

// normalizeCapabilities returns the distinct, non-empty names in caps, sorted.
func normalizeCapabilities(caps []string) []string {
	out := make([]string, 0, len(caps))
	for _, c := range caps {
		if c = strings.TrimSpace(c); c != "" {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	return slices.Compact(out)
}

// missingCapabilities returns the names in need that have does not contain.
func missingCapabilities(have, need []string) []string {
	var missing []string
	for _, c := range need {
		if !slices.Contains(have, c) {
			missing = append(missing, c)
		}
	}
	return missing
}

// unionCapabilities returns the distinct names of a and b, sorted.
func unionCapabilities(a, b []string) []string {
	return normalizeCapabilities(append(append([]string(nil), a...), b...))
}

// sameCapabilities reports whether a and b name the same capabilities.
func sameCapabilities(a, b []string) bool {
	return slices.Equal(normalizeCapabilities(a), normalizeCapabilities(b))
}

// nodeCapabilities returns what an alive member advertises; ok is false when
// membership does not count the node as alive.
func (m *Manager) nodeCapabilities(nodeID string) (caps []string, ok bool) {
	m.mu.RLock()
	mem := m.membership
	m.mu.RUnlock()
	if mem == nil {
		return nil, false
	}
	for _, node := range mem.GetAliveNodes() {
		if node.ID == nodeID {
			return node.Capabilities, true
		}
	}
	return nil, false
}

// supports reports whether an alive member advertises every capability in
// need. A node that is not alive supports nothing: what it would run when it
// comes back is not known.
func (m *Manager) supports(nodeID string, need []string) bool {
	if len(need) == 0 {
		return true
	}
	caps, ok := m.nodeCapabilities(nodeID)
	return ok && len(missingCapabilities(caps, need)) == 0
}

// unsupportedNodes returns the nodes of ids that do not support need.
func (m *Manager) unsupportedNodes(ids []string, need []string) []string {
	var out []string
	for _, id := range ids {
		if id != "" && !m.supports(id, need) {
			out = append(out, id)
		}
	}
	return out
}

// noteRefusedPlacement logs the nodes the ring wanted to place a partition
// on that do not advertise what it requires, when they differ from the last
// round. The caller holds leadershipMu.
func (m *Manager) noteRefusedPlacement(partitionID int32, refused, requires []string) {
	key := strings.Join(refused, ",")
	if m.refusedPlacement[partitionID] == key {
		return
	}
	if m.refusedPlacement == nil {
		m.refusedPlacement = make(map[int32]string)
	}
	m.refusedPlacement[partitionID] = key
	if key == "" {
		log.Printf("[CLUSTER] Partition %d: the ring's placement advertises %v again", partitionID, requires)
		return
	}
	log.Printf("[CLUSTER] Partition %d stays on its replicas: the ring would place it on %s, which do not advertise %v",
		partitionID, key, requires)
}

// admitsVoter reports whether a node may be added to the Raft group: once a
// partition requires capabilities, every voter has to advertise them, since
// a voter may become the Raft leader and place partitions.
func (m *Manager) admitsVoter(node *Node) bool {
	missing := missingCapabilities(node.Capabilities, m.requiredCapabilities())
	if len(missing) == 0 {
		return true
	}
	log.Printf("[CLUSTER] Not adding node %s to Raft: partitions of this cluster require %s, which it does not advertise",
		node.ID, strings.Join(missing, ", "))
	return false
}

// requiredCapabilities returns every capability some committed partition
// requires.
func (m *Manager) requiredCapabilities() []string {
	m.mu.RLock()
	store := m.assignments
	m.mu.RUnlock()
	if store == nil {
		return nil
	}
	var all []string
	for _, info := range store.Partitions() {
		all = append(all, info.Requires...)
	}
	return normalizeCapabilities(all)
}

// PartitionRequiresLifecycle reports whether the committed assignment of a
// partition, as this node has it, requires the lifecycle format.
func (m *Manager) PartitionRequiresLifecycle(partitionID int32) bool {
	m.mu.RLock()
	store := m.assignments
	m.mu.RUnlock()
	if store == nil {
		return false
	}
	info, ok := store.Partition(partitionID)
	return ok && len(missingCapabilities(info.Requires, LifecycleRequirement)) == 0
}

// voterLister is implemented by an assignment store that can list the Raft
// group's voters.
type voterLister interface {
	Voters() ([]Voter, error)
}

// RequirePartitionCapabilities commits that a partition may from now on be
// placed on, led by and handed over to only nodes that advertise every one of
// caps. The requirement is never lifted.
//
// It is committed only when every node the partition is assigned to and every
// Raft voter is alive and advertises all of caps. A replica that could not
// read the partition's records must not hold it, and any voter may become the
// Raft leader, which places partitions and elects their leaders: a voter of
// an older build would do so without knowing of the requirement.
func (m *Manager) RequirePartitionCapabilities(partitionID int32, caps []string) error {
	caps = normalizeCapabilities(caps)
	if len(caps) == 0 {
		return fmt.Errorf("no capability given")
	}
	m.leadershipMu.Lock()
	defer m.leadershipMu.Unlock()
	m.mu.RLock()
	store := m.assignments
	m.mu.RUnlock()
	if store == nil || !store.IsLeader() {
		return ErrNotCoordinator
	}
	info, ok := store.Partition(partitionID)
	if !ok {
		return fmt.Errorf("partition %d has no committed assignment", partitionID)
	}
	if len(missingCapabilities(info.Requires, caps)) == 0 {
		return nil
	}

	nodes := append([]string{info.LeaderID, info.TransferTo}, info.Replicas...)
	if lister, ok := store.(voterLister); ok {
		voters, err := lister.Voters()
		if err != nil {
			return fmt.Errorf("list Raft voters: %w", err)
		}
		for _, v := range voters {
			nodes = append(nodes, v.ID)
		}
	}
	slices.Sort(nodes)
	nodes = slices.Compact(nodes)
	var problems []string
	for _, id := range nodes {
		if id == "" {
			continue
		}
		have, alive := m.nodeCapabilities(id)
		switch missing := missingCapabilities(have, caps); {
		case !alive:
			problems = append(problems, id+" is not reachable")
		case len(missing) > 0:
			problems = append(problems, fmt.Sprintf("%s lacks %s", id, strings.Join(missing, ", ")))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("partition %d cannot require %s: %s", partitionID, strings.Join(caps, ", "), strings.Join(problems, "; "))
	}
	if err := store.RequirePartition(partitionID, caps); err != nil {
		return err
	}
	log.Printf("[CLUSTER] Partition %d now requires %s", partitionID, strings.Join(caps, ", "))
	return nil
}
