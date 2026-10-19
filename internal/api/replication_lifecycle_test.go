package api

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/lifecycle"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newClusterReplica is a follower of a cluster: partitions can switch to
// lifecycle mode only there.
func newClusterReplica(t *testing.T, dataDir string) *replicaLog {
	t.Helper()
	cfg := &types.Config{DataDir: dataDir, ClusterEnabled: true, PartitionCount: 1, ReplicationFactor: 3, MinInSyncReplicas: 2, TickMS: 10, WheelSize: 100, SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 50, DedupTTLHours: 24, BloomCapacity: 1000}
	pm := partition.NewPartitionManager("follower", cfg)
	stop := sync.OnceFunc(func() { pm.Close() })
	t.Cleanup(stop)
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	return &replicaLog{t: t, pm: pm, h: NewReplicationServiceHandler(pm), p: p, stop: stop}
}

// cancelBatch is an event scheduled an hour ahead at offset 0 and its
// cancellation at offset 1, written under term.
func cancelBatch(t *testing.T, term int64) []*types.Event {
	t.Helper()
	original := &types.Event{MessageId: "m-0", Topic: "orders", PartitionId: 0, Offset: 0, Term: term, ScheduleTs: time.Now().Add(time.Hour).UnixMilli(), Payload: []byte("x")}
	cancel, err := lifecycle.Replacement(original, lifecycle.Cancel, "req-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	cancel.Offset, cancel.Term = 1, term
	return []*types.Event{original, cancel}
}

func (r *replicaLog) sendLifecycle(format uint32, term int64, events []*types.Event) (*types.ReplicationAppendResponse, error) {
	return r.h.AppendLifecycle(context.Background(), &types.ReplicationLifecycleAppendRequest{
		LifecycleFormat: format,
		Append:          &types.ReplicationAppendRequest{PartitionId: 0, Term: term, LeaderId: "leader", Events: events},
	})
}

// untouched fails the test if the replica's log or mode changed.
func (r *replicaLog) untouched(why string) {
	r.t.Helper()
	if got := r.log(); len(got) != 0 {
		r.t.Fatalf("%s: the log holds %d entries, want none", why, len(got))
	}
	if r.p.LifecycleEnabled() {
		r.t.Fatalf("%s: the partition switched to lifecycle mode", why)
	}
	if _, err := os.Stat(filepath.Join(r.p.DataDir, lifecycle.Marker)); !os.IsNotExist(err) {
		r.t.Fatalf("%s: a lifecycle marker was written (%v)", why, err)
	}
}

func wantCode(t *testing.T, err error, code codes.Code, why string) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("%s: err = %v, want %s", why, err, code)
	}
}

// A replica takes lifecycle records only for a partition the cluster has
// committed to require the format, in a format it reads, and only well-formed
// ones; every refusal comes before anything is written.
func TestAppendLifecycle_RefusalsWriteNothing(t *testing.T) {
	r := newClusterReplica(t, t.TempDir())
	_, err := r.sendLifecycle(partition.LifecycleFormat, 1, cancelBatch(t, 1))
	wantCode(t, err, codes.FailedPrecondition, "without a requirement source")
	r.untouched("without a requirement source")

	required := false
	r.pm.SetLifecycleRequirement(func(int32) bool { return required })
	_, err = r.sendLifecycle(partition.LifecycleFormat, 1, cancelBatch(t, 1))
	wantCode(t, err, codes.FailedPrecondition, "while the partition does not require the format")
	r.untouched("while the partition does not require the format")

	required = true
	_, err = r.sendLifecycle(partition.LifecycleFormat+1, 1, cancelBatch(t, 1))
	wantCode(t, err, codes.FailedPrecondition, "in a format this build does not read")
	r.untouched("in a format this build does not read")

	batch := cancelBatch(t, 1)
	batch[1].Meta[lifecycle.Key] = strings.Replace(batch[1].Meta[lifecycle.Key], `"cancel"`, `"explode"`, 2)
	_, err = r.sendLifecycle(partition.LifecycleFormat, 1, batch)
	wantCode(t, err, codes.InvalidArgument, "with a malformed record")
	r.untouched("with a malformed record")

	batch = cancelBatch(t, 1)
	batch[1].PartitionId = 7
	_, err = r.sendLifecycle(partition.LifecycleFormat, 1, batch)
	wantCode(t, err, codes.InvalidArgument, "with a record of another partition")
	r.untouched("with a record of another partition")
}

// The first lifecycle append switches the partition to lifecycle mode, writes
// the records and indexes them: the canceled event is no longer one to
// deliver. From then on the plain append, which a leader of an older build
// would send, is refused. After a restart the partition is in lifecycle mode
// again, with the index rebuilt from its log.
func TestAppendLifecycle_SwitchesStoresAndIndexes(t *testing.T) {
	dir := t.TempDir()
	r := newClusterReplica(t, dir)
	r.pm.SetLifecycleRequirement(func(int32) bool { return true })
	resp, err := r.sendLifecycle(partition.LifecycleFormat, 1, cancelBatch(t, 1))
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("lifecycle append: resp=%v err=%v", resp, err)
	}
	if !r.p.LifecycleEnabled() {
		t.Fatal("the partition did not switch to lifecycle mode")
	}
	if format, err := os.ReadFile(filepath.Join(r.p.DataDir, lifecycle.Marker)); err != nil || string(format) != "1\n" {
		t.Fatalf("lifecycle marker = %q (%v), want 1", format, err)
	}
	stored := r.log()
	if len(stored) != 2 {
		t.Fatalf("the log holds %d entries, want 2", len(stored))
	}
	if r.p.ActiveEvent(stored[0]) || r.p.ActiveEvent(stored[1]) {
		t.Fatal("the canceled event, or its cancellation, is still one to deliver")
	}

	_, err = r.h.Append(context.Background(), &types.ReplicationAppendRequest{PartitionId: 0, Term: 1, LeaderId: "leader", Events: entries("next", 1, 2, 2)})
	wantCode(t, err, codes.FailedPrecondition, "plain append to a partition in lifecycle mode")

	r.stop()
	again := newClusterReplica(t, dir)
	if !again.p.LifecycleEnabled() {
		t.Fatal("after a restart the partition is not in lifecycle mode")
	}
	if events := again.log(); len(events) != 2 || again.p.ActiveEvent(events[0]) {
		t.Fatalf("after a restart: %d entries, original active=%v; want 2 and the original canceled", len(events), len(events) > 0 && again.p.ActiveEvent(events[0]))
	}
}

// A node that is not in a cluster does not open lifecycle data without
// --event-lifecycle, whoever wrote it.
func TestLifecycleDataNeedsTheFlagOutsideACluster(t *testing.T) {
	dir := t.TempDir()
	r := newClusterReplica(t, dir)
	r.pm.SetLifecycleRequirement(func(int32) bool { return true })
	if _, err := r.sendLifecycle(partition.LifecycleFormat, 1, cancelBatch(t, 1)); err != nil {
		t.Fatal(err)
	}
	r.stop()

	cfg := &types.Config{DataDir: dir, PartitionCount: 1, ReplicationFactor: 1, MinInSyncReplicas: 1, TickMS: 10, WheelSize: 100, SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 50, DedupTTLHours: 24, BloomCapacity: 1000}
	pm := partition.NewPartitionManager("standalone", cfg)
	defer pm.Close()
	if err := pm.CreatePartition(0, "orders"); err == nil || !strings.Contains(err.Error(), "--event-lifecycle") {
		t.Fatalf("opening lifecycle data standalone without the flag: err = %v", err)
	}
}

// lifecycleSource is a replica in lifecycle mode holding an event and its
// cancellation, serving snapshots.
func lifecycleSource(t *testing.T) (*replicaLog, string) {
	t.Helper()
	source := newClusterReplica(t, t.TempDir())
	source.pm.SetLifecycleRequirement(func(int32) bool { return true })
	if _, err := source.sendLifecycle(partition.LifecycleFormat, 1, cancelBatch(t, 1)); err != nil {
		t.Fatal(err)
	}
	return source, serveReplicationService(t, source.h)
}

// A snapshot of a log in lifecycle mode goes only to a receiver that says it
// reads the format, and says the format in its trailer.
func TestSnapshot_LifecycleLogOnlyToAReceiverThatReadsIt(t *testing.T) {
	source, _ := lifecycleSource(t)
	stream := &mockReplicationSnapshotStream{ctx: context.Background()}
	err := source.h.Snapshot(&types.ReplicationSnapshotRequest{PartitionId: 0, SupportsDeadLetters: true}, stream)
	wantCode(t, err, codes.FailedPrecondition, "snapshot for a receiver that does not read lifecycle records")
	if len(stream.chunks) != 0 {
		t.Fatalf("%d chunks were sent before the refusal", len(stream.chunks))
	}

	stream = &mockReplicationSnapshotStream{ctx: context.Background()}
	if err := source.h.Snapshot(&types.ReplicationSnapshotRequest{PartitionId: 0, SupportsDeadLetters: true, LifecycleFormat: partition.LifecycleFormat}, stream); err != nil {
		t.Fatal(err)
	}
	last := stream.chunks[len(stream.chunks)-1].GetTrailer()
	if last == nil || last.GetLifecycleFormat() != partition.LifecycleFormat {
		t.Fatalf("trailer = %v, want one naming lifecycle format %d", last, partition.LifecycleFormat)
	}
}

// A replica installs a snapshot of a log in lifecycle mode only where the
// cluster has committed the partition's requirement. It switches to
// lifecycle mode before the log is replaced and rebuilds its index from the
// installed log, so the canceled event stays canceled, after a restart too.
func TestSnapshot_LifecycleInstallSwitchesFirst(t *testing.T) {
	_, addr := lifecycleSource(t)

	refused := newClusterReplica(t, t.TempDir())
	if err := refused.pm.SyncPartitionFromLeader(0, addr); err == nil {
		t.Fatal("a lifecycle snapshot was installed without the partition's requirement")
	}
	refused.untouched("lifecycle snapshot without the requirement")

	dir := t.TempDir()
	r := newClusterReplica(t, dir)
	r.pm.SetLifecycleRequirement(func(int32) bool { return true })
	if err := r.pm.SyncPartitionFromLeader(0, addr); err != nil {
		t.Fatal(err)
	}
	if !r.p.LifecycleEnabled() {
		t.Fatal("the replica did not switch to lifecycle mode")
	}
	events := r.log()
	if len(events) != 2 || r.p.ActiveEvent(events[0]) {
		t.Fatalf("after the install: %d entries, original active=%v; want 2 and the original canceled", len(events), len(events) > 0 && r.p.ActiveEvent(events[0]))
	}
	r.stop()
	again := newClusterReplica(t, dir)
	if events := again.log(); !again.p.LifecycleEnabled() || len(events) != 2 || again.p.ActiveEvent(events[0]) {
		t.Fatalf("after a restart: lifecycle mode=%v, %d entries; want lifecycle mode and the original canceled", again.p.LifecycleEnabled(), len(events))
	}
}
