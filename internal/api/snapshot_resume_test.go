package api

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

// flakySnapshots serves snapshots from a real replication handler and can cut
// a transfer short. It records what each transfer actually sent.
type flakySnapshots struct {
	types.ReplicationServiceServer

	mu sync.Mutex
	// failAfterFiles, when positive, breaks the next transfer once that many
	// files have been sent in full; it then resets itself.
	failAfterFiles int
	transfers      []snapshotTransfer
}

type snapshotTransfer struct {
	sentFiles, reusedFiles int
	sentBytes              int64
}

func (f *flakySnapshots) Snapshot(req *types.ReplicationSnapshotRequest, stream grpc.ServerStreamingServer[types.ReplicationSnapshotChunk]) error {
	f.mu.Lock()
	limit := f.failAfterFiles
	f.failAfterFiles = 0
	f.mu.Unlock()

	counting := &countingSnapshotStream{ServerStreamingServer: stream, failAfterFiles: limit}
	err := f.ReplicationServiceServer.Snapshot(req, counting)
	f.mu.Lock()
	f.transfers = append(f.transfers, counting.snapshotTransfer)
	f.mu.Unlock()
	return err
}

func (f *flakySnapshots) transfer(i int) snapshotTransfer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.transfers[i]
}

type countingSnapshotStream struct {
	grpc.ServerStreamingServer[types.ReplicationSnapshotChunk]
	snapshotTransfer
	failAfterFiles int
}

func (c *countingSnapshotStream) Send(chunk *types.ReplicationSnapshotChunk) error {
	if header := chunk.GetHeader(); header != nil {
		if header.GetReuse() {
			c.reusedFiles++
		} else {
			if c.failAfterFiles > 0 && c.sentFiles == c.failAfterFiles {
				return errors.New("connection lost")
			}
			c.sentFiles++
		}
	}
	c.sentBytes += int64(len(chunk.GetData()))
	return c.ServerStreamingServer.Send(chunk)
}

// snapshotSource is a leader whose log spans many small segments.
func snapshotSource(t *testing.T, events int) (*partition.PartitionManager, *partition.Partition, *flakySnapshots, string) {
	t.Helper()
	cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 1, TickMS: 10, WheelSize: 100, SegmentSizeBytes: 2048, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 50, DedupTTLHours: 24, BloomCapacity: 1000}
	pm := partition.NewPartitionManager("node-a", cfg)
	t.Cleanup(func() { pm.Close() })
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	appendOrders(t, p, 0, events)
	service := &flakySnapshots{ReplicationServiceServer: NewReplicationServiceHandler(pm)}
	return pm, p, service, serveReplicationService(t, service)
}

// appendOrders appends n events a few at a time: a segment is only closed
// between batches, and these tests need many segments.
func appendOrders(t *testing.T, p *partition.Partition, from, n int) {
	t.Helper()
	for done := 0; done < n; {
		batch := make([]*types.Event, min(4, n-done))
		for i := range batch {
			batch[i] = &types.Event{MessageId: fmt.Sprintf("order-%d", from+done+i), Topic: "orders", Payload: make([]byte, 200), ScheduleTs: 1}
		}
		if err := p.Wal.AppendBatch(batch); err != nil {
			t.Fatal(err)
		}
		done += len(batch)
	}
}

func expectLog(t *testing.T, r *replicaLog, want int) {
	t.Helper()
	log := r.log()
	if len(log) != want {
		t.Fatalf("replica log has %d entries, want %d", len(log), want)
	}
	for i, event := range log {
		if event.Offset != int64(i) || event.MessageId != fmt.Sprintf("order-%d", i) {
			t.Fatalf("replica log entry %d is %q at offset %d", i, event.MessageId, event.Offset)
		}
	}
}

// A transfer that breaks part-way is not started over. The files that arrived
// stay staged, the retry reports them, and the leader sends only the rest,
// including whatever was written to its log in the meantime.
func TestSnapshot_InterruptedTransferResumes(t *testing.T) {
	_, leader, service, addr := snapshotSource(t, 120)
	replica := newReplicaLog(t, "node-b")

	service.failAfterFiles = 20
	if err := replica.pm.SyncPartitionFromLeader(0, addr); err == nil {
		t.Fatal("a transfer that lost its connection reported success")
	}
	if got := len(replica.log()); got != 0 {
		t.Fatalf("replica log has %d entries after a failed transfer; nothing may be installed", got)
	}
	first := service.transfer(0)
	if first.sentFiles != 20 || first.reusedFiles != 0 {
		t.Fatalf("first transfer sent %d files and reused %d, want 20 and 0", first.sentFiles, first.reusedFiles)
	}

	// The leader keeps taking writes while the replica is away.
	appendOrders(t, leader, 120, 15)

	if err := replica.pm.SyncPartitionFromLeader(0, addr); err != nil {
		t.Fatalf("retry: %v", err)
	}
	second := service.transfer(1)
	if second.reusedFiles != 20 {
		t.Fatalf("retry reused %d files, want the 20 that had arrived", second.reusedFiles)
	}
	if second.sentFiles == 0 {
		t.Fatal("retry sent no files although the transfer was incomplete")
	}
	expectLog(t, replica, 135)
	if _, err := os.Stat(filepath.Join(replica.p.Wal.GetDataDir(), "snapshot-staging")); !os.IsNotExist(err) {
		t.Fatalf("staging directory left behind after the install (err=%v)", err)
	}

	// For comparison, a replica with nothing staged needs every file.
	fresh := newReplicaLog(t, "node-c")
	if err := fresh.pm.SyncPartitionFromLeader(0, addr); err != nil {
		t.Fatal(err)
	}
	full := service.transfer(2)
	if full.reusedFiles != 0 || second.sentBytes >= full.sentBytes {
		t.Fatalf("resumed transfer sent %d bytes, a full one %d (reused %d files)", second.sentBytes, full.sentBytes, full.reusedFiles)
	}
	expectLog(t, fresh, 135)
}

// Staged files are reused only when they are exactly what the leader has. A
// damaged one, and one the leader no longer has, must not end up in the log.
func TestSnapshot_ResumeDoesNotTrustStagedFilesBlindly(t *testing.T) {
	_, _, service, addr := snapshotSource(t, 120)
	replica := newReplicaLog(t, "node-b")

	service.failAfterFiles = 20
	if err := replica.pm.SyncPartitionFromLeader(0, addr); err == nil {
		t.Fatal("a transfer that lost its connection reported success")
	}
	staging := filepath.Join(replica.p.Wal.GetDataDir(), "snapshot-staging", "segments")
	staged, err := filepath.Glob(filepath.Join(staging, "*.log"))
	if err != nil || len(staged) < 3 {
		t.Fatalf("staged segments: %v (err=%v)", staged, err)
	}

	// One staged segment is damaged on disk.
	data, err := os.ReadFile(staged[1])
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-20] ^= 0xff
	if err := os.WriteFile(staged[1], data, 0644); err != nil {
		t.Fatal(err)
	}
	// Another file is there that the leader never had.
	if err := os.WriteFile(filepath.Join(staging, "00000000000000999999.log"), []byte("left over"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := replica.pm.SyncPartitionFromLeader(0, addr); err != nil {
		t.Fatalf("retry: %v", err)
	}
	second := service.transfer(1)
	if second.reusedFiles == 0 || second.reusedFiles >= 20 {
		t.Fatalf("retry reused %d files; the damaged one must be sent again and the others kept", second.reusedFiles)
	}
	expectLog(t, replica, 120)
}
