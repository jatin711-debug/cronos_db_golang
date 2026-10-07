package replication

import (
	"context"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Follower applies streamed WAL appends and bulk snapshots from the partition
// leader onto the local WAL for a single partition replica.
type Follower struct {
	mu           sync.RWMutex
	partitionID  int32
	epoch        int64
	leaderID     string
	leaderAddr   string
	nextOffset   int64
	wal          *storage.WAL
	quit         chan struct{}
	lastSyncTime time.Time
	syncInterval time.Duration
	catchupMode  bool // True when catching up with leader
	nodeID       string
	tlsConfig    *MTLSConfig // optional mTLS for replication connections
}

// NewFollower creates a new follower. tlsConfig may be nil for plaintext dev mode.
func NewFollower(partitionID int32, wal *storage.WAL, nodeID string, tlsConfig *MTLSConfig) *Follower {
	return &Follower{
		partitionID:  partitionID,
		wal:          wal,
		nextOffset:   wal.GetNextOffset(),
		quit:         make(chan struct{}),
		nodeID:       nodeID,
		tlsConfig:    tlsConfig,
		syncInterval: 1 * time.Second,
		catchupMode:  false,
	}
}

// InstallSnapshot pulls a bulk segment snapshot from the leader over the
// internal replication gRPC channel and installs it locally. It is used when a
// follower is far behind or when a brand-new replica joins the partition.
//
// The snapshot replaces the whole local log, so its source must be at least at
// the epoch this replica has accepted (see SetLeader). A node that reports an
// older epoch was superseded as leader and its log may lack acknowledged
// events; its snapshot is refused and the local log is left as it is.
func (f *Follower) InstallSnapshot(ctx context.Context, leaderAddr string, partitionID int32, startOffset int64) error {
	f.mu.Lock()
	if f.catchupMode {
		f.mu.Unlock()
		return fmt.Errorf("already catching up")
	}
	f.catchupMode = true
	walDataDir := f.wal.GetDataDir()
	acceptedEpoch := f.epoch
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		f.catchupMode = false
		f.lastSyncTime = time.Now()
		f.mu.Unlock()
	}()

	log.Printf("[FOLLOWER] Requesting bulk snapshot from leader %s for partition %d", leaderAddr, partitionID)

	creds, err := f.dialCredentials()
	if err != nil {
		return fmt.Errorf("build replication credentials: %w", err)
	}

	conn, err := grpc.NewClient(leaderAddr,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64*1024*1024),
			grpc.MaxCallSendMsgSize(64*1024*1024),
		),
	)
	if err != nil {
		return fmt.Errorf("dial leader %s: %w", leaderAddr, err)
	}
	defer conn.Close()

	// Stage files in a temporary directory so the existing WAL is not touched
	// until the whole snapshot is received and verified. Whatever an earlier,
	// interrupted transfer left there is offered to the leader, which skips
	// the files that are still current; a large partition does not start over
	// because a connection dropped near the end.
	stagingDir := filepath.Join(walDataDir, "snapshot-staging")
	stagingSegments := filepath.Join(stagingDir, "segments")
	stagingIndex := filepath.Join(stagingDir, "index")
	if err := os.MkdirAll(stagingSegments, 0755); err != nil {
		return fmt.Errorf("create staging segments dir: %w", err)
	}
	if err := os.MkdirAll(stagingIndex, 0755); err != nil {
		return fmt.Errorf("create staging index dir: %w", err)
	}
	staged, err := stagedSnapshotFiles(stagingDir)
	if err != nil {
		return fmt.Errorf("inspect staged snapshot files: %w", err)
	}
	have := make([]*types.ReplicationSnapshotHeader, 0, len(staged))
	for _, file := range staged {
		have = append(have, file)
	}

	client := types.NewReplicationServiceClient(conn)
	stream, err := client.Snapshot(ctx, &types.ReplicationSnapshotRequest{
		PartitionId: partitionID,
		StartOffset: startOffset,
		MaxBytes:    1<<63 - 1,
		Have:        have,
	})
	if err != nil {
		return fmt.Errorf("snapshot RPC: %w", err)
	}

	var currentFile *os.File
	var currentPath string
	var currentHash hash.Hash32
	var currentHeader *types.ReplicationSnapshotHeader
	var trailer *types.ReplicationSnapshotTrailer
	var received int64
	finishFile := func() error {
		if currentFile == nil {
			return nil
		}
		defer func() { currentFile.Close(); currentFile = nil }()
		if received != currentHeader.FileSize {
			return fmt.Errorf("snapshot file size mismatch: %s", currentHeader.Filename)
		}
		if currentHash.Sum32() != currentHeader.Crc32 {
			return fmt.Errorf("snapshot checksum mismatch: %s", currentHeader.Filename)
		}
		return currentFile.Sync()
	}
	seen := make(map[string]bool)
	reused := 0

	for {
		chunk, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			cleanupFile(currentFile)
			return fmt.Errorf("receive snapshot chunk: %w", recvErr)
		}

		if header := chunk.GetHeader(); header != nil {
			if err := finishFile(); err != nil {
				return err
			}
			name := header.GetFilename()
			if name == "" || name == "." || name == ".." || filepath.Base(name) != name || strings.ContainsAny(name, "/\\:") || header.FileSize < 0 {
				return fmt.Errorf("invalid snapshot filename or size")
			}
			key := stagedFileKey(name, header.GetIsIndex())
			if seen[key] {
				return fmt.Errorf("duplicate snapshot file")
			}
			seen[key] = true
			if header.GetReuse() {
				// The leader sends no data for a file this replica reported
				// holding. It must be exactly the file that was reported.
				held := staged[key]
				if held == nil || held.GetFileSize() != header.GetFileSize() || held.GetCrc32() != header.GetCrc32() {
					return fmt.Errorf("leader skipped snapshot file %s, which this replica does not hold", name)
				}
				reused++
				continue
			}
			received = 0
			currentHeader = header

			currentPath = stagingSegments
			if header.GetIsIndex() {
				currentPath = stagingIndex
			}
			currentPath = filepath.Join(currentPath, header.GetFilename())

			file, openErr := os.OpenFile(currentPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			if openErr != nil {
				return fmt.Errorf("create staged file %s: %w", header.GetFilename(), openErr)
			}
			currentFile = file
			currentHash = crc32.NewIEEE()
			log.Printf("[FOLLOWER] Receiving snapshot file %s (%d bytes)", header.GetFilename(), header.GetFileSize())
			continue
		}

		if data := chunk.GetData(); data != nil {
			if currentFile == nil {
				return fmt.Errorf("received data before header")
			}
			if int64(len(data)) > currentHeader.FileSize-received {
				cleanupFile(currentFile)
				return fmt.Errorf("snapshot exceeds declared file size")
			}
			received += int64(len(data))
			if _, writeErr := currentFile.Write(data); writeErr != nil {
				cleanupFile(currentFile)
				return fmt.Errorf("write staged file %s: %w", currentHeader.GetFilename(), writeErr)
			}
			if currentHash != nil {
				currentHash.Write(data)
			}
			continue
		}

		if tr := chunk.GetTrailer(); tr != nil {
			trailer = tr
			break
		}
	}

	if err := finishFile(); err != nil {
		return err
	}
	if trailer == nil || !trailer.GetSuccess() {
		return fmt.Errorf("snapshot did not complete successfully")
	}
	if len(seen) == 0 {
		return fmt.Errorf("empty snapshot manifest")
	}
	// Staged files the leader did not mention belong to an older state of its
	// log, for instance a segment retention has removed since.
	for key := range staged {
		if !seen[key] {
			if err := os.Remove(filepath.Join(stagingDir, filepath.FromSlash(key))); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove stale staged file %s: %w", key, err)
			}
		}
	}
	if reused > 0 {
		log.Printf("[FOLLOWER] Snapshot for partition %d reused %d of %d files from an earlier transfer", partitionID, reused, len(seen))
	}
	if trailer.GetEpoch() < acceptedEpoch {
		_ = os.RemoveAll(stagingDir)
		return fmt.Errorf("snapshot source %s is at epoch %d, behind epoch %d this replica has accepted", leaderAddr, trailer.GetEpoch(), acceptedEpoch)
	}
	f.mu.Lock()
	if err := f.wal.InstallCheckpoint(stagingDir, trailer.LastOffset); err != nil {
		f.mu.Unlock()
		return err
	}

	f.nextOffset = f.wal.GetNextOffset()
	if trailer.GetEpoch() > f.epoch {
		f.epoch = trailer.GetEpoch()
	}
	f.mu.Unlock()
	// InstallCheckpoint moved the staged directories into place.
	_ = os.RemoveAll(stagingDir)

	log.Printf("[FOLLOWER] Snapshot installed for partition %d up to offset %d (epoch %d)", partitionID, trailer.GetLastOffset(), trailer.GetEpoch())
	return nil
}

// stagedFileKey identifies a staged snapshot file by its path below the
// staging directory, with forward slashes.
func stagedFileKey(filename string, isIndex bool) string {
	if isIndex {
		return "index/" + filename
	}
	return "segments/" + filename
}

// stagedSnapshotFiles describes the files an earlier transfer left in the
// staging directory, keyed by stagedFileKey. A file that was cut off is listed
// as it is; its size or checksum will not match the leader's, so it is sent
// again.
func stagedSnapshotFiles(stagingDir string) (map[string]*types.ReplicationSnapshotHeader, error) {
	staged := make(map[string]*types.ReplicationSnapshotHeader)
	for _, isIndex := range []bool{false, true} {
		dir := filepath.Join(stagingDir, "segments")
		if isIndex {
			dir = filepath.Join(stagingDir, "index")
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			size, sum, err := fileSizeAndCRC(filepath.Join(dir, entry.Name()))
			if err != nil {
				return nil, err
			}
			staged[stagedFileKey(entry.Name(), isIndex)] = &types.ReplicationSnapshotHeader{
				Filename: entry.Name(),
				FileSize: size,
				Crc32:    sum,
				IsIndex:  isIndex,
			}
		}
	}
	return staged, nil
}

func fileSizeAndCRC(path string) (int64, uint32, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()
	sum := crc32.NewIEEE()
	size, err := io.Copy(sum, file)
	return size, sum.Sum32(), err
}

// cleanupFile closes and ignores errors; helper for defers during failure paths.
func cleanupFile(f *os.File) {
	if f != nil {
		_ = f.Close()
	}
}

// dialCredentials returns the gRPC transport credentials for replication:
// mTLS when configured, otherwise insecure (dev mode).
func (f *Follower) dialCredentials() (credentials.TransportCredentials, error) {
	if f.tlsConfig != nil && f.tlsConfig.Enabled {
		tlsCfg, err := BuildClientTLSConfig(f.tlsConfig)
		if err != nil {
			return nil, err
		}
		return credentials.NewTLS(tlsCfg), nil
	}
	return insecure.NewCredentials(), nil
}

// GetNextOffset returns the next WAL offset this follower expects to receive.
func (f *Follower) GetNextOffset() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.nextOffset
}

// GetEpoch returns the follower's current leadership epoch.
func (f *Follower) GetEpoch() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.epoch
}

// SetEpoch sets the leadership epoch (used during leader failover).
func (f *Follower) SetEpoch(epoch int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.epoch = epoch
}

// Stop signals the follower to stop background replication work.
func (f *Follower) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()

	select {
	case <-f.quit:
		return
	default:
	}
	close(f.quit)

	log.Printf("[FOLLOWER] Stopped replication for partition %d", f.partitionID)
}

// SetWAL attaches or replaces the local WAL and resets NextOffset from it.
func (f *Follower) SetWAL(wal *storage.WAL) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wal = wal
	f.nextOffset = wal.GetNextOffset()
}

// SetLeader updates the known leader identity, address, and epoch.
func (f *Follower) SetLeader(leaderID, leaderAddr string, epoch int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.leaderID = leaderID
	f.leaderAddr = leaderAddr
	f.epoch = epoch

	log.Printf("[FOLLOWER] Updated leader info to %s (epoch: %d)", leaderID, epoch)
	return nil
}

// IsCatchingUp reports whether a bulk snapshot install is currently in progress.
func (f *Follower) IsCatchingUp() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.catchupMode
}

// GetStats returns a snapshot of follower replication progress.
func (f *Follower) GetStats() *FollowerStats {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return &FollowerStats{
		PartitionID:  f.partitionID,
		LeaderID:     f.leaderID,
		Epoch:        f.epoch,
		NextOffset:   f.nextOffset,
		CatchingUp:   f.catchupMode,
		LastSyncTime: f.lastSyncTime,
	}
}

// FollowerStats is a snapshot of follower replication progress for a partition.
type FollowerStats struct {
	// PartitionID is the partition this follower replicates.
	PartitionID int32
	// LeaderID is the current known leader node ID.
	LeaderID string
	// Epoch is the current leadership epoch.
	Epoch int64
	// NextOffset is the next expected WAL offset from the leader.
	NextOffset int64
	// CatchingUp is true while a bulk snapshot install is in progress.
	CatchingUp bool
	// LastSyncTime is when the last successful catch-up or sync completed.
	LastSyncTime time.Time
}
