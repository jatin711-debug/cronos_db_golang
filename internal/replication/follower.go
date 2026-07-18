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
func (f *Follower) InstallSnapshot(ctx context.Context, leaderAddr string, partitionID int32, startOffset int64) error {
	f.mu.Lock()
	if f.catchupMode {
		f.mu.Unlock()
		return fmt.Errorf("already catching up")
	}
	f.catchupMode = true
	walDataDir := f.wal.GetDataDir()
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

	client := types.NewReplicationServiceClient(conn)
	stream, err := client.Snapshot(ctx, &types.ReplicationSnapshotRequest{
		PartitionId: partitionID,
		StartOffset: startOffset,
		MaxBytes:    0,
	})
	if err != nil {
		return fmt.Errorf("snapshot RPC: %w", err)
	}

	// Stage files in a temporary directory so the existing WAL is not touched
	// until the whole snapshot is received and verified.
	stagingDir := filepath.Join(walDataDir, "snapshot-staging")
	stagingSegments := filepath.Join(stagingDir, "segments")
	stagingIndex := filepath.Join(stagingDir, "index")
	if err := os.RemoveAll(stagingDir); err != nil {
		return fmt.Errorf("clean staging dir: %w", err)
	}
	if err := os.MkdirAll(stagingSegments, 0755); err != nil {
		return fmt.Errorf("create staging segments dir: %w", err)
	}
	if err := os.MkdirAll(stagingIndex, 0755); err != nil {
		return fmt.Errorf("create staging index dir: %w", err)
	}

	var currentFile *os.File
	var currentPath string
	var currentHash hash.Hash32
	var currentHeader *types.ReplicationSnapshotHeader
	var trailer *types.ReplicationSnapshotTrailer

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
			cleanupFile(currentFile)
			currentFile = nil
			currentHash = nil
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

	cleanupFile(currentFile)
	currentFile = nil

	if trailer == nil {
		_ = os.RemoveAll(stagingDir)
		return fmt.Errorf("snapshot stream closed without trailer")
	}
	if !trailer.GetSuccess() {
		_ = os.RemoveAll(stagingDir)
		return fmt.Errorf("leader snapshot failed: %s", trailer.GetError())
	}

	// Verify the last received file's checksum if we were mid-file.
	if currentHeader != nil && currentHeader.GetCrc32() != 0 && currentHash != nil {
		if got := currentHash.Sum32(); got != currentHeader.GetCrc32() {
			_ = os.RemoveAll(stagingDir)
			return fmt.Errorf("snapshot checksum mismatch for %s: computed %08x, expected %08x", currentHeader.GetFilename(), got, currentHeader.GetCrc32())
		}
	}

	// Close the local WAL before replacing segment files. This releases file
	// handles on Windows so the directory swap can succeed.
	f.mu.Lock()
	wal := f.wal
	f.mu.Unlock()
	if wal != nil {
		if closeErr := wal.Close(); closeErr != nil {
			_ = os.RemoveAll(stagingDir)
			return fmt.Errorf("close local WAL before snapshot install: %w", closeErr)
		}
	}

	// Atomically swap staged directories into place.
	segmentsDir := filepath.Join(walDataDir, "segments")
	indexDir := filepath.Join(walDataDir, "index")
	oldSegmentsDir := filepath.Join(walDataDir, "segments.old")
	oldIndexDir := filepath.Join(walDataDir, "index.old")

	if err := os.RemoveAll(oldSegmentsDir); err != nil {
		return fmt.Errorf("remove old segments dir: %w", err)
	}
	if err := os.RemoveAll(oldIndexDir); err != nil {
		return fmt.Errorf("remove old index dir: %w", err)
	}
	if err := os.Rename(segmentsDir, oldSegmentsDir); err != nil {
		return fmt.Errorf("rename segments dir: %w", err)
	}
	if err := os.Rename(indexDir, oldIndexDir); err != nil {
		return fmt.Errorf("rename index dir: %w", err)
	}
	if err := os.Rename(stagingSegments, segmentsDir); err != nil {
		return fmt.Errorf("move staged segments: %w", err)
	}
	if err := os.Rename(stagingIndex, indexDir); err != nil {
		return fmt.Errorf("move staged index: %w", err)
	}
	_ = os.RemoveAll(oldSegmentsDir)
	_ = os.RemoveAll(oldIndexDir)

	// Reload WAL to pick up the new segments.
	f.mu.Lock()
	if reloadErr := f.wal.ReloadSegments(); reloadErr != nil {
		f.mu.Unlock()
		return fmt.Errorf("reload WAL after snapshot install: %w", reloadErr)
	}
	f.nextOffset = f.wal.GetNextOffset()
	if trailer.GetEpoch() > f.epoch {
		f.epoch = trailer.GetEpoch()
	}
	f.mu.Unlock()

	log.Printf("[FOLLOWER] Snapshot installed for partition %d up to offset %d (epoch %d)", partitionID, trailer.GetLastOffset(), trailer.GetEpoch())
	return nil
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
