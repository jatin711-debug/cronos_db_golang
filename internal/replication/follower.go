package replication

import (
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
)

// Follower handles replication from leader
type Follower struct {
	mu           sync.RWMutex
	partitionID  int32
	epoch        int64
	leaderID     string
	leaderAddr   string
	nextOffset   int64
	wal          *storage.WAL
	quit         chan struct{}
	active       bool
	lastSyncTime time.Time
	syncInterval time.Duration
	catchupMode  bool // True when catching up with leader
	listener     net.Listener
	nodeID       string
	tlsConfig    *MTLSConfig // optional mTLS for replication listener
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

// SyncFilesFromLeader actively pulls raw disk segment files from the leader
func (f *Follower) SyncFilesFromLeader() error {
	f.mu.Lock()
	if f.catchupMode {
		f.mu.Unlock()
		return fmt.Errorf("already catching up")
	}
	f.catchupMode = true
	leaderAddr := f.leaderAddr
	partitionID := f.partitionID
	walDataDir := f.wal.GetDataDir()
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		f.catchupMode = false
		f.mu.Unlock()
	}()

	log.Printf("[FOLLOWER] Connecting to leader %s for bulk file sync...", leaderAddr)
	conn, err := net.DialTimeout("tcp", leaderAddr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial leader: %w", err)
	}
	defer conn.Close()

	transport := NewTransport(conn)

	// Send Handshake
	hs := &HandshakeMessage{NodeID: f.nodeID}
	pl, _ := hs.Encode()
	if err := transport.WriteMessage(MsgTypeHandshake, pl); err != nil {
		return err
	}

	// Request bulk file stream
	req := &FileTransferRequestMessage{PartitionId: partitionID}
	pl, _ = req.Encode()
	if err := transport.WriteMessage(MsgTypeFileTransferRequest, pl); err != nil {
		return fmt.Errorf("request file transfer: %w", err)
	}

	segmentsDir := filepath.Join(walDataDir, "segments")
	var currentFile *os.File
	var currentFilePath string
	var currentHash hash.Hash32

	for {
		msgType, payload, err := transport.ReadMessage()
		if err != nil {
			if currentFile != nil {
				currentFile.Close()
			}
			if err == io.EOF {
				return fmt.Errorf("leader disconnected during bulk sync")
			}
			return fmt.Errorf("read message: %w", err)
		}

		switch msgType {
		case MsgTypeFileTransferStart:
			msg := &FileTransferStartMessage{}
			if err := msg.Decode(payload); err != nil {
				if currentFile != nil {
					currentFile.Close()
				}
				return err
			}

			f.mu.RLock()
			localEpoch := f.epoch
			f.mu.RUnlock()
			if msg.Epoch < localEpoch {
				if currentFile != nil {
					currentFile.Close()
				}
				return fmt.Errorf("bulk sync rejected stale leader epoch %d (local %d)", msg.Epoch, localEpoch)
			}

			currentFilePath = filepath.Join(segmentsDir, msg.Filename)

			// Open new file for writing
			log.Printf("[FOLLOWER] Receiving segment file: %s (%d bytes)", msg.Filename, msg.FileSize)
			if currentFile != nil {
				currentFile.Close()
			}
			currentFile, err = os.OpenFile(currentFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			if err != nil {
				return fmt.Errorf("create segment file %s: %w", msg.Filename, err)
			}
			currentHash = crc32.NewIEEE()

		case MsgTypeFileTransferData:
			msg := &FileTransferDataMessage{}
			if err := msg.Decode(payload); err != nil {
				if currentFile != nil {
					currentFile.Close()
				}
				return err
			}
			if currentFile == nil {
				return fmt.Errorf("received data without FileTransferStart")
			}
			if _, err := currentFile.Write(msg.Data); err != nil {
				currentFile.Close()
				return fmt.Errorf("write segment data: %w", err)
			}
			if currentHash != nil {
				currentHash.Write(msg.Data)
			}

		case MsgTypeFileTransferEnd:
			msg := &FileTransferEndMessage{}
			if err := msg.Decode(payload); err != nil {
				if currentFile != nil {
					currentFile.Close()
				}
				return err
			}
			if currentFile != nil {
				if err := currentFile.Sync(); err != nil {
					currentFile.Close()
					return fmt.Errorf("sync segment file %s: %w", currentFilePath, err)
				}
				currentFile.Close()
				currentFile = nil
			}
			if !msg.Success {
				return fmt.Errorf("leader reported failure during bulk transfer")
			}

			f.mu.RLock()
			localEpoch := f.epoch
			f.mu.RUnlock()
			if msg.Epoch < localEpoch {
				return fmt.Errorf("bulk sync end rejected stale leader epoch %d (local %d)", msg.Epoch, localEpoch)
			}

			if currentHash != nil {
				receivedChecksum := currentHash.Sum32()
				if msg.FileChecksum != 0 && receivedChecksum != msg.FileChecksum {
					return fmt.Errorf("bulk sync checksum mismatch for %s: computed %x, expected %x", currentFilePath, receivedChecksum, msg.FileChecksum)
				}
				currentHash = nil
			}

			log.Printf("[FOLLOWER] Bulk file sync completed successfully")

			// Reload WAL to pick up new segments
			f.mu.Lock()
			if msg.Epoch > f.epoch {
				f.epoch = msg.Epoch
			}
			f.wal.ReloadSegments()
			f.nextOffset = f.wal.GetNextOffset()
			f.mu.Unlock()

			return nil
		default:
			log.Printf("[FOLLOWER] Ignored unexpected msg type %d during bulk sync", msgType)
		}
	}
}

// GetNextOffset returns next offset to replicate
func (f *Follower) GetNextOffset() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.nextOffset
}

// GetEpoch returns the follower's epoch
func (f *Follower) GetEpoch() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.epoch
}

// SetEpoch sets the epoch (used during leader failover)
func (f *Follower) SetEpoch(epoch int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.epoch = epoch
}

// Stop stops the follower
func (f *Follower) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.active {
		return
	}

	f.active = false
	close(f.quit)

	if f.listener != nil {
		f.listener.Close()
	}

	log.Printf("[FOLLOWER] Stopped replication for partition %d", f.partitionID)
}

// SetWAL sets the WAL for this follower
func (f *Follower) SetWAL(wal *storage.WAL) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wal = wal
	f.nextOffset = wal.GetNextOffset()
}

// SetLeader updates the leader information
func (f *Follower) SetLeader(leaderID, leaderAddr string, epoch int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.leaderID = leaderID
	f.leaderAddr = leaderAddr
	f.epoch = epoch

	log.Printf("[FOLLOWER] Updated leader info to %s (epoch: %d)", leaderID, epoch)
	return nil
}

// IsCatchingUp returns true if follower is catching up
func (f *Follower) IsCatchingUp() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.catchupMode
}

// GetStats returns follower statistics
func (f *Follower) GetStats() *FollowerStats {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return &FollowerStats{
		PartitionID:  f.partitionID,
		LeaderID:     f.leaderID,
		Epoch:        f.epoch,
		NextOffset:   f.nextOffset,
		Active:       f.active,
		CatchingUp:   f.catchupMode,
		LastSyncTime: f.lastSyncTime,
	}
}

// FollowerStats represents follower statistics
type FollowerStats struct {
	PartitionID  int32
	LeaderID     string
	Epoch        int64
	NextOffset   int64
	Active       bool
	CatchingUp   bool
	LastSyncTime time.Time
}
