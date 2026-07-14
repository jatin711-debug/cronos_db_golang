package api

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReplicationServiceHandler implements internal leader/follower replication RPCs.
type ReplicationServiceHandler struct {
	types.UnimplementedReplicationServiceServer
	partitionManager *partition.PartitionManager
}

// NewReplicationServiceHandler creates a replication service handler.
func NewReplicationServiceHandler(pm *partition.PartitionManager) *ReplicationServiceHandler {
	return &ReplicationServiceHandler{partitionManager: pm}
}

// Append appends a leader-provided batch into the local follower WAL.
func (h *ReplicationServiceHandler) Append(ctx context.Context, req *types.ReplicationAppendRequest) (*types.ReplicationAppendResponse, error) {
	_ = ctx

	if req == nil || req.GetPartitionId() < 0 {
		return nil, status.Error(codes.InvalidArgument, "valid partition_id is required")
	}

	if h.partitionManager == nil {
		return nil, status.Error(codes.Unavailable, "partition manager not initialized")
	}

	topic := fmt.Sprintf("partition-%d", req.GetPartitionId())
	if events := req.GetEvents(); len(events) > 0 && events[0] != nil && events[0].GetTopic() != "" {
		topic = events[0].GetTopic()
	}

	p, err := h.partitionManager.GetOrCreateInternalPartition(req.GetPartitionId(), topic)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get partition %d: %v", req.GetPartitionId(), err)
	}
	if p.Wal == nil {
		return nil, status.Errorf(codes.Internal, "partition %d WAL not initialized", req.GetPartitionId())
	}

	// Term validation: reject stale leaders, step up on newer term.
	if req.GetTerm() != 0 {
		if req.GetTerm() < p.Epoch {
			return &types.ReplicationAppendResponse{
				Success:    false,
				Error:      fmt.Sprintf("stale term: got %d, current %d", req.GetTerm(), p.Epoch),
				LastOffset: p.Wal.GetLastOffset(),
				NextOffset: p.Wal.GetNextOffset(),
				Term:       p.Epoch,
			}, nil
		}
		if req.GetTerm() > p.Epoch {
			p.Epoch = req.GetTerm()
		}
	}

	if len(req.GetEvents()) > 0 {
		if err := p.Wal.AppendReplicatedBatch(req.GetEvents()); err != nil {
			return &types.ReplicationAppendResponse{
				Success:    false,
				Error:      fmt.Sprintf("append replicated batch: %v", err),
				LastOffset: p.Wal.GetLastOffset(),
				NextOffset: p.Wal.GetNextOffset(),
				Term:       p.Epoch,
			}, nil
		}
	}

	return &types.ReplicationAppendResponse{
		Success:    true,
		LastOffset: p.Wal.GetLastOffset(),
		NextOffset: p.Wal.GetNextOffset(),
		Term:       p.Epoch,
	}, nil
}

// Sync streams events from local WAL to followers for catch-up replication.
func (h *ReplicationServiceHandler) Sync(req *types.ReplicationSyncRequest, stream grpc.ServerStreamingServer[types.ReplicationSyncResponse]) error {
	if req == nil || req.GetPartitionId() < 0 {
		return status.Error(codes.InvalidArgument, "valid partition_id is required")
	}

	if h.partitionManager == nil {
		return status.Error(codes.Unavailable, "partition manager not initialized")
	}

	p, err := h.partitionManager.GetInternalPartition(req.GetPartitionId())
	if err != nil || p.Wal == nil {
		return status.Errorf(codes.NotFound, "partition %d not found", req.GetPartitionId())
	}

	const batchSize int64 = 100
	offset := req.GetStartOffset()
	maxBytes := req.GetMaxBytes()
	if maxBytes <= 0 {
		maxBytes = 10 << 20 // 10MB default
	}

	var sentBytes int64
	for sentBytes < maxBytes {
		events, err := p.Wal.ReadEvents(offset, offset+batchSize)
		if err != nil {
			return status.Errorf(codes.Internal, "read events: %v", err)
		}
		if len(events) == 0 {
			break
		}

		sendEvents := make([]*types.Event, 0, len(events))
		for _, event := range events {
			eventSize := int64(len(event.GetPayload()))
			if len(sendEvents) > 0 && sentBytes+eventSize > maxBytes {
				break
			}
			sendEvents = append(sendEvents, event)
			sentBytes += eventSize
			if sentBytes >= maxBytes {
				break
			}
		}

		if len(sendEvents) == 0 {
			break
		}

		nextOffset := offset + int64(len(sendEvents))
		hasMore := len(sendEvents) < len(events)
		if !hasMore {
			nextEvents, _ := p.Wal.ReadEvents(nextOffset, nextOffset+1)
			hasMore = len(nextEvents) > 0
		}

		if err := stream.Send(&types.ReplicationSyncResponse{
			Success: true,
			Events:  sendEvents,
			HasMore: hasMore,
		}); err != nil {
			return status.Errorf(codes.Internal, "send: %v", err)
		}

		offset = nextOffset
		if !hasMore {
			break
		}
	}

	return nil
}

// snapshotFile describes one file (segment log or index) to stream to a follower.
type snapshotFile struct {
	filename    string
	path        string
	firstOffset int64
	lastOffset  int64
	fileSize    int64
	crc32       uint32
	isIndex     bool
}

// Snapshot streams the leader's segment files and sparse indexes for a partition
// so a follower can install them as a bulk snapshot. The active segment is flushed
// before streaming so the on-disk view is consistent. Files whose last offset is
// below the requested start offset are skipped.
func (h *ReplicationServiceHandler) Snapshot(req *types.ReplicationSnapshotRequest, stream grpc.ServerStreamingServer[types.ReplicationSnapshotChunk]) error {
	if req == nil || req.GetPartitionId() < 0 {
		return status.Error(codes.InvalidArgument, "valid partition_id is required")
	}
	if h.partitionManager == nil {
		return status.Error(codes.Unavailable, "partition manager not initialized")
	}

	p, err := h.partitionManager.GetInternalPartition(req.GetPartitionId())
	if err != nil || p == nil || p.Wal == nil {
		return status.Errorf(codes.NotFound, "partition %d not found", req.GetPartitionId())
	}

	// Flush the active segment so its on-disk view is consistent with memory.
	if active := p.Wal.GetActiveSegment(); active != nil {
		if flushErr := active.Flush(); flushErr != nil {
			return status.Errorf(codes.Internal, "flush active segment: %v", flushErr)
		}
	}

	maxBytes := req.GetMaxBytes()
	if maxBytes <= 0 {
		maxBytes = 1 << 30 // 1GB default soft cap
	}
	startOffset := req.GetStartOffset()

	// Build the list of files to transfer.
	files, err := h.buildSnapshotFileList(p, startOffset)
	if err != nil {
		return status.Errorf(codes.Internal, "build snapshot file list: %v", err)
	}

	const chunkSize = 1 << 20 // 1MB
	var sentBytes int64

	for _, f := range files {
		if sentBytes+f.fileSize > maxBytes {
			_ = stream.Send(&types.ReplicationSnapshotChunk{
				Payload: &types.ReplicationSnapshotChunk_Trailer{
					Trailer: &types.ReplicationSnapshotTrailer{
						Success:    false,
						Error:      fmt.Sprintf("snapshot exceeded max_bytes limit %d", maxBytes),
						LastOffset: p.Wal.GetHighWatermark(),
						Epoch:      p.Epoch,
					},
				},
			})
			return status.Errorf(codes.ResourceExhausted, "snapshot exceeded max_bytes limit %d", maxBytes)
		}

		// Send header.
		if sendErr := stream.Send(&types.ReplicationSnapshotChunk{
			Payload: &types.ReplicationSnapshotChunk_Header{
				Header: &types.ReplicationSnapshotHeader{
					Filename:    f.filename,
					FirstOffset: f.firstOffset,
					LastOffset:  f.lastOffset,
					FileSize:    f.fileSize,
					Crc32:       f.crc32,
					IsIndex:     f.isIndex,
				},
			},
		}); sendErr != nil {
			return status.Errorf(codes.Internal, "send header: %v", sendErr)
		}

		// Stream file contents in chunks.
		file, openErr := os.Open(f.path)
		if openErr != nil {
			return status.Errorf(codes.Internal, "open snapshot file %s: %v", f.filename, openErr)
		}
		buf := make([]byte, chunkSize)
		for {
			n, readErr := file.Read(buf)
			if n > 0 {
				if sendErr := stream.Send(&types.ReplicationSnapshotChunk{
					Payload: &types.ReplicationSnapshotChunk_Data{Data: buf[:n]},
				}); sendErr != nil {
					_ = file.Close()
					return status.Errorf(codes.Internal, "send data: %v", sendErr)
				}
				sentBytes += int64(n)
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				_ = file.Close()
				return status.Errorf(codes.Internal, "read snapshot file %s: %v", f.filename, readErr)
			}
		}
		_ = file.Close()
	}

	return stream.Send(&types.ReplicationSnapshotChunk{
		Payload: &types.ReplicationSnapshotChunk_Trailer{
			Trailer: &types.ReplicationSnapshotTrailer{
				Success:    true,
				LastOffset: p.Wal.GetHighWatermark(),
				Epoch:      p.Epoch,
			},
		},
	})
}

// buildSnapshotFileList returns segment log and index files that overlap the
// requested start offset, ordered by first offset.
func (h *ReplicationServiceHandler) buildSnapshotFileList(p *partition.Partition, startOffset int64) ([]snapshotFile, error) {
	dataDir := p.Wal.GetDataDir()
	segmentsDir := filepath.Join(dataDir, "segments")
	indexDir := filepath.Join(dataDir, "index")

	var files []snapshotFile
	for _, seg := range p.Wal.GetSegments() {
		if seg.GetLastOffset() < startOffset {
			continue
		}

		logPath := filepath.Join(segmentsDir, seg.GetFilename())
		logInfo, statErr := os.Stat(logPath)
		if statErr != nil {
			return nil, fmt.Errorf("stat segment %s: %w", seg.GetFilename(), statErr)
		}
		logCRC, crcErr := fileCRC32(logPath)
		if crcErr != nil {
			return nil, fmt.Errorf("checksum segment %s: %w", seg.GetFilename(), crcErr)
		}
		files = append(files, snapshotFile{
			filename:    seg.GetFilename(),
			path:        logPath,
			firstOffset: seg.GetFirstOffset(),
			lastOffset:  seg.GetLastOffset(),
			fileSize:    logInfo.Size(),
			crc32:       logCRC,
			isIndex:     false,
		})

		indexFilename := strings.TrimSuffix(seg.GetFilename(), ".log") + ".index"
		indexPath := filepath.Join(indexDir, indexFilename)
		if indexInfo, statErr := os.Stat(indexPath); statErr == nil {
			indexCRC, crcErr := fileCRC32(indexPath)
			if crcErr != nil {
				return nil, fmt.Errorf("checksum index %s: %w", indexFilename, crcErr)
			}
			files = append(files, snapshotFile{
				filename:    indexFilename,
				path:        indexPath,
				firstOffset: seg.GetFirstOffset(),
				lastOffset:  seg.GetFirstOffset(),
				fileSize:    indexInfo.Size(),
				crc32:       indexCRC,
				isIndex:     true,
			})
		}
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].firstOffset != files[j].firstOffset {
			return files[i].firstOffset < files[j].firstOffset
		}
		// Log files before indexes for the same offset.
		return !files[i].isIndex && files[j].isIndex
	})

	return files, nil
}

// fileCRC32 computes the IEEE CRC32 of a file.
func fileCRC32(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	h := crc32.NewIEEE()
	if _, err := io.Copy(h, f); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

// parseOffsetFromFilename extracts the starting offset from a segment filename
// such as "00000000000000001234.log". It is used as a fallback when a segment
// object is not available.
func parseOffsetFromFilename(name string) (int64, error) {
	name = strings.TrimSuffix(name, ".log")
	name = strings.TrimSuffix(name, ".index")
	return strconv.ParseInt(name, 10, 64)
}
