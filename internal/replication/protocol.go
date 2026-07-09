package replication

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/protobuf/proto"
)

// BinaryProtocol constants
const (
	ProtocolVersion = 1
	MagicBytes      = 0xCAFEBABE
)

// Message types
const (
	MsgTypeHandshake           = 1
	MsgTypeHandshakeAck        = 2
	MsgTypeAppendEntries       = 3
	MsgTypeAppendAck           = 4
	MsgTypeHeartbeat           = 5
	MsgTypeHeartbeatAck        = 6
	MsgTypeFileTransferRequest = 7
	MsgTypeFileTransferStart   = 8
	MsgTypeFileTransferData    = 9
	MsgTypeFileTransferEnd     = 10
)

// Transport manages binary protocol connections
type Transport struct {
	conn net.Conn
}

// NewTransport creates a new transport
func NewTransport(conn net.Conn) *Transport {
	return &Transport{conn: conn}
}

// Close closes the transport
func (t *Transport) Close() error {
	return t.conn.Close()
}

var transportWritePool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 65536)
	},
}

// WriteMessage writes a message (header + payload combined into single TCP write)
func (t *Transport) WriteMessage(msgType uint8, payload []byte) error {
	totalLen := 10 + len(payload)
	var buf []byte
	if totalLen <= 65536 {
		x := transportWritePool.Get().([]byte)
		buf = x[:totalLen]
		defer transportWritePool.Put(x)
	} else {
		buf = make([]byte, totalLen)
	}

	binary.BigEndian.PutUint32(buf[0:4], uint32(MagicBytes))
	buf[4] = ProtocolVersion
	buf[5] = msgType
	binary.BigEndian.PutUint32(buf[6:10], uint32(len(payload)))
	copy(buf[10:], payload)

	_, err := t.conn.Write(buf)
	if err != nil {
		return fmt.Errorf("write message: %w", err)
	}
	return nil
}

// WriteProtoMessage serializes a protobuf message directly into the packet buffer
// and writes it to the connection in a single write, avoiding intermediate allocations.
func (t *Transport) WriteProtoMessage(msgType uint8, msg proto.Message) error {
	msgSize := proto.Size(msg)
	totalLen := 10 + msgSize

	var buf []byte
	var isPooled bool
	var pooledBuf []byte

	if totalLen <= 65536 {
		pooledBuf = transportWritePool.Get().([]byte)
		buf = pooledBuf[:10]
		isPooled = true
	} else {
		buf = make([]byte, 10, totalLen)
	}

	binary.BigEndian.PutUint32(buf[0:4], uint32(MagicBytes))
	buf[4] = ProtocolVersion
	buf[5] = msgType
	binary.BigEndian.PutUint32(buf[6:10], uint32(msgSize))

	var err error
	buf, err = proto.MarshalOptions{}.MarshalAppend(buf, msg)
	if err != nil {
		if isPooled {
			transportWritePool.Put(pooledBuf)
		}
		return fmt.Errorf("marshal proto message: %w", err)
	}

	_, err = t.conn.Write(buf)
	if isPooled {
		transportWritePool.Put(pooledBuf)
	}

	if err != nil {
		return fmt.Errorf("write proto message: %w", err)
	}
	return nil
}

// ReadMessage reads a message
func (t *Transport) ReadMessage() (uint8, []byte, error) {
	// Read header
	header := make([]byte, 10)
	if _, err := io.ReadFull(t.conn, header); err != nil {
		return 0, nil, err
	}

	// Validate magic
	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != MagicBytes {
		return 0, nil, fmt.Errorf("invalid magic bytes: %x", magic)
	}

	// Validate version
	version := header[4]
	if version != ProtocolVersion {
		return 0, nil, fmt.Errorf("unsupported protocol version: %d", version)
	}

	msgType := header[5]
	payloadLen := binary.BigEndian.Uint32(header[6:10])

	// Read payload
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(t.conn, payload); err != nil {
			return 0, nil, fmt.Errorf("read payload: %w", err)
		}
	}

	return msgType, payload, nil
}

// ============================================================================
// Protocol Messages
// ============================================================================

// HandshakeMessage
type HandshakeMessage struct {
	NodeID string
}

func (m *HandshakeMessage) Encode() ([]byte, error) {
	return []byte(m.NodeID), nil
}

func (m *HandshakeMessage) Decode(data []byte) error {
	m.NodeID = string(data)
	return nil
}

// AppendEntriesMessage
type AppendEntriesMessage struct {
	PartitionId  int32
	Term         int64
	PrevLogIndex int64
	PrevLogTerm  int64
	CommitIndex  int64
	Events       []*types.Event
}

func (m *AppendEntriesMessage) Encode() ([]byte, error) {
	req := &types.ReplicationAppendRequest{
		PartitionId:        m.PartitionId,
		Events:             m.Events,
		ExpectedNextOffset: m.PrevLogIndex + 1,
		Term:               m.Term,
		PrevLogTerm:        m.PrevLogTerm,
	}
	return proto.Marshal(req)
}

func (m *AppendEntriesMessage) Decode(data []byte) error {
	req := &types.ReplicationAppendRequest{}
	if err := proto.Unmarshal(data, req); err != nil {
		return err
	}
	m.PartitionId = req.PartitionId
	m.Term = req.Term
	m.Events = req.Events
	m.PrevLogIndex = req.ExpectedNextOffset - 1
	m.PrevLogTerm = req.PrevLogTerm
	return nil
}

// AppendAckMessage
type AppendAckMessage struct {
	Term    int64
	Success bool
	Offset  int64
}

func (m *AppendAckMessage) Encode() ([]byte, error) {
	resp := &types.ReplicationAppendResponse{
		Success:    m.Success,
		LastOffset: m.Offset,
		Term:       m.Term,
	}
	return proto.Marshal(resp)
}

func (m *AppendAckMessage) Decode(data []byte) error {
	resp := &types.ReplicationAppendResponse{}
	if err := proto.Unmarshal(data, resp); err != nil {
		return err
	}
	m.Success = resp.Success
	m.Offset = resp.LastOffset
	m.Term = resp.Term
	return nil
}

// HeartbeatMessage keeps the leader-follower connection alive and carries the
// leader's current term and commit index so followers can detect stale leaders.
type HeartbeatMessage struct {
	Term        int64
	CommitIndex int64
}

func (m *HeartbeatMessage) Encode() ([]byte, error) {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], uint64(m.Term))
	binary.BigEndian.PutUint64(buf[8:16], uint64(m.CommitIndex))
	return buf, nil
}

func (m *HeartbeatMessage) Decode(data []byte) error {
	if len(data) < 16 {
		return fmt.Errorf("HeartbeatMessage: data too short")
	}
	m.Term = int64(binary.BigEndian.Uint64(data[0:8]))
	m.CommitIndex = int64(binary.BigEndian.Uint64(data[8:16]))
	return nil
}

// FileTransferRequestMessage requests bulk segment file transfer
type FileTransferRequestMessage struct {
	PartitionId int32
}

func (m *FileTransferRequestMessage) Encode() ([]byte, error) {
	resp := &types.ReplicationSyncRequest{
		PartitionId: m.PartitionId,
	}
	return proto.Marshal(resp)
}

func (m *FileTransferRequestMessage) Decode(data []byte) error {
	req := &types.ReplicationSyncRequest{}
	if err := proto.Unmarshal(data, req); err != nil {
		return err
	}
	m.PartitionId = req.PartitionId
	return nil
}

// FileTransferStartMessage indicates start of file transfer
type FileTransferStartMessage struct {
	Filename        string
	FileSize        int64
	Epoch           int64
	StartOffset     int64
	EndOffset       int64
	SegmentChecksum uint32
}

func (m *FileTransferStartMessage) Encode() ([]byte, error) {
	nameLen := len(m.Filename)
	buf := make([]byte, 8+nameLen+8*4+4)
	binary.BigEndian.PutUint64(buf[0:8], uint64(nameLen))
	copy(buf[8:8+nameLen], m.Filename)
	off := 8 + nameLen
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(m.FileSize))
	off += 8
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(m.Epoch))
	off += 8
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(m.StartOffset))
	off += 8
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(m.EndOffset))
	off += 8
	binary.BigEndian.PutUint32(buf[off:off+4], m.SegmentChecksum)
	return buf, nil
}

func (m *FileTransferStartMessage) Decode(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("FileTransferStartMessage: data too short")
	}
	nameLen := int(binary.BigEndian.Uint64(data[0:8]))
	if len(data) < 8+nameLen+8*4+4 {
		return fmt.Errorf("FileTransferStartMessage: data too short for fields")
	}
	m.Filename = string(data[8 : 8+nameLen])
	off := 8 + nameLen
	m.FileSize = int64(binary.BigEndian.Uint64(data[off : off+8]))
	off += 8
	m.Epoch = int64(binary.BigEndian.Uint64(data[off : off+8]))
	off += 8
	m.StartOffset = int64(binary.BigEndian.Uint64(data[off : off+8]))
	off += 8
	m.EndOffset = int64(binary.BigEndian.Uint64(data[off : off+8]))
	off += 8
	m.SegmentChecksum = binary.BigEndian.Uint32(data[off : off+4])
	return nil
}

// FileTransferDataMessage contains segment file data chunk
type FileTransferDataMessage struct {
	Data []byte
}

func (m *FileTransferDataMessage) Encode() ([]byte, error) {
	return m.Data, nil
}

func (m *FileTransferDataMessage) Decode(data []byte) error {
	m.Data = data
	return nil
}

// FileTransferEndMessage indicates end of file transfer
type FileTransferEndMessage struct {
	Success      bool
	Epoch        int64
	LastOffset   int64
	FileChecksum uint32
}

func (m *FileTransferEndMessage) Encode() ([]byte, error) {
	buf := make([]byte, 1+8+8+4)
	if m.Success {
		buf[0] = 1
	}
	binary.BigEndian.PutUint64(buf[1:9], uint64(m.Epoch))
	binary.BigEndian.PutUint64(buf[9:17], uint64(m.LastOffset))
	binary.BigEndian.PutUint32(buf[17:21], m.FileChecksum)
	return buf, nil
}

func (m *FileTransferEndMessage) Decode(data []byte) error {
	if len(data) < 21 {
		return fmt.Errorf("FileTransferEndMessage: data too short")
	}
	m.Success = data[0] != 0
	m.Epoch = int64(binary.BigEndian.Uint64(data[1:9]))
	m.LastOffset = int64(binary.BigEndian.Uint64(data[9:17]))
	m.FileChecksum = binary.BigEndian.Uint32(data[17:21])
	return nil
}
