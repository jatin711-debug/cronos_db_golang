package replication

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"cronos_db/pkg/types"
	"google.golang.org/protobuf/proto"
)

// BinaryProtocol constants
const (
	ProtocolVersion = 1
	MagicBytes      = 0xCAFEBABE
)

// Message types
const (
	MsgTypeHandshake      = 1
	MsgTypeHandshakeAck   = 2
	MsgTypeAppendEntries  = 3
	MsgTypeAppendAck      = 4
	MsgTypeHeartbeat      = 5
	MsgTypeHeartbeatAck   = 6
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

// WriteMessage writes a message
func (t *Transport) WriteMessage(msgType uint8, payload []byte) error {
	// Header: [Magic:4][Version:1][Type:1][PayloadLen:4]
	header := make([]byte, 10)
	binary.BigEndian.PutUint32(header[0:4], uint32(MagicBytes))
	header[4] = ProtocolVersion
	header[5] = msgType
	binary.BigEndian.PutUint32(header[6:10], uint32(len(payload)))

	// Write header
	if _, err := t.conn.Write(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Write payload
	if len(payload) > 0 {
		if _, err := t.conn.Write(payload); err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
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
	Term         int64
	PrevLogIndex int64
	PrevLogTerm  int64
	CommitIndex  int64
	Events       []*types.Event
}

func (m *AppendEntriesMessage) Encode() ([]byte, error) {
	// Custom binary encoding for speed, or protobuf?
	// Using protobuf for Events is easiest as they are already proto structs.
	// But let's wrap the whole thing in a proto for simplicity of implementation
	// given we have `types.Event`.

	// Create a wrapper proto
	req := &types.ReplicationAppendRequest{
		PartitionId:        0, // TODO
		Events:             m.Events,
		ExpectedNextOffset: m.PrevLogIndex + 1, // Approximation
		Term:               m.Term,
	}
	return proto.Marshal(req)
}

func (m *AppendEntriesMessage) Decode(data []byte) error {
	req := &types.ReplicationAppendRequest{}
	if err := proto.Unmarshal(data, req); err != nil {
		return err
	}
	m.Term = req.Term
	m.Events = req.Events
	m.PrevLogIndex = req.ExpectedNextOffset - 1 // Approximation
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
	return nil
}
