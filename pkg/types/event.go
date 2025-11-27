package types

import "time"

// Event represents a timestamp-triggered event
type Event struct {
	MessageID  string            `json:"message_id"`
	ScheduleTS int64             `json:"schedule_ts"`
	Payload    []byte            `json:"payload"`
	Topic      string            `json:"topic"`
	Meta       map[string]string `json:"meta"`
	CreatedTS  int64             `json:"created_ts"`
	Offset     int64             `json:"offset"`
	Partition  int32             `json:"partition_id"`
	Checksum   uint32            `json:"checksum"`
}

// NewEvent creates a new event
func NewEvent(messageID string, scheduleTS int64, payload []byte, topic string) *Event {
	return &Event{
		MessageID:  messageID,
		ScheduleTS: scheduleTS,
		Payload:    payload,
		Topic:      topic,
		Meta:       make(map[string]string),
		CreatedTS:  time.Now().UnixMilli(),
	}
}

// IsExpired checks if event is past its schedule time
func (e *Event) IsExpired() bool {
	return e.ScheduleTS <= time.Now().UnixMilli()
}

// Age returns age of event in milliseconds
func (e *Event) Age() int64 {
	return time.Now().UnixMilli() - e.CreatedTS
}

// Delivery represents an event being delivered to a subscriber
type Delivery struct {
	Event       *Event
	DeliveryID  string
	Attempt     int32
	AckTimeout  time.Duration
	CreatedTS   int64
}

// NewDelivery creates a new delivery
func NewDelivery(event *Event, attempt int32, ackTimeout time.Duration) *Delivery {
	return &Delivery{
		Event:      event,
		DeliveryID: generateDeliveryID(),
		Attempt:    attempt,
		AckTimeout: ackTimeout,
		CreatedTS:  time.Now().UnixMilli(),
	}
}

// SegmentInfo represents information about a WAL segment
type SegmentInfo struct {
	Filename    string
	FirstOffset int64
	LastOffset  int64
	FirstTS     int64
	LastTS      int64
	SizeBytes   int64
	IsActive    bool
	CreatedTS   int64
}

// Manifest represents the segment manifest
type Manifest struct {
	PartitionID        int32
	Segments           []*SegmentInfo
	HighWatermark      int64
	LastCompactionTS   int64
	CreatedTS          int64
	UpdatedTS          int64
}

// Checkpoint represents periodic state snapshot
type Checkpoint struct {
	PartitionID      int32
	LastOffset       int64
	LastSegment      string
	HighWatermark    int64
	SchedulerState   *SchedulerState
	ConsumerOffsets  map[string]int64
	DedupStats       *DedupStats
	CreatedTS        int64
	UpdatedTS        int64
}

// SchedulerState represents scheduler state
type SchedulerState struct {
	TickMS      int32
	WheelSize   int32
	ActiveTimers int64
	ReadyEvents int64
	NextTickTS  int64
	WatermarkTS int64
}

// DedupStats represents dedup store statistics
type DedupStats struct {
	TTLHours          int32
	ApproximateCount  int64
	LastPruneTS       int64
}

// ConsumerGroup represents a consumer group
type ConsumerGroup struct {
	GroupID           string
	Topic             string
	Partitions        []int32
	CommittedOffsets  map[int32]int64
	MemberOffsets     map[string]int64
	Members           map[string]*ConsumerMember
	LastRebalanceTS   int64
	CreatedTS         int64
	UpdatedTS         int64
}

// ConsumerMember represents a consumer group member
type ConsumerMember struct {
	MemberID        string
	Address         string
	AssignedPartition int32
	Active          bool
	LastSeenTS      int64
	ConnectedTS     int64
}

// PartitionAssignment represents partition ownership
type PartitionAssignment struct {
	PartitionID int32
	LeaderID    string
	ReplicaIDs  []string
	Term        int64
	CreatedTS   int64
	UpdatedTS   int64
}

// ReplicationState represents follower replication state
type ReplicationState struct {
	PartitionID      int32
	LeaderID         string
	NextOffset       int64
	HighWatermark    int64
	BatchesInFlight  int32
	LastAckTS        int64
	Connected        bool
	CreatedTS        int64
	UpdatedTS        int64
}

// DeliveryState represents delivery state for a subscriber
type DeliveryState struct {
	SubscriptionID   string
	ConsumerGroup    string
	PartitionID      int32
	NextOffset       int64
	LastDeliveryTS   int64
	ActiveDeliveries map[string]*Delivery
	Credits          int32
	MaxCredits       int32
}

// ReplayRequest represents a replay request
type ReplayRequest struct {
	Topic          string
	PartitionID    int32
	StartTS        int64
	EndTS          int64
	StartOffset    int64
	Count          int64
	ConsumerGroup  string
	SubscriptionID string
	Speed          float64
}

// ReplayResult represents a replay result
type ReplayResult struct {
	Event          *Event
	ReplayOffset   int64
}

// Partition represents a partition
type Partition struct {
	ID          int32
	Topic       string
	LeaderID    string
	ReplicaIDs  []string
	NextOffset  int64
	HighWatermark int64
	Active      bool
	CreatedTS   int64
	UpdatedTS   int64
}

// Config represents system configuration
type Config struct {
	NodeID         string
	DataDir        string
	GPRCAddress    string
	PartitionCount int
	ReplicationFactor int

	// WAL settings
	SegmentSizeBytes   int64
	IndexInterval      int64
	FsyncMode          string
	FlushIntervalMS    int32

	// Scheduler settings
	TickMS     int
	WheelSize  int
	// Delivery settings
	DefaultAckTimeout   time.Duration
	MaxRetries          int
	RetryBackoff        time.Duration
	MaxDeliveryCredits  int

	// Dedup settings
	DedupTTLHours int

	// Replication settings
	ReplicationBatchSize  int
	ReplicationTimeout    time.Duration

	// Raft settings
	RaftDir       string
	RaftJoinAddr  string
}
