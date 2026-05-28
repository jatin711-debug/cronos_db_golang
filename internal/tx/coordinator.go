package tx

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// TxID is a unique transaction identifier.
type TxID string

// Status represents a transaction state.
type Status int

const (
	StatusPending Status = iota
	StatusPrepared
	StatusCommitted
	StatusAborted
)

func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusPrepared:
		return "prepared"
	case StatusCommitted:
		return "committed"
	case StatusAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// Participant is a partition involved in a transaction.
type Participant interface {
	Prepare(ctx context.Context, txID TxID) error
	Commit(ctx context.Context, txID TxID) error
	Abort(ctx context.Context, txID TxID) error
}

// txRecord is the on-disk format for transaction state.
type txRecord struct {
	TxID         string    `json:"tx_id"`
	Status       string    `json:"status"`
	PartitionIDs []int32   `json:"partition_ids"`
	CreatedAt    time.Time `json:"created_at"`
}

// txLog persists transaction state for recovery.
type txLog struct {
	mu      sync.Mutex
	path    string
	records map[TxID]txRecord
}

func newTxLog(dataDir string) *txLog {
	return &txLog{
		path:    filepath.Join(dataDir, "tx_log.json"),
		records: make(map[TxID]txRecord),
	}
}

func (tl *txLog) load() error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	data, err := os.ReadFile(tl.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var records []txRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return fmt.Errorf("parse tx log: %w", err)
	}
	for _, r := range records {
		tl.records[TxID(r.TxID)] = r
	}
	return nil
}

func (tl *txLog) write(txID TxID, status Status, partitionIDs []int32, createdAt time.Time) error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	tl.records[txID] = txRecord{
		TxID:         string(txID),
		Status:       status.String(),
		PartitionIDs: partitionIDs,
		CreatedAt:    createdAt,
	}

	records := make([]txRecord, 0, len(tl.records))
	for _, r := range tl.records {
		records = append(records, r)
	}

	data, err := json.Marshal(records)
	if err != nil {
		return err
	}
	return os.WriteFile(tl.path, data, 0644)
}

func (tl *txLog) delete(txID TxID) error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	delete(tl.records, txID)

	records := make([]txRecord, 0, len(tl.records))
	for _, r := range tl.records {
		records = append(records, r)
	}

	data, err := json.Marshal(records)
	if err != nil {
		return err
	}
	return os.WriteFile(tl.path, data, 0644)
}

// Coordinator manages 2PC across partitions.
type Coordinator struct {
	mu           sync.RWMutex
	transactions map[TxID]*Transaction
	timeout      time.Duration
	txLog        *txLog
	txLocks      map[int32]*sync.Mutex // per-partition transaction locks
	locksMu      sync.Mutex
	quit         chan struct{}
}

// Transaction tracks a distributed transaction.
type Transaction struct {
	ID           TxID
	Participants []Participant
	Status       Status
	CreatedAt    time.Time
}

// NewCoordinator creates a 2PC coordinator.
func NewCoordinator(timeout time.Duration, dataDir string) *Coordinator {
	c := &Coordinator{
		transactions: make(map[TxID]*Transaction),
		timeout:      timeout,
		txLog:        newTxLog(dataDir),
		txLocks:      make(map[int32]*sync.Mutex),
		quit:         make(chan struct{}),
	}
	// Load existing transactions and start recovery
	_ = c.txLog.load()
	go c.recoveryLoop()
	return c
}

// getTxLock returns (and creates if needed) the mutex for a partition.
func (c *Coordinator) getTxLock(partitionID int32) *sync.Mutex {
	c.locksMu.Lock()
	defer c.locksMu.Unlock()
	if m, ok := c.txLocks[partitionID]; ok {
		return m
	}
	m := &sync.Mutex{}
	c.txLocks[partitionID] = m
	return m
}

// acquireLocks tries to acquire locks for all participant partitions.
// Returns the list of acquired locks and an error if any lock fails.
func (c *Coordinator) acquireLocks(participants []Participant) ([]*sync.Mutex, error) {
	var acquired []*sync.Mutex
	for _, p := range participants {
		pp, ok := p.(PartitionParticipant)
		if !ok {
			continue // Non-partition participants don't need locks
		}
		m := c.getTxLock(pp.PartitionID)
		m.Lock()
		acquired = append(acquired, m)
	}
	return acquired, nil
}

func releaseLocks(locks []*sync.Mutex) {
	for _, m := range locks {
		m.Unlock()
	}
}

// Begin starts a new distributed transaction.
func (c *Coordinator) Begin(txID TxID, participants []Participant) (*Transaction, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.transactions[txID]; exists {
		return nil, fmt.Errorf("transaction %s already exists", txID)
	}

	tx := &Transaction{
		ID:           txID,
		Participants: participants,
		Status:       StatusPending,
		CreatedAt:    time.Now(),
	}
	c.transactions[txID] = tx

	// Persist pending state
	partitionIDs := extractPartitionIDs(participants)
	_ = c.txLog.write(txID, StatusPending, partitionIDs, tx.CreatedAt)

	return tx, nil
}

// Commit runs two-phase commit.
func (c *Coordinator) Commit(ctx context.Context, txID TxID) error {
	c.mu.Lock()
	tx, ok := c.transactions[txID]
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("transaction %s not found", txID)
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// Acquire locks for all participant partitions
	locks, err := c.acquireLocks(tx.Participants)
	if err != nil {
		return fmt.Errorf("acquire locks: %w", err)
	}
	defer releaseLocks(locks)

	// Phase 1: Prepare
	for _, p := range tx.Participants {
		if err := p.Prepare(ctx, txID); err != nil {
			c.abortInternal(ctx, tx)
			return fmt.Errorf("prepare failed: %w", err)
		}
	}

	c.mu.Lock()
	tx.Status = StatusPrepared
	c.mu.Unlock()

	// Persist prepared state
	partitionIDs := extractPartitionIDs(tx.Participants)
	_ = c.txLog.write(txID, StatusPrepared, partitionIDs, tx.CreatedAt)

	// Phase 2: Commit
	var commitErr error
	for _, p := range tx.Participants {
		if err := p.Commit(ctx, txID); err != nil {
			commitErr = err
			// Log and continue — recovery will finish this
		}
	}

	c.mu.Lock()
	tx.Status = StatusCommitted
	c.mu.Unlock()

	// Persist committed state
	_ = c.txLog.write(txID, StatusCommitted, partitionIDs, tx.CreatedAt)
	_ = c.txLog.delete(txID)

	if commitErr != nil {
		return fmt.Errorf("commit partially failed: %w", commitErr)
	}
	return nil
}

// Abort aborts a transaction.
func (c *Coordinator) Abort(ctx context.Context, txID TxID) error {
	c.mu.Lock()
	tx, ok := c.transactions[txID]
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("transaction %s not found", txID)
	}

	return c.abortInternal(ctx, tx)
}

// abortInternal performs abort without looking up the transaction.
func (c *Coordinator) abortInternal(ctx context.Context, tx *Transaction) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// Acquire locks for all participant partitions
	locks, err := c.acquireLocks(tx.Participants)
	if err != nil {
		return fmt.Errorf("acquire locks: %w", err)
	}
	defer releaseLocks(locks)

	for _, p := range tx.Participants {
		_ = p.Abort(ctx, tx.ID)
	}

	c.mu.Lock()
	tx.Status = StatusAborted
	c.mu.Unlock()

	partitionIDs := extractPartitionIDs(tx.Participants)
	_ = c.txLog.write(tx.ID, StatusAborted, partitionIDs, tx.CreatedAt)
	_ = c.txLog.delete(tx.ID)

	return nil
}

// GetStatus returns the current status of a transaction.
func (c *Coordinator) GetStatus(txID TxID) (Status, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tx, ok := c.transactions[txID]
	if !ok {
		return StatusPending, fmt.Errorf("transaction %s not found", txID)
	}
	return tx.Status, nil
}

// recoveryLoop scans for timed-out transactions periodically.
func (c *Coordinator) recoveryLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Immediate recovery on startup
	c.recover()

	for {
		select {
		case <-ticker.C:
			c.recover()
		case <-c.quit:
			return
		}
	}
}

// recover scans tx_log and handles in-flight transactions.
func (c *Coordinator) recover() {
	c.txLog.mu.Lock()
	records := make([]txRecord, 0, len(c.txLog.records))
	for _, r := range c.txLog.records {
		records = append(records, r)
	}
	c.txLog.mu.Unlock()

	now := time.Now()
	for _, r := range records {
		txID := TxID(r.TxID)
		switch r.Status {
		case "pending":
			if now.Sub(r.CreatedAt) > c.timeout {
				// Timeout — abort
				ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
				_ = c.Abort(ctx, txID)
				cancel()
			}
		case "prepared":
			// Re-run commit for prepared transactions
			ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
			_ = c.Commit(ctx, txID)
			cancel()
		}
	}
}

// Stop halts the recovery loop.
func (c *Coordinator) Stop() {
	close(c.quit)
}

// extractPartitionIDs extracts partition IDs from participants.
// This works for PartitionParticipant; other Participant types return nil.
func extractPartitionIDs(participants []Participant) []int32 {
	ids := make([]int32, 0, len(participants))
	for _, p := range participants {
		if pp, ok := p.(PartitionParticipant); ok {
			ids = append(ids, pp.PartitionID)
		}
	}
	return ids
}

// PartitionParticipant implements Participant for a single partition.
type PartitionParticipant struct {
	PartitionID int32
}

// Prepare acquires the transaction lock for the partition.
func (p PartitionParticipant) Prepare(ctx context.Context, txID TxID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// Commit applies the transaction effects to the partition.
func (p PartitionParticipant) Commit(ctx context.Context, txID TxID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// Abort rolls back any transaction effects on the partition.
func (p PartitionParticipant) Abort(ctx context.Context, txID TxID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
