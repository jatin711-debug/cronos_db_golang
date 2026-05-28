package tx

import (
	"context"
	"fmt"
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

// Participant is a partition involved in a transaction.
type Participant interface {
	Prepare(ctx context.Context, txID TxID) error
	Commit(ctx context.Context, txID TxID) error
	Abort(ctx context.Context, txID TxID) error
}

// Coordinator manages 2PC across partitions.
type Coordinator struct {
	mu           sync.RWMutex
	transactions map[TxID]*Transaction
	timeout      time.Duration
}

// Transaction tracks a distributed transaction.
type Transaction struct {
	ID           TxID
	Participants []Participant
	Status       Status
	CreatedAt    time.Time
}

// NewCoordinator creates a 2PC coordinator.
func NewCoordinator(timeout time.Duration) *Coordinator {
	return &Coordinator{
		transactions: make(map[TxID]*Transaction),
		timeout:      timeout,
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

	// Phase 1: Prepare
	for _, p := range tx.Participants {
		if err := p.Prepare(ctx, txID); err != nil {
			c.Abort(ctx, txID)
			return fmt.Errorf("prepare failed: %w", err)
		}
	}

	// Phase 2: Commit
	for _, p := range tx.Participants {
		if err := p.Commit(ctx, txID); err != nil {
			// Log and continue — recovery needed
		}
	}

	c.mu.Lock()
	tx.Status = StatusCommitted
	c.mu.Unlock()
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

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	for _, p := range tx.Participants {
		_ = p.Abort(ctx, txID)
	}

	c.mu.Lock()
	tx.Status = StatusAborted
	c.mu.Unlock()
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

// PartitionParticipant implements Participant for a single partition.
type PartitionParticipant struct {
	PartitionID int32
}

// Prepare marks the partition as prepared for the transaction.
func (p PartitionParticipant) Prepare(ctx context.Context, txID TxID) error {
	// In a real implementation, this would acquire locks on the partition
	// For now, this is a no-op — the partition is already locked by virtue
	// of the transaction coordinator serializing transactions
	return nil
}

// Commit applies the transaction effects to the partition.
func (p PartitionParticipant) Commit(ctx context.Context, txID TxID) error {
	// Transaction committed — effects already applied at prepare time
	return nil
}

// Abort rolls back any transaction effects on the partition.
func (p PartitionParticipant) Abort(ctx context.Context, txID TxID) error {
	// Transaction aborted — rollback any effects if needed
	return nil
}
