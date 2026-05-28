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
			_ = err
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
