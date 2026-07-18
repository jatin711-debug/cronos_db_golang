// Package tx implements distributed two-phase commit (2PC) coordination across
// partitions, including durable transaction logging and crash recovery.
package tx

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// TxID is a unique distributed transaction identifier.
type TxID string

// Status represents a transaction state in the 2PC lifecycle.
type Status int

const (
	// StatusPending indicates the transaction has begun but not yet prepared.
	StatusPending Status = iota
	// StatusPrepared indicates all participants voted yes in phase 1.
	StatusPrepared
	// StatusCommitting indicates phase 2 commit is in progress (possibly partial).
	StatusCommitting
	// StatusCommitted indicates the transaction fully committed.
	StatusCommitted
	// StatusAborted indicates the transaction was aborted.
	StatusAborted
)

// String returns a human-readable name for the transaction status.
func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusPrepared:
		return "prepared"
	case StatusCommitting:
		return "committing"
	case StatusCommitted:
		return "committed"
	case StatusAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// Participant is a resource manager involved in a distributed transaction
// (typically a partition that writes prepare/commit/abort WAL markers).
type Participant interface {
	// Prepare runs phase-1 prepare for txID.
	Prepare(ctx context.Context, txID TxID) error
	// Commit runs phase-2 commit for txID.
	Commit(ctx context.Context, txID TxID) error
	// Abort aborts txID on this participant.
	Abort(ctx context.Context, txID TxID) error
}

// txRecord is the on-disk format for transaction state.
type txRecord struct {
	// TxID is the transaction identifier.
	TxID string `json:"tx_id"`
	// Status is the durable status string (e.g. prepared, committed, aborted).
	Status string `json:"status"`
	// PartitionIDs lists partitions participating in the transaction.
	PartitionIDs []int32 `json:"partition_ids"`
	// CreatedAt is when the transaction was first recorded.
	CreatedAt time.Time `json:"created_at"`
	// CommittedAt is set when the transaction commits (nil if not committed).
	CommittedAt *time.Time `json:"committed_at,omitempty"`
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

	var committedAt *time.Time
	if status == StatusCommitted {
		now := time.Now()
		committedAt = &now
	}

	tl.records[txID] = txRecord{
		TxID:         string(txID),
		Status:       status.String(),
		PartitionIDs: partitionIDs,
		CreatedAt:    createdAt,
		CommittedAt:  committedAt,
	}

	return tl.persistLocked()
}

func (tl *txLog) delete(txID TxID) error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	delete(tl.records, txID)
	return tl.persistLocked()
}

// persistLocked writes the current records to disk atomically: write to a temp
// file, fsync the temp file, rename it over the real path, then fsync the
// parent directory so the rename is durable. tl.mu must be held.
func (tl *txLog) persistLocked() error {
	records := make([]txRecord, 0, len(tl.records))
	for _, r := range tl.records {
		records = append(records, r)
	}

	data, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("marshal tx records: %w", err)
	}

	return utils.AtomicWriteFile(tl.path, data, 0644)
}

// Coordinator manages 2PC across partitions with a durable transaction log and
// a background recovery loop for incomplete transactions.
type Coordinator struct {
	mu             sync.RWMutex
	transactions   map[TxID]*Transaction
	timeout        time.Duration
	txLog          *txLog
	txLocks        map[int32]*sync.Mutex // per-partition transaction locks
	locksMu        sync.Mutex
	quit           chan struct{}
	pm             *partition.PartitionManager
	failedAttempts map[TxID]int
	nextRetryTime  map[TxID]time.Time
	attemptsMu     sync.Mutex
}

// Transaction tracks an in-memory distributed transaction and its participants.
type Transaction struct {
	// ID is the unique transaction identifier.
	ID TxID
	// Participants are the resource managers taking part in this transaction.
	Participants []Participant
	// Status is the current 2PC status.
	Status Status
	// CreatedAt is when the transaction was begun.
	CreatedAt time.Time
}

// NewCoordinator creates a 2PC coordinator with the given per-operation timeout
// and durable log under dataDir. It loads existing log records and starts recovery.
func NewCoordinator(timeout time.Duration, dataDir string) *Coordinator {
	c := &Coordinator{
		transactions:   make(map[TxID]*Transaction),
		timeout:        timeout,
		txLog:          newTxLog(dataDir),
		txLocks:        make(map[int32]*sync.Mutex),
		quit:           make(chan struct{}),
		failedAttempts: make(map[TxID]int),
		nextRetryTime:  make(map[TxID]time.Time),
	}
	// Load existing transactions and rebuild in-memory state
	_ = c.txLog.load()
	c.recoverTransactions()
	go c.recoveryLoop()
	return c
}

// recoverTransactions rebuilds the in-memory transactions map from the persisted tx log.
// This ensures that GetStatus works for transactions that were in-flight during a crash.
func (c *Coordinator) recoverTransactions() {
	c.txLog.mu.Lock()
	records := make([]txRecord, 0, len(c.txLog.records))
	for _, r := range c.txLog.records {
		records = append(records, r)
	}
	c.txLog.mu.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, r := range records {
		txID := TxID(r.TxID)
		if _, exists := c.transactions[txID]; exists {
			continue
		}

		// Rebuild participants from persisted partition IDs
		participants := make([]Participant, 0, len(r.PartitionIDs))
		for _, pid := range r.PartitionIDs {
			participants = append(participants, PartitionParticipant{PartitionID: pid})
		}

		status := parseStatus(r.Status)
		c.transactions[txID] = &Transaction{
			ID:           txID,
			Participants: participants,
			Status:       status,
			CreatedAt:    r.CreatedAt,
		}
	}
}

// SetPartitionManager registers the partition manager and runs lock recovery.
func (c *Coordinator) SetPartitionManager(pm *partition.PartitionManager) {
	c.mu.Lock()
	c.pm = pm
	// Wire PM to all recovered transaction participants
	for _, tx := range c.transactions {
		for i, p := range tx.Participants {
			if pp, ok := p.(PartitionParticipant); ok {
				pp.PM = pm
				tx.Participants[i] = pp
			}
		}
	}
	c.mu.Unlock()

	// Rebuild and recover prepared locks now that PM is wired
	c.RecoverPreparedLocks()
}

// RecoverPreparedLocks scans all active partitions for prepared transactions and locks them.
func (c *Coordinator) RecoverPreparedLocks() {
	if c.pm == nil {
		return
	}

	partitions := c.pm.ListPartitions()
	for _, part := range partitions {
		if part.Wal == nil {
			continue
		}
		lastOffset := part.Wal.GetLastOffset()
		if lastOffset < 0 {
			continue
		}

		activePrepared := make(map[string]bool)
		const chunkSize = 20000
		for start := int64(0); start <= lastOffset; start += chunkSize {
			end := start + chunkSize - 1
			if end > lastOffset {
				end = lastOffset
			}
			events, err := part.Wal.ReadEvents(start, end)
			if err != nil {
				break
			}
			for _, event := range events {
				if event.Topic == "__transaction_log" {
					txID := event.Meta["tx_id"]
					action := event.Meta["tx_action"]
					if action == "prepare" {
						activePrepared[txID] = true
					} else if action == "commit" || action == "abort" {
						delete(activePrepared, txID)
					}
				}
			}
		}

		// Re-acquire locks for any prepared transactions
		for txID := range activePrepared {
			slog.Info("Recovered active prepared transaction from WAL; will re-drive resolution", "tx_id", txID, "partition", part.ID)
			// Note: we intentionally do NOT lock the partition here. The recovery
			// loop (and any explicit Commit/Abort) will acquire locks via
			// acquireLocks(), which is deadlock-free because it sorts by partition
			// ID. Locking here without ever releasing caused a permanent lock leak.

			// Ensure the transaction exists in memory so GetStatus can report it
			// and the recovery loop can re-drive resolution when a txLog record is
			// also present.
			c.mu.Lock()
			id := TxID(txID)
			if _, exists := c.transactions[id]; !exists {
				c.transactions[id] = &Transaction{
					ID:        id,
					Status:    StatusPending,
					CreatedAt: time.Now(),
				}
			}
			c.mu.Unlock()
		}
	}
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
// Locks are acquired in a globally consistent order (sorted by partition ID)
// to prevent deadlocks between concurrent transactions touching overlapping
// partition sets. The returned error is currently always nil.
func (c *Coordinator) acquireLocks(participants []Participant) ([]*sync.Mutex, error) {
	// Extract partition IDs and sort to guarantee a global locking order.
	ids := make([]int32, 0, len(participants))
	for _, p := range participants {
		pp, ok := p.(PartitionParticipant)
		if !ok {
			continue // Non-partition participants don't need locks
		}
		ids = append(ids, pp.PartitionID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var acquired []*sync.Mutex
	for _, id := range ids {
		m := c.getTxLock(id)
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

	// Inject the partition manager into any PartitionParticipant that lacks one.
	// Callers (e.g. the gRPC TransactionService handler) build participants with
	// only a PartitionID; without PM wired, Prepare/Commit silently write no WAL
	// markers — a durable no-op. Centralizing the injection here makes every entry
	// path correct.
	if c.pm != nil {
		for i, p := range participants {
			if pp, ok := p.(PartitionParticipant); ok && pp.PM == nil {
				pp.PM = c.pm
				participants[i] = pp
			}
		}
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
	if err := c.txLog.write(txID, StatusPending, partitionIDs, tx.CreatedAt); err != nil {
		return nil, fmt.Errorf("persist pending transaction: %w", err)
	}

	return tx, nil
}

// Prepare runs phase 1 (prepare) only and persists prepared state.
func (c *Coordinator) Prepare(ctx context.Context, txID TxID) error {
	c.mu.Lock()
	tx, ok := c.transactions[txID]
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("transaction %s not found", txID)
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// Acquire locks for all participant partitions.
	locks, err := c.acquireLocks(tx.Participants)
	if err != nil {
		return fmt.Errorf("acquire locks: %w", err)
	}
	defer releaseLocks(locks)

	c.mu.RLock()
	status := tx.Status
	c.mu.RUnlock()

	if status == StatusPrepared {
		return nil
	}
	if status == StatusCommitted {
		return fmt.Errorf("transaction %s already committed", txID)
	}
	if status == StatusAborted {
		return fmt.Errorf("transaction %s already aborted", txID)
	}

	for _, p := range tx.Participants {
		if err := p.Prepare(ctx, txID); err != nil {
			_ = c.abortInternal(ctx, tx)
			return fmt.Errorf("prepare failed: %w", err)
		}
	}

	c.mu.Lock()
	tx.Status = StatusPrepared
	c.mu.Unlock()

	partitionIDs := extractPartitionIDs(tx.Participants)
	if err := c.txLog.write(txID, StatusPrepared, partitionIDs, tx.CreatedAt); err != nil {
		return fmt.Errorf("persist prepared transaction: %w", err)
	}

	return nil
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

	c.mu.Lock()
	status := tx.Status
	c.mu.Unlock()

	// Phase 1: Prepare (only if status is pending)
	if status == StatusPending {
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
		if err := c.txLog.write(txID, StatusPrepared, partitionIDs, tx.CreatedAt); err != nil {
			return fmt.Errorf("persist prepared transaction: %w", err)
		}
	}
	// StatusPrepared or StatusCommitting means Prepare already succeeded; skip
	// to Phase 2.

	// Phase 2: Commit
	var commitErr error
	for _, p := range tx.Participants {
		if err := p.Commit(ctx, txID); err != nil {
			commitErr = err
			// Log and continue — recovery will finish this
		}
	}

	if commitErr != nil {
		// Partial failure: stay in Committing so recovery re-drives the commit.
		// Do NOT transition to Committed or delete the log record yet.
		c.mu.Lock()
		tx.Status = StatusCommitting
		c.mu.Unlock()

		partitionIDs := extractPartitionIDs(tx.Participants)
		if err := c.txLog.write(txID, StatusCommitting, partitionIDs, tx.CreatedAt); err != nil {
			return fmt.Errorf("commit partially failed (%v) and could not persist committing state: %w", commitErr, err)
		}
		return fmt.Errorf("commit partially failed: %w", commitErr)
	}

	// Full success: now safe to mark committed and remove the log.
	c.mu.Lock()
	tx.Status = StatusCommitted
	c.mu.Unlock()

	partitionIDs := extractPartitionIDs(tx.Participants)
	if err := c.txLog.write(txID, StatusCommitted, partitionIDs, tx.CreatedAt); err != nil {
		return fmt.Errorf("persist committed transaction: %w", err)
	}
	if err := c.txLog.delete(txID); err != nil {
		return fmt.Errorf("delete committed transaction log: %w", err)
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
	if err := c.txLog.write(tx.ID, StatusAborted, partitionIDs, tx.CreatedAt); err != nil {
		return fmt.Errorf("persist aborted transaction: %w", err)
	}
	if err := c.txLog.delete(tx.ID); err != nil {
		return fmt.Errorf("delete aborted transaction log: %w", err)
	}

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
// It only re-drives transactions whose in-memory status matches the log status,
// preventing races with goroutines that are actively handling the same transaction.
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

		// Exponential backoff check
		c.attemptsMu.Lock()
		nextRetry, hasRetry := c.nextRetryTime[txID]
		c.attemptsMu.Unlock()
		if hasRetry && now.Before(nextRetry) {
			continue // Skip until backoff expires
		}

		// Check in-memory status — if another goroutine has already advanced
		// the transaction past the log status, skip it.
		c.mu.RLock()
		tx, exists := c.transactions[txID]
		var inMemStatus Status
		if exists {
			inMemStatus = tx.Status
		}
		c.mu.RUnlock()

		if !exists {
			continue
		}

		var err error
		switch r.Status {
		case "pending":
			if inMemStatus != StatusPending {
				continue // Already advanced by another goroutine
			}
			if now.Sub(r.CreatedAt) > c.timeout {
				// Timeout — abort
				ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
				err = c.Abort(ctx, txID)
				cancel()
			}
		case "prepared":
			if inMemStatus != StatusPrepared {
				continue // Already advanced by another goroutine
			}
			// Re-run commit for prepared transactions
			ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
			err = c.Commit(ctx, txID)
			cancel()
		case "committing":
			if inMemStatus != StatusCommitting && inMemStatus != StatusPrepared {
				continue
			}
			// Re-run commit for transactions that were in the middle of commit
			// when the process crashed. If the in-memory status is still
			// Prepared, we advance it first so Commit can proceed normally.
			if inMemStatus == StatusPrepared {
				c.mu.Lock()
				tx.Status = StatusCommitting
				c.mu.Unlock()
			}
			ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
			err = c.Commit(ctx, txID)
			cancel()
		case "committed":
			if inMemStatus != StatusCommitted {
				continue // Already fully committed or deleted
			}
			// Re-run commit for partially committed transactions
			ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
			err = c.Commit(ctx, txID)
			cancel()
		}

		c.attemptsMu.Lock()
		if err != nil {
			attempts := c.failedAttempts[txID] + 1
			c.failedAttempts[txID] = attempts
			// Exponential backoff: 2^attempts seconds, capped at 5 minutes (300 seconds)
			backoffSecs := int64(1) << attempts
			if backoffSecs > 300 {
				backoffSecs = 300
			}
			c.nextRetryTime[txID] = time.Now().Add(time.Duration(backoffSecs) * time.Second)
			slog.Warn("Recovery attempt failed; scheduled retry with backoff", "tx_id", txID, "error", err, "attempts", attempts, "backoff_seconds", backoffSecs)
		} else {
			delete(c.failedAttempts, txID)
			delete(c.nextRetryTime, txID)
		}
		c.attemptsMu.Unlock()
	}
}

// Stop halts the recovery loop.
func (c *Coordinator) Stop() {
	close(c.quit)
}

// parseStatus converts a string status back to a Status enum.
func parseStatus(s string) Status {
	switch s {
	case "pending":
		return StatusPending
	case "prepared":
		return StatusPrepared
	case "committing":
		return StatusCommitting
	case "committed":
		return StatusCommitted
	case "aborted":
		return StatusAborted
	default:
		return StatusPending
	}
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

// PartitionParticipant implements Participant for a single local partition by
// writing prepare/commit/abort markers to that partition's WAL.
type PartitionParticipant struct {
	// PartitionID is the local partition participating in the transaction.
	PartitionID int32
	// PM is the partition manager used to resolve the partition and its WAL.
	// May be injected by the Coordinator when Begin is called.
	PM *partition.PartitionManager
}

// Prepare writes a prepare WAL marker for txID on the participant partition.
func (p PartitionParticipant) Prepare(ctx context.Context, txID TxID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if p.PM != nil {
		part, err := p.PM.GetInternalPartition(p.PartitionID)
		if err != nil {
			return fmt.Errorf("get partition %d: %w", p.PartitionID, err)
		}
		if part.Wal != nil {
			event := &types.Event{
				MessageId: fmt.Sprintf("tx:%s:prepare", txID),
				Topic:     "__transaction_log",
				Payload:   []byte("prepare"),
				CreatedTs: time.Now().UnixNano(),
				Meta: map[string]string{
					"tx_id":     string(txID),
					"tx_action": "prepare",
				},
			}
			if err := part.Wal.AppendEvent(event); err != nil {
				return fmt.Errorf("write prepare WAL marker: %w", err)
			}
		}
	}
	return nil
}

// Commit writes a commit WAL marker for txID on the participant partition.
func (p PartitionParticipant) Commit(ctx context.Context, txID TxID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if p.PM != nil {
		part, err := p.PM.GetInternalPartition(p.PartitionID)
		if err != nil {
			return fmt.Errorf("get partition %d: %w", p.PartitionID, err)
		}
		if part.Wal != nil {
			event := &types.Event{
				MessageId: fmt.Sprintf("tx:%s:commit", txID),
				Topic:     "__transaction_log",
				Payload:   []byte("commit"),
				CreatedTs: time.Now().UnixNano(),
				Meta: map[string]string{
					"tx_id":     string(txID),
					"tx_action": "commit",
				},
			}
			if err := part.Wal.AppendEvent(event); err != nil {
				return fmt.Errorf("write commit WAL marker: %w", err)
			}
		}
	}
	return nil
}

// Abort writes an abort WAL marker for txID on the participant partition.
func (p PartitionParticipant) Abort(ctx context.Context, txID TxID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if p.PM != nil {
		part, err := p.PM.GetInternalPartition(p.PartitionID)
		if err != nil {
			return fmt.Errorf("get partition %d: %w", p.PartitionID, err)
		}
		if part.Wal != nil {
			event := &types.Event{
				MessageId: fmt.Sprintf("tx:%s:abort", txID),
				Topic:     "__transaction_log",
				Payload:   []byte("abort"),
				CreatedTs: time.Now().UnixNano(),
				Meta: map[string]string{
					"tx_id":     string(txID),
					"tx_action": "abort",
				},
			}
			if err := part.Wal.AppendEvent(event); err != nil {
				return fmt.Errorf("write abort WAL marker: %w", err)
			}
		}
	}
	return nil
}
