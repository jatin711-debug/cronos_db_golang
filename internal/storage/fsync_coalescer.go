package storage

import (
	"log"
	"sync"
	"time"
)

// FsyncCoalescer centralizes periodic buffer flush and fsync across multiple WALs.
// Instead of one goroutine per WAL, a single sweep flushes dirty buffers and then
// syncs the underlying segments. This reduces goroutine overhead and aligns fsync
// syscalls across partitions so the storage subsystem can service them together.
//
// The coalescer is safe for concurrent registration/unregistration and will skip
// WALs that are no longer registered.
type FsyncCoalescer struct {
	mu        sync.Mutex
	walSet    map[*WAL]struct{}
	interval  time.Duration
	quit      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// NewFsyncCoalescer starts a background sweep loop. interval should be the same
// as the desired WAL flush interval (e.g. 100ms).
func NewFsyncCoalescer(interval time.Duration) *FsyncCoalescer {
	c := &FsyncCoalescer{
		walSet:   make(map[*WAL]struct{}),
		interval: interval,
		quit:     make(chan struct{}),
	}
	c.wg.Add(1)
	go c.loop()
	return c
}

// Register adds a WAL to the coalescer. Safe to call multiple times.
func (c *FsyncCoalescer) Register(w *WAL) {
	c.mu.Lock()
	c.walSet[w] = struct{}{}
	c.mu.Unlock()
}

// Unregister removes a WAL from the coalescer. WALs should unregister before
// they are closed to avoid the coalescer touching a closed segment.
func (c *FsyncCoalescer) Unregister(w *WAL) {
	c.mu.Lock()
	delete(c.walSet, w)
	c.mu.Unlock()
}

// Close stops the background sweep. It does NOT perform a final flush; callers
// should flush WALs before closing the coalescer.
func (c *FsyncCoalescer) Close() {
	c.closeOnce.Do(func() {
		close(c.quit)
	})
	c.wg.Wait()
}

func (c *FsyncCoalescer) loop() {
	defer c.wg.Done()
	if c.interval <= 0 {
		return
	}
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.sweep()
		case <-c.quit:
			return
		}
	}
}

// sweep flushes dirty buffers and syncs segments for all registered WALs.
func (c *FsyncCoalescer) sweep() {
	c.mu.Lock()
	wals := make([]*WAL, 0, len(c.walSet))
	for w := range c.walSet {
		wals = append(wals, w)
	}
	c.mu.Unlock()

	type flushResult struct {
		seg       *Segment
		needsSync bool
		err       error
	}
	results := make([]flushResult, len(wals))

	var flushWg sync.WaitGroup
	for i, w := range wals {
		flushWg.Add(1)
		go func(idx int, wal *WAL) {
			defer flushWg.Done()
			seg, needsSync, err := wal.backgroundFlushBuffer()
			results[idx] = flushResult{seg: seg, needsSync: needsSync, err: err}
		}(i, w)
	}
	flushWg.Wait()

	// Sync flushed segments concurrently. Sequential fsyncs would leave each WAL
	// waiting for the disk; concurrent fsyncs let the I/O scheduler coalesce.
	var syncWg sync.WaitGroup
	for i, r := range results {
		if r.err != nil {
			wals[i].recordFlushError()
			log.Printf("[FsyncCoalescer] WAL-%d buffer flush error: %v", wals[i].partitionID, r.err)
			continue
		}
		if r.seg == nil || !r.needsSync {
			continue
		}
		syncWg.Add(1)
		go func(idx int, seg *Segment) {
			defer syncWg.Done()
			if err := seg.Sync(); err != nil {
				wals[idx].recordFlushError()
				log.Printf("[FsyncCoalescer] WAL-%d segment sync error: %v", wals[idx].partitionID, err)
			}
		}(i, r.seg)
	}
	syncWg.Wait()
}
