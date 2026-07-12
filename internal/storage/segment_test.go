package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestSegment_AppendAndRead(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := NewSegment(tmpDir, 0, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Append 5 events
	for i := 0; i < 5; i++ {
		event := makeEvent(int64(i), fmt.Sprintf("msg-%d", i), "topic")
		if err := seg.AppendEvent(event, 2); err != nil {
			t.Fatalf("AppendEvent %d failed: %v", i, err)
		}
	}

	if err := seg.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if seg.GetLastOffset() != 4 {
		t.Errorf("expected last offset 4, got %d", seg.GetLastOffset())
	}

	// Read each event back
	for i := int64(0); i < 5; i++ {
		ev, err := seg.ReadEvent(i)
		if err != nil {
			t.Fatalf("ReadEvent %d failed: %v", i, err)
		}
		if ev.GetMessageId() != fmt.Sprintf("msg-%d", i) {
			t.Errorf("event %d: expected msg-%d, got %s", i, i, ev.GetMessageId())
		}
	}
}

func TestSegment_AppendBatch(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := NewSegment(tmpDir, 100, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	events := make([]*types.Event, 20)
	for i := 0; i < 20; i++ {
		events[i] = makeEvent(int64(100+i), fmt.Sprintf("batch-%d", i), "batch-topic")
	}

	if err := seg.AppendBatch(events, 5); err != nil {
		t.Fatalf("AppendBatch failed: %v", err)
	}

	if err := seg.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if seg.GetFirstOffset() != 100 {
		t.Errorf("expected first offset 100, got %d", seg.GetFirstOffset())
	}
	if seg.GetLastOffset() != 119 {
		t.Errorf("expected last offset 119, got %d", seg.GetLastOffset())
	}
	if seg.GetSize() <= 0 {
		t.Error("expected non-zero segment size")
	}
}

func TestSegment_AppendBatchLocked(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := NewSegment(tmpDir, 0, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	events := make([]*types.Event, 10)
	for i := 0; i < 10; i++ {
		events[i] = makeEvent(int64(i), fmt.Sprintf("locked-%d", i), "locked-topic")
	}

	// AppendBatchLocked requires caller to hold the segment lock.
	seg.mu.Lock()
	err = seg.AppendBatchLocked(events, 3)
	seg.mu.Unlock()
	if err != nil {
		t.Fatalf("AppendBatchLocked failed: %v", err)
	}

	if seg.GetLastOffset() != 9 {
		t.Errorf("expected last offset 9, got %d", seg.GetLastOffset())
	}
}

func TestSegment_AppendBatchUnsafe(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := NewSegment(tmpDir, 0, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	events := make([]*types.Event, 10)
	for i := 0; i < 10; i++ {
		events[i] = makeEvent(int64(i), fmt.Sprintf("unsafe-%d", i), "unsafe-topic")
	}

	// AppendBatchUnsafe is a convenience wrapper that acquires s.mu.
	if err := seg.AppendBatchUnsafe(events, 3); err != nil {
		t.Fatalf("AppendBatchUnsafe failed: %v", err)
	}

	if seg.GetLastOffset() != 9 {
		t.Errorf("expected last offset 9, got %d", seg.GetLastOffset())
	}
}

func TestSegment_MetadataValues(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := NewSegment(tmpDir, 500, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	if seg.GetFirstOffset() != 500 {
		t.Errorf("expected first offset 500, got %d", seg.GetFirstOffset())
	}
	if !seg.IsActive() {
		t.Error("expected segment to be active")
	}
	if seg.GetFilename() != "00000000000000000500.log" {
		t.Errorf("expected filename 00000000000000000500.log, got %s", seg.GetFilename())
	}
}

func TestSegment_ReadEventsByOffsetRange(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := NewSegment(tmpDir, 0, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Append 10 events
	for i := 0; i < 10; i++ {
		event := makeEvent(int64(i), fmt.Sprintf("range-%d", i), "range-topic")
		if err := seg.AppendEvent(event, 2); err != nil {
			t.Fatalf("AppendEvent %d failed: %v", i, err)
		}
	}
	seg.Flush()

	// Read range [3, 7]
	events, err := seg.ReadEventsByOffsetRange(3, 7)
	if err != nil {
		t.Fatalf("ReadEventsByOffsetRange failed: %v", err)
	}

	if len(events) != 5 {
		t.Errorf("expected 5 events in range, got %d", len(events))
	}
	for i, ev := range events {
		expected := int64(3 + i)
		if ev.Offset != expected {
			t.Errorf("event %d: expected offset %d, got %d", i, expected, ev.Offset)
		}
	}

	// Read range beyond segment
	events, err = seg.ReadEventsByOffsetRange(100, 200)
	if err != nil {
		t.Fatalf("ReadEventsByOffsetRange beyond range failed: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events beyond range, got %d", len(events))
	}
}

func TestSegment_Encryption(t *testing.T) {
	tmpDir := t.TempDir()

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	cipher, err := NewSegmentCipher(key, 0)
	if err != nil {
		t.Fatalf("NewSegmentCipher failed: %v", err)
	}

	seg, err := NewSegment(tmpDir, 0, true, cipher)
	if err != nil {
		t.Fatalf("NewSegment with encryption failed: %v", err)
	}
	defer seg.Close()

	// Append events
	for i := 0; i < 5; i++ {
		event := makeEvent(int64(i), fmt.Sprintf("enc-%d", i), "enc-topic")
		if err := seg.AppendEvent(event, 2); err != nil {
			t.Fatalf("AppendEvent %d failed: %v", i, err)
		}
	}
	if err := seg.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Read back
	for i := 0; i < 5; i++ {
		ev, err := seg.ReadEvent(int64(i))
		if err != nil {
			t.Fatalf("ReadEvent %d failed: %v", i, err)
		}
		if ev.GetMessageId() != fmt.Sprintf("enc-%d", i) {
			t.Errorf("event %d: expected enc-%d, got %s", i, i, ev.GetMessageId())
		}
	}
}

func TestSegment_OpenExisting(t *testing.T) {
	tmpDir := t.TempDir()

	// Create and write segment
	seg1, err := NewSegment(tmpDir, 0, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	for i := 0; i < 5; i++ {
		event := makeEvent(int64(i), fmt.Sprintf("persist-%d", i), "persist-topic")
		if err := seg1.AppendEvent(event, 2); err != nil {
			t.Fatalf("AppendEvent %d failed: %v", i, err)
		}
	}
	seg1.Flush()
	seg1.Close()

	// Reopen and read
	seg2, err := OpenSegment(tmpDir, "00000000000000000000.log", nil)
	if err != nil {
		t.Fatalf("OpenSegment failed: %v", err)
	}
	defer seg2.Close()

	if seg2.GetLastOffset() != 4 {
		t.Errorf("expected last offset 4 after reopen, got %d", seg2.GetLastOffset())
	}

	ev, err := seg2.ReadEvent(2)
	if err != nil {
		t.Fatalf("ReadEvent 2 failed: %v", err)
	}
	if ev.GetMessageId() != "persist-2" {
		t.Errorf("expected persist-2, got %s", ev.GetMessageId())
	}
}

// TestSegment_IndexTruncatedOnRecovery is a regression test: when scan()
// truncates a corrupt segment tail, the sparse index must be reconciled so it
// no longer contains entries whose FilePosition points past the new (shorter)
// segment EOF. Without the fix in scan(), FindByOffset can hand back a
// position into the truncated region and reads return garbage.
func TestSegment_IndexTruncatedOnRecovery(t *testing.T) {
	tmpDir := t.TempDir()

	// Build a segment with 5 events, indexing every event (interval=1) so the
	// index has one entry per event at offsets 0..4.
	seg, err := NewSegment(tmpDir, 0, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := seg.AppendEvent(makeEvent(int64(i), fmt.Sprintf("idx-%d", i), "idx-topic"), 1); err != nil {
			t.Fatalf("AppendEvent %d failed: %v", i, err)
		}
	}
	filename := seg.GetFilename()
	seg.Flush()
	seg.Close()

	// Corrupt the tail: truncate the segment file to remove the last 2 events
	// worth of bytes and write garbage. First find where event 3 starts.
	// We reopen to find offsets, then corrupt the file directly.
	segPath := filepath.Join(tmpDir, "segments", filename)
	stat, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("stat segment: %v", err)
	}
	origSize := stat.Size()

	// Determine the byte position of event offset 3 by reading back via a
	// clean open, then truncate the file just past offset 2 and append junk.
	clean, err := OpenSegment(tmpDir, filename, nil)
	if err != nil {
		t.Fatalf("OpenSegment clean: %v", err)
	}
	pos, found := clean.index.FindByOffset(3)
	cleanSize := clean.sizeBytes
	clean.Close()
	if !found {
		t.Fatalf("expected index entry for offset 3")
	}
	// Truncate to remove events 3 and 4, then write a corrupt partial record
	// so scan() treats it as a truncated tail.
	truncateAt := pos
	if err := os.Truncate(segPath, truncateAt); err != nil {
		t.Fatalf("truncate segment: %v", err)
	}
	// Append a junk partial record (bad length) to force scan to stop here.
	f, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open segment for junk: %v", err)
	}
	junk := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x01, 0x02} // impossible length + garbage
	if _, err := f.Write(junk); err != nil {
		f.Close()
		t.Fatalf("write junk: %v", err)
	}
	f.Close()

	_ = origSize
	_ = cleanSize

	// Reopen: scan() should detect the corrupt tail, truncate the segment, and
	// — with the fix — also truncate the index so no entry points past EOF.
	recovered, err := OpenSegment(tmpDir, filename, nil)
	if err != nil {
		t.Fatalf("OpenSegment after corruption failed: %v", err)
	}
	defer recovered.Close()

	// Last good offset must be 2 (events 3 and 4 were removed).
	if recovered.GetLastOffset() != 2 {
		t.Fatalf("expected last offset 2 after recovery, got %d", recovered.GetLastOffset())
	}

	// The index must not contain any entry past the recovered segment size.
	for _, e := range recovered.index.GetEntries() {
		if e.FilePosition > recovered.sizeBytes {
			t.Errorf("stale index entry: offset=%d filePos=%d > segmentSize=%d (index not truncated on recovery)",
				e.Offset, e.FilePosition, recovered.sizeBytes)
		}
	}
	// And FindByOffset must not hand back a position past EOF for a high offset.
	if pos, found := recovered.index.FindByOffset(4); found {
		if pos > recovered.sizeBytes {
			t.Errorf("FindByOffset(4) returned filePos=%d past segmentSize=%d", pos, recovered.sizeBytes)
		}
	}
}

func TestSegment_OpenExistingWithEncryption(t *testing.T) {
	tmpDir := t.TempDir()

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	cipher, _ := NewSegmentCipher(key, 0)

	// Create encrypted segment
	seg1, err := NewSegment(tmpDir, 0, true, cipher)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	for i := 0; i < 3; i++ {
		event := makeEvent(int64(i), fmt.Sprintf("encpersist-%d", i), "enc-topic")
		if err := seg1.AppendEvent(event, 2); err != nil {
			t.Fatalf("AppendEvent %d failed: %v", i, err)
		}
	}
	seg1.Flush()
	seg1.Close()

	// Reopen with same cipher
	seg2, err := OpenSegment(tmpDir, "00000000000000000000.log", cipher)
	if err != nil {
		t.Fatalf("OpenSegment failed: %v", err)
	}
	defer seg2.Close()

	if seg2.GetLastOffset() != 2 {
		t.Errorf("expected last offset 2, got %d", seg2.GetLastOffset())
	}

	ev, err := seg2.ReadEvent(1)
	if err != nil {
		t.Fatalf("ReadEvent 1 failed: %v", err)
	}
	if ev.GetMessageId() != "encpersist-1" {
		t.Errorf("expected encpersist-1, got %s", ev.GetMessageId())
	}
}

func TestSegment_Delete(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := NewSegment(tmpDir, 0, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	for i := 0; i < 3; i++ {
		event := makeEvent(int64(i), fmt.Sprintf("delete-%d", i), "delete-topic")
		if err := seg.AppendEvent(event, 2); err != nil {
			t.Fatalf("AppendEvent %d failed: %v", i, err)
		}
	}
	seg.Flush()
	filename := seg.GetFilename()
	seg.Close()

	// Delete the segment
	seg2, err := OpenSegment(tmpDir, filename, nil)
	if err != nil {
		t.Fatalf("OpenSegment failed: %v", err)
	}
	if err := seg2.Delete(); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify file is gone
	filePath := filepath.Join(tmpDir, "segments", filename)
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Error("expected segment file to be deleted")
	}
}

func TestSegment_FlushBuffer(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := NewSegment(tmpDir, 0, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}

	// FlushBuffer on empty segment should succeed
	if err := seg.FlushBuffer(); err != nil {
		t.Fatalf("FlushBuffer failed: %v", err)
	}

	seg.Close()

	// FlushBuffer on closed segment should also succeed (no-op)
	if err := seg.FlushBuffer(); err != nil {
		t.Fatalf("FlushBuffer on closed segment failed: %v", err)
	}
}

func TestSegment_Sync(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := NewSegment(tmpDir, 0, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}

	event := makeEvent(0, "sync-test", "sync-topic")
	if err := seg.AppendEvent(event, 1); err != nil {
		t.Fatalf("AppendEvent failed: %v", err)
	}

	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	seg.Close()

	// Sync on closed segment is a no-op
	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync on closed segment failed: %v", err)
	}
}

func TestSegment_IsFull(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := NewSegment(tmpDir, 0, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	if seg.IsFull(100) {
		t.Error("empty segment should not be full")
	}
	// IsFull(0) is a degenerate case - skip it as it's not meaningful
}

func TestSegment_TruncateCorruptTail(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := NewSegment(tmpDir, 0, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}

	// Append 5 valid events
	for i := 0; i < 5; i++ {
		event := makeEvent(int64(i), fmt.Sprintf("valid-%d", i), "valid-topic")
		if err := seg.AppendEvent(event, 2); err != nil {
			t.Fatalf("AppendEvent %d failed: %v", i, err)
		}
	}
	seg.Flush()
	seg.Close()

	// Manually corrupt the file by appending garbage at the end
	filePath := filepath.Join(tmpDir, "segments", seg.GetFilename())
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open file for corruption: %v", err)
	}
	garbage := make([]byte, 100)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	f.Write(garbage)
	f.Close()

	// Reopen segment - scan should truncate the corrupt tail
	seg2, err := OpenSegment(tmpDir, seg.GetFilename(), nil)
	if err != nil {
		t.Fatalf("OpenSegment after corruption failed: %v", err)
	}
	defer seg2.Close()

	if seg2.GetLastOffset() != 4 {
		t.Errorf("expected last offset 4 after truncating corrupt tail, got %d", seg2.GetLastOffset())
	}
}

func TestSegment_TruncateToPosition(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip on Windows due to file locking issues with truncate on open files")
	}

	tmpDir := t.TempDir()

	seg, err := NewSegment(tmpDir, 0, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}

	// Append some events
	for i := 0; i < 5; i++ {
		event := makeEvent(int64(i), fmt.Sprintf("trunc-%d", i), "trunc-topic")
		if err := seg.AppendEvent(event, 2); err != nil {
			t.Fatalf("AppendEvent %d failed: %v", i, err)
		}
	}
	seg.Flush()

	sizeBefore := seg.GetSize()
	if sizeBefore <= 64 {
		t.Skip("segment too small to test truncation meaningfully")
	}

	// Truncate to half the size (call while segment is still open)
	midPoint := sizeBefore / 2
	if err := seg.truncateToPosition(midPoint); err != nil {
		t.Fatalf("truncateToPosition failed: %v", err)
	}

	seg.Close()

	if seg.GetSize() != midPoint {
		t.Errorf("expected size %d after truncate, got %d", midPoint, seg.GetSize())
	}
}

func TestSegment_Header(t *testing.T) {
	tmpDir := t.TempDir()

	seg, err := NewSegment(tmpDir, 42, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	// Header is 64 bytes; seg tracks buffered size internally
	if seg.GetSize() < 64 {
		t.Errorf("expected segment size >= 64, got %d", seg.GetSize())
	}
}

func TestSegment_InvalidHeaderRejected(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a valid segment.
	seg, err := NewSegment(tmpDir, 0, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	seg.Flush()
	seg.Close()

	// Corrupt the header magic.
	filePath := filepath.Join(tmpDir, "segments", seg.GetFilename())
	f, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open file for corruption: %v", err)
	}
	if _, err := f.WriteAt([]byte("BADHDR"), 0); err != nil {
		t.Fatalf("write corrupt header: %v", err)
	}
	f.Close()

	// Reopen should fail because the header is invalid.
	if _, err := OpenSegment(tmpDir, seg.GetFilename(), nil); err == nil {
		t.Fatal("expected OpenSegment to fail with invalid header, got nil")
	}
}

func TestSegment_HeaderCRCMismatchRejected(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a valid segment.
	seg, err := NewSegment(tmpDir, 0, true, nil)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	seg.Flush()
	seg.Close()

	// Corrupt the version byte (offset 7) without touching the CRC.
	filePath := filepath.Join(tmpDir, "segments", seg.GetFilename())
	f, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open file for corruption: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, 7); err != nil {
		t.Fatalf("write corrupt version byte: %v", err)
	}
	f.Close()

	// Reopen should fail because the header CRC no longer matches.
	if _, err := OpenSegment(tmpDir, seg.GetFilename(), nil); err == nil {
		t.Fatal("expected OpenSegment to fail with header CRC mismatch, got nil")
	}
}

// makeEvent is a test helper to create a types.Event
func makeEvent(offset int64, msgID string, topic string) *types.Event {
	return &types.Event{
		MessageId:   msgID,
		Topic:       topic,
		Payload:     []byte("payload"),
		Offset:      offset,
		PartitionId: 0,
		ScheduleTs:  1000 + offset,
	}
}

// --- Benchmark ---

func BenchmarkSegment_AppendEvent(b *testing.B) {
	tmpDir := b.TempDir()
	seg, _ := NewSegment(tmpDir, 0, true, nil)
	defer seg.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		event := &types.Event{
			MessageId:   fmt.Sprintf("msg-%d", i),
			Topic:       "topic",
			Payload:     []byte("payload data"),
			Offset:      int64(i),
			PartitionId: 0,
			ScheduleTs:  1000,
		}
		seg.AppendEvent(event, 1000)
	}
}

func BenchmarkSegment_ReadEvent(b *testing.B) {
	tmpDir := b.TempDir()
	seg, _ := NewSegment(tmpDir, 0, true, nil)
	defer seg.Close()

	// Populate
	for i := 0; i < 1000; i++ {
		event := &types.Event{
			MessageId:   fmt.Sprintf("msg-%d", i),
			Topic:       "topic",
			Payload:     []byte("payload data"),
			Offset:      int64(i),
			PartitionId: 0,
			ScheduleTs:  1000,
		}
		seg.AppendEvent(event, 1000)
	}
	seg.Flush()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		seg.ReadEvent(int64(i % 1000))
	}
}

func BenchmarkSegment_EncryptDecrypt(b *testing.B) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	cipher, _ := NewSegmentCipher(key, 0)
	plaintext := make([]byte, 4096)
	for i := range plaintext {
		plaintext[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ct, _ := cipher.Encrypt(plaintext)
		cipher.Decrypt(ct)
	}
}
