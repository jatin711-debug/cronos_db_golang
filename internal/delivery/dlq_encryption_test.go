package delivery

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func dlqCipher(t *testing.T, key byte) *storage.SegmentCipher {
	t.Helper()
	cipher, err := storage.NewSegmentCipher(bytes.Repeat([]byte{key}, 32), 0)
	if err != nil {
		t.Fatal(err)
	}
	return cipher
}

func dlqFiles(t *testing.T, dataDir string) map[string][]byte {
	t.Helper()
	files := make(map[string][]byte)
	names, err := filepath.Glob(filepath.Join(dataDir, "dlq", "*.dlq"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		files[name] = data
	}
	return files
}

// A dead-lettered event is stored whole. On a node that encrypts at rest its
// payload must not sit in the queue's files in the clear, and the queue must
// refuse to open with the wrong key rather than come up empty.
func TestDeadLetterQueue_EncryptsEntriesAtRest(t *testing.T) {
	dir := t.TempDir()
	const secret = "card-number-4111111111111111"
	event := func(id string) *types.Event {
		return &types.Event{MessageId: id, Topic: "payments", Payload: []byte(secret), Offset: 7}
	}

	dlq, err := NewEncryptedDeadLetterQueue(dir, 0, dlqCipher(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"keep", "remove"} {
		if err := dlq.Add(event(id), "delivery-"+id, 5, "handler failed for "+secret, "worker-1"); err != nil {
			t.Fatal(err)
		}
	}
	if err := dlq.Remove("delivery-remove"); err != nil {
		t.Fatal(err)
	}
	if err := dlq.Close(); err != nil {
		t.Fatal(err)
	}
	for name, data := range dlqFiles(t, dir) {
		for _, clear := range []string{secret, "payments", "delivery-keep", "worker-1"} {
			if bytes.Contains(data, []byte(clear)) {
				t.Fatalf("%s holds %q in the clear", filepath.Base(name), clear)
			}
		}
	}

	// The same key reads everything back, removals included.
	reopened, err := NewEncryptedDeadLetterQueue(dir, 0, dlqCipher(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	entries := reopened.Get()
	if len(entries) != 1 || entries[0].DeliveryID != "delivery-keep" || string(entries[0].Event.GetPayload()) != secret {
		t.Fatalf("reopened queue holds %d entries, want the one that was kept with its payload", len(entries))
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := NewEncryptedDeadLetterQueue(dir, 0, dlqCipher(t, 2)); err == nil || !strings.Contains(err.Error(), "key") {
		t.Fatalf("the queue opened with the wrong key: %v", err)
	}
	if _, err := NewDeadLetterQueue(dir, 0); err == nil || !strings.Contains(err.Error(), "key") {
		t.Fatalf("the queue opened without a key although its entries are encrypted: %v", err)
	}
}

// Entries written before encryption was switched on are still read once it is.
func TestDeadLetterQueue_ReadsEntriesWrittenBeforeEncryption(t *testing.T) {
	dir := t.TempDir()
	plain, err := NewDeadLetterQueue(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := plain.Add(&types.Event{MessageId: "old", Payload: []byte("p")}, "delivery-old", 5, "failed", "worker-1"); err != nil {
		t.Fatal(err)
	}
	if err := plain.Close(); err != nil {
		t.Fatal(err)
	}

	encrypted, err := NewEncryptedDeadLetterQueue(dir, 0, dlqCipher(t, 1))
	if err != nil {
		t.Fatalf("open with encryption switched on: %v", err)
	}
	defer encrypted.Close()
	if err := encrypted.Add(&types.Event{MessageId: "new", Payload: []byte("p")}, "delivery-new", 5, "failed", "worker-1"); err != nil {
		t.Fatal(err)
	}
	if got := encrypted.Count(); got != 2 {
		t.Fatalf("queue holds %d entries, want the old one and the new one", got)
	}
}

// A damaged record is skipped, counted and reported; the records around it
// are kept and the file is not rewritten.
func TestDeadLetterQueue_ReportsDamageAndKeepsTheRest(t *testing.T) {
	dir := t.TempDir()
	dlq, err := NewDeadLetterQueue(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"first", "second", "third"} {
		if err := dlq.Add(&types.Event{MessageId: id, Payload: []byte("payload-of-" + id)}, "delivery-"+id, 5, "failed", "worker-1"); err != nil {
			t.Fatal(err)
		}
	}
	if err := dlq.Close(); err != nil {
		t.Fatal(err)
	}

	// One byte of the second record's body flips on disk.
	var damaged string
	var content []byte
	for name, data := range dlqFiles(t, dir) {
		if at := bytes.Index(data, []byte("delivery-second")); at >= 0 {
			data[at] ^= 0xff
			damaged, content = name, data
			if err := os.WriteFile(name, data, 0644); err != nil {
				t.Fatal(err)
			}
		}
	}
	if damaged == "" {
		t.Fatal("setup: the second record was not found on disk")
	}

	reopened, err := NewDeadLetterQueue(dir, 0)
	if err != nil {
		t.Fatalf("a queue with one damaged record did not open: %v", err)
	}
	defer reopened.Close()
	var ids []string
	for _, entry := range reopened.Get() {
		ids = append(ids, entry.DeliveryID)
	}
	if strings.Join(ids, ",") != "delivery-first,delivery-third" {
		t.Fatalf("entries after the damage: %v, want the first and the third", ids)
	}
	if stats := reopened.GetStats(); stats.CorruptRecords != 1 {
		t.Fatalf("stats report %d corrupt records, want 1", stats.CorruptRecords)
	}
	if after, err := os.ReadFile(damaged); err != nil || !bytes.Equal(after, content) {
		t.Fatalf("the damaged file was rewritten (err=%v)", err)
	}
}
