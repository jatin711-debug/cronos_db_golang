package recoverystate

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestRecoveryPagesValidateBoundsEncryptionAndInterruptedFiles(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	cipher, err := storage.NewSegmentCipher(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), Filename)
	m := Manifest{Partition: 0, Epoch: 4, First: 0, Last: 9, AcceptedThrough: 7}
	if err := Write(path, m, cipher, func(emit func(*types.ReplicationProgressRequest) error) error {
		return emit(&types.ReplicationProgressRequest{PartitionId: 0, Term: 4, Groups: []*types.ConsumerGroupProgress{{GroupId: "workers", Topic: "orders", CommittedOffset: 3, CompletedOffsets: []int64{7}}}, DeadLetters: []*types.DeadLetterRecord{{PartitionId: 0, DeliveryId: "failed", Event: &types.Event{PartitionId: 0, Payload: []byte("secret-payload")}}}})
	}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("secret-payload")) {
		t.Fatal("recovery snapshot exposed an encrypted dead-letter payload")
	}
	if _, err := Read(path, nil, nil); err == nil {
		t.Fatal("encrypted recovery opened without its key")
	}
	pages := 0
	got, err := Read(path, cipher, func(*types.ReplicationProgressRequest) error { pages++; return nil })
	if err != nil || got.AcceptedThrough != 7 || pages != 1 {
		t.Fatalf("read: %v %d %v", got, pages, err)
	}
	for _, n := range []int{1, len(data) / 2, len(data) - 1} {
		broken := filepath.Join(t.TempDir(), Filename)
		if err := os.WriteFile(broken, data[:n], 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Read(broken, cipher, nil); err == nil {
			t.Fatalf("accepted file cut at %d", n)
		}
	}
	bad := filepath.Join(t.TempDir(), Filename)
	if err := Write(bad, m, nil, func(emit func(*types.ReplicationProgressRequest) error) error {
		return emit(&types.ReplicationProgressRequest{Term: 4, Groups: []*types.ConsumerGroupProgress{{GroupId: "workers", CompletedOffsets: []int64{8}}}})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(bad, nil, nil); err == nil {
		t.Fatal("accepted completion beyond the committed prefix")
	}
}
