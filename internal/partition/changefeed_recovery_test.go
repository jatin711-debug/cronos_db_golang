package partition

import (
	"os"
	"path/filepath"
	"testing"
)

func TestChangeFeedCorruptCursorReplaysPending(t *testing.T) {
	for _, body := range []string{"{broken", "{}", "null", `{"offset":null}`, `{"offset":-2}`, `{"offset":99}`, "missing"} {
		t.Run(body, func(t *testing.T) {
			cfg := unacceptedTestConfig(t)
			cfg.FsyncMode = "every_event"
			sink := &feedSink{}
			sink.refuse.Store(true)
			pm := NewPartitionManager("node-1", cfg)
			pm.SetChangeFeed(sink.take)
			p := startedPartition(t, pm, 0)
			publish(t, p, 3)
			cursorPath := filepath.Join(p.DataDir, feedCursorFile)
			if err := pm.Close(); err != nil {
				t.Fatal(err)
			}
			if body == "missing" {
				if err := os.Remove(cursorPath); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(cursorPath, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			restored := feedManager(t, cfg, sink)
			if err := restored.CreatePartition(0, "orders"); err != nil {
				t.Fatal(err)
			}
			rp, err := restored.GetInternalPartition(0)
			if err != nil {
				t.Fatal(err)
			}
			position, _ := rp.ChangeFeedPosition()
			if position != -1 {
				t.Fatalf("CDC cursor advanced to %d with 3 unexported WAL records and sink still down", position)
			}
			if err := restored.StartPartition(0); err != nil {
				t.Fatal(err)
			}
			sink.refuse.Store(false)
			sink.expect(t, 0, "[0 1 2]")
		})
	}

}
