package storage

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRestoreRejectsVersionOneBeforePublishing(t *testing.T) {
	for _, manifestVersion := range []int{backupVersionWALOnly, BackupVersionPartition} {
		t.Run(string(rune('0'+manifestVersion)), func(t *testing.T) {
			backup := t.TempDir()
			partitionDir := filepath.Join(backup, "partitions", "0")
			segment, err := NewSegmentWithSize(partitionDir, 0, true, nil, 1024)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(partitionDir, "segments", segment.GetFilename())
			if err := segment.Close(); err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			var header [64]byte
			if _, err := file.ReadAt(header[:], 0); err != nil {
				t.Fatal(err)
			}
			header[7] = 1
			binary.BigEndian.PutUint32(header[60:], crc32.ChecksumIEEE(header[:60]))
			if _, err := file.WriteAt(header[:], 0); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			partition := BackupPartition{PartitionID: 0}
			if manifestVersion == BackupVersionPartition {
				partition.Files, err = ListBackupFiles(partitionDir)
				if err != nil {
					t.Fatal(err)
				}
			}
			data, err := json.Marshal(BackupManifest{Version: manifestVersion, Partitions: []BackupPartition{partition}})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(backup, BackupManifestName), data, 0600); err != nil {
				t.Fatal(err)
			}
			target := t.TempDir()
			if _, err := RestoreBackup(backup, target, RestoreOptions{}); err == nil || !strings.Contains(err.Error(), "unsupported segment version: 1") {
				t.Fatalf("restore did not reject version 1: %v", err)
			}
			if _, err := os.Stat(filepath.Join(target, "partitions", "0")); !os.IsNotExist(err) {
				t.Fatalf("restore published a partition: %v", err)
			}
		})
	}
}
