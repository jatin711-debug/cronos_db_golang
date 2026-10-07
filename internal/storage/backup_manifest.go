package storage

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// BackupManifestName is the file at the root of a backup that describes it.
const BackupManifestName = "backup.json"

// Backup manifest versions. Version 1 backups hold only the event log and list
// no files; version 2 backups hold a partition's whole durable state and list
// every file with its size and checksum.
const (
	backupVersionWALOnly   = 1
	BackupVersionPartition = 2
)

// BackupManifest describes one backup generation.
type BackupManifest struct {
	Version int       `json:"version"`
	Scope   string    `json:"scope"`
	Created time.Time `json:"created"`
	// NodeID is the node the backup was taken on.
	NodeID string `json:"node_id,omitempty"`
	// CutAt is the instant at which every partition's log was cut: each
	// backed-up log holds exactly the events appended before it.
	CutAt time.Time `json:"cut_at,omitzero"`
	// Encryption is set when the logs are encrypted at rest.
	Encryption *BackupEncryption `json:"encryption,omitempty"`
	Partitions []BackupPartition `json:"partitions"`
}

// BackupEncryption identifies the key an encrypted backup needs. A backup
// never contains the key itself: whoever holds a copy of the backup must not
// thereby hold the means to read it.
type BackupEncryption struct {
	// KeyCheck is derived from the master key and identifies it without
	// revealing it. Restore compares it with the key the operator supplies.
	KeyCheck string `json:"key_check"`
}

// KeyCheckValue derives the value recorded as BackupEncryption.KeyCheck.
func KeyCheckValue(masterKey []byte) string {
	mac := hmac.New(sha256.New, masterKey)
	mac.Write([]byte("cronosdb backup key check v1"))
	return hex.EncodeToString(mac.Sum(nil)[:16])
}

// RestoreOptions adjusts RestoreBackup.
type RestoreOptions struct {
	// EncryptionKeyFile, when set, is checked against the key the backup was
	// encrypted with before anything is restored.
	EncryptionKeyFile string
}

// BackupPartition is one partition's part of a backup. Its files live under
// partitions/<id> in the backup, laid out as in a node's data directory.
type BackupPartition struct {
	PartitionID int32 `json:"partition_id"`
	// LastOffset is the last event offset in the backed-up log.
	LastOffset int64 `json:"last_offset"`
	// Components names what was captured besides the log, for an operator
	// reading the manifest.
	Components []string `json:"components,omitempty"`
	// Files lists every file of the partition with its size and CRC-32, so a
	// restore can tell a damaged or incomplete backup from a good one.
	Files []BackupFile `json:"files,omitempty"`
}

// BackupFile is one file of a backed-up partition. Path is relative to the
// partition's directory and uses forward slashes.
type BackupFile struct {
	Path  string `json:"path"`
	Size  int64  `json:"size"`
	CRC32 uint32 `json:"crc32"`
}

// ListBackupFiles returns every file under root with its size and checksum,
// ordered by path.
func ListBackupFiles(root string) ([]BackupFile, error) {
	var files []BackupFile
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		size, sum, err := checksumFile(path)
		if err != nil {
			return err
		}
		files = append(files, BackupFile{Path: filepath.ToSlash(rel), Size: size, CRC32: sum})
		return nil
	})
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, err
}

func checksumFile(path string) (int64, uint32, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()
	hash := crc32.NewIEEE()
	size, err := io.Copy(hash, file)
	return size, hash.Sum32(), err
}

// ReadBackupManifest loads and checks the manifest of the backup at backupDir.
func ReadBackupManifest(backupDir string) (*BackupManifest, error) {
	data, err := os.ReadFile(filepath.Join(backupDir, BackupManifestName))
	if err != nil {
		return nil, fmt.Errorf("read backup manifest: %w", err)
	}
	var manifest BackupManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parse backup manifest: %w", err)
	}
	if manifest.Version != backupVersionWALOnly && manifest.Version != BackupVersionPartition {
		return nil, fmt.Errorf("backup manifest version %d is not supported", manifest.Version)
	}
	if len(manifest.Partitions) == 0 {
		return nil, fmt.Errorf("backup manifest lists no partitions")
	}
	seen := make(map[int32]bool)
	for _, partition := range manifest.Partitions {
		if partition.PartitionID < 0 || seen[partition.PartitionID] {
			return nil, fmt.Errorf("backup manifest lists partition %d twice or with an invalid ID", partition.PartitionID)
		}
		seen[partition.PartitionID] = true
		for _, file := range partition.Files {
			if !filepath.IsLocal(filepath.FromSlash(file.Path)) {
				return nil, fmt.Errorf("backup manifest names a file outside partition %d: %q", partition.PartitionID, file.Path)
			}
		}
	}
	return &manifest, nil
}

// RestoreBackup copies the backup at backupDir into dataDir, the data
// directory of a node that is not running, and returns the backup's manifest.
//
// dataDir must not already hold data for any partition in the backup. Every
// partition is first copied to a staging directory, each file checked against
// the size and checksum recorded when the backup was taken; only when all of
// them are complete are they given their final names. A damaged or incomplete
// backup therefore restores nothing. A version 1 backup lists no files, so its
// log is copied without that check.
//
// A node started on dataDir afterwards rebuilds what a backup does not hold:
// timers, from the log, and the dedup records of events appended after the
// dedup store was captured. An encrypted backup is restored as ciphertext and
// the node needs the original key; opts can verify that key up front.
func RestoreBackup(backupDir, dataDir string, opts RestoreOptions) (*BackupManifest, error) {
	manifest, err := ReadBackupManifest(backupDir)
	if err != nil {
		return nil, err
	}
	if opts.EncryptionKeyFile != "" {
		if manifest.Encryption == nil {
			return nil, fmt.Errorf("an encryption key was given, but the backup is not encrypted")
		}
		key, err := LoadMasterKey(opts.EncryptionKeyFile)
		if err != nil {
			return nil, err
		}
		if KeyCheckValue(key) != manifest.Encryption.KeyCheck {
			return nil, fmt.Errorf("%s is not the key this backup was encrypted with", opts.EncryptionKeyFile)
		}
	}
	partitionsDir := filepath.Join(dataDir, "partitions")
	for _, partition := range manifest.Partitions {
		target := filepath.Join(partitionsDir, fmt.Sprint(partition.PartitionID))
		entries, err := os.ReadDir(target)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if len(entries) > 0 {
			return nil, fmt.Errorf("%s already holds data; restore into an empty data directory", target)
		}
	}
	if err := os.MkdirAll(partitionsDir, 0755); err != nil {
		return nil, err
	}

	staged := make([]string, 0, len(manifest.Partitions))
	discard := func() {
		for _, dir := range staged {
			_ = os.RemoveAll(dir)
		}
	}
	for _, partition := range manifest.Partitions {
		id := fmt.Sprint(partition.PartitionID)
		staging := filepath.Join(partitionsDir, ".restoring-"+id)
		if err := os.RemoveAll(staging); err != nil {
			discard()
			return nil, err
		}
		staged = append(staged, staging)
		if err := restorePartitionFiles(filepath.Join(backupDir, "partitions", id), staging, partition); err != nil {
			discard()
			return nil, fmt.Errorf("restore partition %d: %w", partition.PartitionID, err)
		}
	}
	for i, partition := range manifest.Partitions {
		target := filepath.Join(partitionsDir, fmt.Sprint(partition.PartitionID))
		if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if err := os.Rename(staged[i], target); err != nil {
			return nil, fmt.Errorf("publish restored partition %d: %w", partition.PartitionID, err)
		}
	}
	if err := SyncDirectory(partitionsDir); err != nil {
		return nil, err
	}
	return manifest, nil
}

// restorePartitionFiles copies one partition from a backup into staging.
func restorePartitionFiles(source, staging string, partition BackupPartition) error {
	if len(partition.Files) == 0 {
		return RestoreWAL(source, staging)
	}
	dirs := map[string]bool{staging: true}
	for _, file := range partition.Files {
		dst := filepath.Join(staging, filepath.FromSlash(file.Path))
		dirs[filepath.Dir(dst)] = true
		if err := copyVerified(filepath.Join(source, filepath.FromSlash(file.Path)), dst, file); err != nil {
			return err
		}
	}
	for dir := range dirs {
		if err := SyncDirectory(dir); err != nil {
			return err
		}
	}
	return nil
}

// copyVerified copies src to dst and fails unless the bytes copied have the
// size and checksum the manifest recorded for the file.
func copyVerified(src, dst string, want BackupFile) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("backup is missing %s: %w", want.Path, err)
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	hash := crc32.NewIEEE()
	size, copyErr := io.Copy(io.MultiWriter(out, hash), in)
	if copyErr == nil {
		copyErr = out.Sync()
	}
	if closeErr := out.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return fmt.Errorf("copy %s: %w", want.Path, copyErr)
	}
	if size != want.Size || hash.Sum32() != want.CRC32 {
		return fmt.Errorf("%s does not match the backup manifest (%d bytes, checksum %08x; recorded %d bytes, checksum %08x)",
			want.Path, size, hash.Sum32(), want.Size, want.CRC32)
	}
	return nil
}
