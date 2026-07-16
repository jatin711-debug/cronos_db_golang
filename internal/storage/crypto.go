package storage

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// EncryptionConfig holds at-rest encryption settings.
type EncryptionConfig struct {
	Enabled   bool
	MasterKey []byte // 32 bytes for AES-256
	KeyFile   string // Path to file containing master key
}

// LoadMasterKey loads the encryption key from file. The key must be exactly 32
// bytes (256 bits) for AES-256-GCM. On Unix, the file must not be readable or
// writable by group or others.
func LoadMasterKey(keyFile string) ([]byte, error) {
	if keyFile == "" {
		return nil, fmt.Errorf("encryption enabled but no key file specified")
	}
	info, err := os.Stat(keyFile)
	if err != nil {
		return nil, fmt.Errorf("stat master key file: %w", err)
	}
	if runtime.GOOS != "windows" {
		mode := info.Mode().Perm()
		if mode&077 != 0 {
			return nil, fmt.Errorf("master key file %s must not be readable or writable by group or others (mode %#o)", keyFile, mode)
		}
	}

	data, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("read master key: %w", err)
	}
	trimmed := strings.TrimSpace(string(data))
	if len(trimmed) != 32 {
		return nil, fmt.Errorf("master key must be exactly 32 bytes, got %d", len(trimmed))
	}
	key := make([]byte, 32)
	copy(key, trimmed)
	return key, nil
}

// SegmentCipher provides AES-256-GCM encryption for WAL segments and metadata files.
// The AEAD is built once and reused, which removes per-record key-schedule and allocation
// overhead. WAL records use a deterministic counter nonce derived from the partition ID and
// the ciphertext's byte position in the segment; this guarantees GCM nonce uniqueness without
// reading from crypto/rand on every record.
type SegmentCipher struct {
	key         []byte
	aead        cipher.AEAD
	partitionID int32
}

// NewSegmentCipher creates a cipher from a 32-byte key and the owning partition ID.
// The partition ID is mixed into the record counter nonce so records from different
// partitions cannot share a nonce.
func NewSegmentCipher(key []byte, partitionID int32) (*SegmentCipher, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("master key must be 32 bytes for AES-256, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}
	return &SegmentCipher{
		key:         key,
		aead:        gcm,
		partitionID: partitionID,
	}, nil
}

// nonceForRecord builds a deterministic 12-byte GCM nonce from the partition ID and the
// ciphertext's byte position within its segment file. Positions are unique and monotonic
// within a partition, so the same nonce is never reused with the same key.
func (c *SegmentCipher) nonceForRecord(ciphertextPos int64) [12]byte {
	var nonce [12]byte
	binary.BigEndian.PutUint32(nonce[:4], uint32(c.partitionID))
	binary.BigEndian.PutUint64(nonce[4:], uint64(ciphertextPos))
	return nonce
}

// Encrypt encrypts plaintext with AES-256-GCM using a random nonce. It is intended for
// small metadata files (EncryptedFile) rather than WAL records. The returned format is
// nonce || ciphertext.
func (c *SegmentCipher) Encrypt(plaintext []byte) ([]byte, error) {
	if c.aead == nil {
		return nil, fmt.Errorf("cipher not initialized")
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return c.aead.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt decrypts ciphertext produced by Encrypt. Expects nonce || ciphertext.
func (c *SegmentCipher) Decrypt(ciphertext []byte) ([]byte, error) {
	if c.aead == nil {
		return nil, fmt.Errorf("cipher not initialized")
	}
	ns := c.aead.NonceSize()
	if len(ciphertext) < ns {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce, ct := ciphertext[:ns], ciphertext[ns:]
	return c.aead.Open(nil, nonce, ct, nil)
}

// Record cipher-format versions.
//
// v1 (legacy, decrypt-only): [1][ciphertext], nonce = partitionID || position.
//   The position-derived counter nonce reused the same (key, nonce) pair across
//   segments (positions restart per segment), which is catastrophic for AES-GCM.
//   We still DECRYPT v1 so previously-written data remains readable.
// v2 (current): [2][nonce 12B][ciphertext], nonce = 12 random bytes per record.
//   A fresh random nonce per record removes the counter-collision class entirely.
const (
	recordCipherV1 = byte(1)
	recordCipherV2 = byte(2)
)

// EncryptRecord encrypts a WAL record payload with a fresh random 12-byte GCM
// nonce (format v2). The returned layout is [version=2][nonce 12B][ciphertext].
// ciphertextPos is accepted for API compatibility but is no longer used to derive
// the nonce — deriving nonces from position was the source of GCM nonce reuse.
func (c *SegmentCipher) EncryptRecord(plaintext []byte, ciphertextPos int64) ([]byte, error) {
	if c.aead == nil {
		return nil, fmt.Errorf("cipher not initialized")
	}
	_ = ciphertextPos // retained for signature compatibility; v2 uses a random nonce
	ns := c.aead.NonceSize()
	out := make([]byte, 1+ns)
	out[0] = recordCipherV2
	if _, err := io.ReadFull(rand.Reader, out[1:1+ns]); err != nil {
		return nil, fmt.Errorf("generate record nonce: %w", err)
	}
	// Seal appends the ciphertext after the version+nonce header we already wrote.
	return c.aead.Seal(out, out[1:1+ns], plaintext, nil), nil
}

// DecryptRecord decrypts a WAL record ciphertext produced by EncryptRecord.
// It handles both v2 (random nonce embedded in the record) and legacy v1
// (position-derived nonce). ciphertextPos must be the byte offset of the
// ciphertext (after the 4-byte length prefix) inside the segment file; it is
// only used to reconstruct the v1 nonce.
func (c *SegmentCipher) DecryptRecord(ciphertext []byte, ciphertextPos int64) ([]byte, error) {
	if c.aead == nil {
		return nil, fmt.Errorf("cipher not initialized")
	}
	if len(ciphertext) == 0 {
		return nil, fmt.Errorf("empty record ciphertext")
	}
	version := ciphertext[0]
	switch version {
	case recordCipherV1:
		nonce := c.nonceForRecord(ciphertextPos)
		return c.aead.Open(nil, nonce[:], ciphertext[1:], nil)
	case recordCipherV2:
		ns := c.aead.NonceSize()
		if len(ciphertext) < 1+ns {
			return nil, fmt.Errorf("record too short for v2 nonce")
		}
		nonce := ciphertext[1 : 1+ns]
		return c.aead.Open(nil, nonce, ciphertext[1+ns:], nil)
	default:
		return nil, fmt.Errorf("unsupported record cipher version: %d", version)
	}
}

// EncryptedFile wraps a file with transparent encryption/decryption.
// The format is: [magic 4B][version 1B][encrypted payload]
// Magic: "CRNE" (Cronos Encrypted)
type EncryptedFile struct {
	cipher *SegmentCipher
	path   string
}

const encryptedFileMagic = "CRNE"
const encryptedFileVersion = byte(1)

// NewEncryptedFile creates an encrypted file wrapper.
func NewEncryptedFile(path string, cipher *SegmentCipher) *EncryptedFile {
	return &EncryptedFile{path: path, cipher: cipher}
}

// ReadAll reads and decrypts the entire file.
func (ef *EncryptedFile) ReadAll() ([]byte, error) {
	data, err := os.ReadFile(ef.path)
	if err != nil {
		return nil, err
	}
	if len(data) < 5 {
		return nil, fmt.Errorf("file too small to be encrypted")
	}
	if subtle.ConstantTimeCompare([]byte(data[:4]), []byte(encryptedFileMagic)) != 1 {
		return nil, fmt.Errorf("invalid encrypted file magic")
	}
	if data[4] != encryptedFileVersion {
		return nil, fmt.Errorf("unsupported encrypted file version: %d", data[4])
	}
	return ef.cipher.Decrypt(data[5:])
}

// WriteAll encrypts and writes data to file atomically.
func (ef *EncryptedFile) WriteAll(plaintext []byte) error {
	encrypted, err := ef.cipher.Encrypt(plaintext)
	if err != nil {
		return err
	}
	out := make([]byte, 0, 5+len(encrypted))
	out = append(out, []byte(encryptedFileMagic)...)
	out = append(out, encryptedFileVersion)
	out = append(out, encrypted...)

	return utils.AtomicWriteFile(ef.path, out, 0640)
}
