package storage

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/subtle"
	"fmt"
	"io"
	"os"
)

// EncryptionConfig holds at-rest encryption settings.
type EncryptionConfig struct {
	Enabled    bool
	MasterKey  []byte // 32 bytes for AES-256
	KeyFile    string // Path to file containing master key
}

// LoadMasterKey loads the encryption key from file or environment.
func LoadMasterKey(keyFile string) ([]byte, error) {
	if keyFile == "" {
		return nil, fmt.Errorf("encryption enabled but no key file specified")
	}
	data, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("read master key: %w", err)
	}
	// Trim whitespace/newlines
	key := make([]byte, 32)
	copy(key, data)
	return key, nil
}

// SegmentCipher provides AES-256-GCM encryption for WAL segments.
type SegmentCipher struct {
	key []byte
}

// NewSegmentCipher creates a cipher from a 32-byte key.
func NewSegmentCipher(key []byte) (*SegmentCipher, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("master key must be 32 bytes for AES-256, got %d", len(key))
	}
	return &SegmentCipher{key: key}, nil
}

// Encrypt encrypts plaintext with AES-256-GCM. Returns ciphertext || nonce.
func (c *SegmentCipher) Encrypt(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt decrypts ciphertext. Expects nonce || ciphertext.
func (c *SegmentCipher) Decrypt(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	ns := gcm.NonceSize()
	if len(ciphertext) < ns {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce, ct := ciphertext[:ns], ciphertext[ns:]
	return gcm.Open(nil, nonce, ct, nil)
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

	tmp := ef.path + ".tmp"
	if err := os.WriteFile(tmp, out, 0640); err != nil {
		return err
	}
	return os.Rename(tmp, ef.path)
}
