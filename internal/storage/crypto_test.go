package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestSegmentCipher_EncryptDecrypt(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	cipher, err := NewSegmentCipher(key)
	if err != nil {
		t.Fatalf("NewSegmentCipher failed: %v", err)
	}

	tests := []struct {
		name      string
		plaintext []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("hello")},
		{"typical event", []byte(`{"user":"alice","email":"alice@example.com","id":12345}`)},
		{"large", make([]byte, 1024*1024)}, // 1MB
		{"binary zeros", bytes.Repeat([]byte{0}, 100)},
		{"binary pattern", bytes.Repeat([]byte{0xDE, 0xAD, 0xBE, 0xEF}, 50)},
		{"all bytes", func() []byte { b := make([]byte, 256); for i := range b { b[i] = byte(i) }; return b }()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ciphertext, err := cipher.Encrypt(tc.plaintext)
			if err != nil {
				t.Fatalf("Encrypt failed: %v", err)
			}

			// Ciphertext should be different from plaintext (unless empty)
			if len(tc.plaintext) > 0 && bytes.Equal(ciphertext, tc.plaintext) {
				t.Error("ciphertext should differ from plaintext")
			}

			// Ciphertext should include nonce (at least GCM nonce size = 12 bytes)
			if len(ciphertext) < 12 {
				t.Errorf("ciphertext too short: %d bytes", len(ciphertext))
			}

			decrypted, err := cipher.Decrypt(ciphertext)
			if err != nil {
				t.Fatalf("Decrypt failed: %v", err)
			}

			if !bytes.Equal(decrypted, tc.plaintext) {
				t.Errorf("decrypted mismatch: got %v, want %v", decrypted, tc.plaintext)
			}
		})
	}
}

func TestSegmentCipher_DecryptErrors(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	cipher, err := NewSegmentCipher(key)
	if err != nil {
		t.Fatalf("NewSegmentCipher failed: %v", err)
	}

	tests := []struct {
		name       string
		ciphertext []byte
	}{
		{"too short", []byte("short")},
		{"invalid nonce", bytes.Repeat([]byte{0xFF}, 20)},
		{"corrupt ciphertext", func() []byte {
			ct, _ := cipher.Encrypt([]byte("test"))
			ct[len(ct)-1] ^= 0xFF // flip last byte
			return ct
		}()},
		{"truncated", func() []byte {
			ct, _ := cipher.Encrypt([]byte("test data that is longer"))
			return ct[:len(ct)/2]
		}()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := cipher.Decrypt(tc.ciphertext)
			if err == nil {
				t.Error("expected error but got none")
			}
		})
	}
}

func TestSegmentCipher_WrongKey(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	for i := range key1 {
		key1[i] = byte(i)
		key2[i] = byte(i + 1)
	}

	cipher1, _ := NewSegmentCipher(key1)
	cipher2, _ := NewSegmentCipher(key2)

	plaintext := []byte("secret message")
	ciphertext, err := cipher1.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	_, err = cipher2.Decrypt(ciphertext)
	if err == nil {
		t.Error("expected decryption to fail with wrong key")
	}
}

func TestNewSegmentCipher_InvalidKey(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"short", []byte("tooshort")},
		{"long", make([]byte, 64)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewSegmentCipher(tc.key)
			if err == nil {
				t.Error("expected error but got none")
			}
		})
	}
}

func TestLoadMasterKey(t *testing.T) {
	t.Run("valid 32-byte key file", func(t *testing.T) {
		tmpDir := t.TempDir()
		keyPath := filepath.Join(tmpDir, "key")
		keyData := make([]byte, 32)
		for i := range keyData {
			keyData[i] = byte(i)
		}
		if err := os.WriteFile(keyPath, keyData, 0600); err != nil {
			t.Fatalf("write key file: %v", err)
		}

		key, err := LoadMasterKey(keyPath)
		if err != nil {
			t.Fatalf("LoadMasterKey failed: %v", err)
		}
		if len(key) != 32 {
			t.Errorf("expected 32-byte key, got %d bytes", len(key))
		}
	})

	t.Run("key file with trailing newline", func(t *testing.T) {
		tmpDir := t.TempDir()
		keyPath := filepath.Join(tmpDir, "key")
		keyData := make([]byte, 32)
		for i := range keyData {
			keyData[i] = byte(i)
		}
		if err := os.WriteFile(keyPath, append(keyData, '\n'), 0600); err != nil {
			t.Fatalf("write key file: %v", err)
		}

		key, err := LoadMasterKey(keyPath)
		if err != nil {
			t.Fatalf("LoadMasterKey failed: %v", err)
		}
		if len(key) != 32 {
			t.Errorf("expected 32-byte key, got %d bytes", len(key))
		}
	})

	t.Run("missing key file", func(t *testing.T) {
		_, err := LoadMasterKey("/nonexistent/key")
		if err == nil {
			t.Error("expected error for missing file")
		}
	})

	t.Run("empty path", func(t *testing.T) {
		_, err := LoadMasterKey("")
		if err == nil {
			t.Error("expected error for empty path")
		}
	})
}

func TestEncryptedFile(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	cipher, err := NewSegmentCipher(key)
	if err != nil {
		t.Fatalf("NewSegmentCipher: %v", err)
	}

	tmpDir := t.TempDir()
	encPath := filepath.Join(tmpDir, "encrypted.dat")

	t.Run("write and read back", func(t *testing.T) {
		ef := NewEncryptedFile(encPath, cipher)
		plaintext := []byte("hello world this is encrypted")

		if err := ef.WriteAll(plaintext); err != nil {
			t.Fatalf("WriteAll failed: %v", err)
		}

		readBack, err := ef.ReadAll()
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}

		if !bytes.Equal(readBack, plaintext) {
			t.Errorf("read back mismatch: got %q, want %q", readBack, plaintext)
		}
	})

	t.Run("overwrite existing", func(t *testing.T) {
		ef := NewEncryptedFile(encPath, cipher)

		plaintext1 := []byte("first version")
		if err := ef.WriteAll(plaintext1); err != nil {
			t.Fatalf("WriteAll failed: %v", err)
		}

		plaintext2 := []byte("second version")
		if err := ef.WriteAll(plaintext2); err != nil {
			t.Fatalf("WriteAll failed: %v", err)
		}

		readBack, err := ef.ReadAll()
		if err != nil {
			t.Fatalf("ReadAll failed: %v", err)
		}

		if !bytes.Equal(readBack, plaintext2) {
			t.Errorf("expected second version, got %q", readBack)
		}
	})

	t.Run("invalid magic", func(t *testing.T) {
		ef := NewEncryptedFile(encPath, cipher)
		if err := os.WriteFile(encPath, []byte("BADD"), 0644); err != nil {
			t.Fatalf("write bad magic: %v", err)
		}
		_, err := ef.ReadAll()
		if err == nil {
			t.Error("expected error for invalid magic")
		}
	})

	t.Run("invalid version", func(t *testing.T) {
		ef := NewEncryptedFile(encPath, cipher)
		if err := os.WriteFile(encPath, []byte("CRNE\x02"), 0644); err != nil {
			t.Fatalf("write bad version: %v", err)
		}
		_, err := ef.ReadAll()
		if err == nil {
			t.Error("expected error for invalid version")
		}
	})

	t.Run("file too small", func(t *testing.T) {
		ef := NewEncryptedFile(encPath, cipher)
		_, err := ef.ReadAll()
		if err == nil {
			t.Error("expected error for small file")
		}
	})
}