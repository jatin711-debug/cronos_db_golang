package schema

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRegistryLoadFailsClosed(t *testing.T) {
	for _, data := range []string{
		`{`,
		`null`,
		`{"topic":"orders","version":1,"type":"xml","definition":"x"}`,
		`{"topic":"orders","version":1,"type":"json","definition":"{"}`,
		`{"topic":"orders","version":0,"type":"json","definition":"{}"}`,
	} {
		t.Run(data, func(t *testing.T) {
			dir := t.TempDir()
			r, err := NewRegistry(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := r.Register("orders", TypeJSON, `{"type":"object"}`); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(r.dir, "broken.json"), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if err := r.Load(); err == nil {
				t.Fatal("invalid schema file must fail reload")
			}
			if err := r.Validate("orders", []byte(`123`)); err == nil {
				t.Fatal("failed reload discarded active validation")
			}
			if _, err := NewRegistry(dir); err == nil {
				t.Fatal("invalid schema file must fail startup")
			}
		})
	}
}

func TestRegistryLoadRejectsDuplicateVersions(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Register("orders", TypeJSON, `{"type":"object"}`); err != nil {
		t.Fatal(err)
	}
	if err := r.Load(); err != nil {
		t.Fatal(err)
	}
	if err := r.Load(); err != nil {
		t.Fatalf("reload must be idempotent: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(r.dir, "orders_v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.dir, "copy.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRegistry(dir); err == nil {
		t.Fatal("duplicate versions must fail startup")
	}
}
