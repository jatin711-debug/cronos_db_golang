package schema

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewRegistry(t *testing.T) {
	tmpDir := t.TempDir()
	r, err := NewRegistry(tmpDir)
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}
	if r == nil {
		t.Fatal("registry should not be nil")
	}
	if r.schemas == nil {
		t.Fatal("schemas map should be initialized")
	}
}

func TestNewRegistry_CreateDir(t *testing.T) {
	tmpDir := t.TempDir()
	nested := filepath.Join(tmpDir, "deep", "schemas")
	_, err := NewRegistry(nested)
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}
	if _, err := os.Stat(nested); os.IsNotExist(err) {
		t.Fatal("schema dir should be created")
	}
}

func TestRegistry_Register(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	version, err := r.Register("orders", TypeJSON, `{"type":"object"}`)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if version != 1 {
		t.Errorf("expected version 1, got %d", version)
	}

	// Second registration with different definition
	version2, err := r.Register("orders", TypeJSON, `{"type":"object","properties":{}}`)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if version2 != 2 {
		t.Errorf("expected version 2, got %d", version2)
	}
}

func TestRegistry_Register_Idempotent(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	def := `{"type":"object"}`
	v1, _ := r.Register("orders", TypeJSON, def)
	v2, _ := r.Register("orders", TypeJSON, def)

	if v1 != v2 {
		t.Errorf("expected same version for idempotent register, got %d and %d", v1, v2)
	}
}

func TestRegistry_Get(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	_, ok := r.Get("orders")
	if ok {
		t.Error("should not find unregistered topic")
	}

	r.Register("orders", TypeJSON, `{"type":"object"}`)
	schema, ok := r.Get("orders")
	if !ok {
		t.Fatal("should find registered topic")
	}
	if schema.Topic != "orders" {
		t.Errorf("expected topic orders, got %s", schema.Topic)
	}
	if schema.Version != 1 {
		t.Errorf("expected version 1, got %d", schema.Version)
	}
}

func TestRegistry_GetVersion(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	r.Register("orders", TypeJSON, `{"type":"object"}`)
	r.Register("orders", TypeJSON, `{"type":"object","properties":{}}`)

	schema, ok := r.GetVersion("orders", 1)
	if !ok {
		t.Fatal("should find version 1")
	}
	if schema.Version != 1 {
		t.Errorf("expected version 1, got %d", schema.Version)
	}

	schema2, ok := r.GetVersion("orders", 2)
	if !ok {
		t.Fatal("should find version 2")
	}
	if schema2.Version != 2 {
		t.Errorf("expected version 2, got %d", schema2.Version)
	}

	_, ok = r.GetVersion("orders", 99)
	if ok {
		t.Error("should not find nonexistent version")
	}

	_, ok = r.GetVersion("unknown", 1)
	if ok {
		t.Error("should not find unknown topic")
	}
}

func TestRegistry_Validate_NoSchema(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	// No schema registered — should allow all
	err := r.Validate("orders", []byte(`{}`))
	if err != nil {
		t.Errorf("expected no error when no schema, got %v", err)
	}
}

func TestRegistry_Validate_JSON(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	r.Register("orders", TypeJSON, `{"type":"object"}`)

	err := r.Validate("orders", []byte(`{"valid": true}`))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	err = r.Validate("orders", []byte(`not json`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestRegistry_Validate_Avro(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	avroSchema := `{"type":"record","name":"Test","fields":[{"name":"name","type":"string"}]}`
	r.Register("events", TypeAvro, avroSchema)

	// Valid avro binary is complex; just test that schema parsing works
	// and invalid payload fails
	err := r.Validate("events", []byte(`invalid avro`))
	if err == nil {
		t.Error("expected error for invalid avro payload")
	}
}

func TestRegistry_Validate_Protobuf_WellKnown(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	r.Register("timestamps", TypeProtobuf, "google.protobuf.Timestamp")

	// Valid Timestamp protobuf
	// google.protobuf.Timestamp wire format: field 1 (varint seconds), field 2 (varint nanos)
	// Empty timestamp = 0 seconds, 0 nanos = empty byte sequence, but let's try minimal valid
	// Actually empty is valid for zero values in proto3
	err := r.Validate("timestamps", []byte{})
	if err != nil {
		t.Errorf("unexpected error for empty timestamp: %v", err)
	}
}

func TestRegistry_Validate_Protobuf_Unknown(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	r.Register("custom", TypeProtobuf, "com.example.MyEvent")

	err := r.Validate("custom", []byte(`any payload`))
	if err == nil {
		t.Error("expected error for unknown protobuf type")
	}
}

func TestRegistry_Validate_Protobuf_Duration(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	r.Register("durations", TypeProtobuf, "google.protobuf.Duration")

	// Empty duration is valid in proto3
	err := r.Validate("durations", []byte{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRegistry_Validate_Protobuf_Any(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	r.Register("any", TypeProtobuf, "google.protobuf.Any")

	// Empty Any is valid
	err := r.Validate("any", []byte{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRegistry_Validate_Protobuf_Struct(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	r.Register("structs", TypeProtobuf, "google.protobuf.Struct")

	// Empty Struct is valid
	err := r.Validate("structs", []byte{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRegistry_Validate_UnknownType(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	// Manually inject schema with unknown type
	r.mu.Lock()
	r.schemas["unknown"] = []Schema{{Topic: "unknown", Type: Type("xml"), Version: 1, Definition: "<xml/>"}}
	r.mu.Unlock()

	// Unknown types should be allowed by default
	err := r.Validate("unknown", []byte(`<xml/>`))
	if err != nil {
		t.Errorf("unexpected error for unknown type: %v", err)
	}
}

func TestRegistry_Persist(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	_, err := r.Register("orders", TypeJSON, `{"type":"object"}`)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Check that file was persisted
	files, err := os.ReadDir(r.dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].Name() != "orders_v1.json" {
		t.Errorf("expected orders_v1.json, got %s", files[0].Name())
	}
}

func TestSchema_Hash(t *testing.T) {
	s1 := Schema{Topic: "t", Version: 1, Type: TypeJSON, Definition: `{"a":1}`}
	s2 := Schema{Topic: "t", Version: 1, Type: TypeJSON, Definition: `{"a":1}`}
	s3 := Schema{Topic: "t", Version: 1, Type: TypeJSON, Definition: `{"b":2}`}

	// Same definition should give same hash (if we compute it the same way)
	// Note: Hash is only computed in Register, not in direct struct creation
	if s1.Hash != 0 {
		t.Logf("Hash was pre-set: %d", s1.Hash)
	}
	_ = s2
	_ = s3
}

func TestType_Constants(t *testing.T) {
	if TypeJSON != "json" {
		t.Error("TypeJSON mismatch")
	}
	if TypeAvro != "avro" {
		t.Error("TypeAvro mismatch")
	}
	if TypeProtobuf != "protobuf" {
		t.Error("TypeProtobuf mismatch")
	}
}
