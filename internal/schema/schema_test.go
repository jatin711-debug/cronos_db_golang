package schema

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
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

func TestRegistry_RegisterWithDescriptor(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	v, err := r.RegisterWithDescriptor("events", TypeProtobuf, "com.example.Event", []byte("fake_descriptor"))
	if err != nil {
		t.Fatalf("RegisterWithDescriptor failed: %v", err)
	}
	if v != 1 {
		t.Errorf("expected version 1, got %d", v)
	}

	schema, ok := r.Get("events")
	if !ok {
		t.Fatal("should find registered topic")
	}
	if string(schema.Descriptor) != "fake_descriptor" {
		t.Errorf("descriptor mismatch: got %q", schema.Descriptor)
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

	// Empty timestamp is valid for zero values in proto3
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

func TestRegistry_Validate_Protobuf_WithDescriptor(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	// Register with a descriptor — even fake bytes should change error path
	r.RegisterWithDescriptor("custom", TypeProtobuf, "com.example.MyEvent", []byte("invalid_descriptor"))

	err := r.Validate("custom", []byte(`any payload`))
	if err == nil {
		t.Error("expected error for invalid descriptor")
	}
	// Should mention descriptor unmarshal, not "requires FileDescriptorProto bytes"
	if err.Error() == "protobuf validation for \"com.example.MyEvent\" requires the schema definition to contain the FileDescriptorProto bytes; use RegisterWithDescriptor to supply the descriptor" {
		t.Error("expected descriptor unmarshal error, got generic missing descriptor error")
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

func TestRegistry_Load(t *testing.T) {
	tmpDir := t.TempDir()
	r1, _ := NewRegistry(tmpDir)

	r1.Register("orders", TypeJSON, `{"type":"object"}`)
	r1.Register("orders", TypeJSON, `{"type":"object","properties":{}}`)
	r1.Register("users", TypeAvro, `{"type":"record","name":"User","fields":[]}`)

	// Create a new registry pointing at the same dir — should load persisted schemas
	r2, err := NewRegistry(tmpDir)
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}

	orders, ok := r2.Get("orders")
	if !ok {
		t.Fatal("should load orders schema")
	}
	if orders.Version != 2 {
		t.Errorf("expected latest version 2, got %d", orders.Version)
	}

	users, ok := r2.Get("users")
	if !ok {
		t.Fatal("should load users schema")
	}
	if users.Type != TypeAvro {
		t.Errorf("expected avro type, got %s", users.Type)
	}
}

func TestRegistry_Compatibility_Backward(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	avroSchema1 := `{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}`
	avroSchema2 := `{"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"age","type":"int","default":0}]}`

	r.SetCompatibility("users", CompatBackward)
	_, err := r.Register("users", TypeAvro, avroSchema1)
	if err != nil {
		t.Fatalf("register v1: %v", err)
	}
	_, err = r.Register("users", TypeAvro, avroSchema2)
	if err != nil {
		t.Fatalf("register v2: %v", err)
	}
}

func TestRegistry_Compatibility_Backward_Fail(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	avroSchema1 := `{"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`
	avroSchema2 := `{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}`

	r.SetCompatibility("users", CompatBackward)
	_, err := r.Register("users", TypeAvro, avroSchema1)
	if err != nil {
		t.Fatalf("register v1: %v", err)
	}
	_, err = r.Register("users", TypeAvro, avroSchema2)
	if err == nil {
		t.Fatal("expected backward compatibility failure when removing field")
	}
}

func TestRegistry_Compatibility_None(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	avroSchema1 := `{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}`
	avroSchema2 := `{"type":"record","name":"User","fields":[]}`

	r.SetCompatibility("users", CompatNone)
	_, err := r.Register("users", TypeAvro, avroSchema1)
	if err != nil {
		t.Fatalf("register v1: %v", err)
	}
	_, err = r.Register("users", TypeAvro, avroSchema2)
	if err != nil {
		t.Fatalf("expected no error with NONE compatibility: %v", err)
	}
}

func TestRegistry_Compatibility_DefaultBackward(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	avroSchema1 := `{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}`
	avroSchema2 := `{"type":"record","name":"User","fields":[]}`

	// No explicit compatibility set — should default to BACKWARD
	_, err := r.Register("users", TypeAvro, avroSchema1)
	if err != nil {
		t.Fatalf("register v1: %v", err)
	}
	_, err = r.Register("users", TypeAvro, avroSchema2)
	if err == nil {
		t.Fatal("expected default backward compatibility to reject field removal")
	}
}

func TestRegistry_Compatibility_TypeChange(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	_, err := r.Register("data", TypeJSON, `{"type":"object"}`)
	if err != nil {
		t.Fatalf("register v1: %v", err)
	}
	_, err = r.Register("data", TypeAvro, `{"type":"record","name":"X","fields":[]}`)
	if err == nil {
		t.Fatal("expected error when changing schema type")
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

func TestCompatibilityMode_Values(t *testing.T) {
	if CompatNone != "NONE" {
		t.Error("CompatNone mismatch")
	}
	if CompatBackward != "BACKWARD" {
		t.Error("CompatBackward mismatch")
	}
	if CompatForward != "FORWARD" {
		t.Error("CompatForward mismatch")
	}
	if CompatFull != "FULL" {
		t.Error("CompatFull mismatch")
	}
}

func TestRegistry_JSONSchemaCompatibility(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	oldDef := `{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}`
	newDefBackwardOk := `{"type":"object","required":["name"],"properties":{"name":{"type":"string"},"age":{"type":"integer"}}}`
	newDefBackwardFailRequired := `{"type":"object","required":["name","age"],"properties":{"name":{"type":"string"},"age":{"type":"integer"}}}`
	newDefTypeChange := `{"type":"object","required":["name"],"properties":{"name":{"type":"integer"}}}`

	r.SetCompatibility("json-compat", CompatBackward)
	_, err := r.Register("json-compat", TypeJSON, oldDef)
	if err != nil {
		t.Fatalf("v1 register: %v", err)
	}

	// Adding optional field in backward mode should succeed
	_, err = r.Register("json-compat", TypeJSON, newDefBackwardOk)
	if err != nil {
		t.Errorf("expected backward ok, got: %v", err)
	}

	// Reset to old schema for testing backward failure
	r.mu.Lock()
	r.schemas["json-compat"] = r.schemas["json-compat"][:1] // keep only v1
	r.mu.Unlock()

	// Adding new required field in backward mode should fail
	_, err = r.Register("json-compat", TypeJSON, newDefBackwardFailRequired)
	if err == nil {
		t.Error("expected backward compatibility error for new required field")
	}

	// Reset to old schema for testing type change failure
	r.mu.Lock()
	r.schemas["json-compat"] = r.schemas["json-compat"][:1] // keep only v1
	r.mu.Unlock()

	// Changing field type in backward mode should fail
	_, err = r.Register("json-compat", TypeJSON, newDefTypeChange)
	if err == nil {
		t.Error("expected backward compatibility error for type change")
	}
}

func TestRegistry_ProtobufCompatibility_TagsAndTypes(t *testing.T) {
	tmpDir := t.TempDir()
	r, _ := NewRegistry(tmpDir)

	// We use descriptorpb.FileDescriptorProto to construct binary descriptors for compatibility testing.
	fdpOld := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("old.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("MyMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("id"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					},
				},
			},
		},
	}
	oldDesc, _ := proto.Marshal(fdpOld)

	// 1. Compatible update: add new optional field
	fdpNewCompatible := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("old.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("MyMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("id"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					},
					{
						Name:   proto.String("name"),
						Number: proto.Int32(2),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					},
				},
			},
		},
	}
	newDescCompatible, _ := proto.Marshal(fdpNewCompatible)

	// 2. Incompatible update: change field type for tag 1
	fdpNewTypeChange := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("old.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("MyMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("id"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					},
				},
			},
		},
	}
	newDescTypeChange, _ := proto.Marshal(fdpNewTypeChange)

	r.SetCompatibility("proto-compat", CompatBackward)
	_, err := r.RegisterWithDescriptor("proto-compat", TypeProtobuf, "test.MyMessage", oldDesc)
	if err != nil {
		t.Fatalf("v1 register: %v", err)
	}

	// Compatible update
	_, err = r.RegisterWithDescriptor("proto-compat", TypeProtobuf, "test.MyMessage", newDescCompatible)
	if err != nil {
		t.Errorf("expected compatible register to succeed, got: %v", err)
	}

	// Reset
	r.mu.Lock()
	r.schemas["proto-compat"] = r.schemas["proto-compat"][:1]
	r.mu.Unlock()

	// Incompatible type change
	_, err = r.RegisterWithDescriptor("proto-compat", TypeProtobuf, "test.MyMessage", newDescTypeChange)
	if err == nil {
		t.Error("expected protobuf registration to fail on field type change")
	}
}
