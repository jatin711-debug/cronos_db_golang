package schema

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Type identifies the schema format.
type Type string

const (
	TypeJSON     Type = "json"
	TypeAvro     Type = "avro"
	TypeProtobuf Type = "protobuf"
)

// Schema represents a versioned schema for a topic.
type Schema struct {
	Topic      string `json:"topic"`
	Version    int    `json:"version"`
	Type       Type   `json:"type"`
	Definition string `json:"definition"`
	Hash       uint64 `json:"hash"`
	Descriptor []byte `json:"descriptor,omitempty"` // FileDescriptorProto bytes for protobuf
}

// CompatibilityMode defines schema evolution policy.
type CompatibilityMode string

const (
	CompatNone     CompatibilityMode = "NONE"
	CompatBackward CompatibilityMode = "BACKWARD"
	CompatForward  CompatibilityMode = "FORWARD"
	CompatFull     CompatibilityMode = "FULL"
)

// Registry manages topic schemas with versioning.
type Registry struct {
	mu          sync.RWMutex
	schemas     map[string][]Schema          // topic -> sorted by version ascending
	compatModes map[string]CompatibilityMode // topic -> compatibility mode
	dir         string
}

// NewRegistry creates a schema registry.
func NewRegistry(dataDir string) (*Registry, error) {
	dir := filepath.Join(dataDir, "schemas")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create schema dir: %w", err)
	}
	r := &Registry{
		schemas:     make(map[string][]Schema),
		compatModes: make(map[string]CompatibilityMode),
		dir:         dir,
	}
	if err := r.Load(); err != nil {
		// Non-fatal: registry starts empty if load fails
		_ = err
	}
	return r, nil
}

// Register adds a new schema version for a topic.
func (r *Registry) Register(topic string, schemaType Type, definition string) (int, error) {
	return r.RegisterWithDescriptor(topic, schemaType, definition, nil)
}

// RegisterWithDescriptor adds a new schema version with an optional protobuf descriptor.
func (r *Registry) RegisterWithDescriptor(topic string, schemaType Type, definition string, descriptor []byte) (int, error) {
	h := fnv.New64a()
	h.Write([]byte(definition))
	if len(descriptor) > 0 {
		h.Write(descriptor)
	}
	hash := h.Sum64()

	r.mu.Lock()
	defer r.mu.Unlock()

	versions := r.schemas[topic]
	newVersion := 1
	if len(versions) > 0 {
		last := versions[len(versions)-1]
		if last.Hash == hash {
			return last.Version, nil // Idempotent
		}
		// Compatibility check
		mode := r.compatModes[topic]
		if mode == "" {
			mode = CompatBackward
		}
		if mode != CompatNone {
			if err := checkCompatibility(last, Schema{Topic: topic, Type: schemaType, Definition: definition, Descriptor: descriptor}, mode); err != nil {
				return 0, fmt.Errorf("incompatible schema: %w", err)
			}
		}
		newVersion = last.Version + 1
	}

	schema := Schema{
		Topic:      topic,
		Version:    newVersion,
		Type:       schemaType,
		Definition: definition,
		Hash:       hash,
		Descriptor: descriptor,
	}
	r.schemas[topic] = append(versions, schema)

	// Persist to disk
	if err := r.persist(schema); err != nil {
		return 0, err
	}
	return newVersion, nil
}

// SetCompatibility sets the compatibility mode for a topic.
func (r *Registry) SetCompatibility(topic string, mode CompatibilityMode) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.compatModes[topic] = mode
}

// Get returns the latest schema for a topic.
func (r *Registry) Get(topic string) (Schema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions, ok := r.schemas[topic]
	if !ok || len(versions) == 0 {
		return Schema{}, false
	}
	return versions[len(versions)-1], true
}

// GetVersion returns a specific schema version.
func (r *Registry) GetVersion(topic string, version int) (Schema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions, ok := r.schemas[topic]
	if !ok {
		return Schema{}, false
	}
	for _, s := range versions {
		if s.Version == version {
			return s, true
		}
	}
	return Schema{}, false
}

// Validate checks if a payload conforms to the latest schema.
// For JSON schemas, performs basic structural validation.
// For AVRO schemas, validates binary-encoded data against the schema.
// For PROTOBUF schemas, validates well-known types or notes full validation requires descriptor.
func (r *Registry) Validate(topic string, payload []byte) error {
	schema, ok := r.Get(topic)
	if !ok {
		return nil // No schema registered = allow all
	}

	switch schema.Type {
	case TypeJSON:
		return validateJSONSchema(schema.Definition, payload)
	case TypeAvro:
		return validateAvro(schema.Definition, payload)
	case TypeProtobuf:
		return validateProtobuf(schema.Definition, payload, schema.Descriptor)
	default:
		return nil // Unknown schema type — allow by default
	}
}

// Load restores schemas from disk.
func (r *Registry) Load() error {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return fmt.Errorf("read schema dir: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(r.dir, entry.Name()))
		if err != nil {
			continue
		}
		var s Schema
		if err := json.Unmarshal(data, &s); err != nil {
			continue
		}
		r.schemas[s.Topic] = append(r.schemas[s.Topic], s)
	}
	// Sort versions ascending for each topic
	for topic, versions := range r.schemas {
		sort.Slice(versions, func(i, j int) bool {
			return versions[i].Version < versions[j].Version
		})
		r.schemas[topic] = versions
	}
	return nil
}

func (r *Registry) persist(s Schema) error {
	path := filepath.Join(r.dir, fmt.Sprintf("%s_v%d.json", s.Topic, s.Version))
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
