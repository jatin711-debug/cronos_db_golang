package schema

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
)

// Type identifies the schema format.
type Type string

const (
	TypeJSON   Type = "json"
	TypeAvro   Type = "avro"
	TypeProtobuf Type = "protobuf"
)

// Schema represents a versioned schema for a topic.
type Schema struct {
	Topic     string `json:"topic"`
	Version   int    `json:"version"`
	Type      Type   `json:"type"`
	Definition string `json:"definition"`
	Hash      uint64 `json:"hash"`
}

// Registry manages topic schemas with versioning.
type Registry struct {
	mu      sync.RWMutex
	schemas map[string][]Schema // topic -> sorted by version ascending
	dir     string
}

// NewRegistry creates a schema registry.
func NewRegistry(dataDir string) (*Registry, error) {
	dir := filepath.Join(dataDir, "schemas")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create schema dir: %w", err)
	}
	return &Registry{
		schemas: make(map[string][]Schema),
		dir:     dir,
	}, nil
}

// Register adds a new schema version for a topic.
func (r *Registry) Register(topic string, schemaType Type, definition string) (int, error) {
	h := fnv.New64a()
	h.Write([]byte(definition))
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
		newVersion = last.Version + 1
	}

	schema := Schema{
		Topic:      topic,
		Version:    newVersion,
		Type:       schemaType,
		Definition: definition,
		Hash:       hash,
	}
	r.schemas[topic] = append(versions, schema)

	// Persist to disk
	if err := r.persist(schema); err != nil {
		return 0, err
	}
	return newVersion, nil
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
func (r *Registry) Validate(topic string, payload []byte) error {
	schema, ok := r.Get(topic)
	if !ok {
		return nil // No schema registered = allow all
	}

	switch schema.Type {
	case TypeJSON:
		return validateJSON(payload)
	default:
		return nil // TODO: avro/protobuf validation
	}
}

func validateJSON(payload []byte) error {
	var v interface{}
	return json.Unmarshal(payload, &v)
}

func (r *Registry) persist(s Schema) error {
	path := filepath.Join(r.dir, fmt.Sprintf("%s_v%d.json", s.Topic, s.Version))
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
