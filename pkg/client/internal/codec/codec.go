// Package codec defines a minimal payload serialization interface for the client SDK.
package codec

// Codec allows pluggable payload serialization for producer values and consumer decode helpers.
type Codec interface {
	// Name returns a stable codec identifier (e.g. "json").
	Name() string
	// Encode serializes v into payload bytes.
	Encode(v any) ([]byte, error)
	// Decode deserializes data into out (out must be a non-nil pointer).
	Decode(data []byte, out any) error
}
