package codec

// Codec allows pluggable payload serialization.
type Codec interface {
	Name() string
	Encode(v any) ([]byte, error)
	Decode(data []byte, out any) error
}
