package client

import (
	"encoding/json"
	"fmt"
)

const (
	// MetaCodecNameKey stores the codec name in event metadata when encoding Values.
	MetaCodecNameKey = "client.codec"
)

// Codec is a pluggable payload serialization strategy used by Producer/Consumer helpers.
type Codec interface {
	// Name returns a stable codec identifier stored in event metadata.
	Name() string
	// Encode serializes v into payload bytes.
	Encode(v any) ([]byte, error)
	// Decode deserializes data into out (out must be a non-nil pointer).
	Decode(data []byte, out any) error
}

// JSONCodec encodes and decodes values as JSON.
type JSONCodec struct{}

// Name implements Codec.
func (JSONCodec) Name() string { return "json" }

// Encode implements Codec.
func (JSONCodec) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Decode implements Codec.
func (JSONCodec) Decode(data []byte, out any) error {
	return json.Unmarshal(data, out)
}

// encodeWithCodec encodes v and returns payload bytes plus the codec name.
func encodeWithCodec(codec Codec, v any) ([]byte, string, error) {
	if codec == nil {
		return nil, "", fmt.Errorf("codec is required for value encoding")
	}
	payload, err := codec.Encode(v)
	if err != nil {
		return nil, "", err
	}
	return payload, codec.Name(), nil
}

// DecodePayload decodes data into out using the provided codec.
func DecodePayload(codec Codec, data []byte, out any) error {
	if codec == nil {
		return fmt.Errorf("codec is required")
	}
	if out == nil {
		return fmt.Errorf("decode output target is required")
	}
	return codec.Decode(data, out)
}
