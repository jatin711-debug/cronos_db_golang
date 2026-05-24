package client

import (
	"encoding/json"
	"fmt"
)

const (
	// MetaCodecNameKey stores codec name in event metadata.
	MetaCodecNameKey = "client.codec"
)

// Codec allows pluggable payload serialization.
type Codec interface {
	Name() string
	Encode(v any) ([]byte, error)
	Decode(data []byte, out any) error
}

// JSONCodec encodes values as JSON.
type JSONCodec struct{}

func (JSONCodec) Name() string { return "json" }

func (JSONCodec) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (JSONCodec) Decode(data []byte, out any) error {
	return json.Unmarshal(data, out)
}

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

// DecodePayload decodes a payload into out with the provided codec.
func DecodePayload(codec Codec, data []byte, out any) error {
	if codec == nil {
		return fmt.Errorf("codec is required")
	}
	if out == nil {
		return fmt.Errorf("decode output target is required")
	}
	return codec.Decode(data, out)
}
