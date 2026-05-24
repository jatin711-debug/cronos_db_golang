package client

import "testing"

func TestJSONCodecRoundTrip(t *testing.T) {
	codec := JSONCodec{}
	input := map[string]any{
		"name": "cronos",
		"v":    float64(1),
	}

	data, err := codec.Encode(input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var out map[string]any
	if err := codec.Decode(data, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["name"] != "cronos" {
		t.Fatalf("unexpected decoded value: %#v", out["name"])
	}
}

func TestDecodePayloadRequiresCodec(t *testing.T) {
	if err := DecodePayload(nil, []byte("{}"), &map[string]any{}); err == nil {
		t.Fatal("expected error when codec is nil")
	}
}
