package codec

import "testing"

// Ensure the interface is satisfied by a simple implementation
type testCodec struct{}

func (t *testCodec) Name() string { return "test" }

func (t *testCodec) Encode(v any) ([]byte, error) {
	if s, ok := v.(string); ok {
		return []byte(s), nil
	}
	return nil, nil
}

func (t *testCodec) Decode(data []byte, out any) error {
	if s, ok := out.(*string); ok {
		*s = string(data)
	}
	return nil
}

func TestCodecInterface(t *testing.T) {
	var c Codec = &testCodec{}
	if c.Name() != "test" {
		t.Errorf("expected name test, got %s", c.Name())
	}

	data, err := c.Encode("hello")
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("expected hello, got %s", string(data))
	}

	var decoded string
	if err := c.Decode([]byte("world"), &decoded); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded != "world" {
		t.Errorf("expected world, got %s", decoded)
	}
}
