package schema

import (
	"encoding/binary"
	"strings"
	"testing"
)

func TestAvroValidationRejectsOversizedMapBlock(t *testing.T) {
	const schema = `{"type":"record","name":"Payload","fields":[{"name":"attrs","type":{"type":"map","values":"null"}}]}`
	var block [binary.MaxVarintLen64]byte
	n := binary.PutVarint(block[:], 65_537)
	err := validateAvro(schema, block[:n])
	if err == nil || !strings.Contains(err.Error(), "map") {
		t.Fatalf("oversized map block should be rejected by the decoder cap: %v", err)
	}
}
