package schema

import (
	"fmt"

	"github.com/iskorotkov/avro/v2"
)

// Publish limits payloads to 4 MiB. Bound decoder allocations as well: Avro
// block counts come from the payload and need independent array/map limits.
var avroValidationCodec = avro.Config{
	MaxByteSliceSize:  4 << 20,
	MaxSliceAllocSize: 65_536,
	MaxMapAllocSize:   65_536,
}.Freeze()

// validateAvro validates that binary payload conforms to an Avro schema definition.
func validateAvro(schemaDef string, payload []byte) error {
	schema, err := avro.Parse(schemaDef)
	if err != nil {
		return fmt.Errorf("parse avro schema: %w", err)
	}

	// Unmarshal into a generic map — any decode error means payload doesn't match schema
	var dummy map[string]interface{}
	err = avroValidationCodec.Unmarshal(schema, payload, &dummy)
	if err != nil {
		return fmt.Errorf("avro decode mismatch: %w", err)
	}
	return nil
}
