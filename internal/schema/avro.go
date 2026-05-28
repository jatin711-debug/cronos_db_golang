package schema

import (
	"fmt"

	"github.com/hamba/avro/v2"
)

// validateAvro validates that binary payload conforms to an Avro schema definition.
func validateAvro(schemaDef string, payload []byte) error {
	schema, err := avro.Parse(schemaDef)
	if err != nil {
		return fmt.Errorf("parse avro schema: %w", err)
	}

	// Unmarshal into a generic map — any decode error means payload doesn't match schema
	var dummy map[string]interface{}
	err = avro.Unmarshal(schema, payload, &dummy)
	if err != nil {
		return fmt.Errorf("avro decode mismatch: %w", err)
	}
	return nil
}
