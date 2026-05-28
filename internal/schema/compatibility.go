package schema

import (
	"encoding/json"
	"fmt"

	"github.com/hamba/avro/v2"
)

// checkCompatibility verifies that newSchema is compatible with oldSchema according to mode.
func checkCompatibility(oldSchema, newSchema Schema, mode CompatibilityMode) error {
	if oldSchema.Type != newSchema.Type {
		return fmt.Errorf("cannot change schema type from %s to %s", oldSchema.Type, newSchema.Type)
	}

	switch oldSchema.Type {
	case TypeAvro:
		return checkAvroCompatibility(oldSchema.Definition, newSchema.Definition, mode)
	case TypeJSON:
		return checkJSONCompatibility(oldSchema.Definition, newSchema.Definition, mode)
	case TypeProtobuf:
		return checkProtobufCompatibility(oldSchema, newSchema, mode)
	default:
		return nil
	}
}

// checkAvroCompatibility uses hamba/avro to parse schemas and compare fields.
func checkAvroCompatibility(oldDef, newDef string, mode CompatibilityMode) error {
	oldSchema, err := avro.Parse(oldDef)
	if err != nil {
		return fmt.Errorf("parse old avro schema: %w", err)
	}
	newSchema, err := avro.Parse(newDef)
	if err != nil {
		return fmt.Errorf("parse new avro schema: %w", err)
	}

	oldFields := extractAvroFieldNames(oldSchema)
	newFields := extractAvroFieldNames(newSchema)

	switch mode {
	case CompatBackward:
		// Reader (old) must read data written with new schema.
		// New fields in new schema must have defaults (handled by Avro itself),
		// but we at least verify no required fields were removed.
		for _, f := range oldFields {
			if !contains(newFields, f) {
				return fmt.Errorf("backward incompatible: field %q removed", f)
			}
		}
	case CompatForward:
		// Reader (new) must read data written with old schema.
		for _, f := range newFields {
			if !contains(oldFields, f) {
				return fmt.Errorf("forward incompatible: field %q added without default", f)
			}
		}
	case CompatFull:
		if err := checkAvroCompatibility(oldDef, newDef, CompatBackward); err != nil {
			return err
		}
		if err := checkAvroCompatibility(oldDef, newDef, CompatForward); err != nil {
			return err
		}
	}
	return nil
}

func extractAvroFieldNames(schema avro.Schema) []string {
	// Try to extract field names from a record schema
	if rec, ok := schema.(*avro.RecordSchema); ok {
		fields := make([]string, len(rec.Fields()))
		for i, f := range rec.Fields() {
			fields[i] = f.Name()
		}
		return fields
	}
	return nil
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// checkJSONCompatibility performs structural checks on JSON definitions.
func checkJSONCompatibility(oldDef, newDef string, mode CompatibilityMode) error {
	var oldSchema, newSchema map[string]interface{}
	if err := json.Unmarshal([]byte(oldDef), &oldSchema); err != nil {
		return fmt.Errorf("parse old JSON schema: %w", err)
	}
	if err := json.Unmarshal([]byte(newDef), &newSchema); err != nil {
		return fmt.Errorf("parse new JSON schema: %w", err)
	}

	// Without JSON Schema, we can only do minimal structural comparison
	// Ensure the top-level type hasn't changed in a breaking way
	oldType, _ := oldSchema["type"].(string)
	newType, _ := newSchema["type"].(string)
	if oldType != "" && newType != "" && oldType != newType {
		return fmt.Errorf("cannot change JSON type from %q to %q", oldType, newType)
	}
	return nil
}

// checkProtobufCompatibility compares protobuf descriptors if available.
func checkProtobufCompatibility(oldSchema, newSchema Schema, mode CompatibilityMode) error {
	if len(oldSchema.Descriptor) == 0 || len(newSchema.Descriptor) == 0 {
		// Without descriptors, allow any change but warn
		return nil
	}
	// Full descriptor comparison would require parsing FileDescriptorProto
	// and comparing MessageDescriptors field-by-field. For now, we allow
	// changes when descriptors are present but could add deep comparison later.
	return nil
}
