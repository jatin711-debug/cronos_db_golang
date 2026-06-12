package schema

import (
	"encoding/json"
	"fmt"

	"github.com/hamba/avro/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
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

type avroField struct {
	name       string
	typ        string
	hasDefault bool
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

	oldFields := extractAvroFields(oldSchema)
	newFields := extractAvroFields(newSchema)

	switch mode {
	case CompatBackward:
		// Reader (old) must read data written with new schema.
		// - All fields in old must exist in new, OR if they are removed in new, they must have a default value in old.
		for name, oldF := range oldFields {
			if _, exists := newFields[name]; !exists {
				if !oldF.hasDefault {
					return fmt.Errorf("backward incompatible: field %q was removed and has no default value in old schema", name)
				}
			}
		}
	case CompatForward:
		// Reader (new) must read data written with old schema.
		// - All fields in new must exist in old, OR if they are added in new, they must have a default value in new.
		for name, newF := range newFields {
			if _, exists := oldFields[name]; !exists {
				if !newF.hasDefault {
					return fmt.Errorf("forward incompatible: field %q was added and has no default value in new schema", name)
				}
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

func extractAvroFields(schema avro.Schema) map[string]avroField {
	fields := make(map[string]avroField)
	if rec, ok := schema.(*avro.RecordSchema); ok {
		for _, f := range rec.Fields() {
			fields[f.Name()] = avroField{
				name:       f.Name(),
				typ:        f.Type().String(),
				hasDefault: f.HasDefault(),
			}
		}
	}
	return fields
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

type jsonSchema struct {
	Type       string                 `json:"type"`
	Required   []string               `json:"required"`
	Properties map[string]*jsonSchema `json:"properties"`
}

// checkJSONCompatibility performs structural checks on JSON definitions.
func checkJSONCompatibility(oldDef, newDef string, mode CompatibilityMode) error {
	var oldS, newS jsonSchema
	if err := json.Unmarshal([]byte(oldDef), &oldS); err != nil {
		return fmt.Errorf("parse old JSON schema: %w", err)
	}
	if err := json.Unmarshal([]byte(newDef), &newS); err != nil {
		return fmt.Errorf("parse new JSON schema: %w", err)
	}

	return checkJSONCompat(&oldS, &newS, mode)
}

func checkJSONCompat(oldS, newS *jsonSchema, mode CompatibilityMode) error {
	if oldS.Type != "" && newS.Type != "" && oldS.Type != newS.Type {
		return fmt.Errorf("cannot change JSON type from %q to %q", oldS.Type, newS.Type)
	}

	switch mode {
	case CompatBackward:
		// Required in old must still be required in new
		for _, req := range oldS.Required {
			if !contains(newS.Required, req) {
				return fmt.Errorf("backward incompatible: required field %q is no longer required in new schema", req)
			}
		}
		// Newly added properties in new must not be required
		for name := range newS.Properties {
			if _, ok := oldS.Properties[name]; !ok {
				if contains(newS.Required, name) {
					return fmt.Errorf("backward incompatible: newly added field %q cannot be required", name)
				}
			}
		}
		// Check nested properties type consistency
		for name, oldProp := range oldS.Properties {
			if newProp, ok := newS.Properties[name]; ok {
				if err := checkJSONCompat(oldProp, newProp, mode); err != nil {
					return fmt.Errorf("field %q: %w", name, err)
				}
			}
		}

	case CompatForward:
		// Required in new must be required in old
		for _, req := range newS.Required {
			if !contains(oldS.Required, req) {
				return fmt.Errorf("forward incompatible: new schema has new required field %q which was not required in old schema", req)
			}
		}
		// Check nested properties type consistency
		for name, oldProp := range oldS.Properties {
			if newProp, ok := newS.Properties[name]; ok {
				if err := checkJSONCompat(oldProp, newProp, mode); err != nil {
					return fmt.Errorf("field %q: %w", name, err)
				}
			}
		}

	case CompatFull:
		if err := checkJSONCompat(oldS, newS, CompatBackward); err != nil {
			return err
		}
		if err := checkJSONCompat(oldS, newS, CompatForward); err != nil {
			return err
		}
	}
	return nil
}

// checkProtobufCompatibility compares protobuf descriptors if available.
func checkProtobufCompatibility(oldSchema, newSchema Schema, mode CompatibilityMode) error {
	if len(oldSchema.Descriptor) == 0 || len(newSchema.Descriptor) == 0 {
		return nil // Skip if descriptors are missing
	}

	var oldFDP, newFDP descriptorpb.FileDescriptorProto
	if err := proto.Unmarshal(oldSchema.Descriptor, &oldFDP); err != nil {
		return fmt.Errorf("parse old protobuf descriptor: %w", err)
	}
	if err := proto.Unmarshal(newSchema.Descriptor, &newFDP); err != nil {
		return fmt.Errorf("parse new protobuf descriptor: %w", err)
	}

	// Map old message types by name
	oldMsgs := make(map[string]*descriptorpb.DescriptorProto)
	for _, m := range oldFDP.MessageType {
		oldMsgs[m.GetName()] = m
	}

	for _, newMsg := range newFDP.MessageType {
		oldMsg, exists := oldMsgs[newMsg.GetName()]
		if !exists {
			continue // New message types are always compatible
		}

		// Map fields by tag number
		oldFieldsByNum := make(map[int32]*descriptorpb.FieldDescriptorProto)
		for _, f := range oldMsg.Field {
			oldFieldsByNum[f.GetNumber()] = f
		}

		newFieldsByNum := make(map[int32]*descriptorpb.FieldDescriptorProto)
		for _, f := range newMsg.Field {
			newFieldsByNum[f.GetNumber()] = f
		}

		// 1. Tag type safety: tag type must match if it exists in both
		for num, newField := range newFieldsByNum {
			if oldField, exists := oldFieldsByNum[num]; exists {
				if oldField.GetType() != newField.GetType() {
					return fmt.Errorf("protobuf incompatible for message %q: field %d (%q) changed type from %s to %s",
						newMsg.GetName(), num, newField.GetName(), oldField.GetType().String(), newField.GetType().String())
				}
			}
		}

		// 2. Compatibility rules based on mode
		switch mode {
		case CompatBackward:
			// Required in old must still exist and be required in new
			for num, oldField := range oldFieldsByNum {
				if oldField.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REQUIRED {
					newField, exists := newFieldsByNum[num]
					if !exists {
						return fmt.Errorf("backward incompatible for message %q: required field %q (tag %d) was removed",
							newMsg.GetName(), oldField.GetName(), num)
					}
					if newField.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REQUIRED {
						return fmt.Errorf("backward incompatible for message %q: required field %q (tag %d) changed label to %s",
							newMsg.GetName(), oldField.GetName(), num, newField.GetLabel().String())
					}
				}
			}

		case CompatForward:
			// Newly added fields in new must not be required
			for num, newField := range newFieldsByNum {
				if _, exists := oldFieldsByNum[num]; !exists {
					if newField.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REQUIRED {
						return fmt.Errorf("forward incompatible for message %q: new field %q (tag %d) cannot be required",
							newMsg.GetName(), newField.GetName(), num)
					}
				}
			}

		case CompatFull:
			if err := checkProtoCompat(oldMsg, newMsg, CompatBackward); err != nil {
				return err
			}
			if err := checkProtoCompat(oldMsg, newMsg, CompatForward); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkProtoCompat(oldMsg, newMsg *descriptorpb.DescriptorProto, mode CompatibilityMode) error {
	oldFieldsByNum := make(map[int32]*descriptorpb.FieldDescriptorProto)
	for _, f := range oldMsg.Field {
		oldFieldsByNum[f.GetNumber()] = f
	}

	newFieldsByNum := make(map[int32]*descriptorpb.FieldDescriptorProto)
	for _, f := range newMsg.Field {
		newFieldsByNum[f.GetNumber()] = f
	}

	switch mode {
	case CompatBackward:
		for num, oldField := range oldFieldsByNum {
			if oldField.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REQUIRED {
				newField, exists := newFieldsByNum[num]
				if !exists {
					return fmt.Errorf("backward incompatible for message %q: required field %q (tag %d) was removed",
						newMsg.GetName(), oldField.GetName(), num)
				}
				if newField.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REQUIRED {
					return fmt.Errorf("backward incompatible for message %q: required field %q (tag %d) changed label to %s",
						newMsg.GetName(), oldField.GetName(), num, newField.GetLabel().String())
				}
			}
		}

	case CompatForward:
		for num, newField := range newFieldsByNum {
			if _, exists := oldFieldsByNum[num]; !exists {
				if newField.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REQUIRED {
					return fmt.Errorf("forward incompatible for message %q: new field %q (tag %d) cannot be required",
						newMsg.GetName(), newField.GetName(), num)
				}
			}
		}
	}
	return nil
}
