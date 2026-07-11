package schema

import (
	"encoding/json"
	"fmt"
	"math"
)

// validateJSONSchema validates that a JSON payload conforms to a simple JSON
// Schema definition. It supports the subset used by the registry:
//   - type: object, array, string, number, integer, boolean, null
//   - properties (for objects)
//   - items (for arrays)
//
// Unknown schema keywords are ignored so the registry stays permissive by
// default.
func validateJSONSchema(schemaDef string, payload []byte) error {
	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(schemaDef), &schema); err != nil {
		return fmt.Errorf("parse json schema: %w", err)
	}

	var value interface{}
	if err := json.Unmarshal(payload, &value); err != nil {
		return fmt.Errorf("invalid json payload: %w", err)
	}

	return validateJSONValue(schema, value)
}

func validateJSONValue(schema map[string]interface{}, value interface{}) error {
	if schema == nil {
		return nil
	}

	if typ, ok := schema["type"].(string); ok && typ != "" {
		if err := checkJSONType(typ, value); err != nil {
			return err
		}
	}

	typ, _ := schema["type"].(string)
	switch typ {
	case "object":
		obj, ok := value.(map[string]interface{})
		if !ok {
			return fmt.Errorf("expected object, got %T", value)
		}
		if props, ok := schema["properties"].(map[string]interface{}); ok {
			for name, propSchema := range props {
				propMap, ok := propSchema.(map[string]interface{})
				if !ok {
					continue
				}
				if v, exists := obj[name]; exists {
					if err := validateJSONValue(propMap, v); err != nil {
						return fmt.Errorf("property %q: %w", name, err)
					}
				}
			}
		}
	case "array":
		arr, ok := value.([]interface{})
		if !ok {
			return fmt.Errorf("expected array, got %T", value)
		}
		if itemsSchema, ok := schema["items"].(map[string]interface{}); ok {
			for i, item := range arr {
				if err := validateJSONValue(itemsSchema, item); err != nil {
					return fmt.Errorf("item %d: %w", i, err)
				}
			}
		}
	}

	return nil
}

func checkJSONType(expected string, value interface{}) error {
	switch expected {
	case "object":
		if _, ok := value.(map[string]interface{}); !ok {
			return fmt.Errorf("expected object, got %T", value)
		}
	case "array":
		if _, ok := value.([]interface{}); !ok {
			return fmt.Errorf("expected array, got %T", value)
		}
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
	case "number":
		if !isJSONNumber(value) {
			return fmt.Errorf("expected number, got %T", value)
		}
	case "integer":
		if !isJSONInteger(value) {
			return fmt.Errorf("expected integer, got %T", value)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected boolean, got %T", value)
		}
	case "null":
		if value != nil {
			return fmt.Errorf("expected null, got %T", value)
		}
	}
	return nil
}

func isJSONNumber(value interface{}) bool {
	switch value.(type) {
	case float64, int, int64, int32, uint, uint64:
		return true
	}
	return false
}

func isJSONInteger(value interface{}) bool {
	switch v := value.(type) {
	case int, int64, int32, uint, uint64:
		return true
	case float64:
		_, frac := math.Modf(v)
		return frac == 0
	}
	return false
}
