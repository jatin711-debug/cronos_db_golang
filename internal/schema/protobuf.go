package schema

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// validateProtobuf validates a binary protobuf payload.
// It handles well-known types directly. For user-defined message types,
// schema.Definition is expected to be the fully-qualified message name
// e.g. "com.example.MyEvent". Without the corresponding FileDescriptor,
// full validation cannot be performed, so we return an informative error.
func validateProtobuf(schemaDef string, payload []byte) error {
	// Try well-known types first — these are always available
	switch schemaDef {
	case "google.protobuf.Timestamp":
		var ts timestamppb.Timestamp
		if err := proto.Unmarshal(payload, &ts); err != nil {
			return fmt.Errorf("invalid Timestamp: %w", err)
		}
		return nil

	case "google.protobuf.Duration":
		var d durationpb.Duration
		if err := proto.Unmarshal(payload, &d); err != nil {
			return fmt.Errorf("invalid Duration: %w", err)
		}
		return nil

	case "google.protobuf.Any":
		var a anypb.Any
		if err := proto.Unmarshal(payload, &a); err != nil {
			return fmt.Errorf("invalid Any: %w", err)
		}
		return nil

	case "google.protobuf.Struct":
		var s structpb.Struct
		if err := proto.Unmarshal(payload, &s); err != nil {
			return fmt.Errorf("invalid Struct: %w", err)
		}
		return nil
	}

	// For user-defined message types, full validation requires the
	// FileDescriptorProto to be embedded in schema.Definition.
	// Without it we cannot validate unknown message types.
	return fmt.Errorf(
		"protobuf validation for %q requires the schema definition to contain the FileDescriptorProto bytes; note: validation is skipped — implement schema registry with embedded descriptors for full %s support",
		schemaDef, schemaDef,
	)
}
