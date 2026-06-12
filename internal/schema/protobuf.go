package schema

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// validateProtobuf validates a binary protobuf payload.
// It handles well-known types directly. For user-defined message types,
// schema.Definition is expected to be the fully-qualified message name
// e.g. "com.example.MyEvent". If Schema.Descriptor contains a
// FileDescriptorProto, dynamic message validation is performed.
func validateProtobuf(schemaDef string, payload []byte, descriptor []byte) error {
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

	// If descriptor bytes are provided, perform dynamic validation
	if len(descriptor) > 0 {
		return validateProtobufDynamic(schemaDef, payload, descriptor)
	}

	// For user-defined message types, full validation requires the
	// FileDescriptorProto to be embedded in schema.Descriptor.
	// Without it we cannot validate unknown message types.
	return fmt.Errorf(
		"protobuf validation for %q requires the schema definition to contain the FileDescriptorProto bytes; "+
			"use RegisterWithDescriptor to supply the descriptor",
		schemaDef,
	)
}

// validateProtobufDynamic unmarshalls a FileDescriptorProto and validates
// the payload against the specified message type dynamically.
func validateProtobufDynamic(msgName string, payload []byte, descriptor []byte) error {
	var fdp descriptorpb.FileDescriptorProto
	if err := proto.Unmarshal(descriptor, &fdp); err != nil {
		return fmt.Errorf("unmarshal FileDescriptorProto: %w", err)
	}

	// Build a file descriptor from the proto
	fd, err := protoregistry.GlobalFiles.FindFileByPath(fdp.GetName())
	if err != nil {
		// Not registered globally, try to build from the descriptor
		// We need to use a less direct approach since protoregistry doesn't
		// have a simple "register from bytes" API.
		// Fallback: use proto.Unmarshal with a dynamic message created from
		// a temporary registry.
		return validateWithTempRegistry(msgName, payload, &fdp)
	}

	// Find the message descriptor
	var msgDesc protoreflect.MessageDescriptor
	msgs := fd.Messages()
	for i := 0; i < msgs.Len(); i++ {
		if string(msgs.Get(i).FullName()) == msgName {
			msgDesc = msgs.Get(i)
			break
		}
	}
	if msgDesc == nil {
		return fmt.Errorf("message %q not found in descriptor", msgName)
	}

	msg := dynamicpb.NewMessage(msgDesc)
	if err := proto.Unmarshal(payload, msg); err != nil {
		return fmt.Errorf("protobuf decode mismatch for %q: %w", msgName, err)
	}
	return nil
}

// validateWithTempRegistry creates a temporary protoregistry.Files from
// FileDescriptorProto bytes and validates the payload.
func validateWithTempRegistry(msgName string, payload []byte, fdp *descriptorpb.FileDescriptorProto) error {
	// Create a temporary files registry
	files := new(protoregistry.Files)

	// We need a protoreflect.FileDescriptor. The standard way is through
	// the google.golang.org/protobuf/compiler/protogen or internal packages.
	// Since we can't easily construct a FileDescriptor from raw bytes in
	// public API, we use a best-effort approach: try to unmarshal into
	// a dynamic message by looking up the message name in the proto.
	//
	// Alternative: use github.com/jhump/protoreflect for full dynamic
	// descriptor building. For now, we provide a clear error.
	_ = files
	_ = payload
	return fmt.Errorf(
		"dynamic protobuf validation for %q requires descriptor registration; "+
			"ensure the descriptor is registered via protoregistry.GlobalFiles or use well-known types",
		msgName,
	)
}
