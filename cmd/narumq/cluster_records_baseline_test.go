package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func baselineUnknownFieldPath(m protoreflect.Message) string {
	if len(m.GetUnknown()) != 0 {
		return string(m.Descriptor().FullName())
	}
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
			continue
		}
		if field.IsList() {
			list := m.Get(field).List()
			for j := 0; j < list.Len(); j++ {
				if path := baselineUnknownFieldPath(list.Get(j).Message()); path != "" {
					return fmt.Sprintf("%s.%s[%d] -> %s", m.Descriptor().FullName(), field.Name(), j, path)
				}
			}
		} else if m.Has(field) {
			if path := baselineUnknownFieldPath(m.Get(field).Message()); path != "" {
				return fmt.Sprintf("%s.%s -> %s", m.Descriptor().FullName(), field.Name(), path)
			}
		}
	}
	return ""
}

// Traceability: CLU-11; docs/specs/cluster-session-and-delivery.md §8, §11.29.
func TestInactiveFeatureRecordsDecodeWithBaselineSchema(t *testing.T) {
	wire, err := os.ReadFile(filepath.Join("..", "..", "internal", "adapters", "raft", "records", "testdata", "replicated-state-v1", "baseline-schema-1.0.binpb"))
	if err != nil {
		t.Fatal(err)
	}
	descriptor := new(descriptorpb.FileDescriptorProto)
	if err := proto.Unmarshal(wire, descriptor); err != nil {
		t.Fatal(err)
	}
	baseline, err := protodesc.NewFile(descriptor, nil)
	if err != nil {
		t.Fatal(err)
	}
	check := func(t *testing.T, wire []byte, name protoreflect.Name) {
		t.Helper()
		message := dynamicpb.NewMessage(baseline.Messages().ByName(name))
		if err := proto.Unmarshal(wire, message); err != nil {
			t.Fatal(err)
		}
		if path := baselineUnknownFieldPath(message.ProtoReflect()); path != "" {
			t.Fatalf("inactive-feature fixture has unknown fields at %s", path)
		}
	}
	for index, command := range bridgeTypedCommandFixtures() {
		t.Run(fmt.Sprintf("command_%d", index), func(t *testing.T) {
			wire, err := encodeMembershipCommand(command)
			if err != nil {
				t.Fatalf("encode fixture %d: %v", index, err)
			}
			check(t, wire, "CommandEnvelope")
		})
	}
	t.Run("snapshot", func(t *testing.T) {
		snapshot := bridgeSnapshotFixture()
		snapshot.SessionMetadata = nil
		wire, err := encodeMembershipSnapshot(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		check(t, wire, "SnapshotEnvelope")
	})
}
