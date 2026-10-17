package records

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func loadBaselineSchema(t *testing.T) protoreflect.FileDescriptor {
	t.Helper()
	wire, err := os.ReadFile(filepath.Join("testdata", "replicated-state-v1", "baseline-schema-1.0.binpb"))
	if err != nil {
		t.Fatal(err)
	}
	descriptor := new(descriptorpb.FileDescriptorProto)
	if err := proto.Unmarshal(wire, descriptor); err != nil {
		t.Fatal(err)
	}
	file, err := protodesc.NewFile(descriptor, nil)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

// Traceability: CLU-11; docs/specs/cluster-session-and-delivery.md §8, §11.29.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-208, cluster-session-and-delivery.md:CLU-SCN-224, cluster-session-and-delivery.md:CLU-SCN-236
func TestReplicatedStateSchemaPreservesBaseline(t *testing.T) {
	baseline := loadBaselineSchema(t)
	current := File_docs_contracts_replicated_state_v1_proto
	descriptors := make(map[protoreflect.FullName]protoreflect.Descriptor)
	var collect func(protoreflect.MessageDescriptors, protoreflect.EnumDescriptors)
	collect = func(messages protoreflect.MessageDescriptors, enums protoreflect.EnumDescriptors) {
		for i := 0; i < enums.Len(); i++ {
			e := enums.Get(i)
			descriptors[e.FullName()] = e
		}
		for i := 0; i < messages.Len(); i++ {
			m := messages.Get(i)
			descriptors[m.FullName()] = m
			collect(m.Messages(), m.Enums())
		}
	}
	collect(current.Messages(), current.Enums())
	var check func(protoreflect.MessageDescriptors, protoreflect.EnumDescriptors)
	check = func(messages protoreflect.MessageDescriptors, enums protoreflect.EnumDescriptors) {
		for i := 0; i < enums.Len(); i++ {
			before := enums.Get(i)
			after, ok := descriptors[before.FullName()].(protoreflect.EnumDescriptor)
			if !ok {
				t.Errorf("missing enum %s", before.FullName())
				continue
			}
			for j := 0; j < before.Values().Len(); j++ {
				value := before.Values().Get(j)
				got := after.Values().ByName(value.Name())
				if got == nil || got.Number() != value.Number() {
					t.Errorf("changed enum value %s", value.FullName())
				}
			}
			for j := 0; j < before.ReservedNames().Len(); j++ {
				name := before.ReservedNames().Get(j)
				if !after.ReservedNames().Has(name) {
					t.Errorf("lost reserved enum name %s.%s", before.FullName(), name)
				}
			}
			for j := 0; j < before.ReservedRanges().Len(); j++ {
				r := before.ReservedRanges().Get(j)
				found := false
				for k := 0; k < after.ReservedRanges().Len(); k++ {
					a := after.ReservedRanges().Get(k)
					if a[0] <= r[0] && a[1] >= r[1] {
						found = true
					}
				}
				if !found {
					t.Errorf("lost reserved enum range %s %v", before.FullName(), r)
				}
			}
		}
		for i := 0; i < messages.Len(); i++ {
			before := messages.Get(i)
			after, ok := descriptors[before.FullName()].(protoreflect.MessageDescriptor)
			if !ok {
				t.Errorf("missing message %s", before.FullName())
				continue
			}
			var maximum protoreflect.FieldNumber
			for j := 0; j < before.Fields().Len(); j++ {
				field := before.Fields().Get(j)
				if field.Number() > maximum {
					maximum = field.Number()
				}
				got := after.Fields().ByName(field.Name())
				if got == nil {
					t.Errorf("missing field %s", field.FullName())
					continue
				}
				if got.Number() != field.Number() || got.Kind() != field.Kind() || got.Cardinality() != field.Cardinality() || got.HasPresence() != field.HasPresence() || got.IsMap() != field.IsMap() {
					t.Errorf("changed field %s", field.FullName())
				}
				if field.Message() != nil && (got.Message() == nil || got.Message().FullName() != field.Message().FullName()) {
					t.Errorf("changed message type %s", field.FullName())
				}
				if field.Enum() != nil && (got.Enum() == nil || got.Enum().FullName() != field.Enum().FullName()) {
					t.Errorf("changed enum type %s", field.FullName())
				}
				if field.ContainingOneof() != nil && (got.ContainingOneof() == nil || got.ContainingOneof().FullName() != field.ContainingOneof().FullName()) {
					t.Errorf("changed oneof %s", field.FullName())
				}
			}
			for j := 0; j < after.Fields().Len(); j++ {
				field := after.Fields().Get(j)
				if before.Fields().ByNumber(field.Number()) == nil && field.Number() <= maximum {
					t.Errorf("new field %s number %d must exceed baseline maximum %d", field.FullName(), field.Number(), maximum)
				}
			}
			for j := 0; j < before.ReservedNames().Len(); j++ {
				name := before.ReservedNames().Get(j)
				if !after.ReservedNames().Has(name) {
					t.Errorf("lost reserved name %s.%s", before.FullName(), name)
				}
			}
			for j := 0; j < before.ReservedRanges().Len(); j++ {
				r := before.ReservedRanges().Get(j)
				found := false
				for k := 0; k < after.ReservedRanges().Len(); k++ {
					a := after.ReservedRanges().Get(k)
					if a[0] <= r[0] && a[1] >= r[1] {
						found = true
					}
				}
				if !found {
					t.Errorf("lost reserved range %s %v", before.FullName(), r)
				}
			}
			check(before.Messages(), before.Enums())
		}
	}
	check(baseline.Messages(), baseline.Enums())
}

// Traceability: CLU-11; docs/specs/cluster-session-and-delivery.md §8, §11.29.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-208
func TestProducerVectorsDecodeWithBaselineSchema(t *testing.T) {
	baseline := loadBaselineSchema(t)
	names := []string{"stage_join", "ownership_acquire", "subscription_add", "subscription_remove", "delivery_publish", "empty_snapshot"}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			envelope := protoreflect.Name("CommandEnvelope")
			if name == "empty_snapshot" {
				envelope = "SnapshotEnvelope"
			}
			message := dynamicpb.NewMessage(baseline.Messages().ByName(envelope))
			if err := proto.Unmarshal(readStateVector(t, name), message); err != nil {
				t.Fatal(err)
			}
			if err := rejectUnknown(message.ProtoReflect()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
