package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"narumq/internal/application/membership"
)

func TestClusterFaultControlReadsCrashAndIsolation(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	path := filepath.Join(t.TempDir(), "fault.json")
	control := newClusterFaultControl(path, logger)
	if !control.enabled() {
		t.Fatal("control with a path is disabled")
	}
	if got := control.crash(); got != clusterFailpointNone {
		t.Fatalf("missing control crash = %q, want none", got)
	}
	if control.isolates(2) {
		t.Fatal("missing control isolates peer 2")
	}

	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if got := control.crash(); got != clusterFailpointNone || control.isolates(2) {
		t.Fatalf("empty control = (%q, isolates peer 2=%t), want (none, false)", got, control.isolates(2))
	}

	for _, data := range []string{
		"{",
		`{"crash":"unknown-point","isolatePeers":[2]}`,
	} {
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		if got := control.crash(); got != clusterFailpointNone {
			t.Fatalf("invalid control %q crash = %q, want none", data, got)
		}
		if control.isolates(2) {
			t.Fatalf("invalid control %q isolates peer 2", data)
		}
	}

	valid := `{"crash":"delivery-wal-before-write","isolatePeers":[2,9]}`
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := control.crash(); got != failpointDeliveryWALBeforeWrite {
		t.Fatalf("valid control crash = %q, want %q", got, failpointDeliveryWALBeforeWrite)
	}
	if !control.isolates(2) || !control.isolates(9) || control.isolates(3) {
		t.Fatal("valid control isolation list was not applied")
	}

	if err := os.WriteFile(path, []byte(`{"isolatePeers":[3]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := control.crash(); got != clusterFailpointNone || control.isolates(2) || !control.isolates(3) {
		t.Fatalf("updated control = (crash=%q, isolates 2=%t, isolates 3=%t), want (none, false, true)",
			got, control.isolates(2), control.isolates(3))
	}

	disabled := newClusterFaultControl("", logger)
	if disabled.enabled() || disabled.crash() != clusterFailpointNone || disabled.isolates(2) {
		t.Fatal("empty control path did not remain disabled")
	}
}

func TestClusterFaultControlHoldsCommandKinds(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	path := filepath.Join(t.TempDir(), "fault.json")
	control := newClusterFaultControl(path, logger)

	if err := os.WriteFile(path, []byte(`{"holdCommands":["ownership","delivery"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if !control.holds(clusterHeldOwnership) || !control.holds(clusterHeldDelivery) || control.holds(clusterHeldSubscription) {
		t.Fatal("valid hold command kinds were not applied")
	}

	if err := os.WriteFile(path, []byte(`{"holdCommands":["retained"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if control.holds(clusterHeldOwnership) || control.holds(clusterHeldSubscription) || control.holds(clusterHeldDelivery) ||
		control.crash() != clusterFailpointNone {
		t.Fatal("invalid held command kind did not disable the full control")
	}

	tests := []struct {
		name    string
		command membership.CommandDTO
		kind    clusterHeldCommandKind
		label   string
	}{
		{
			name: "ownership",
			command: membership.CommandDTO{Ownership: &membership.OwnershipChangeDTO{
				ClientID: "client", ExpectedEpoch: 0,
			}},
			kind:  clusterHeldOwnership,
			label: "kind=ownership client=client expected_epoch=0",
		},
		{
			name: "subscription",
			command: membership.CommandDTO{Subscription: &membership.SubscriptionChangeDTO{
				ClientID: "client", Filter: "events/#", Epoch: 4,
			}},
			kind:  clusterHeldSubscription,
			label: "kind=subscription client=client filter=events/# epoch=4",
		},
		{
			name: "delivery",
			command: membership.CommandDTO{Delivery: &membership.DeliveryChangeDTO{
				ClientID: "client", Action: membership.DeliveryActionComplete, Epoch: 5,
			}},
			kind:  clusterHeldDelivery,
			label: "kind=delivery client=client action=6 epoch=5",
		},
		{
			name:    "retained",
			command: membership.CommandDTO{Retained: &membership.RetainedChangeDTO{}},
			label:   "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kind, label := clusterHeldCommandLabel(test.command)
			if kind != test.kind || label != test.label {
				t.Fatalf("clusterHeldCommandLabel() = (%q, %q), want (%q, %q)", kind, label, test.kind, test.label)
			}
		})
	}
}

func TestContainsDeliveryAckCommand(t *testing.T) {
	encode := func(action membership.DeliveryAction) []byte {
		t.Helper()
		delivery := &membership.DeliveryChangeDTO{
			TenantID: "tenant", ClientID: "publisher", Action: action, ProposedAtUnixMillis: 1,
		}
		switch action {
		case membership.DeliveryActionPublish:
			delivery.Topic = "events/test"
			delivery.Payload = []byte("x")
			delivery.PublishQoS = 1
			delivery.Targets = []membership.DeliveryTargetDTO{{ClientID: "subscriber"}}
		case membership.DeliveryActionReceive:
			delivery.OwnerNodeID = 1
			delivery.Epoch = 1
			delivery.PacketID = 7
			delivery.Topic = "events/test"
			delivery.Payload = []byte("x")
		case membership.DeliveryActionSend:
			delivery.OwnerNodeID = 1
			delivery.Epoch = 1
			delivery.Sequence = 1
			delivery.PacketID = 7
		case membership.DeliveryActionAttach:
			delivery.OwnerNodeID = 1
			delivery.Epoch = 1
		}
		data, err := encodeMembershipCommand(membership.CommandDTO{
			SchemaMajor: membership.SchemaMajor,
			SchemaMinor: membership.SchemaMinor,
			OperationID: "00000000-0000-4000-8000-000000000001",
			Delivery:    delivery,
		})
		if err != nil {
			t.Fatalf("encode action %d: %v", action, err)
		}
		return data
	}

	for _, action := range []membership.DeliveryAction{membership.DeliveryActionPublish, membership.DeliveryActionReceive} {
		if !containsDeliveryAckCommand([][]byte{encode(action)}) {
			t.Errorf("action %d was not recognized as a durable ACK command", action)
		}
	}
	for _, action := range []membership.DeliveryAction{membership.DeliveryActionSend, membership.DeliveryActionAttach} {
		if containsDeliveryAckCommand([][]byte{encode(action)}) {
			t.Errorf("action %d was recognized as a durable ACK command", action)
		}
	}
	if containsDeliveryAckCommand([][]byte{[]byte("not a command")}) {
		t.Fatal("unparseable command was recognized as a durable ACK command")
	}
	if !containsDeliveryAckCommand([][]byte{[]byte("not a command"), encode(membership.DeliveryActionPublish)}) {
		t.Fatal("valid durable ACK command after an invalid entry was not found")
	}
}
