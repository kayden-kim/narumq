package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"narumq/internal/application/membership"
)

type clusterFaultControlFile struct {
	Crash             string   `json:"crash"`
	IsolatePeers      []uint64 `json:"isolatePeers"`
	DropProposalPeers []uint64 `json:"dropProposalPeers"`
	HoldCommands      []string `json:"holdCommands"`
	FreeBytes         *uint64  `json:"freeBytes"`
}

type clusterHeldCommandKind string

const (
	clusterHeldOwnership    clusterHeldCommandKind = "ownership"
	clusterHeldSubscription clusterHeldCommandKind = "subscription"
	clusterHeldDelivery     clusterHeldCommandKind = "delivery"

	clusterFaultHoldPollInterval = 20 * time.Millisecond
	clusterFaultHoldLimit        = 60 * time.Second
)

type clusterFaultControl struct {
	path   string
	logger *slog.Logger
}

func newClusterFaultControl(path string, logger *slog.Logger) *clusterFaultControl {
	return &clusterFaultControl{path: path, logger: logger}
}

func (c *clusterFaultControl) enabled() bool {
	return c != nil && c.path != ""
}

func (c *clusterFaultControl) read() (clusterFaultControlFile, clusterFailpoint, bool) {
	if !c.enabled() {
		return clusterFaultControlFile{}, clusterFailpointNone, false
	}
	data, err := os.ReadFile(c.path)
	if errors.Is(err, os.ErrNotExist) || err == nil && len(data) == 0 {
		return clusterFaultControlFile{}, clusterFailpointNone, false
	}
	if err != nil {
		c.logger.Error("invalid cluster fault control", "path", c.path, "error", err)
		return clusterFaultControlFile{}, clusterFailpointNone, false
	}
	var control clusterFaultControlFile
	if err := json.Unmarshal(data, &control); err != nil {
		c.logger.Error("invalid cluster fault control", "path", c.path, "error", err)
		return clusterFaultControlFile{}, clusterFailpointNone, false
	}
	point, err := parseClusterFailpoint(control.Crash)
	if err != nil {
		c.logger.Error("invalid cluster fault control", "path", c.path, "error", err)
		return clusterFaultControlFile{}, clusterFailpointNone, false
	}
	for _, heldKind := range control.HoldCommands {
		switch clusterHeldCommandKind(heldKind) {
		case clusterHeldOwnership, clusterHeldSubscription, clusterHeldDelivery:
		default:
			c.logger.Error("invalid cluster fault control", "path", c.path, "error", fmt.Errorf("unknown held command kind %q", heldKind))
			return clusterFaultControlFile{}, clusterFailpointNone, false
		}
	}
	return control, point, true
}

func (c *clusterFaultControl) freeBytesOverride() (uint64, bool) {
	control, _, ok := c.read()
	if !ok || control.FreeBytes == nil {
		return 0, false
	}
	return *control.FreeBytes, true
}

func (c *clusterFaultControl) crash() clusterFailpoint {
	_, point, ok := c.read()
	if !ok {
		return clusterFailpointNone
	}
	return point
}

func (c *clusterFaultControl) isolates(peerID uint64) bool {
	control, _, ok := c.read()
	if !ok {
		return false
	}
	for _, isolatedPeerID := range control.IsolatePeers {
		if isolatedPeerID == peerID {
			return true
		}
	}
	return false
}

// dropsProposals reports whether forwarded MsgProp frames addressed to peerID are discarded after Raft emitted them.
func (c *clusterFaultControl) dropsProposals(peerID uint64) bool {
	control, _, ok := c.read()
	if !ok {
		return false
	}
	for _, droppedPeerID := range control.DropProposalPeers {
		if droppedPeerID == peerID {
			return true
		}
	}
	return false
}

func (c *clusterFaultControl) holds(kind clusterHeldCommandKind) bool {
	control, _, ok := c.read()
	if !ok {
		return false
	}
	for _, heldKind := range control.HoldCommands {
		if clusterHeldCommandKind(heldKind) == kind {
			return true
		}
	}
	return false
}

func clusterHeldCommandLabel(command membership.CommandDTO) (clusterHeldCommandKind, string) {
	switch {
	case command.Ownership != nil:
		return clusterHeldOwnership, fmt.Sprintf("kind=ownership client=%s expected_epoch=%d",
			command.Ownership.ClientID, command.Ownership.ExpectedEpoch)
	case command.Subscription != nil:
		return clusterHeldSubscription, fmt.Sprintf("kind=subscription client=%s filter=%s epoch=%d",
			command.Subscription.ClientID, command.Subscription.Filter, command.Subscription.Epoch)
	case command.Delivery != nil:
		return clusterHeldDelivery, fmt.Sprintf("kind=delivery client=%s action=%d epoch=%d",
			command.Delivery.ClientID, uint32(command.Delivery.Action), command.Delivery.Epoch)
	default:
		return "", ""
	}
}

func (c *clusterFaultControl) holdCommand(command membership.CommandDTO) (string, bool) {
	if !c.enabled() {
		return "", false
	}
	kind, label := clusterHeldCommandLabel(command)
	if kind == "" || !c.holds(kind) {
		return "", false
	}
	writeClusterFaultMarker("narumq failpoint holding: " + label)
	deadline := time.Now().Add(clusterFaultHoldLimit)
	for c.holds(kind) {
		if !time.Now().Before(deadline) {
			c.logger.Warn("cluster fault hold limit reached", "command", label)
			break
		}
		time.Sleep(clusterFaultHoldPollInterval)
	}
	return label, true
}

func (c *clusterFaultControl) reportHeldResult(label string, result membership.ApplyResultDTO, err error) {
	suffix := "outcome=conflict conflict=" + string(result.Conflict)
	switch {
	case err != nil:
		suffix = "outcome=error error=" + err.Error()
	case result.Outcome == membership.ApplyOutcomeApplied:
		suffix = "outcome=applied"
	}
	writeClusterFaultMarker("narumq failpoint held result: " + label + " " + suffix)
}

func containsDeliveryAckCommand(entries [][]byte) bool {
	for _, entry := range entries {
		command, err := decodeMembershipCommand(entry)
		if err == nil && isDeliveryAckCommand(command) {
			return true
		}
	}
	return false
}

func isDeliveryAckCommand(command membership.CommandDTO) bool {
	return command.Delivery != nil && (command.Delivery.Action == membership.DeliveryActionPublish ||
		command.Delivery.Action == membership.DeliveryActionReceive)
}
