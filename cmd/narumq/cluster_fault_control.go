package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"

	"narumq/internal/application/membership"
)

type clusterFaultControlFile struct {
	Crash        string   `json:"crash"`
	IsolatePeers []uint64 `json:"isolatePeers"`
}

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
	return control, point, true
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
