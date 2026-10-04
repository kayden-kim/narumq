package membership

import (
	"math"
	"slices"
)

// PlanRebalanceTransfer selects the first transferable local ownership candidate.
func PlanRebalanceTransfer(
	localNodeID, targetNodeID uint64,
	candidates []OwnershipStateDTO,
	skip func(OwnershipStateDTO) bool,
) (OwnershipChangeDTO, bool) {
	for _, candidate := range candidates {
		if candidate.OwnerNodeID != localNodeID || candidate.Epoch == math.MaxUint64 {
			continue
		}
		if skip != nil && skip(candidate) {
			continue
		}
		return OwnershipChangeDTO{
			TenantID:      candidate.TenantID,
			ClientID:      candidate.ClientID,
			ExpectedEpoch: candidate.Epoch,
			OwnerNodeID:   targetNodeID,
			Epoch:         candidate.Epoch + 1,
			Action:        OwnershipActionTransfer,
		}, true
	}
	return OwnershipChangeDTO{}, false
}

// SelectRebalanceTarget chooses the least-loaded connected participant other than the local node.
func SelectRebalanceTarget(
	localNodeID uint64,
	participants []ConnectionRebalanceReportDTO,
	connected []uint64,
) (uint64, bool) {

	var target ConnectionRebalanceReportDTO
	found := false
	for _, participant := range participants {
		if participant.NodeID == localNodeID {
			continue
		}
		if !slices.Contains(connected, participant.NodeID) {
			continue
		}
		if !found || participant.Connections < target.Connections ||
			(participant.Connections == target.Connections && participant.NodeID < target.NodeID) {
			target = participant
			found = true
		}
	}
	if !found {
		return 0, false
	}
	return target.NodeID, true
}
