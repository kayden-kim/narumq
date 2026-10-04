package membership

type RemovalHandoffBlock string

const (
	RemovalHandoffBlockNone               RemovalHandoffBlock = ""
	RemovalHandoffBlockNoSuccessor        RemovalHandoffBlock = "no_successor_voter"
	RemovalHandoffBlockEpochExhausted     RemovalHandoffBlock = "ownership_epoch_exhausted"
	RemovalHandoffBlockOrphanSubscription RemovalHandoffBlock = "orphan_subscription"
	RemovalHandoffBlockReplicationFactor  RemovalHandoffBlock = "replication_factor_unsatisfied"
)

type RemovalHandoffPlanDTO struct {
	SuccessorNodeID uint64
	Transfers       []OwnershipChangeDTO
	Block           RemovalHandoffBlock
	RemoveReady     bool
}

// PlanRemovalHandoff computes the session ownership handoff plan before removing a cluster voter.
func PlanRemovalHandoff(
	leavingMemberID uint64,
	voterIDs []uint64,
	ownerships []OwnershipStateDTO,
	subscriptions []SubscriptionStateDTO,
) RemovalHandoffPlanDTO {
	var successor uint64
	foundSuccessor := false
	for _, id := range voterIDs {
		if id != leavingMemberID {
			if !foundSuccessor || id < successor {
				successor = id
				foundSuccessor = true
			}
		}
	}
	if !foundSuccessor {
		return RemovalHandoffPlanDTO{Block: RemovalHandoffBlockNoSuccessor}
	}

	var transfers []OwnershipChangeDTO
	for _, o := range ownerships {
		if o.OwnerNodeID == leavingMemberID {
			if o.Epoch == ^uint64(0) {
				return RemovalHandoffPlanDTO{
					SuccessorNodeID: successor,
					Block:           RemovalHandoffBlockEpochExhausted,
				}
			}
			transfers = append(transfers, OwnershipChangeDTO{
				TenantID:      o.TenantID,
				ClientID:      o.ClientID,
				ExpectedEpoch: o.Epoch,
				OwnerNodeID:   successor,
				Epoch:         o.Epoch + 1,
				Action:        OwnershipActionTransfer,
			})
		}
	}
	if len(transfers) != 0 {
		return RemovalHandoffPlanDTO{
			SuccessorNodeID: successor,
			Transfers:       transfers,
			RemoveReady:     false,
		}
	}

	for _, sub := range subscriptions {
		if sub.OwnerNodeID == leavingMemberID {
			return RemovalHandoffPlanDTO{
				SuccessorNodeID: successor,
				Block:           RemovalHandoffBlockOrphanSubscription,
			}
		}
	}

	return RemovalHandoffPlanDTO{
		SuccessorNodeID: successor,
		RemoveReady:     true,
	}
}
