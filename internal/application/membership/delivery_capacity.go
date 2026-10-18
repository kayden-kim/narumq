package membership

const (
	DeliveryCapacityEntries     = "delivery_entries"
	DeliveryCapacityBytes       = "delivery_bytes"
	DeliveryCapacityTenantQuota = "tenant_quota"
)

// DeliveryCapacityDTO is a read-only admission estimate for one durable PUBLISH change.
type DeliveryCapacityDTO struct {
	Fits        bool
	Reason      string
	TenantQuota *TenantQuotaRejectionDTO
}

// DeliveryPublishCapacity estimates committed capacity without creating records or purging expiry.
func (machine *StateMachine) DeliveryPublishCapacity(change DeliveryChangeDTO) DeliveryCapacityDTO {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	if change.Action != DeliveryActionPublish || len(change.Targets) == 0 || (change.ExpiresAtUnixMillis > 0 && change.ExpiresAtUnixMillis <= change.ProposedAtUnixMillis) {
		return DeliveryCapacityDTO{Fits: true}
	}
	n, b := len(change.Targets), 0
	for _, target := range change.Targets {
		b += deliveryEntryBytes(sessionKey{tenantID: change.TenantID, clientID: target.ClientID}, change.Topic, change.Payload) + len(target.SharedFilter)
	}
	if machine.deliveryEntries+n <= MaxDeliveryEntries && machine.deliveryBytes+b <= MaxDeliveryBytes && machine.tenantQuotaExceededLocked(change.TenantID, 0, n, b, b) == nil {
		return DeliveryCapacityDTO{Fits: true}
	}
	n, b = 0, 0
	for _, target := range change.Targets {
		key := sessionKey{tenantID: change.TenantID, clientID: target.ClientID}
		if _, eligible := machine.deliveryTargetQoSLocked(key, change.Topic, change.PublishQoS); !eligible {
			continue
		}
		if record, exists := machine.delivery[key]; exists && (record.NextSequence == ^uint64(0) || len(record.Messages) >= MaxDeliveryMessagesPerSession) {
			continue
		}
		n++
		b += deliveryEntryBytes(key, change.Topic, change.Payload) + len(target.SharedFilter)
	}
	if machine.deliveryEntries+n > MaxDeliveryEntries {
		return DeliveryCapacityDTO{Reason: DeliveryCapacityEntries}
	}
	if machine.deliveryBytes+b > MaxDeliveryBytes {
		return DeliveryCapacityDTO{Reason: DeliveryCapacityBytes}
	}
	if rejection := machine.tenantQuotaExceededLocked(change.TenantID, 0, n, b, b); rejection != nil {
		return DeliveryCapacityDTO{Reason: DeliveryCapacityTenantQuota, TenantQuota: rejection}
	}
	return DeliveryCapacityDTO{Fits: true}
}
