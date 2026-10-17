package tcp

import (
	"time"

	"narumq/internal/application/publish"
	"narumq/internal/application/tenantlimit"
	"narumq/internal/domain/mqtt"
)

func (connection *connection) admitTenantPublish(packet *mqtt.Publish) (bool, error) {
	limits := connection.server.config.TenantLimits
	if limits == nil || connection.principal.Subject() == "" {
		return true, nil
	}
	wait, rejected := limits.ReservePublish(connection.principal.TenantID(), len(packet.Payload))
	if rejected != "" {
		return false, connection.rejectTenantPublish(packet, rejected)
	}
	if wait <= 0 {
		return true, nil
	}
	select {
	case <-connection.stop:
		return false, nil
	default:
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		if connection.isStopping() {
			return false, nil
		}
		return true, nil
	case <-connection.stop:
		return false, nil
	}
}

func (connection *connection) rejectTenantPublish(packet *mqtt.Publish, dimension tenantlimit.Dimension) error {
	if connection.version != mqtt.ProtocolV5 {
		connection.requestClose()
		return nil
	}
	reasonCode := byte(0x97)
	reasonString := publish.FormatRejectionReason(publish.RejectionClassTenantLimit, string(dimension))
	switch packet.QoS {
	case mqtt.QoSAtLeastOnce:
		return connection.enqueueControl(&mqtt.PubAck{
			Version: connection.version, PacketID: packet.PacketID, ReasonCode: reasonCode, ReasonString: reasonString,
		})
	case mqtt.QoSExactlyOnce:
		return connection.enqueueControl(&mqtt.PubRec{
			Version: connection.version, PacketID: packet.PacketID, ReasonCode: reasonCode, ReasonString: reasonString,
		})
	default:
		if dimension == tenantlimit.DimensionMessageRate || dimension == tenantlimit.DimensionByteRate {
			reasonCode = 0x96
		}
		if err := connection.enqueueAndWait(&mqtt.Disconnect{Version: connection.version, ReasonCode: reasonCode}); err != nil {
			return err
		}
		connection.requestClose()
		return nil
	}
}
