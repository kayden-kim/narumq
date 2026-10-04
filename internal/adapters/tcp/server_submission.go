package tcp

import (
	"errors"

	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/publish"
	"narumq/internal/domain/delivery"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

type submittedPhaseWork struct {
	work       phaseWork
	inbound    inboundState
	publish    publish.PendingPublish
	receive    publish.PendingReceive
	err        error
	outbound   deliveryqueue.OutboundDTO
	transition deliveryqueue.PendingTransition
	complete   bool
	toSend     mqtt.Packet
}

func (connection *connection) canBeginInboundPublish(packet *mqtt.Publish) bool {
	connection.deliveryMu.Lock()
	defer connection.deliveryMu.Unlock()
	existing, exists := connection.inbound[packet.PacketID]
	return !exists || (existing.state.IsComplete() && !packet.DUP)
}

func (connection *connection) beginInboundPublish(work phaseWork) submittedPhaseWork {
	submitted := submittedPhaseWork{work: work}
	packet := work.packet.(*mqtt.Publish)
	if connection.principal.Subject() != "" {
		topic, err := tenancy.NewTopic(connection.principal.TenantID(), packet.Topic)
		if err != nil || connection.server.config.Policy.AuthorizeTopic(connection.principal, security.ActionPublish, topic) != nil {
			submitted.err = security.ErrNotAuthorized
			return submitted
		}
	}
	if !connection.server.clusterProjectionReady() {
		submitted.err = ErrClusterNotReady
		return submitted
	}
	state, action, err := delivery.NewInbound(*packet)
	if err != nil {
		submitted.err = ErrProtocol
		return submitted
	}
	connection.deliveryMu.Lock()
	delete(connection.inbound, packet.PacketID)
	connection.deliveryMu.Unlock()
	submitted.inbound = inboundState{state: state, receivedAt: work.receivedAt}
	if packet.QoS == mqtt.QoSAtLeastOnce {
		submitted.publish, submitted.err = connection.server.publications.BeginPublish(
			connection.sessionID, *action.Deliver, work.receivedAt,
		)
	} else {
		submitted.receive, submitted.err = connection.server.publications.BeginReceiveQoS2(
			connection.sessionID, connection.ownership.OwnerNodeID, connection.ownership.Epoch,
			*packet, work.receivedAt, connection.principal,
		)
	}
	return submitted
}

func (connection *connection) completeInboundPublish(submitted submittedPhaseWork) error {
	packet := submitted.work.packet.(*mqtt.Publish)
	err := submitted.err
	var result publish.ResultDTO
	if err == nil {
		if packet.QoS == mqtt.QoSAtLeastOnce {
			result, err = submitted.publish.Wait()
		} else {
			err = submitted.receive.Wait()
		}
	}
	// Wait every submitted future even after socket shutdown. It owns the
	// request deadline and the runtime's count of in-progress local commits.
	if connection.isStopping() {
		return nil
	}
	if errors.Is(err, security.ErrNotAuthorized) {
		return connection.rejectUnauthorizedPublish(*packet)
	}
	if errors.Is(err, publish.ErrDurableDeliveryCapacity) || errors.Is(err, publish.ErrPublishBackpressure) {
		if connection.version != mqtt.ProtocolV5 {
			connection.requestClose()
			return nil
		}
		if packet.QoS == mqtt.QoSAtLeastOnce {
			return connection.enqueueControl(&mqtt.PubAck{Version: connection.version, PacketID: packet.PacketID, ReasonCode: 0x97})
		}
		return connection.enqueueControl(&mqtt.PubRec{Version: connection.version, PacketID: packet.PacketID, ReasonCode: 0x97})
	}
	if err != nil {
		return err
	}
	accepted, action, err := submitted.inbound.state.Accept()
	if err != nil {
		return ErrProtocol
	}
	connection.deliveryMu.Lock()
	if connection.cleaned || connection.isStopping() {
		connection.deliveryMu.Unlock()
		return nil
	}
	submitted.inbound.state = accepted
	connection.inbound[packet.PacketID] = submitted.inbound
	connection.deliveryMu.Unlock()
	if action.Send != nil {
		if err := connection.enqueueControl(action.Send); err != nil {
			return err
		}
	}
	for _, sessionID := range result.Delivered {
		connection.server.wakeSession(sessionID)
	}
	for _, sessionID := range result.Saturated {
		connection.server.closeSession(sessionID)
	}
	return nil
}

func (connection *connection) beginInboundRelease(work phaseWork, packet *mqtt.PubRel) (submittedPhaseWork, bool) {
	connection.deliveryMu.Lock()
	entry, exists := connection.inbound[packet.PacketID]
	if !exists || entry.state.IsComplete() {
		connection.deliveryMu.Unlock()
		return submittedPhaseWork{}, false
	}
	next, action, err := entry.state.Receive(packet)
	connection.deliveryMu.Unlock()
	if err != nil || action.Deliver == nil || action.Deliver.Retain {
		return submittedPhaseWork{}, false
	}
	submitted := submittedPhaseWork{
		work: work, inbound: inboundState{state: next, receivedAt: entry.receivedAt},
	}
	submitted.publish, submitted.err = connection.server.publications.BeginQoS2Release(
		connection.sessionID, connection.ownership.OwnerNodeID, connection.ownership.Epoch,
		*action.Deliver, entry.receivedAt, connection.principal,
	)
	return submitted, true
}

func (connection *connection) completeInboundRelease(submitted submittedPhaseWork) error {
	err := submitted.err
	var result publish.ResultDTO
	if err == nil {
		result, err = submitted.publish.Wait()
	}
	if connection.isStopping() {
		return nil
	}
	if err != nil {
		return err
	}
	accepted, action, err := submitted.inbound.state.Accept()
	if err != nil {
		return ErrProtocol
	}
	packet := submitted.work.packet.(*mqtt.PubRel)
	connection.deliveryMu.Lock()
	if connection.cleaned || connection.isStopping() {
		connection.deliveryMu.Unlock()
		return nil
	}
	submitted.inbound.state = accepted
	connection.inbound[packet.PacketID] = submitted.inbound
	connection.deliveryMu.Unlock()
	if action.Send != nil {
		if err := connection.enqueueControl(action.Send); err != nil {
			return err
		}
	}
	for _, sessionID := range result.Delivered {
		connection.server.wakeSession(sessionID)
	}
	for _, sessionID := range result.Saturated {
		connection.server.closeSession(sessionID)
	}
	return nil
}

func (connection *connection) beginOutboundAck(work phaseWork, packetID mqtt.PacketIdentifier) submittedPhaseWork {
	submitted := submittedPhaseWork{work: work}
	if packetID == 0 {
		submitted.err = ErrProtocol
		return submitted
	}
	connection.deliveryMu.Lock()
	outbound, exists := connection.outbound[packetID]
	if !exists {
		connection.deliveryMu.Unlock()
		return submitted
	}
	next, action, err := outbound.State.Receive(work.packet)
	connection.deliveryMu.Unlock()
	if err != nil {
		submitted.err = ErrProtocol
		return submitted
	}
	outbound.State = next
	submitted.outbound, submitted.complete, submitted.toSend = outbound, action.Complete, action.Send
	if action.Complete {
		submitted.transition, submitted.err = connection.server.deliveries.BeginCompleteOutbound(outbound)
	} else {
		submitted.transition, submitted.err = connection.server.deliveries.BeginReplaceOutbound(outbound)
	}
	return submitted
}

func (connection *connection) completeOutboundAck(submitted submittedPhaseWork) error {
	err := submitted.err
	if err == nil && submitted.transition != nil {
		err = submitted.transition.Wait()
	}
	if connection.isStopping() {
		return nil
	}
	if err != nil || submitted.transition == nil {
		return err
	}
	connection.deliveryMu.Lock()
	if connection.cleaned || connection.isStopping() {
		connection.deliveryMu.Unlock()
		return nil
	}
	if submitted.complete {
		delete(connection.outbound, submitted.outbound.PacketID)
		connection.packetIDs.release(submitted.outbound.PacketID)
	} else {
		connection.outbound[submitted.outbound.PacketID] = submitted.outbound
	}
	connection.deliveryMu.Unlock()
	if submitted.complete {
		connection.wakeDelivery()
	}
	if pubrel, ok := submitted.toSend.(*mqtt.PubRel); ok {
		pubrel.Version = connection.version
	}
	if submitted.toSend != nil {
		return connection.enqueueControl(submitted.toSend)
	}
	return nil
}
