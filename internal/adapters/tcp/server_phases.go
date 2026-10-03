package tcp

import (
	"fmt"
	"sync"
	"time"

	"narumq/internal/domain/mqtt"
)

const connectionPhaseCapacity = 16

type phaseWork struct {
	packet     mqtt.Packet
	key        uint32
	sequence   uint64
	previous   uint64
	bytes      int
	lane       int
	receivedAt time.Time
	barrier    chan struct{}
}

type phaseOrder struct {
	scheduled uint64
	completed uint64
}

// connectionPhases preserves each handshake phase's FIFO order and the whole
// input order of each directional PacketID, including duplicates and ID reuse.
// Only independent exchanges can overlap their durable I/O.
type connectionPhases struct {
	queues      [2]chan phaseWork
	pending     [2]chan submittedPhaseWork
	pendingDone [2]chan struct{}
	queued      [2]int
	workers     sync.WaitGroup
	mu          sync.Mutex
	changed     *sync.Cond
	orders      map[uint32]phaseOrder
	sequence    uint64
	queuedBytes int
}

func newConnectionPhases() *connectionPhases {
	phases := &connectionPhases{
		queues: [2]chan phaseWork{
			make(chan phaseWork, connectionPhaseCapacity),
			make(chan phaseWork, connectionPhaseCapacity),
		},
		pending: [2]chan submittedPhaseWork{
			make(chan submittedPhaseWork, connectionPhaseCapacity),
			make(chan submittedPhaseWork, connectionPhaseCapacity),
		},
		pendingDone: [2]chan struct{}{make(chan struct{}, 1), make(chan struct{}, 1)},
		orders:      make(map[uint32]phaseOrder),
	}
	phases.changed = sync.NewCond(&phases.mu)
	return phases
}

func (phases *connectionPhases) start(connection *connection) {
	phases.workers.Add(4)
	for lane := range phases.queues {
		go phases.runLane(connection, lane)
		go phases.completeLane(connection, lane)
	}
}

func (phases *connectionPhases) notifyStop() {
	phases.mu.Lock()
	phases.changed.Broadcast()
	phases.mu.Unlock()
}

func (connection *connection) dispatchPhased(packet mqtt.Packet, bytes int) error {
	phases := connection.phases
	lane, key := -1, uint32(0)
	var receivedAt time.Time
	switch value := packet.(type) {
	case *mqtt.Publish:
		lane = 0
		if value.PacketID != 0 {
			key = 1<<16 | uint32(value.PacketID)
		}
		receivedAt = connection.server.config.Clock.Now()
	case *mqtt.PubAck:
		lane, key = 0, 2<<16|uint32(value.PacketID)
	case *mqtt.PubRec:
		lane, key = 0, 2<<16|uint32(value.PacketID)
	case *mqtt.PubRel:
		lane, key = 1, 1<<16|uint32(value.PacketID)
	case *mqtt.PubComp:
		lane, key = 1, 2<<16|uint32(value.PacketID)
	default:
		if err := phases.flush(connection); err != nil {
			return err
		}
		return connection.dispatch(packet)
	}
	work := phaseWork{packet: packet, key: key, bytes: bytes, lane: lane, receivedAt: receivedAt}
	phases.mu.Lock()
	for (phases.queued[lane] >= connectionPhaseCapacity || phases.queuedBytes+bytes > connection.server.config.MaxPacketSize+5) && !connection.isStopping() {
		phases.changed.Wait()
	}
	if connection.isStopping() {
		phases.mu.Unlock()
		return ErrConnectionClosed
	}
	if key != 0 {
		order := phases.orders[key]
		phases.sequence++
		work.sequence, work.previous = phases.sequence, order.scheduled
		order.scheduled = work.sequence
		phases.orders[key] = order
	}
	phases.queuedBytes += bytes
	phases.queued[lane]++
	phases.mu.Unlock()
	select {
	case <-connection.stop:
		return ErrConnectionClosed
	case phases.queues[lane] <- work:
		return nil
	}
}

func (phases *connectionPhases) flush(connection *connection) error {
	done := make(chan struct{}, len(phases.queues))
	for _, queue := range phases.queues {
		select {
		case <-connection.stop:
			return ErrConnectionClosed
		case queue <- phaseWork{barrier: done}:
		}
	}
	for range phases.queues {
		select {
		case <-connection.stop:
			return ErrConnectionClosed
		case <-done:
		}
	}
	return nil
}

func (phases *connectionPhases) waitOrder(connection *connection, work phaseWork) bool {
	phases.mu.Lock()
	for work.key != 0 && phases.orders[work.key].completed != work.previous && !connection.isStopping() {
		phases.changed.Wait()
	}
	phases.mu.Unlock()
	return !connection.isStopping()
}

func (phases *connectionPhases) flushPending(connection *connection, lane int) bool {
	select {
	case <-connection.stop:
		return false
	case phases.pending[lane] <- submittedPhaseWork{work: phaseWork{barrier: phases.pendingDone[lane]}}:
	}
	select {
	case <-connection.stop:
		return false
	case <-phases.pendingDone[lane]:
		return true
	}
}

func (phases *connectionPhases) runLane(connection *connection, lane int) {
	defer phases.workers.Done()
	defer close(phases.pending[lane])
	for {
		var work phaseWork
		select {
		case <-connection.stop:
			return
		case work = <-phases.queues[lane]:
		}
		if connection.isStopping() {
			return
		}
		if work.barrier != nil {
			phases.pending[lane] <- submittedPhaseWork{work: work}
			continue
		}
		if !phases.waitOrder(connection, work) {
			return
		}
		if packet, ok := work.packet.(*mqtt.Publish); ok &&
			packet.QoS > mqtt.QoSAtMostOnce && !packet.Retain &&
			connection.canBeginInboundPublish(packet) {
			// Preparation and fresh-barrier submission remain FIFO. The fixed
			// completion worker waits for commit and acknowledges in FIFO order.
			submitted := connection.beginInboundPublish(work)
			// Once submitted, the future must reach the completion worker even
			// if shutdown races this send. The shared 16-item limit bounds it.
			phases.pending[lane] <- submitted
			continue
		}
		if rel, ok := work.packet.(*mqtt.PubRel); ok {
			if submitted, handled := connection.beginInboundRelease(work, rel); handled {
				phases.pending[lane] <- submitted
				continue
			}
		}
		var ackID mqtt.PacketIdentifier
		switch packet := work.packet.(type) {
		case *mqtt.PubAck:
			ackID = packet.PacketID
		case *mqtt.PubRec:
			ackID = packet.PacketID
		case *mqtt.PubComp:
			ackID = packet.PacketID
		}
		if work.key>>16 == 2 {
			phases.pending[lane] <- connection.beginOutboundAck(work, ackID)
			continue
		}
		if !phases.flushPending(connection, lane) {
			return
		}
		var err error
		if packet, ok := work.packet.(*mqtt.Publish); ok {
			err = connection.dispatchPublishAt(*packet, work.receivedAt)
		} else {
			err = connection.dispatch(work.packet)
		}
		if !phases.finishWork(connection, work, err) {
			return
		}
	}
}

func (phases *connectionPhases) completeLane(connection *connection, lane int) {
	defer phases.workers.Done()
	for submitted := range phases.pending[lane] {
		if submitted.work.barrier != nil {
			submitted.work.barrier <- struct{}{}
			continue
		}
		var err error
		switch submitted.work.packet.(type) {
		case *mqtt.PubRel:
			err = connection.completeInboundRelease(submitted)
		case *mqtt.Publish:
			err = connection.completeInboundPublish(submitted)
		default:
			err = connection.completeOutboundAck(submitted)
		}
		// Do not return on error: drain every future until the dispatcher closes
		// the channel, including futures queued before socket shutdown.
		phases.finishWork(connection, submitted.work, err)
	}
}

func (phases *connectionPhases) finishWork(connection *connection, work phaseWork, err error) bool {
	phases.mu.Lock()
	if work.key != 0 {
		order := phases.orders[work.key]
		if order.scheduled == work.sequence {
			delete(phases.orders, work.key)
		} else {
			order.completed = work.sequence
			phases.orders[work.key] = order
		}
	}
	phases.queuedBytes -= work.bytes
	phases.queued[work.lane]--
	phases.changed.Broadcast()
	phases.mu.Unlock()
	if err != nil {
		if !connection.isStopping() {
			connection.server.logger.Warn("MQTT connection handling failed", "error",
				fmt.Errorf("dispatch MQTT %s: %w", packetTypeName(work.packet), err).Error())
		}
		connection.requestClose()
		return false
	}
	return true
}
