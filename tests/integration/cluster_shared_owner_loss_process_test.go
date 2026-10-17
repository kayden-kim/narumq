package integration

import (
	"encoding/binary"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const sharedOwnerLossFilter = "$share/g/matrix/owner-loss/#"

type sharedOwnerLossMember struct {
	id      string
	node    int
	conn    net.Conn
	closed  atomic.Bool
	resumed bool
	mu      sync.Mutex
	recv    map[string]int
}

func (member *sharedOwnerLossMember) received(payload string) bool {
	member.mu.Lock()
	defer member.mu.Unlock()
	return member.recv[payload] > 0
}

// read acknowledges every QoS 1 PUBLISH and records its payload until the connection closes.
func (member *sharedOwnerLossMember) read(wg *sync.WaitGroup) {
	defer wg.Done()
	defer member.closed.Store(true)
	for {
		kind, flags, body, err := clusterDurableProcessReadRaw(member.conn)
		if err != nil {
			return
		}
		if kind != 3 {
			continue
		}
		publish, err := clusterMatrixDecodePublish(flags, body, true)
		if err != nil {
			continue
		}
		member.mu.Lock()
		if member.recv == nil {
			member.recv = make(map[string]int)
		}
		member.recv[publish.payload]++
		member.mu.Unlock()
		if publish.qos == 1 {
			_ = clusterDurableProcessWrite(member.conn, 0x40, []byte{byte(publish.packetID >> 8), byte(publish.packetID)})
		}
	}
}

func sharedOwnerLossConnect(address, token, clientID string, cleanStart bool, expirySeconds uint32) (net.Conn, bool, error) {
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return nil, false, err
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	flags := byte(0xC0)
	if cleanStart {
		flags |= 0x02
	}
	properties := []byte{0x11, byte(expirySeconds >> 24), byte(expirySeconds >> 16), byte(expirySeconds >> 8), byte(expirySeconds), 0x21, 0, 0x20}
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 5, flags, 0, 60, byte(len(properties))}
	body = append(body, properties...)
	for _, field := range []string{clientID, "jwt", token} {
		body = append(body, byte(len(field)>>8), byte(len(field)))
		body = append(body, field...)
	}
	if _, err := conn.Write(clusterMembershipProcessEncodeMQTTPacket(0x10, body)); err != nil {
		_ = conn.Close()
		return nil, false, err
	}
	kind, _, connack, err := clusterDurableProcessReadRaw(conn)
	if err != nil || kind != 2 || len(connack) < 2 || connack[1] != 0 {
		_ = conn.Close()
		return nil, false, fmt.Errorf("connack kind=%d body=%x err=%v", kind, connack, err)
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, connack[0]&1 == 1, nil
}

func sharedOwnerLossSubscribe(conn net.Conn, filter string) error {
	body := []byte{0, 1, 0, byte(len(filter) >> 8), byte(len(filter))}
	body = append(body, filter...)
	body = append(body, 0x01)
	if err := clusterDurableProcessWrite(conn, 0x82, body); err != nil {
		return err
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()
	kind, _, ack, err := clusterDurableProcessReadRaw(conn)
	if err != nil || kind != 9 || len(ack) < 4 || ack[3] >= 0x80 {
		return fmt.Errorf("suback kind=%d body=%x err=%v", kind, ack, err)
	}
	return nil
}

// sharedOwnerLossPublish sends one QoS 1 PUBLISH and reports whether the broker PUBACKed it.
func sharedOwnerLossPublish(conn net.Conn, packetID uint16, payload string) bool {
	topic := "matrix/owner-loss/job"
	body := []byte{byte(len(topic) >> 8), byte(len(topic))}
	body = append(body, topic...)
	body = append(body, byte(packetID>>8), byte(packetID), 0)
	body = append(body, payload...)
	if err := clusterDurableProcessWrite(conn, 0x32, body); err != nil {
		return false
	}
	_ = conn.SetReadDeadline(time.Now().Add(12 * time.Second))
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()
	for {
		kind, _, ack, err := clusterDurableProcessReadRaw(conn)
		if err != nil {
			return false
		}
		if kind == 4 && len(ack) >= 2 && binary.BigEndian.Uint16(ack[:2]) == packetID {
			return len(ack) == 2 || ack[2] < 0x80
		}
	}
}

// TestDynamicClusterSharedSubscriptionOwnerLossProcessAcceptance kills the leader that owns two of
// four shared-group members and publishes through a survivor. Every PUBACKed QoS 1 message must reach
// a member. Clean members of the dead node that reconnect must not lose queued messages.
// Persistent members receive their queue when they return (ADR-0060).
//
// Traceability: MQTT-07, CLU-05; docs/specs/cluster-session-and-delivery.md §11.35.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-256
func TestDynamicClusterSharedSubscriptionOwnerLossProcessAcceptance(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		name := "clean-members"
		expiry := uint32(0)
		if persistent {
			name, expiry = "persistent-members", 300
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture, nodes := clusterDurableProcessStart(t)
			for _, node := range nodes {
				clusterMembershipProcessWaitForStatus(t, fixture.ctx, node, "/health/ready", 200)
			}
			leader, _ := clusterMembershipProcessFindLeader(t, fixture.ctx, nodes, 3)
			clusterProcessActivateFeature(t, fixture.ctx, nodes, "shared-subscription-reroute")
			leaderIndex := int(leader) - 1
			var survivors []int
			for i := range nodes {
				if i != leaderIndex {
					survivors = append(survivors, i)
				}
			}

			layout := []int{leaderIndex, leaderIndex, survivors[0], survivors[1]}
			members := make([]*sharedOwnerLossMember, len(layout))
			var readers sync.WaitGroup
			defer func() {
				for _, member := range members {
					if member != nil {
						_ = member.conn.Close()
					}
				}
				readers.Wait()
			}()
			for i, node := range layout {
				id := fmt.Sprintf("owner-loss-m%d", i)
				conn, _, err := sharedOwnerLossConnect(fixture.mqttAddresses[node], fixture.accessToken, id, true, expiry)
				if err != nil {
					t.Fatalf("connect %s: %v", id, err)
				}
				members[i] = &sharedOwnerLossMember{id: id, node: node, conn: conn}
				if err := sharedOwnerLossSubscribe(conn, sharedOwnerLossFilter); err != nil {
					t.Fatalf("subscribe %s: %v", id, err)
				}
				readers.Add(1)
				go members[i].read(&readers)
			}
			publisher, _, err := sharedOwnerLossConnect(fixture.mqttAddresses[survivors[0]], fixture.accessToken, "owner-loss-publisher", true, 0)
			if err != nil {
				t.Fatalf("connect publisher: %v", err)
			}
			defer func() { _ = publisher.Close() }()
			// Let the shared subscriptions reach every node's route projection.
			time.Sleep(2 * time.Second)

			packetID := uint16(1)
			for i := range 8 {
				if !sharedOwnerLossPublish(publisher, packetID, fmt.Sprintf("base-%d", i)) {
					t.Fatalf("baseline publish %d was not acknowledged", i)
				}
				packetID++
			}
			if err := nodes[leaderIndex].crash(10 * time.Second); err != nil {
				t.Fatalf("crash leader: %v", err)
			}

			var acknowledged []string
			for i := range 40 {
				payload := fmt.Sprintf("post-%02d", i)
				if sharedOwnerLossPublish(publisher, packetID, payload) {
					acknowledged = append(acknowledged, payload)
				}
				packetID++
				time.Sleep(200 * time.Millisecond)
			}
			if len(acknowledged) == 0 {
				t.Fatal("no post-kill PUBLISH was acknowledged")
			}

			// Members whose node died come back on a survivor. Clean members reset and subscribe again;
			// persistent members resume their session and receive the queue.
			join := func(member *sharedOwnerLossMember, node int) {
				conn, present, err := sharedOwnerLossConnect(fixture.mqttAddresses[node], fixture.accessToken, member.id, !persistent, expiry)
				if err != nil {
					t.Fatalf("reconnect %s: %v", member.id, err)
				}
				if persistent && !present {
					_ = conn.Close()
					t.Fatalf("persistent member %s lost its session", member.id)
				}
				returned := &sharedOwnerLossMember{id: member.id + "'", node: node, conn: conn}
				members = append(members, returned)
				if !persistent {
					if err := sharedOwnerLossSubscribe(conn, sharedOwnerLossFilter); err != nil {
						t.Fatalf("resubscribe %s: %v", member.id, err)
					}
				}
				member.resumed = true
				readers.Add(1)
				go returned.read(&readers)
			}
			for _, member := range members[:2] {
				join(member, survivors[1])
			}

			deadline := time.Now().Add(20 * time.Second)
			var missing []string
			for {
				// A live member whose connection the broker dropped during the election window
				// is a persistent session that resumes and receives its queue, like the dead node's members.
				if persistent {
					for _, member := range members[2:4] {
						if member.closed.Load() && !member.resumed {
							join(member, member.node)
						}
					}
				}
				missing = missing[:0]
				for _, payload := range acknowledged {
					delivered := false
					for _, member := range members {
						delivered = delivered || member.received(payload)
					}
					if !delivered {
						missing = append(missing, payload)
					}
				}
				if len(missing) == 0 || time.Now().After(deadline) {
					break
				}
				time.Sleep(250 * time.Millisecond)
			}
			if len(missing) != 0 {
				sort.Strings(missing)
				for _, member := range members {
					member.mu.Lock()
					var got []string
					for payload := range member.recv {
						got = append(got, payload)
					}
					member.mu.Unlock()
					sort.Strings(got)
					t.Logf("%s received %d: %s", member.id, len(got), strings.Join(got, ","))
				}
				t.Fatalf("%d of %d acknowledged post-kill messages reached no member: %s", len(missing), len(acknowledged), strings.Join(missing, ","))
			}
		})
	}
}
