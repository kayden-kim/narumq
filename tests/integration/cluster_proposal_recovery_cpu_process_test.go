package integration

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Promoted from .artifacts/analysis/n1-delivery/it/n1_repro_test.go. The MQTT packets,
// six persistent subscribers, 40 ms publish cadence and kill/observation windows are unchanged.
type n1Member struct {
	id      string
	node    int
	conn    net.Conn
	mu      sync.Mutex
	recv    map[string]int
	closed  time.Time
	closeEr error
}

func n1Connect(address, token, clientID string, clean bool, expiry uint32) (net.Conn, bool, byte, error) {
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return nil, false, 0, err
	}
	_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
	flags := byte(0xC0)
	if clean {
		flags |= 0x02
	}
	props := []byte{0x11, byte(expiry >> 24), byte(expiry >> 16), byte(expiry >> 8), byte(expiry), 0x21, 0, 0x20}
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 5, flags, 0, 60, byte(len(props))}
	body = append(body, props...)
	for _, field := range []string{clientID, "jwt", token} {
		body = append(body, byte(len(field)>>8), byte(len(field)))
		body = append(body, field...)
	}
	if _, err := conn.Write(clusterMembershipProcessEncodeMQTTPacket(0x10, body)); err != nil {
		_ = conn.Close()
		return nil, false, 0, err
	}
	kind, _, response, err := clusterDurableProcessReadRaw(conn)
	if err != nil || kind != 2 || len(response) < 2 {
		_ = conn.Close()
		return nil, false, 0, fmt.Errorf("connack kind=%d body=%x err=%v", kind, response, err)
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, response[0]&1 == 1, response[1], nil
}

func n1Subscribe(conn net.Conn, id uint16, filter string) error {
	body := []byte{byte(id >> 8), byte(id), 0, byte(len(filter) >> 8), byte(len(filter))}
	body = append(body, filter...)
	body = append(body, 0x01)
	if err := clusterDurableProcessWrite(conn, 0x82, body); err != nil {
		return err
	}
	_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))
	defer conn.SetReadDeadline(time.Time{})
	kind, _, ack, err := clusterDurableProcessReadRaw(conn)
	if err != nil || kind != 9 || len(ack) < 4 || ack[3] >= 0x80 {
		return fmt.Errorf("suback kind=%d body=%x err=%v", kind, ack, err)
	}
	return nil
}

func (m *n1Member) reader(group *sync.WaitGroup) {
	defer group.Done()
	for {
		kind, flags, body, err := clusterDurableProcessReadRaw(m.conn)
		if err != nil {
			m.mu.Lock()
			m.closed, m.closeEr = time.Now(), err
			m.mu.Unlock()
			return
		}
		if kind != 3 {
			continue
		}
		delivery, err := clusterMatrixDecodePublish(flags, body, true)
		if err != nil {
			continue
		}
		m.mu.Lock()
		if m.recv == nil {
			m.recv = map[string]int{}
		}
		m.recv[delivery.payload]++
		m.mu.Unlock()
		if delivery.qos == 1 {
			_ = clusterDurableProcessWrite(m.conn, 0x40, []byte{byte(delivery.packetID >> 8), byte(delivery.packetID)})
		}
	}
}

type n1Ack struct {
	ok bool
}

func n1Publish(conn net.Conn, topic string, id uint16, payload string) n1Ack {
	body := []byte{byte(len(topic) >> 8), byte(len(topic))}
	body = append(body, topic...)
	body = append(body, byte(id>>8), byte(id), 0)
	body = append(body, payload...)
	if err := clusterDurableProcessWrite(conn, 0x32, body); err != nil {
		return n1Ack{}
	}
	_ = conn.SetReadDeadline(time.Now().Add(12 * time.Second))
	defer conn.SetReadDeadline(time.Time{})
	for {
		kind, _, ack, err := clusterDurableProcessReadRaw(conn)
		if err != nil {
			return n1Ack{}
		}
		if kind == 4 && len(ack) >= 2 && binary.BigEndian.Uint16(ack[:2]) == id {
			return n1Ack{ok: len(ack) < 3 || ack[2] < 0x80}
		}
	}
}

// Traceability: CLU-07; docs/specs/cluster-session-and-delivery.md §11.33.
// Acceptance: NARUMQ_LOAD_QUALITY
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-241
func TestN1PersistentClose(t *testing.T) {
	if os.Getenv("NARUMQ_LOAD_QUALITY") != "1" {
		t.Skip("set NARUMQ_LOAD_QUALITY=1 to run the N1 CPU-contention persistent-subscriber acceptance")
	}
	// N1_BURN=24 N1_ROUNDS=8 reproduces the recorded CPU-contention matrix. Defaults also
	// create CPU contention; N1_FEATURE may explicitly be empty to run the failing control.
	burn, rounds := 2, 3
	for key, target := range map[string]*int{"N1_BURN": &burn, "N1_ROUNDS": &rounds} {
		if raw, present := os.LookupEnv(key); present {
			value, err := strconv.Atoi(raw)
			if err != nil || value < 1 {
				t.Fatalf("%s must be a positive integer", key)
			}
			*target = value
		}
	}
	feature, present := os.LookupEnv("N1_FEATURE")
	if !present {
		feature = "proposal-recovery"
	}
	stopBurn := make(chan struct{})
	var burnGroup sync.WaitGroup
	defer func() { close(stopBurn); burnGroup.Wait() }()
	for range burn {
		burnGroup.Add(1)
		go func() {
			defer burnGroup.Done()
			x := 0
			for {
				select {
				case <-stopBurn:
					return
				default:
					for i := range 1_000_000 {
						x += i
					}
				}
			}
		}()
	}
	for round := range rounds {
		t.Run(fmt.Sprintf("round%d", round), func(t *testing.T) {
			fixture, nodes := clusterDurableProcessStart(t)
			leader, _ := clusterMembershipProcessFindLeader(t, fixture.ctx, nodes, 3)
			leaderIndex := int(leader) - 1
			clusterProcessActivateFeature(t, fixture.ctx, nodes, "shared-subscription-reroute")
			if feature != "" {
				clusterProcessActivateFeature(t, fixture.ctx, nodes, feature)
			}
			var survivors []int
			for index, node := range nodes {
				clusterMembershipProcessWaitForStatus(t, fixture.ctx, node, "/health/ready", 200)
				if index != leaderIndex {
					survivors = append(survivors, index)
				}
			}
			var readers sync.WaitGroup
			var subscribers []*n1Member
			defer func() {
				for _, member := range subscribers {
					_ = member.conn.Close()
				}
				readers.Wait()
			}()
			clientIDs := []string{"recovery-sub", "durable-takeover-sub", "durable-offline-sub", "durable-ack-sub", "durable-backup-sub", "durable-will-sub"}
			for index := range 6 {
				node := survivors[index%2]
				id := clientIDs[index]
				conn, _, code, err := n1Connect(fixture.mqttAddresses[node], fixture.accessToken, id, true, 300)
				if err != nil || code != 0 {
					t.Fatalf("connect %s: %v code=%d", id, err, code)
				}
				member := &n1Member{id: id, node: node, conn: conn}
				subscribers = append(subscribers, member)
				if err := n1Subscribe(conn, 1, "recovery/alert/#"); err != nil {
					t.Fatalf("subscribe: %v", err)
				}
				readers.Add(1)
				go member.reader(&readers)
			}
			publisher, _, code, err := n1Connect(fixture.mqttAddresses[survivors[0]], fixture.accessToken, "recovery-pub", true, 0)
			if err != nil || code != 0 {
				t.Fatalf("connect publisher: %v code=%d", err, code)
			}
			defer publisher.Close()
			time.Sleep(time.Second)
			stop := make(chan struct{})
			var publisherGroup sync.WaitGroup
			var acks []n1Ack
			publisherGroup.Add(1)
			go func() {
				defer publisherGroup.Done()
				ticker := time.NewTicker(40 * time.Millisecond)
				defer ticker.Stop()
				for id := uint16(1); ; id++ {
					select {
					case <-stop:
						return
					case <-ticker.C:
					}
					ack := n1Publish(publisher, "recovery/alert/x", id, fmt.Sprintf("m-%d", id))
					acks = append(acks, ack)
					if !ack.ok {
						return
					}
				}
			}()
			defer func() { close(stop); publisherGroup.Wait() }()
			time.Sleep(3 * time.Second)
			killAt := time.Now()
			if err := nodes[leaderIndex].crash(10 * time.Second); err != nil {
				t.Fatalf("crash: %v", err)
			}
			time.Sleep(9 * time.Second)
			// Stop the publisher before reading its acknowledgements, without ending the subscriber observation.
			_ = publisher.Close()
			publisherGroup.Wait()
			closes, quorumCloses := 0, 0
			for _, member := range subscribers {
				member.mu.Lock()
				if !member.closed.IsZero() {
					closes++
					t.Logf("subscriber %s node=%d closed %v after kill: %v", member.id, member.node, member.closed.Sub(killAt), member.closeEr)
				}
				member.mu.Unlock()
			}
			logs := t.TempDir()
			for index, node := range nodes {
				text := node.logs.String()
				if err := os.WriteFile(filepath.Join(logs, fmt.Sprintf("node-%d.log", index+1)), []byte(text), 0o600); err != nil {
					t.Fatal(err)
				}
				if index != leaderIndex {
					quorumCloses += strings.Count(text, "dispatch MQTT PUBACK: quorum unavailable")
				}
			}
			t.Logf("N1SUMMARY feature=%q round=%d publishes=%d subscriberCloses=%d pubackQuorumCloses=%d", feature, round, len(acks), closes, quorumCloses)
			if len(acks) == 0 {
				t.Error("publisher produced no acknowledgements")
			}
			if closes != 0 || quorumCloses != 0 {
				t.Errorf("subscriber closes=%d PUBACK quorum closes=%d, want zero", closes, quorumCloses)
			}
		})
	}
}
