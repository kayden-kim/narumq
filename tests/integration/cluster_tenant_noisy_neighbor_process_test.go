package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Traceability: TEN-09; docs/specs/tenancy-and-security.md §5.3; docs/decisions/ADR-0054-tenant-noisy-neighbor-latency-acceptance.md.
// Scenarios: tenancy-and-security.md:SEC-SCN-030
func TestDynamicClusterTenantNoisyNeighborProcessAcceptance(t *testing.T) {
	fixture := newClusterMembershipProcessFixture(t, 3, "")
	issuer := newClusterProcessJWTIssuer(t, "ten09-key")
	now := time.Now()
	victimToken := issuer.sign(t, "ten09-victim", now.Add(time.Hour), nil)
	noisyToken := issuer.sign(t, "ten09-noisy", now.Add(time.Hour), nil)
	securityPath := clusterNoisyNeighborSecurityConfig(t, fixture, issuer.jwksURL)

	nodes := make([]*clusterMembershipProcess, 3)
	for index := range nodes {
		config := clusterTenantLimitsProcessConfig(t, fixture.bootstrapConfig(uint64(index+1)), securityPath)
		process, err := fixture.harness.start(fmt.Sprintf("noisy-neighbor node %d", index+1), config, fixture.managementAddresses[index])
		if err != nil {
			t.Fatalf("start noisy-neighbor node %d: %v", index+1, err)
		}
		nodes[index] = process
	}
	ctx := fixture.ctx
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)
	for _, node := range nodes {
		clusterMembershipProcessWaitForStatus(t, ctx, node, "/health/ready", http.StatusOK)
	}

	var (
		closing             atomic.Bool
		readerWG            sync.WaitGroup
		clients             = make([]net.Conn, 0, 8)
		activeNoisyCancel   context.CancelFunc
		activeNoisyWriterWG *sync.WaitGroup
	)
	t.Cleanup(func() {
		if activeNoisyCancel != nil {
			activeNoisyCancel()
		}
		closing.Store(true)
		for _, client := range clients {
			_ = client.Close()
		}
		if activeNoisyWriterWG != nil {
			activeNoisyWriterWG.Wait()
		}
		readerWG.Wait()
	})
	defer closing.Store(true)

	connect := func(token, clientID string) net.Conn {
		conn := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], token, clientID, true)
		clients = append(clients, conn)
		return conn
	}
	victimSub := connect(victimToken, "victim-sub")
	clusterDurableProcessSubscribe(t, victimSub, 1, "victim/latency", 1)
	noisySub := connect(noisyToken, "noisy-sub")
	clusterDurableProcessSubscribe(t, noisySub, 1, "noisy/#", 0)
	clusterDurableProcessWaitForSubscriptions(t, ctx, nodes, "tenant-victim", "victim-sub|victim/latency|1")
	clusterDurableProcessWaitForSubscriptions(t, ctx, nodes, "tenant-noisy", "noisy-sub|noisy/#|0")
	victimPub := connect(victimToken, "victim-pub")
	noisyPublishers := make([]net.Conn, 4)
	for index := range noisyPublishers {
		noisyPublishers[index] = connect(noisyToken, fmt.Sprintf("noisy-pub-%d", index+1))
	}

	const noisyPublisherCount = 4
	const noisyInflightLimit = 16
	slots := make([]chan struct{}, noisyPublisherCount)
	for index := range slots {
		slots[index] = make(chan struct{}, noisyInflightLimit)
	}
	errs := make(chan error, 16)
	reportError := func(err error) {
		select {
		case errs <- err:
		default:
		}
	}
	for index, conn := range noisyPublishers {
		readerWG.Add(1)
		go func(publisherIndex int, publisher net.Conn, inflight chan struct{}) {
			defer readerWG.Done()
			for {
				kind, _, _, err := clusterDurableProcessReadRaw(publisher)
				if err != nil {
					if !closing.Load() {
						reportError(fmt.Errorf("noisy-pub-%d read: %w", publisherIndex+1, err))
					}
					return
				}
				if kind != 4 {
					reportError(fmt.Errorf("noisy-pub-%d received MQTT packet kind %d, want PUBACK", publisherIndex+1, kind))
					return
				}
				select {
				case <-inflight:
				default:
					reportError(fmt.Errorf("noisy-pub-%d received PUBACK without an in-flight publish", publisherIndex+1))
					return
				}
			}
		}(index, conn, slots[index])
	}

	var (
		arrivalsMu   sync.Mutex
		noisyArrival []time.Time
	)
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		for {
			kind, _, _, err := clusterDurableProcessReadRaw(noisySub)
			if err != nil {
				if !closing.Load() {
					reportError(fmt.Errorf("noisy-sub read: %w", err))
				}
				return
			}
			if kind != 3 {
				reportError(fmt.Errorf("noisy-sub received MQTT packet kind %d, want PUBLISH", kind))
				return
			}
			arrivalsMu.Lock()
			noisyArrival = append(noisyArrival, time.Now())
			arrivalsMu.Unlock()
		}
	}()

	checkErrors := func(round int) {
		select {
		case err := <-errs:
			t.Fatalf("noisy-neighbor round %d: %v", round, err)
		default:
		}
	}

	var offered atomic.Uint64
	startNoisy := func() func() time.Time {
		writerCtx, cancel := context.WithCancel(context.Background())
		var writerWG sync.WaitGroup
		activeNoisyCancel = cancel
		activeNoisyWriterWG = &writerWG
		for index, conn := range noisyPublishers {
			writerWG.Add(1)
			go func(publisherIndex int, publisher net.Conn, inflight chan struct{}) {
				defer writerWG.Done()
				ticker := time.NewTicker(8 * time.Millisecond)
				defer ticker.Stop()
				var packetID uint16
				topic := fmt.Sprintf("noisy/load/%d", publisherIndex+1)
				payload := bytes.Repeat([]byte("n"), 256)
				for {
					select {
					case <-writerCtx.Done():
						return
					case <-ticker.C:
					}
					select {
					case inflight <- struct{}{}:
					case <-writerCtx.Done():
						return
					}
					packetID++
					if packetID == 0 {
						packetID++
					}
					body := make([]byte, 0, 2+len(topic)+2+len(payload))
					body = append(body, byte(len(topic)>>8), byte(len(topic)))
					body = append(body, topic...)
					body = append(body, byte(packetID>>8), byte(packetID))
					body = append(body, payload...)
					if err := clusterDurableProcessWrite(publisher, 0x32, body); err != nil {
						reportError(fmt.Errorf("noisy-pub-%d write: %w", publisherIndex+1, err))
						return
					}
					offered.Add(1)
				}
			}(index, conn, slots[index])
		}
		return func() time.Time {
			cancel()
			writerWG.Wait()
			activeNoisyCancel = nil
			activeNoisyWriterWG = nil
			deadline := time.Now().Add(5 * time.Second)
			for {
				pending := 0
				for _, inflight := range slots {
					pending += len(inflight)
				}
				if pending == 0 {
					return time.Now()
				}
				if time.Now().After(deadline) {
					t.Fatalf("noisy in-flight did not drain: %d publishes remain", pending)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	measure := func(count int, sequence *uint64) []time.Duration {
		samples := make([]time.Duration, 0, count)
		start := time.Now()
		for index := range count {
			due := start.Add(time.Duration(index) * 20 * time.Millisecond)
			if delay := time.Until(due); delay > 0 {
				time.Sleep(delay)
			}
			*sequence = *sequence + 1
			packetID := uint16((*sequence-1)%65535 + 1)
			payload := fmt.Sprintf("v-%d", *sequence)
			t0 := time.Now()
			clusterDurableProcessPublish(t, victimPub, "victim/latency", 1, packetID, payload)
			delivery := clusterMatrixReadPublish(t, victimSub, false, 5*time.Second)
			if delivery.topic != "victim/latency" || delivery.payload != payload || delivery.qos != 1 {
				t.Fatalf("victim delivery topic=%q payload=%q qos=%d, want topic=%q payload=%q qos=1", delivery.topic, delivery.payload, delivery.qos, "victim/latency", payload)
			}
			latency := time.Since(t0)
			clusterMatrixAck(t, victimSub, delivery)
			clusterDurableProcessAck(t, victimPub, 4, packetID)
			samples = append(samples, latency)
		}
		return samples
	}

	var sequence uint64
	_ = measure(50, &sequence)
	baseline := make([]time.Duration, 0, 500)
	noisy := make([]time.Duration, 0, 500)
	type noisyRoundResult struct {
		got   int
		limit int
	}
	rounds := make([]noisyRoundResult, 0, 5)
	for round := range 5 {
		baseline = append(baseline, measure(100, &sequence)...)
		enable := time.Now()
		stopNoisy := startNoisy()
		time.Sleep(time.Second)
		noisy = append(noisy, measure(100, &sequence)...)
		quiesce := stopNoisy()
		time.Sleep(500 * time.Millisecond)
		checkErrors(round + 1)

		arrivalsMu.Lock()
		arrivals := append([]time.Time(nil), noisyArrival...)
		arrivalsMu.Unlock()
		got := 0
		for _, arrival := range arrivals {
			if !arrival.Before(enable) && !arrival.After(quiesce) {
				got++
			}
		}
		limit := 50 + int(math.Ceil(50*quiesce.Sub(enable).Seconds()))
		if got < 50 || got > limit {
			t.Fatalf("noisy round %d received %d messages in %s, want 50..%d", round+1, got, quiesce.Sub(enable), limit)
		}
		rounds = append(rounds, noisyRoundResult{got: got, limit: limit})
	}
	checkErrors(5)

	baselineP99 := clusterTenantNoisyNeighborP99(baseline)
	noisyP99 := clusterTenantNoisyNeighborP99(noisy)
	if baselineP99 <= 0 {
		t.Fatalf("baseline p99 = %s, want positive duration", baselineP99)
	}
	if 2*noisyP99 > 3*baselineP99 {
		t.Fatalf("TEN-09 victim p99 under noisy load = %s, baseline p99 = %s, ratio %.3f > 1.5", noisyP99, baselineP99, float64(noisyP99)/float64(baselineP99))
	}

	latencySummary := func(samples []time.Duration) (p50, p99, maximum time.Duration) {
		ordered := slices.Clone(samples)
		slices.Sort(ordered)
		p50 = ordered[(50*len(ordered)+99)/100-1]
		p99 = ordered[(99*len(ordered)+99)/100-1]
		maximum = ordered[len(ordered)-1]
		return p50, p99, maximum
	}
	baselineP50, _, baselineMax := latencySummary(baseline)
	noisyP50, _, noisyMax := latencySummary(noisy)
	ratio := float64(noisyP99) / float64(baselineP99)
	t.Logf("TEN-09 baseline p50=%s p99=%s max=%s; noisy p50=%s p99=%s max=%s; p99 ratio=%.3f; noisy received per round=%v; offered=%d",
		baselineP50, baselineP99, baselineMax, noisyP50, noisyP99, noisyMax, ratio, rounds, offered.Load())
}

func clusterNoisyNeighborSecurityConfig(t *testing.T, fixture *clusterMembershipProcessFixture, jwksURL string) string {
	t.Helper()
	identities := []map[string]any{
		{"issuer": "https://issuer.example", "subject": "ten09-victim", "tenantId": "tenant-victim", "credentialId": "ten09-victim-credential", "allowedClientIds": []string{"victim-pub", "victim-sub"}, "roles": []string{"client"}},
		{"issuer": "https://issuer.example", "subject": "ten09-noisy", "tenantId": "tenant-noisy", "credentialId": "ten09-noisy-credential", "allowedClientIds": []string{"noisy-sub", "noisy-pub-1", "noisy-pub-2", "noisy-pub-3", "noisy-pub-4"}, "roles": []string{"client"}},
	}
	var acl []map[string]any
	for _, tenant := range []string{"tenant-victim", "tenant-noisy"} {
		for _, action := range []string{"publish", "subscribe"} {
			acl = append(acl, map[string]any{"tenantId": tenant, "role": "client", "effect": "allow", "action": action, "topicFilter": "#"})
		}
	}
	config := map[string]any{
		"jwt":        map[string]any{"issuer": "https://issuer.example", "audience": "narumq", "algorithms": []string{"RS256"}, "jwksUrl": jwksURL, "cacheTtlMillis": 300000, "refreshCooldownMillis": 30000, "clockSkewMillis": 30000, "httpTimeoutMillis": 5000},
		"identities": identities,
		"acl":        acl,
		"tenantLimits": map[string]any{
			"maxWaitMillis": 1000,
			"default":       map[string]any{"messagesPerSecond": 200},
			"tenants":       []map[string]any{{"tenantId": "tenant-noisy", "messagesPerSecond": 50}},
		},
	}
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		t.Fatalf("encode noisy-neighbor security config: %v", err)
	}
	path := filepath.Join(fixture.tempDir, "mqtt-security-noisy-neighbor.json")
	clusterMembershipProcessWriteFile(t, path, data, 0o600)
	return path
}

func clusterTenantNoisyNeighborP99(samples []time.Duration) time.Duration {
	ordered := slices.Clone(samples)
	slices.Sort(ordered)
	index := (99*len(ordered)+99)/100 - 1
	return ordered[index]
}
