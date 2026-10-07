package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"narumq/internal/domain/mqtt"
)

type routeProbeRecord struct {
	Probe                   int    `json:"probe"`
	Topic                   string `json:"topic"`
	SubscriberID            string `json:"subscriberId"`
	SubscriberNode          string `json:"subscriberNode"`
	PublisherID             string `json:"publisherId"`
	PublisherNode           string `json:"publisherNode"`
	T0                      string `json:"t0"`
	T1                      string `json:"t1,omitempty"`
	T2                      string `json:"t2,omitempty"`
	SubscribeAckNanoseconds int64  `json:"subscribeAckNanoseconds"`
	RouteVisibleNanoseconds int64  `json:"routeVisibleNanoseconds"`
	Transmissions           int    `json:"transmissions"`
	DeliveriesObserved      int    `json:"deliveriesObserved"`
	AdditionalDeliveries    int    `json:"additionalDeliveries"`
	LateDeliveries          int    `json:"lateDeliveries"`
	Succeeded               bool   `json:"succeeded"`
	Error                   string `json:"error,omitempty"`
}

type routePublishResult struct {
	transmissions int
	err           error
}

func runRouteProbes(ctx context.Context, profile Profile, nodes []string, publishers map[string]*runClient, subscribers []*runClient, col *collector) error {
	if len(nodes) != bp04NodeCount {
		return errors.New("route propagation requires exactly three broker nodes")
	}
	if len(subscribers) != profile.RouteProbes {
		return fmt.Errorf("route subscriber count %d does not match route probe count %d", len(subscribers), profile.RouteProbes)
	}
	sequences := make(map[string]*uint64, bp04NodeCount)
	for node := 1; node <= bp04NodeCount; node++ {
		var sequence uint64
		sequences[fmt.Sprintf("bp04-route-pub-%d", node)] = &sequence
	}
	for probeIndex := 0; probeIndex < profile.RouteProbes; probeIndex++ {
		subscriber := subscribers[probeIndex]
		if subscriber == nil || subscriber.client == nil {
			return fmt.Errorf("route subscriber %d is not connected", probeIndex)
		}
		subscriberNodeIndex := probeIndex % bp04NodeCount
		publisherNodeIndex := (probeIndex + 1) % bp04NodeCount
		publisherID := fmt.Sprintf("bp04-route-pub-%d", publisherNodeIndex+1)
		publisher := publishers[publisherID]
		if publisher == nil || publisher.client == nil {
			return fmt.Errorf("route publisher %s is not connected", publisherID)
		}
		topic := fmt.Sprintf("bp04/route/%03d", probeIndex)
		probeCtx, cancelProbe := context.WithCancel(ctx)
		publishCtx, cancelPublish := context.WithCancel(probeCtx)
		publishResult := make(chan routePublishResult, 1)
		sequence := sequences[publisherID]
		start := make(chan time.Time, 1)
		go func() {
			publishRouteProbe(publishCtx, publisher, profile, topic, sequence, <-start, publishResult, col)
		}()
		record := routeProbeRecord{
			Probe:          probeIndex,
			Topic:          topic,
			SubscriberID:   subscriber.id,
			SubscriberNode: fmt.Sprintf("node%d", subscriberNodeIndex+1),
			PublisherID:    publisherID,
			PublisherNode:  fmt.Sprintf("node%d", publisherNodeIndex+1),
		}
		t0 := time.Now()
		deadline := t0.Add(time.Duration(profile.RouteTimeoutMillis) * time.Millisecond)
		record.T0 = t0.UTC().Format(time.RFC3339Nano)
		col.beginRouteProbe(topic, deadline)
		timer := time.NewTimer(time.Until(deadline))
		start <- t0

		writeErr := subscriber.client.Write(&mqtt.Subscribe{
			Version:       mqtt.ProtocolV5,
			PacketID:      1,
			Subscriptions: []mqtt.Subscription{{Filter: topic, QoS: mqtt.QoSAtMostOnce}},
		}, time.Second)
		if writeErr != nil {
			record.Error = fmt.Sprintf("write SUBSCRIBE: %v", writeErr)
			cancelPublish()
			result := <-publishResult
			record.Transmissions = result.transmissions
			if result.err != nil {
				record.Error = errors.Join(errors.New(record.Error), result.err).Error()
			}
			timer.Stop()
			cancelProbe()
			col.recordRouteProbe(record)
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
			continue
		}

		var ack *routeSubAck
		var firstDelivery *routeDelivery
		var publisherFinished bool
		for record.Error == "" && (ack == nil || firstDelivery == nil) {
			select {
			case value := <-subscriber.routeSubAcks:
				if value.packetID != 1 || len(value.reasonCodes) != 1 || value.reasonCodes[0] != 0x00 {
					col.protocolError()
					record.Error = fmt.Sprintf("unexpected SUBACK packetId=%d reasonCodes=%v", value.packetID, value.reasonCodes)
					continue
				}
				ack = &value
				record.T1 = value.at.UTC().Format(time.RFC3339Nano)
				record.SubscribeAckNanoseconds = value.at.Sub(t0).Nanoseconds()
			case value := <-subscriber.routeDeliveries:
				if value.topic != topic {
					col.recordUnexpectedDelivery("", 0, value.topic, value.at)
					record.Error = fmt.Sprintf("received route topic %q while waiting for %q", value.topic, topic)
					continue
				}
				if firstDelivery == nil && value.at.Before(deadline) {
					firstDelivery = &value
					record.T2 = value.at.UTC().Format(time.RFC3339Nano)
					record.RouteVisibleNanoseconds = value.at.Sub(t0).Nanoseconds()
					cancelPublish()
				}
			case result := <-publishResult:
				publisherFinished = true
				record.Transmissions = result.transmissions
				if result.err != nil {
					record.Error = fmt.Sprintf("route PUBLISH failed: %v", result.err)
				}
				publishResult = nil
			case <-timer.C:
				record.Error = "route probe timed out waiting for SUBACK and matching PUBLISH"
			case <-probeCtx.Done():
				if cause := context.Cause(ctx); cause != nil {
					record.Error = cause.Error()
				}
			}
		}
		cancelPublish()
		if !publisherFinished {
			result := <-publishResult
			record.Transmissions = result.transmissions
			if result.err != nil && record.Error == "" {
				record.Error = fmt.Sprintf("route PUBLISH failed: %v", result.err)
			}
		}
		timer.Stop()
		cancelProbe()
		if record.Error == "" && ack != nil && firstDelivery != nil {
			record.Succeeded = true
		} else if record.Error == "" {
			record.Error = "route probe did not observe both a successful SUBACK and a delivery"
		}
		col.recordRouteProbe(record)
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
	}
	return nil
}

func publishRouteProbe(ctx context.Context, publisher *runClient, profile Profile, topic string, sequence *uint64, start time.Time, result chan<- routePublishResult, col *collector) {
	interval := time.Duration(profile.RouteProbeIntervalMillis) * time.Millisecond
	next := start
	transmissions := 0
	for {
		if err := waitUntil(ctx, next); err != nil {
			result <- routePublishResult{transmissions: transmissions}
			return
		}
		*sequence++
		payload := makePayload(profile.Seed, *sequence, 4, profile.PayloadBytes)
		if err := publisher.client.Write(&mqtt.Publish{
			Version: mqtt.ProtocolV5,
			QoS:     mqtt.QoSAtMostOnce,
			Topic:   topic,
			Payload: payload,
		}, time.Second); err != nil {
			col.transportError()
			result <- routePublishResult{transmissions: transmissions, err: err}
			return
		}
		transmissions++
		next = next.Add(interval)
	}
}
