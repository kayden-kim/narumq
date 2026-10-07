package main

import (
	"fmt"
	"strings"
)

const bp02NodeCount = 3

const bp02ClusterID = "narumq-bp02"

type streamSpec struct {
	name, tenant, publisherID string
	qos                       byte
	subscriberCount           int
	persistent                bool
	filter                    string
}

type clientSpec struct {
	id, tenant, role                  string
	stream, index, ordinal, nodeIndex int
	filter                            string
	subscribeQoS                      byte
	persistent, tls                   bool
}

var bp02Streams = [8]streamSpec{
	{name: "a_telemetry", tenant: "bp02-a", publisherID: "bp02-a-pub-telemetry", qos: 0, subscriberCount: 6, filter: "bp02/a/telemetry/+"},
	{name: "a_alert", tenant: "bp02-a", publisherID: "bp02-a-pub-alert", qos: 1, subscriberCount: 4, persistent: true, filter: "bp02/a/alert/#"},
	{name: "a_command", tenant: "bp02-a", publisherID: "bp02-a-pub-command", qos: 2, subscriberCount: 4, persistent: true},
	{name: "a_job", tenant: "bp02-a", publisherID: "bp02-a-pub-job", qos: 1, subscriberCount: 4, filter: "$share/bp02a/bp02/a/job/#"},
	{name: "b_telemetry", tenant: "bp02-b", publisherID: "bp02-b-pub-telemetry", qos: 0, subscriberCount: 3, filter: "bp02/b/telemetry/+"},
	{name: "b_alert", tenant: "bp02-b", publisherID: "bp02-b-pub-alert", qos: 1, subscriberCount: 2, persistent: true, filter: "bp02/b/alert/#"},
	{name: "a_state", tenant: "bp02-a", publisherID: "bp02-a-pub-state", qos: 1, subscriberCount: 2, filter: "bp02/a/state/#"},
	{name: "b_state", tenant: "bp02-b", publisherID: "bp02-b-pub-state", qos: 1, subscriberCount: 2, filter: "bp02/b/state/#"},
}

// bp02ClientPlan returns every workload client in its stable planned ordinal order.
func bp02ClientPlan(profile Profile) []clientSpec {
	clients := make([]clientSpec, 0, 8+23+profile.IdleClientsTenantA+profile.IdleClientsTenantB+4)
	appendClient := func(spec clientSpec) {
		spec.ordinal = len(clients)
		spec.nodeIndex = spec.ordinal % bp02NodeCount
		spec.tls = spec.ordinal%profile.TLSEveryNthClient == 0
		clients = append(clients, spec)
	}
	for stream := range 6 {
		s := bp02Streams[stream]
		appendClient(clientSpec{id: s.publisherID, tenant: s.tenant, role: "publisher", stream: stream, index: -1})
	}
	for stream := 6; stream < 8; stream++ {
		s := bp02Streams[stream]
		appendClient(clientSpec{id: s.publisherID, tenant: s.tenant, role: "publisher", stream: stream, index: -1})
	}
	for stream := range 6 {
		s := bp02Streams[stream]
		for index := range s.subscriberCount {
			filter := s.filter
			if stream == 2 {
				filter = fmt.Sprintf("bp02/a/command/dev%02d", index)
			}
			suffix := []string{"telemetry", "alert", "command", "job", "telemetry", "alert"}[stream]
			appendClient(clientSpec{id: fmt.Sprintf("%s-sub-%s-%02d", s.tenant, suffix, index), tenant: s.tenant, role: "subscriber", stream: stream, index: index, filter: filter, subscribeQoS: s.qos, persistent: s.persistent})
		}
	}
	for index := range profile.IdleClientsTenantA {
		appendClient(clientSpec{id: fmt.Sprintf("bp02-a-idle-%03d", index), tenant: "bp02-a", role: "idle", stream: -1, index: index})
	}
	for index := range profile.IdleClientsTenantB {
		appendClient(clientSpec{id: fmt.Sprintf("bp02-b-idle-%03d", index), tenant: "bp02-b", role: "idle", stream: -1, index: index})
	}
	for stream := 6; stream < 8; stream++ {
		s := bp02Streams[stream]
		for index := range s.subscriberCount {
			appendClient(clientSpec{id: fmt.Sprintf("%s-sub-state-%02d", s.tenant, index), tenant: s.tenant, role: "retained-subscriber", stream: stream, index: index, filter: s.filter, subscribeQoS: s.qos})
		}
	}
	return clients
}

// bp02SetupClients returns planned clients that connect before workload phases.
func bp02SetupClients(profile Profile) []clientSpec {
	clients := bp02ClientPlan(profile)
	setup := make([]clientSpec, 0, len(clients)-4)
	for _, client := range clients {
		if client.role != "retained-subscriber" {
			setup = append(setup, client)
		}
	}
	return setup
}

// bp02Subscribers returns only the six normal delivery streams.
func bp02Subscribers(profile Profile) []clientSpec {
	clients := bp02ClientPlan(profile)
	subscribers := make([]clientSpec, 0, 23)
	for _, client := range clients {
		if client.role == "subscriber" {
			subscribers = append(subscribers, client)
		}
	}
	return subscribers
}

func bp02AllowedClientIDs(profile Profile, tenant string) []string {
	ids := make([]string, 0)
	seen := make(map[string]struct{})
	appendID := func(id string) {
		if _, exists := seen[id]; exists {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for _, client := range bp02ClientPlan(profile) {
		if client.tenant == tenant {
			appendID(client.id)
		}
	}
	if tenant == "bp02-a" {
		for from := 1; from <= bp02NodeCount; from++ {
			for to := 1; to <= bp02NodeCount; to++ {
				if from != to {
					appendID(fmt.Sprintf("bp02-probe-pub-%d-%d", from, to))
					appendID(fmt.Sprintf("bp02-probe-sub-%d-%d", from, to))
				}
			}
		}
		for node := 1; node <= bp02NodeCount; node++ {
			appendID(fmt.Sprintf("bp02-probe-tls-%d", node))
		}
	}
	return ids
}

func publishTopic(stream int, sequence uint64) string {
	if stream < 0 || stream >= len(bp02Streams) || sequence == 0 {
		return ""
	}
	index := sequence - 1
	switch stream {
	case 0:
		return fmt.Sprintf("bp02/a/telemetry/dev%02d", index%8)
	case 1:
		return fmt.Sprintf("bp02/a/alert/zone%d", index%4)
	case 2:
		return fmt.Sprintf("bp02/a/command/dev%02d", index%4)
	case 3:
		return fmt.Sprintf("bp02/a/job/%d", index%4)
	case 4:
		return fmt.Sprintf("bp02/b/telemetry/dev%02d", index%8)
	case 5:
		return fmt.Sprintf("bp02/b/alert/zone%d", index%4)
	case 6:
		return fmt.Sprintf("bp02/a/state/dev%02d", index)
	case 7:
		return fmt.Sprintf("bp02/b/state/dev%02d", index)
	default:
		return ""
	}
}

func retainedTopic(profile Profile, stream int, sequence uint64) string {
	count := profile.RetainedTopicsTenantA
	if stream == 7 {
		count = profile.RetainedTopicsTenantB
	}
	if (stream != 6 && stream != 7) || sequence == 0 || count <= 0 {
		return ""
	}
	return publishTopic(stream, (sequence-1)%uint64(count)+1)
}

func topicMatches(filter, topic string) bool {
	if strings.HasPrefix(filter, "$share/") {
		groupAndFilter := strings.TrimPrefix(filter, "$share/")
		group, sharedFilter, found := strings.Cut(groupAndFilter, "/")
		if !found || group == "" || sharedFilter == "" {
			return false
		}
		filter = sharedFilter
	}
	if filter == "" || topic == "" {
		return false
	}
	filterLevels, topicLevels := strings.Split(filter, "/"), strings.Split(topic, "/")
	for index, level := range filterLevels {
		if level == "#" {
			if index == 0 && strings.HasPrefix(topicLevels[0], "$") {
				return false
			}
			return index == len(filterLevels)-1
		}
		if index >= len(topicLevels) {
			return false
		}
		if level == "+" {
			if index == 0 && strings.HasPrefix(topicLevels[index], "$") {
				return false
			}
			continue
		}
		if level != topicLevels[index] {
			return false
		}
	}
	return len(filterLevels) == len(topicLevels)
}

func expectedRecipients(subscribers []clientSpec, stream int, topic string) []int {
	recipients := make([]int, 0)
	for index, subscriber := range subscribers {
		if subscriber.stream != stream || !topicMatches(subscriber.filter, topic) {
			continue
		}
		recipients = append(recipients, index)
	}
	return recipients
}
