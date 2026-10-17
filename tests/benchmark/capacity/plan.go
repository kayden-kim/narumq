package main

import (
	"fmt"
	"math/bits"
	"strings"
)

const (
	roleSubscriber      = "subscriber"
	roleStateSubscriber = "state-subscriber"
	rolePublisher       = "publisher"
	roleIdle            = "idle"
)

// streamDef is one of the 8 §3.8 streams, replicated per cell.
type streamDef struct {
	name        string
	kind        byte // tenant kind: 'a' or 'b'
	qos         byte
	subscribers int
	persistent  bool
	shared      bool
	retained    bool
	segment     string // topic segment after the cell level
	cycle       int    // distinct topics; 0 means the profile's retained topic count
}

var streamDefs = [cellStreams]streamDef{
	{name: "a_telemetry", kind: 'a', qos: 0, subscribers: 6, segment: "telemetry", cycle: 8},
	{name: "a_alert", kind: 'a', qos: 1, subscribers: 4, persistent: true, segment: "alert", cycle: 4},
	{name: "a_command", kind: 'a', qos: 2, subscribers: 4, persistent: true, segment: "command", cycle: 4},
	{name: "a_job", kind: 'a', qos: 1, subscribers: 4, shared: true, segment: "job", cycle: 4},
	{name: "b_telemetry", kind: 'b', qos: 0, subscribers: 3, segment: "telemetry", cycle: 8},
	{name: "b_alert", kind: 'b', qos: 1, subscribers: 2, persistent: true, segment: "alert", cycle: 4},
	{name: "a_state", kind: 'a', qos: 1, subscribers: 2, retained: true, segment: "state"},
	{name: "b_state", kind: 'b', qos: 1, subscribers: 2, retained: true, segment: "state"},
}

type clientSpec struct {
	id, tenant, role string
	cell, stream     int // stream is -1 for idle clients
	index, ordinal   int // index within stream or idle pool; ordinal within cell
	filter           string
	qos              byte
	persistent, tls  bool
}

// recipient is the oracle result for one topic of one stream: bits are subscriber indexes
// within the stream. A shared stream expects exactly one copy among the set bits.
type recipient struct {
	mask   uint8
	shared bool
}

func (r recipient) expected() int {
	if r.shared {
		return min(1, bits.OnesCount8(r.mask))
	}
	return bits.OnesCount8(r.mask)
}

type streamPlan struct {
	def        *streamDef
	topics     []string
	recipients []recipient
	subs       []int // indexes into cellPlan.clients, ordered by subscriber index
}

type cellPlan struct {
	cell      int
	clients   []clientSpec
	streams   [cellStreams]streamPlan
	publisher [cellStreams]int // indexes into clients
}

func tenantID(kind byte, cell int) string { return fmt.Sprintf("bp02-%c-c%03d", kind, cell) }

func topicRoot(kind byte, cell int) string { return fmt.Sprintf("bp02/%c/c%03d", kind, cell) }

func sharedGroup(cell int) string { return fmt.Sprintf("bp02a-c%03d", cell) }

func streamCycle(p Profile, stream int) int {
	if def := streamDefs[stream]; def.cycle > 0 {
		return def.cycle
	}
	if stream == 6 {
		return p.RetainedTopicsTenantA
	}
	return p.RetainedTopicsTenantB
}

func streamTopic(cell, stream, index int) string {
	def := streamDefs[stream]
	root := topicRoot(def.kind, cell)
	switch def.segment {
	case "telemetry", "command", "state":
		return fmt.Sprintf("%s/%s/dev%02d", root, def.segment, index)
	case "alert":
		return fmt.Sprintf("%s/alert/zone%d", root, index)
	default:
		return fmt.Sprintf("%s/job/%d", root, index)
	}
}

func subscriberFilter(cell, stream, index int) string {
	def := streamDefs[stream]
	root := topicRoot(def.kind, cell)
	switch {
	case stream == 2:
		return streamTopic(cell, stream, index)
	case def.shared:
		return "$share/" + sharedGroup(cell) + "/" + root + "/job/#"
	case def.segment == "telemetry":
		return root + "/telemetry/+"
	default:
		return root + "/" + def.segment + "/#"
	}
}

func publisherID(kind byte, cell int, segment string) string {
	return fmt.Sprintf("bp02-%c-c%03d-pub-%s", kind, cell, segment)
}

// buildCell returns the 53 clients of one cell in the §3.8 planned order: stream 0-5
// publishers, state publishers, stream 0-5 subscribers, idle A then B, state subscribers.
func buildCell(p Profile, cell int) *cellPlan {
	plan := &cellPlan{cell: cell, clients: make([]clientSpec, 0, cellClients)}
	add := func(spec clientSpec) int {
		spec.cell = cell
		spec.ordinal = len(plan.clients)
		spec.tls = spec.ordinal%p.TLSEveryNthClient == 0
		plan.clients = append(plan.clients, spec)
		return spec.ordinal
	}
	for stream := range cellStreams {
		def := &streamDefs[stream]
		plan.streams[stream].def = def
	}
	addPublisher := func(stream int) {
		def := &streamDefs[stream]
		plan.publisher[stream] = add(clientSpec{
			id: publisherID(def.kind, cell, def.segment), tenant: tenantID(def.kind, cell),
			role: rolePublisher, stream: stream, qos: def.qos,
		})
	}
	for stream := range 6 {
		addPublisher(stream)
	}
	addPublisher(6)
	addPublisher(7)
	addSubscribers := func(stream int, role string) {
		def := &streamDefs[stream]
		for index := range def.subscribers {
			ordinal := add(clientSpec{
				id:     fmt.Sprintf("bp02-%c-c%03d-sub-%s-%02d", def.kind, cell, def.segment, index),
				tenant: tenantID(def.kind, cell), role: role, stream: stream, index: index,
				filter: subscriberFilter(cell, stream, index), qos: def.qos, persistent: def.persistent,
			})
			plan.streams[stream].subs = append(plan.streams[stream].subs, ordinal)
		}
	}
	for stream := range 6 {
		addSubscribers(stream, roleSubscriber)
	}
	for index := range idleTenantA {
		add(clientSpec{id: fmt.Sprintf("bp02-a-c%03d-idle-%03d", cell, index), tenant: tenantID('a', cell), role: roleIdle, stream: -1, index: index})
	}
	for index := range idleTenantB {
		add(clientSpec{id: fmt.Sprintf("bp02-b-c%03d-idle-%03d", cell, index), tenant: tenantID('b', cell), role: roleIdle, stream: -1, index: index})
	}
	addSubscribers(6, roleStateSubscriber)
	addSubscribers(7, roleStateSubscriber)

	for stream := range cellStreams {
		sp := &plan.streams[stream]
		cycle := streamCycle(p, stream)
		sp.topics = make([]string, cycle)
		sp.recipients = make([]recipient, cycle)
		for index := range cycle {
			topic := streamTopic(cell, stream, index)
			sp.topics[index] = topic
			var mask uint8
			for subIndex, ordinal := range sp.subs {
				if topicMatches(plan.clients[ordinal].filter, topic) {
					mask |= 1 << subIndex
				}
			}
			sp.recipients[index] = recipient{mask: mask, shared: sp.def.shared}
		}
	}
	return plan
}

// publishTopic returns the topic of message seq (1-based) of a stream.
func (plan *cellPlan) publishTopic(stream int, seq uint64) string {
	sp := &plan.streams[stream]
	return sp.topics[(seq-1)%uint64(len(sp.topics))]
}

func (plan *cellPlan) recipientFor(stream int, seq uint64) recipient {
	sp := &plan.streams[stream]
	return sp.recipients[(seq-1)%uint64(len(sp.recipients))]
}

// tenantClientIDs lists the exact client IDs a tenant's identity may use in one cell.
func tenantClientIDs(p Profile, kind byte, cell int) []string {
	tenant := tenantID(kind, cell)
	ids := make([]string, 0, 32)
	for _, spec := range buildCell(p, cell).clients {
		if spec.tenant == tenant {
			ids = append(ids, spec.id)
		}
	}
	return ids
}

func countTLSClients(p Profile) int {
	count := 0
	for ordinal := range cellClients {
		if ordinal%p.TLSEveryNthClient == 0 {
			count++
		}
	}
	return count
}

// topicMatches is the independent MQTT oracle: it strips a shared prefix and matches '+' and a final '#'.
func topicMatches(filter, topic string) bool {
	if strings.HasPrefix(filter, "$share/") {
		_, sharedFilter, found := strings.Cut(strings.TrimPrefix(filter, "$share/"), "/")
		if !found || sharedFilter == "" {
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
			return index == len(filterLevels)-1 && !(index == 0 && strings.HasPrefix(topicLevels[0], "$"))
		}
		if index >= len(topicLevels) {
			return false
		}
		if level == "+" {
			if index == 0 && strings.HasPrefix(topicLevels[0], "$") {
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
