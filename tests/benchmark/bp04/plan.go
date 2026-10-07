package main

import (
	"fmt"
	"strings"
)

const (
	bp04NodeCount   = 3
	bp04ClusterID   = "narumq-bp04"
	bp04TenantID    = "bp04"
	bp04SharedGroup = "bp04g"

	classFanout   = "fanout"
	classWildcard = "wildcard"
	classOverlap  = "overlap"
	classShared   = "shared"
)

type subscriberSpec struct {
	id, class          string
	ordinal, nodeIndex int
	filters            []string
}

type publisherSpec struct {
	id, class   string
	streamIndex int
	nodeIndex   int
}

func workloadSubscribers(profile Profile) []subscriberSpec {
	classes := []struct {
		name  string
		count int
	}{
		{classFanout, profile.FanoutSubscribers},
		{classWildcard, profile.WildcardSubscribers},
		{classOverlap, profile.OverlapSubscribers},
		{classShared, profile.SharedSubscribers},
	}
	subscribers := make([]subscriberSpec, 0, profile.FanoutSubscribers+profile.WildcardSubscribers+profile.OverlapSubscribers+profile.SharedSubscribers)
	ordinal := 0
	for _, class := range classes {
		for index := 0; index < class.count; index++ {
			subscriber := subscriberSpec{
				id:        fmt.Sprintf("bp04-sub-%s-%03d", class.name, index+1),
				class:     class.name,
				ordinal:   ordinal,
				nodeIndex: ordinal % bp04NodeCount,
			}
			switch class.name {
			case classFanout:
				subscriber.filters = []string{"bp04/fanout/all"}
			case classWildcard:
				group := (index / 6) % 4
				switch index % 6 {
				case 0:
					subscriber.filters = []string{fmt.Sprintf("bp04/wild/r%d/+", group)}
				case 1:
					subscriber.filters = []string{fmt.Sprintf("bp04/wild/+/d%d", group)}
				case 2:
					subscriber.filters = []string{"bp04/wild/#"}
				case 3:
					subscriber.filters = []string{fmt.Sprintf("bp04/wild/r%d/#", group)}
				case 4:
					subscriber.filters = []string{fmt.Sprintf("bp04/+/r%d/d%d", group, group)}
				case 5:
					subscriber.filters = []string{fmt.Sprintf("bp04/wild/r%d/d%d", group, group)}
				}
			case classOverlap:
				subscriber.filters = []string{"bp04/ovl/a/b", "bp04/ovl/+/b", "bp04/ovl/#"}
			case classShared:
				subscriber.filters = []string{fmt.Sprintf("$share/%s/bp04/shared/#", bp04SharedGroup)}
			}
			subscribers = append(subscribers, subscriber)
			ordinal++
		}
	}
	return subscribers
}

func workloadPublishers() []publisherSpec {
	return []publisherSpec{
		{id: "bp04-pub-fanout", class: classFanout, streamIndex: 0, nodeIndex: 0},
		{id: "bp04-pub-wildcard", class: classWildcard, streamIndex: 1, nodeIndex: 1},
		{id: "bp04-pub-overlap", class: classOverlap, streamIndex: 2, nodeIndex: 2},
		{id: "bp04-pub-shared", class: classShared, streamIndex: 3, nodeIndex: 0},
	}
}

func publishTopic(class string, sequence uint64) string {
	ordinal := sequence
	if ordinal > 0 {
		ordinal--
	}
	switch class {
	case classFanout:
		return "bp04/fanout/all"
	case classWildcard:
		return fmt.Sprintf("bp04/wild/r%d/d%d", ordinal%4, ordinal/4%4)
	case classOverlap:
		return "bp04/ovl/a/b"
	case classShared:
		return fmt.Sprintf("bp04/shared/j%d", ordinal%8)
	default:
		return ""
	}
}

func bp04ClientIDs(profile Profile) []string {
	subscribers := workloadSubscribers(profile)
	publishers := workloadPublishers()
	ids := make([]string, 0, len(subscribers)+len(publishers)+bp04NodeCount+profile.RouteProbes+bp04NodeCount*(bp04NodeCount-1)*2)
	for _, subscriber := range subscribers {
		ids = append(ids, subscriber.id)
	}
	for _, publisher := range publishers {
		ids = append(ids, publisher.id)
	}
	for node := 1; node <= bp04NodeCount; node++ {
		ids = append(ids, fmt.Sprintf("bp04-route-pub-%d", node))
	}
	for probe := 0; probe < profile.RouteProbes; probe++ {
		ids = append(ids, fmt.Sprintf("bp04-route-sub-%03d", probe))
	}
	for publisherNode := 1; publisherNode <= bp04NodeCount; publisherNode++ {
		for subscriberNode := 1; subscriberNode <= bp04NodeCount; subscriberNode++ {
			if publisherNode == subscriberNode {
				continue
			}
			ids = append(ids,
				fmt.Sprintf("bp04-probe-pub-%d-%d", publisherNode, subscriberNode),
				fmt.Sprintf("bp04-probe-sub-%d-%d", publisherNode, subscriberNode),
			)
		}
	}
	return ids
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
	filterLevels := strings.Split(filter, "/")
	topicLevels := strings.Split(topic, "/")
	for index, filterLevel := range filterLevels {
		if filterLevel == "#" {
			if index == 0 && strings.HasPrefix(topicLevels[0], "$") {
				return false
			}
			return index == len(filterLevels)-1
		}
		if index >= len(topicLevels) {
			return false
		}
		if filterLevel == "+" {
			if index == 0 && strings.HasPrefix(topicLevels[index], "$") {
				return false
			}
			continue
		}
		if filterLevel != topicLevels[index] {
			return false
		}
	}
	return len(filterLevels) == len(topicLevels)
}

func expectedRecipients(subscribers []subscriberSpec, class, topic string) []int {
	recipients := make([]int, 0)
	for index, subscriber := range subscribers {
		if subscriber.class != class {
			continue
		}
		for _, filter := range subscriber.filters {
			if topicMatches(filter, topic) {
				recipients = append(recipients, index)
				break
			}
		}
	}
	return recipients
}
