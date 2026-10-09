package audit

import (
	"fmt"
	"sync"
	"unicode/utf8"

	portsaudit "narumq/internal/application/ports/audit"
)

type perSecondLimiter struct {
	mu          sync.Mutex
	limit       int
	window      int64
	count       int
	initialized bool
}

func (limiter *perSecondLimiter) admit(unixSecond int64) bool {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	if !limiter.initialized || limiter.window != unixSecond {
		limiter.window = unixSecond
		limiter.count = 0
		limiter.initialized = true
	}
	if limiter.count >= limiter.limit {
		return false
	}
	limiter.count++
	return true
}

func brokerProducer(clusterID string, nodeID uint64, releaseVersion string) portsaudit.AuditProducerDTO {
	return portsaudit.AuditProducerDTO{
		Component:  "BROKER",
		InstanceID: fmt.Sprintf("%s/%d", clusterID, nodeID),
		Version:    releaseVersion,
	}
}

func validProducerIdentity(clusterID string, nodeID uint64, releaseVersion string) bool {
	return clusterID != "" && nodeID != 0 && utf8.ValidString(releaseVersion) && utf8.RuneCountInString(releaseVersion) > 0 && utf8.RuneCountInString(releaseVersion) <= 64
}
