package integration

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// Traceability: CLU-14; docs/specs/performance-and-verification.md §3.4.
func TestDynamicClusterConcurrentFreshReadProcessAcceptance(t *testing.T) {
	fixture, nodes := clusterDurableProcessStart(t)
	leader, _ := clusterMembershipProcessFindLeader(t, fixture.ctx, nodes, 3)
	follower := nodes[0]
	if leader == 1 {
		follower = nodes[1]
	}
	start := make(chan struct{})
	results := make(chan error, 64)
	started := time.Now()
	for index := range 64 {
		go func() {
			<-start
			ctx, cancel := context.WithTimeout(fixture.ctx, 4*time.Second)
			defer cancel()
			summary, status, body, err := clusterMembershipProcessGetSummary(ctx, follower.managementURL(), clusterMembershipProcessAdmin)
			if err == nil && (status != http.StatusOK || !summary.QuorumAvailable || summary.LeaderNodeID == nil || summary.MemberCount != 3) {
				err = fmt.Errorf("request %d status=%d summary=%+v body=%s", index, status, summary, body)
			}
			results <- err
		}()
	}
	close(start)
	for range 64 {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	t.Logf("64 concurrent follower summaries completed with fresh quorum proofs in %s", time.Since(started))
}
