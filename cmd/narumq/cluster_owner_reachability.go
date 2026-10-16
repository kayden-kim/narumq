package main

// ownerReachable is the shared-subscription routing liveness signal (ADR-0060). A peer counts as
// unreachable only while its transport is known and has no connection. The local node and nodes the
// transport does not know are reachable. The answer is a routing preference and never changes ownership.
func (runtime *clusterRuntime) ownerReachable(ownerNodeID uint64) bool {
	if ownerNodeID == runtime.config.NodeID {
		return true
	}
	status, err := runtime.transport.Status(ownerNodeID)
	return err != nil || status.Connected
}
