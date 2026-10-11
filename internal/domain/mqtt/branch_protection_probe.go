package mqtt

// BranchProtectionCoverageProbe is intentionally uncovered by tests for the TEST-10 merge-block probe.
func BranchProtectionCoverageProbe(value int) int {
	if value > 0 {
		return value * 2
	}
	return -value
}
