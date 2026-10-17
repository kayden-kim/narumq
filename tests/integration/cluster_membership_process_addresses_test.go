package integration

import (
	"net"
	"strconv"
	"testing"
)

func TestClusterMembershipProcessPortPoolStaysBelowDynamicRange(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name         string
		dynamicRange string
		wantLow      int
		wantHigh     int
	}{
		{name: "host without a dynamic range file", dynamicRange: "", wantLow: 10000, wantHigh: 32768},
		{name: "Linux default range", dynamicRange: "32768\t60999\n", wantLow: 10000, wantHigh: 32768},
		{name: "dynamic range starts above the ceiling", dynamicRange: "49152\t65535\n", wantLow: 10000, wantHigh: 32768},
		{name: "dynamic range starts below the ceiling", dynamicRange: "20000\t60999\n", wantLow: 10000, wantHigh: 20000},
		{name: "dynamic range leaves no room", dynamicRange: "1024\t65535\n", wantLow: 0, wantHigh: 0},
		{name: "dynamic range barely leaves room", dynamicRange: "10500\t65535\n", wantLow: 0, wantHigh: 0},
		{name: "malformed range file", dynamicRange: "not a range", wantLow: 10000, wantHigh: 32768},
		{name: "range file with one number", dynamicRange: "20000\n", wantLow: 10000, wantHigh: 32768},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			low, high := clusterMembershipProcessPortPool(test.dynamicRange)
			if low != test.wantLow || high != test.wantHigh {
				t.Fatalf("clusterMembershipProcessPortPool(%q) = [%d, %d), want [%d, %d)",
					test.dynamicRange, low, high, test.wantLow, test.wantHigh)
			}
		})
	}
}

func TestClusterMembershipProcessAddressesAreDistinctLoopbackPortsInsidePool(t *testing.T) {
	t.Parallel()

	const count = 24
	addresses := clusterMembershipProcessAddresses(t, count)
	if len(addresses) != count {
		t.Fatalf("reserved %d addresses, want %d", len(addresses), count)
	}
	low, high := clusterMembershipProcessCurrentPortPool()
	seen := make(map[string]struct{}, count)
	for _, address := range addresses {
		host, portText, err := net.SplitHostPort(address)
		if err != nil || host != "127.0.0.1" {
			t.Fatalf("reserved address %q host=%q err=%v, want a 127.0.0.1 listener address", address, host, err)
		}
		if _, duplicate := seen[address]; duplicate {
			t.Fatalf("address %q was reserved twice in %v", address, addresses)
		}
		seen[address] = struct{}{}
		port, err := strconv.Atoi(portText)
		if err != nil {
			t.Fatalf("reserved address %q has no numeric port: %v", address, err)
		}
		if high > low && (port < low || port >= high) {
			t.Fatalf("reserved address %q is outside the pool [%d, %d)", address, low, high)
		}
	}
}
