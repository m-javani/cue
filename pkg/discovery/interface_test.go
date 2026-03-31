// Copyright 2026 M. Javani
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package discovery

import (
	"context"
	"testing"
)

// Test that all resolvers implement the interface
func TestResolversImplementInterface(t *testing.T) {
	var _ AddressResolver = DNSResolver{}
	var _ AddressResolver = ServiceResolver{}
	var _ AddressResolver = StaticResolver{}
}

// Test interface usage with different resolvers
func TestAddressResolver_Usage(t *testing.T) {
	resolvers := []struct {
		name      string
		resolver  AddressResolver
		nodeID    string
		expected  string
		shouldErr bool
	}{
		{
			name: "DNSResolver with mock",
			resolver: newDNSResolverWithResolver(8080, "service.local",
				mockResolver{addrs: []string{"192.168.1.1"}}),
			nodeID:   "api",
			expected: "192.168.1.1:8080",
		},
		{
			name: "StaticResolver",
			resolver: StaticResolver{
				Addresses: map[string]string{"node1": "192.168.1.1:8080"},
			},
			nodeID:   "node1",
			expected: "192.168.1.1:8080",
		},
	}

	for _, tt := range resolvers {
		t.Run(tt.name, func(t *testing.T) {
			addr, err := tt.resolver.Resolve(context.Background(), tt.nodeID)
			if tt.shouldErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if addr != tt.expected {
				t.Errorf("got %v, want %v", addr, tt.expected)
			}
		})
	}
}
