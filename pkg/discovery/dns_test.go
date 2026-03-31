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
	"errors"
	"testing"
)

// mockResolver implements dnsLookuper for testing
type mockResolver struct {
	addrs []string
	err   error
}

func (m mockResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return m.addrs, m.err
	}
}

func TestNewDNSResolver(t *testing.T) {
	resolver := NewDNSResolver(8080, "service.local")

	// Verify the resolver was created with correct values
	if resolver.Port != 8080 {
		t.Errorf("expected port 8080, got %d", resolver.Port)
	}
	if resolver.Domain != "service.local" {
		t.Errorf("expected domain service.local, got %s", resolver.Domain)
	}
	if resolver.resolver == nil {
		t.Error("expected default resolver to be set")
	}
}

func TestDNSResolver_Resolve(t *testing.T) {
	tests := []struct {
		name     string
		resolver DNSResolver
		nodeID   string
		want     string
		wantErr  bool
	}{
		{
			name: "success with domain",
			resolver: newDNSResolverWithResolver(8080, "service.local",
				mockResolver{addrs: []string{"192.168.1.1"}}),
			nodeID: "api",
			want:   "192.168.1.1:8080",
		},
		{
			name: "success without domain",
			resolver: newDNSResolverWithResolver(8080, "",
				mockResolver{addrs: []string{"10.0.0.1"}}),
			nodeID: "api.service.local",
			want:   "10.0.0.1:8080",
		},
		{
			name: "success with IPv6",
			resolver: newDNSResolverWithResolver(9090, "service.local",
				mockResolver{addrs: []string{"2001:db8::1"}}),
			nodeID: "api",
			want:   "[2001:db8::1]:9090",
		},
		{
			name: "DNS lookup returns error",
			resolver: newDNSResolverWithResolver(8080, "service.local",
				mockResolver{err: errors.New("no such host")}),
			nodeID:  "api",
			wantErr: true,
		},
		{
			name: "DNS lookup returns no IPs",
			resolver: newDNSResolverWithResolver(8080, "service.local",
				mockResolver{addrs: []string{}}),
			nodeID:  "api",
			wantErr: true,
		},
		{
			name: "empty nodeID",
			resolver: newDNSResolverWithResolver(8080, "service.local",
				mockResolver{err: errors.New("lookup: no such host")}),
			nodeID:  "",
			wantErr: true,
		},
		{
			name: "context cancellation",
			resolver: newDNSResolverWithResolver(8080, "service.local",
				mockResolver{addrs: []string{"192.168.1.1"}}),
			nodeID:  "api",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			if tt.name == "context cancellation" {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err := tt.resolver.Resolve(ctx, tt.nodeID)
				if err == nil {
					t.Error("expected error with canceled context")
				}
				return
			}

			got, err := tt.resolver.Resolve(ctx, tt.nodeID)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// Integration test using real DNS (skipped in short mode)
func TestDNSResolver_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Test with a real domain that should resolve
	resolver := NewDNSResolver(8080, "")

	// Use localhost which should always resolve
	addr, err := resolver.Resolve(context.Background(), "localhost")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Should be something like "127.0.0.1:8080" or "[::1]:8080"
	if addr == "" {
		t.Error("expected non-empty address")
	}

	// Test with domain
	resolverWithDomain := NewDNSResolver(9090, "localhost")
	addr, err = resolverWithDomain.Resolve(context.Background(), "api")
	if err != nil {
		t.Errorf("unexpected error with domain: %v", err)
	}
	if addr == "" {
		t.Error("expected non-empty address with domain")
	}
}

// Use the shared test suite
func TestDNSResolver_Suite(t *testing.T) {
	resolver := newDNSResolverWithResolver(8080, "service.local",
		mockResolver{addrs: []string{"192.168.1.1"}})

	RunResolverTests(t, TestSuite{
		Name:       "dns_resolver_suite",
		Resolver:   resolver,
		NodeID:     "api",
		Expected:   "192.168.1.1:8080",
		ShouldFail: false,
	})
}
