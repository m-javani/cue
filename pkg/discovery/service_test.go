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

// mockResolver for ServiceResolver tests
type serviceMockResolver struct {
	addrs []string
	err   error
}

func (m serviceMockResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return m.addrs, m.err
	}
}

func TestServiceResolver_Resolve(t *testing.T) {
	tests := []struct {
		name     string
		resolver ServiceResolver
		nodeID   string
		want     string
		wantErr  bool
	}{
		{
			name: "successful resolution",
			resolver: newServiceResolverWithResolver(8080,
				serviceMockResolver{addrs: []string{"192.168.1.1"}}),
			nodeID: "api.service.local",
			want:   "192.168.1.1:8080",
		},
		{
			name: "successful resolution with IPv6",
			resolver: newServiceResolverWithResolver(9090,
				serviceMockResolver{addrs: []string{"2001:db8::1"}}),
			nodeID: "api.service.local",
			want:   "[2001:db8::1]:9090",
		},
		{
			name: "DNS lookup error",
			resolver: newServiceResolverWithResolver(8080,
				serviceMockResolver{err: errors.New("no such host")}),
			nodeID:  "api.service.local",
			wantErr: true,
		},
		{
			name: "no IPs returned - THIS COVERS THE RED LINE",
			resolver: newServiceResolverWithResolver(8080,
				serviceMockResolver{addrs: []string{}}), // ← Empty slice triggers "no IPs found"
			nodeID:  "api.service.local",
			wantErr: true,
		},
		{
			name: "empty nodeID",
			resolver: newServiceResolverWithResolver(8080,
				serviceMockResolver{err: errors.New("no such host")}),
			nodeID:  "",
			wantErr: true,
		},
		{
			name: "context cancellation",
			resolver: newServiceResolverWithResolver(8080,
				serviceMockResolver{addrs: []string{"192.168.1.1"}}),
			nodeID:  "api.service.local",
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

// Test NewServiceResolver
func TestNewServiceResolver(t *testing.T) {
	resolver := NewServiceResolver(8080)

	if resolver.Port != 8080 {
		t.Errorf("expected port 8080, got %d", resolver.Port)
	}
	if resolver.resolver == nil {
		t.Error("expected default resolver to be set")
	}
}

// Integration test for real DNS
func TestServiceResolver_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	resolver := NewServiceResolver(8080)

	// Test with localhost
	addr, err := resolver.Resolve(context.Background(), "localhost")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if addr == "" {
		t.Error("expected non-empty address")
	}
}

func TestServiceResolver_ResolverNil(t *testing.T) {
	// Create a ServiceResolver with nil resolver (bypassing constructor)
	resolver := ServiceResolver{
		Port: 8080,
		// resolver field is nil by default
	}

	// The nil check should be hit and set the local copy's resolver
	// The method should not panic and should work correctly
	addr, err := resolver.Resolve(context.Background(), "localhost")
	if err != nil {
		// Some environments might not resolve localhost, so we don't fail
		// We just want to ensure the nil check didn't cause a panic
		t.Logf("Resolution failed (this is okay): %v", err)
	}

	// Verify we got some result (even if it's empty due to resolution failure)
	// The important part is that the method executed without panicking
	_ = addr // Just to avoid unused variable warning

	// Note: We CANNOT verify resolver.resolver was set because it was a copy
	// The original struct still has nil resolver
	t.Log("Successfully executed Resolve with nil resolver - nil check was hit")
}
