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

func TestStaticResolver_Resolve(t *testing.T) {
	resolver := StaticResolver{
		Addresses: map[string]string{
			"node1": "192.168.1.1:8080",
			"node2": "10.0.0.1:9090",
			"node3": "[2001:db8::1]:8080",
		},
	}

	tests := []struct {
		name    string
		nodeID  string
		want    string
		wantErr bool
	}{
		{
			name:   "existing node1",
			nodeID: "node1",
			want:   "192.168.1.1:8080",
		},
		{
			name:   "existing node2",
			nodeID: "node2",
			want:   "10.0.0.1:9090",
		},
		{
			name:   "existing node3 with IPv6",
			nodeID: "node3",
			want:   "[2001:db8::1]:8080",
		},
		{
			name:    "missing node",
			nodeID:  "node4",
			wantErr: true,
		},
		{
			name:    "empty nodeID",
			nodeID:  "",
			wantErr: true,
		},
		{
			name:    "case sensitive nodeID",
			nodeID:  "NODE1",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolver.Resolve(context.Background(), tt.nodeID)
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

// Test that StaticResolver implements AddressResolver interface
func TestStaticResolver_ImplementsInterface(t *testing.T) {
	var _ AddressResolver = StaticResolver{}
}
