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

package model

import (
	"testing"
)

func TestClusterNodeStatus_ToUin32(t *testing.T) {
	tests := []struct {
		name   string
		status ClusterNodeStatus
		want   uint32
	}{
		{
			name:   "FollowerActive to uint32",
			status: NodeStatusFollowerActive,
			want:   71,
		},
		{
			name:   "Unavailable to uint32",
			status: NodeStatusUnavailable,
			want:   72,
		},
		{
			name:   "LeaderActive to uint32",
			status: NodeStatusLeaderActive,
			want:   73,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.ToUin32(); got != tt.want {
				t.Errorf("ClusterNodeStatus.ToUin32() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClusterNodeStatus_String(t *testing.T) {
	tests := []struct {
		name   string
		status ClusterNodeStatus
		want   string
	}{
		{
			name:   "FollowerActive string representation",
			status: NodeStatusFollowerActive,
			want:   "follower",
		},
		{
			name:   "LeaderActive string representation",
			status: NodeStatusLeaderActive,
			want:   "leader",
		},
		{
			name:   "Unavailable string representation",
			status: NodeStatusUnavailable,
			want:   "unavailable",
		},
		{
			name:   "Unknown status string representation",
			status: ClusterNodeStatus(99),
			want:   "unknown",
		},
		{
			name:   "Zero value string representation",
			status: ClusterNodeStatus(0),
			want:   "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.String(); got != tt.want {
				t.Errorf("ClusterNodeStatus.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClusterNodeStatusFromUint32(t *testing.T) {
	tests := []struct {
		name  string
		value uint32
		want  ClusterNodeStatus
	}{
		{
			name:  "Convert 71 to FollowerActive",
			value: 71,
			want:  NodeStatusFollowerActive,
		},
		{
			name:  "Convert 73 to LeaderActive",
			value: 73,
			want:  NodeStatusLeaderActive,
		},
		{
			name:  "Convert 72 to Unavailable",
			value: 72,
			want:  NodeStatusUnavailable,
		},
		{
			name:  "Convert 0 to Unavailable",
			value: 0,
			want:  NodeStatusUnavailable,
		},
		{
			name:  "Convert 99 to Unavailable",
			value: 99,
			want:  NodeStatusUnavailable,
		},
		{
			name:  "Convert 74 to Unavailable",
			value: 74,
			want:  NodeStatusUnavailable,
		},
		{
			name:  "Convert max uint32 to Unavailable",
			value: ^uint32(0),
			want:  NodeStatusUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClusterNodeStatusFromUint32(tt.value); got != tt.want {
				t.Errorf("ClusterNodeStatusFromUint32() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Test constants values to ensure they match expected values
func TestClusterNodeStatusConstants(t *testing.T) {
	if NodeStatusFollowerActive != 71 {
		t.Errorf("NodeStatusFollowerActive = %v, want 71", NodeStatusFollowerActive)
	}
	if NodeStatusUnavailable != 72 {
		t.Errorf("NodeStatusUnavailable = %v, want 72", NodeStatusUnavailable)
	}
	if NodeStatusLeaderActive != 73 {
		t.Errorf("NodeStatusLeaderActive = %v, want 73", NodeStatusLeaderActive)
	}
}

// Test round-trip conversion
func TestClusterNodeStatusRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		status ClusterNodeStatus
	}{
		{
			name:   "Round trip for FollowerActive",
			status: NodeStatusFollowerActive,
		},
		{
			name:   "Round trip for LeaderActive",
			status: NodeStatusLeaderActive,
		},
		{
			name:   "Round trip for Unavailable",
			status: NodeStatusUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert to uint32 and back
			uintVal := tt.status.ToUin32()
			converted := ClusterNodeStatusFromUint32(uintVal)

			if converted != tt.status {
				t.Errorf("Round trip conversion failed: got %v, want %v", converted, tt.status)
			}

			// Also verify string representation matches
			if converted.String() != tt.status.String() {
				t.Errorf("String representation mismatch: got %v, want %v", converted.String(), tt.status.String())
			}
		})
	}
}
