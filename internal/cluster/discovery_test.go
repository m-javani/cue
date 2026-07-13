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

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
	"github.com/m-javani/cue/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewServiceDiscovery(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	tests := []struct {
		name              string
		seed              []model.PeerInfo
		selfNodeID        string
		discoveryKind     internal.DiscoveryKind
		discoveryHTTPHost string
		refreshInterval   time.Duration
		expectError       bool
		errorContains     string
	}{
		{
			name: "successful static discovery",
			seed: []model.PeerInfo{
				{NodeID: "node1", Host: "127.0.0.1"},
				{NodeID: "node2", Host: "127.0.0.1"},
			},
			selfNodeID:        "node1",
			discoveryKind:     internal.DiscoveryKindStatic,
			discoveryHTTPHost: "",
			refreshInterval:   30 * time.Second,
			expectError:       false,
		},
		{
			name: "self node not in seed - gets added",
			seed: []model.PeerInfo{
				{NodeID: "node2", Host: "127.0.0.1"},
			},
			selfNodeID:        "node1",
			discoveryKind:     internal.DiscoveryKindStatic,
			discoveryHTTPHost: "",
			refreshInterval:   30 * time.Second,
			expectError:       false,
		},
		{
			name: "http discovery missing endpoint",
			seed: []model.PeerInfo{
				{NodeID: "node1", Host: "127.0.0.1"},
			},
			selfNodeID:        "node1",
			discoveryKind:     internal.DiscoveryKindHttp,
			discoveryHTTPHost: "",
			refreshInterval:   30 * time.Second,
			expectError:       true,
			errorContains:     "http endpoint required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sd, err := NewServiceDiscovery(
				logger,
				tt.seed,
				tt.selfNodeID,
				tt.discoveryKind,
				tt.discoveryHTTPHost,
				tt.refreshInterval,
			)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				assert.Nil(t, sd)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, sd)
				assert.Equal(t, tt.selfNodeID, sd.selfNodeID)
				assert.Equal(t, tt.discoveryKind, sd.discoveryKind)
				assert.Equal(t, tt.discoveryHTTPHost, sd.discoveryHTTPHost)
				assert.Equal(t, tt.refreshInterval, sd.refreshInterval)
			}
		})
	}
}

func TestServiceDiscovery_Lookup(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	seed := []model.PeerInfo{
		{NodeID: "node1", Host: "127.0.0.1"},
		{NodeID: "node2", Host: "127.0.0.1"},
		{NodeID: "node3", Host: "127.0.0.1"},
	}

	sd, err := NewServiceDiscovery(logger, seed, "node1", internal.DiscoveryKindStatic, "", 30*time.Second)
	require.NoError(t, err)

	tests := []struct {
		name     string
		nodeID   string
		expectOk bool
	}{
		{
			name:     "existing node",
			nodeID:   "node2",
			expectOk: true,
		},
		{
			name:     "non-existent node",
			nodeID:   "node4",
			expectOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peer, ok := sd.Lookup(tt.nodeID)
			assert.Equal(t, tt.expectOk, ok)
			if tt.expectOk {
				assert.Equal(t, tt.nodeID, peer.NodeID)
			}
		})
	}
}

func TestServiceDiscovery_ListPeers(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	seed := []model.PeerInfo{
		{NodeID: "node1", Host: "127.0.0.1"},
		{NodeID: "node2", Host: "127.0.0.1"},
	}

	sd, err := NewServiceDiscovery(logger, seed, "node1", internal.DiscoveryKindStatic, "", 30*time.Second)
	require.NoError(t, err)

	peers := sd.ListPeers()
	assert.Len(t, peers, 2)

	// Verify it's a copy
	peers[0] = model.PeerInfo{NodeID: "modified", Host: "127.0.0.1"}
	newPeers := sd.ListPeers()
	assert.NotEqual(t, peers[0], newPeers[0])
}

func TestServiceDiscovery_ListPeersRaftIDs(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	seed := []model.PeerInfo{
		{NodeID: "node1", Host: "127.0.0.1"},
		{NodeID: "node2", Host: "127.0.0.1"},
	}

	sd, err := NewServiceDiscovery(logger, seed, "node1", internal.DiscoveryKindStatic, "", 30*time.Second)
	require.NoError(t, err)

	raftIDs := sd.ListPeersRaftIDs()
	assert.Len(t, raftIDs, 2)

	expectedID1 := utils.StringToUint64("node1")
	expectedID2 := utils.StringToUint64("node2")
	assert.Contains(t, raftIDs, expectedID1)
	assert.Contains(t, raftIDs, expectedID2)
}

func TestServiceDiscovery_GetNodeIDFromRaftID(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	seed := []model.PeerInfo{
		{NodeID: "node1", Host: "127.0.0.1"},
		{NodeID: "node2", Host: "127.0.0.1"},
	}

	sd, err := NewServiceDiscovery(logger, seed, "node1", internal.DiscoveryKindStatic, "", 30*time.Second)
	require.NoError(t, err)

	raftID := utils.StringToUint64("node2")
	nodeID, ok := sd.GetNodeIDFromRaftID(raftID)
	assert.True(t, ok)
	assert.Equal(t, "node2", nodeID)

	// Non-existent raft ID
	_, ok = sd.GetNodeIDFromRaftID(999999)
	assert.False(t, ok)
}

func TestServiceDiscovery_GetRaftIDFromNode(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	seed := []model.PeerInfo{
		{NodeID: "node1", Host: "127.0.0.1"},
		{NodeID: "node2", Host: "127.0.0.1"},
	}

	sd, err := NewServiceDiscovery(logger, seed, "node1", internal.DiscoveryKindStatic, "", 30*time.Second)
	require.NoError(t, err)

	raftID, ok := sd.GetRaftIDFromNode("node2")
	assert.True(t, ok)
	assert.Equal(t, utils.StringToUint64("node2"), raftID)

	// Non-existent node
	_, ok = sd.GetRaftIDFromNode("node3")
	assert.False(t, ok)
}

func TestServiceDiscovery_UpdatePeers(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	seed := []model.PeerInfo{
		{NodeID: "node1", Host: "127.0.0.1"},
	}

	sd, err := NewServiceDiscovery(logger, seed, "node1", internal.DiscoveryKindStatic, "", 30*time.Second)
	require.NoError(t, err)

	newPeers := []model.PeerInfo{
		{NodeID: "node1", Host: "127.0.0.1"},
		{NodeID: "node2", Host: "127.0.0.1"},
		{NodeID: "node3", Host: "127.0.0.1"},
	}

	sd.UpdatePeers(newPeers)
	peers := sd.ListPeers()
	assert.Len(t, peers, 3)

	// Verify consistency
	assert.True(t, sd.ValidateConsistency())
}

func TestServiceDiscovery_MergePeers(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	seed := []model.PeerInfo{
		{NodeID: "node1", Host: "127.0.0.1"},
		{NodeID: "node2", Host: "127.0.0.1"},
	}

	sd, err := NewServiceDiscovery(logger, seed, "node1", internal.DiscoveryKindStatic, "", 30*time.Second)
	require.NoError(t, err)

	newPeers := []model.PeerInfo{
		{NodeID: "node3", Host: "127.0.0.1"},
		{NodeID: "node4", Host: "127.0.0.1"},
	}

	sd.MergePeers(newPeers)
	peers := sd.ListPeers()
	assert.Len(t, peers, 4)

	// Try merging duplicate
	sd.MergePeers([]model.PeerInfo{{NodeID: "node1", Host: "127.0.0.1"}})
	peers = sd.ListPeers()
	assert.Len(t, peers, 4) // Should stay the same

	// Verify consistency
	assert.True(t, sd.ValidateConsistency())
}

func TestServiceDiscovery_ValidateConsistency(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	seed := []model.PeerInfo{
		{NodeID: "node1", Host: "127.0.0.1"},
		{NodeID: "node2", Host: "127.0.0.1"},
	}

	sd, err := NewServiceDiscovery(logger, seed, "node1", internal.DiscoveryKindStatic, "", 30*time.Second)
	require.NoError(t, err)

	assert.True(t, sd.ValidateConsistency())

	// Corrupt internal state to test inconsistency
	sd.peers.Lock()
	sd.peersList = []model.PeerInfo{{NodeID: "node3", Host: "127.0.0.1"}}
	sd.peers.Unlock()

	assert.False(t, sd.ValidateConsistency())
}

func TestServiceDiscovery_HTTPDiscovery(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	tests := []struct {
		name          string
		response      interface{}
		statusCode    int
		expectedPeers []model.PeerInfo
		expectError   bool
	}{
		{
			name: "versioned response",
			response: httpPeersResponse{
				Version: 1,
				Peers: []model.PeerInfo{
					{NodeID: "node1", Host: "127.0.0.1"},
					{NodeID: "node2", Host: "127.0.0.1"},
				},
			},
			statusCode: http.StatusOK,
			expectedPeers: []model.PeerInfo{
				{NodeID: "node1", Host: "127.0.0.1"},
				{NodeID: "node2", Host: "127.0.0.1"},
			},
			expectError: false,
		},
		{
			name: "bare array response",
			response: []model.PeerInfo{
				{NodeID: "node1", Host: "127.0.0.1"},
				{NodeID: "node2", Host: "127.0.0.1"},
			},
			statusCode: http.StatusOK,
			expectedPeers: []model.PeerInfo{
				{NodeID: "node1", Host: "127.0.0.1"},
				{NodeID: "node2", Host: "127.0.0.1"},
			},
			expectError: false,
		},
		{
			name: "unsupported version",
			response: httpPeersResponse{
				Version: 2,
				Peers: []model.PeerInfo{
					{NodeID: "node1", Host: "127.0.0.1"},
				},
			},
			statusCode:  http.StatusOK,
			expectError: true,
		},
		{
			name:          "empty response",
			response:      []model.PeerInfo{},
			statusCode:    http.StatusOK,
			expectedPeers: []model.PeerInfo{},
			expectError:   false,
		},
		{
			name:        "server error",
			response:    nil,
			statusCode:  http.StatusInternalServerError,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test HTTP server
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				if tt.response != nil {
					json.NewEncoder(w).Encode(tt.response)
				}
			}))
			defer server.Close()

			seed := []model.PeerInfo{{NodeID: "self", Host: "127.0.0.1"}}
			sd, err := NewServiceDiscovery(
				logger,
				seed,
				"self",
				internal.DiscoveryKindHttp,
				server.URL,
				10*time.Millisecond,
			)
			require.NoError(t, err)

			// Run sync - capture the error from refreshFromHTTP
			peers, err := sd.refreshFromHTTP(context.Background())

			if tt.expectError {
				assert.Error(t, err)
				// Verify peers unchanged from seed
				currentPeers := sd.ListPeers()
				assert.Len(t, currentPeers, 1)
				assert.Equal(t, "self", currentPeers[0].NodeID)
			} else {
				assert.NoError(t, err)
				if len(tt.expectedPeers) > 0 {
					assert.Equal(t, tt.expectedPeers, peers)
				} else {
					assert.Empty(t, peers)
				}
			}
		})
	}
}

func TestServiceDiscovery_Run(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	// Track number of calls
	var callCount int
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		mu.Unlock()

		// Return multiple peers including the self node
		response := httpPeersResponse{
			Version: 1,
			Peers: []model.PeerInfo{
				{
					NodeID: "self",
					Host:   "127.0.0.1",
					Identity: model.TLSIdentity{
						Kind:  model.IdentityIP,
						Value: "127.0.0.1",
					},
				},
				{
					NodeID: fmt.Sprintf("node-%d", callCount),
					Host:   "127.0.0.1",
					Identity: model.TLSIdentity{
						Kind:  model.IdentityIP,
						Value: "127.0.0.1",
					},
				},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	seed := []model.PeerInfo{
		{
			NodeID: "self",
			Host:   "127.0.0.1",
			Identity: model.TLSIdentity{
				Kind:  model.IdentityIP,
				Value: "127.0.0.1",
			},
		},
	}
	sd, err := NewServiceDiscovery(
		logger,
		seed,
		"self",
		internal.DiscoveryKindHttp,
		server.URL,
		10*time.Millisecond,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run discovery in background
	go sd.Run(ctx)

	// Wait for at least one tick
	time.Sleep(1000 * time.Millisecond)

	// Cancel to stop the goroutine
	cancel()

	// Give a moment for the goroutine to clean up
	time.Sleep(50 * time.Millisecond)

	// Verify the peer list has been updated
	peers := sd.ListPeers()

	// Should have at least 2 peers (self + one from HTTP)
	assert.GreaterOrEqual(t, len(peers), 2, "should have self + at least one peer from HTTP")

	// Verify call count
	mu.Lock()
	count := callCount
	mu.Unlock()
	assert.GreaterOrEqual(t, count, 1, "should have called refresh at least once")
}

func TestServiceDiscovery_Run_Static(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("HTTP endpoint should not be called for static discovery")
	}))
	defer server.Close()

	seed := []model.PeerInfo{{NodeID: "node1", Host: "127.0.0.1"}}
	sd, err := NewServiceDiscovery(
		logger,
		seed,
		"node1",
		internal.DiscoveryKindStatic,
		server.URL,
		10*time.Millisecond,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	// Run should return immediately for static
	sd.Run(ctx)

	// Peers should remain unchanged
	peers := sd.ListPeers()
	assert.Len(t, peers, 1)
	assert.Equal(t, "node1", peers[0].NodeID)
}

func TestServiceDiscovery_ConcurrentAccess(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	seed := []model.PeerInfo{
		{NodeID: "node1", Host: "127.0.0.1"},
		{NodeID: "node2", Host: "127.0.0.1"},
	}

	sd, err := NewServiceDiscovery(logger, seed, "node1", internal.DiscoveryKindStatic, "", 30*time.Second)
	require.NoError(t, err)

	var wg sync.WaitGroup

	// Concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sd.ListPeers()
			_ = sd.ListPeersRaftIDs()
			_, _ = sd.Lookup("node1")
			_, _ = sd.GetRaftIDFromNode("node1")
			_, _ = sd.GetNodeIDFromRaftID(utils.StringToUint64("node1"))
		}()
	}

	// Concurrent writes
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sd.UpdatePeers([]model.PeerInfo{
				{NodeID: fmt.Sprintf("node%d", i), Host: "127.0.0.1"},
			})
		}(i)
	}

	wg.Wait()

	// Should remain consistent
	assert.True(t, sd.ValidateConsistency())
}

func TestServiceDiscovery_HTTPDiscovery_ContextCancellation(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		response := httpPeersResponse{
			Version: 1,
			Peers: []model.PeerInfo{
				{NodeID: "node1", Host: "127.0.0.1"},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	seed := []model.PeerInfo{{NodeID: "self", Host: "127.0.0.1"}}
	sd, err := NewServiceDiscovery(
		logger,
		seed,
		"self",
		internal.DiscoveryKindHttp,
		server.URL,
		10*time.Millisecond,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	peers, err := sd.refreshFromHTTP(ctx)
	assert.Error(t, err)
	assert.Nil(t, peers)
}

func TestServiceDiscovery_UpdateInternalState(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	seed := []model.PeerInfo{{NodeID: "node1", Host: "127.0.0.1"}}
	sd, err := NewServiceDiscovery(logger, seed, "node1", internal.DiscoveryKindStatic, "", 30*time.Second)
	require.NoError(t, err)

	newPeers := []model.PeerInfo{
		{NodeID: "node1", Host: "127.0.0.1"},
		{NodeID: "node2", Host: "127.0.0.1"},
		{NodeID: "node3", Host: "127.0.0.1"},
	}

	sd.updateInternalState(newPeers)

	peers := sd.ListPeers()
	assert.Len(t, peers, 3)

	raftIDs := sd.ListPeersRaftIDs()
	assert.Len(t, raftIDs, 3)

	for _, peer := range newPeers {
		raftID := utils.StringToUint64(peer.NodeID)
		foundRaftID, ok := sd.GetRaftIDFromNode(peer.NodeID)
		assert.True(t, ok)
		assert.Equal(t, raftID, foundRaftID)

		foundNode, ok := sd.GetNodeIDFromRaftID(raftID)
		assert.True(t, ok)
		assert.Equal(t, peer.NodeID, foundNode)
	}

	assert.True(t, sd.ValidateConsistency())
}

func TestLoadDiscoveryFile(t *testing.T) {
	tests := []struct {
		name          string
		content       string
		expectedPeers []model.PeerInfo
		expectError   bool
		errorContains string
	}{
		{
			name: "valid discovery file",
			content: `
nodes:
  - node_id: node1
    host: 192.168.1.10
    identity:
      kind: dns
      value: node1.example.com
  - node_id: node2
    host: 192.168.1.11
    identity:
      kind: ip
      value: 192.168.1.11
  - node_id: node3
    identity:
      kind: spiffe
      value: spiffe://example.org/node3
  - node_id: node4
    host: 192.168.1.12
    identity:
      kind: dns
      value: node4.example.com
`,
			expectedPeers: []model.PeerInfo{
				{
					NodeID: "node1",
					Host:   "192.168.1.10",
					Identity: model.TLSIdentity{
						Kind:  model.IdentityDNS,
						Value: "node1.example.com",
					},
				},
				{
					NodeID: "node2",
					Host:   "192.168.1.11",
					Identity: model.TLSIdentity{
						Kind:  model.IdentityIP,
						Value: "192.168.1.11",
					},
				},
				{
					NodeID: "node3",
					Host:   "", // Empty host - will fallback to NodeID
					Identity: model.TLSIdentity{
						Kind:  model.IdentitySPIFFE,
						Value: "spiffe://example.org/node3",
					},
				},
				{
					NodeID: "node4",
					Host:   "192.168.1.12",
					Identity: model.TLSIdentity{
						Kind:  model.IdentityDNS,
						Value: "node4.example.com",
					},
				},
			},
			expectError: false,
		},
		{
			name: "file with no nodes",
			content: `
nodes: []
`,
			expectedPeers: []model.PeerInfo{},
			expectError:   true,
			errorContains: "no nodes defined",
		},
		{
			name: "duplicate node_id",
			content: `
nodes:
  - node_id: node1
    host: 192.168.1.10
    identity:
      kind: dns
      value: node1.example.com
  - node_id: node1
    host: 192.168.1.11
    identity:
      kind: ip
      value: 192.168.1.11
`,
			expectError:   true,
			errorContains: "duplicate node_id",
		},
		{
			name: "missing required node_id",
			content: `
nodes:
  - host: 192.168.1.10
    identity:
      kind: dns
      value: node1.example.com
`,
			expectError:   true,
			errorContains: "node_id is required",
		},
		{
			name: "invalid identity kind",
			content: `
nodes:
  - node_id: node1
    host: 192.168.1.10
    identity:
      kind: invalid
      value: node1.example.com
`,
			expectError:   true,
			errorContains: "unknown identity kind",
		},
		{
			name: "empty identity value",
			content: `
nodes:
  - node_id: node1
    host: 192.168.1.10
    identity:
      kind: dns
      value: ""
`,
			expectError:   true,
			errorContains: "identity value is required",
		},
		{
			name: "SPIFFE identity without spiffe:// prefix",
			content: `
nodes:
  - node_id: node1
    host: 192.168.1.10
    identity:
      kind: spiffe
      value: example.org/node1
`,
			expectError:   true,
			errorContains: "SPIFFE identity must start with spiffe://",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp file
			tmpFile, err := os.CreateTemp("", "discovery-*.yaml")
			require.NoError(t, err)
			defer os.Remove(tmpFile.Name())

			_, err = tmpFile.WriteString(tt.content)
			require.NoError(t, err)
			err = tmpFile.Close()
			require.NoError(t, err)

			peers, err := LoadDiscoveryFile(tmpFile.Name())

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				assert.Nil(t, peers)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedPeers, peers)
			}
		})
	}
}

func TestLoadDiscoveryFile_FileNotFound(t *testing.T) {
	peers, err := LoadDiscoveryFile("/nonexistent/file.yaml")
	assert.Error(t, err)
	assert.Nil(t, peers)
	assert.Contains(t, err.Error(), "failed to read discovery file")
}

func TestLoadDiscoveryFile_InvalidYAML(t *testing.T) {
	// Create temp file with invalid YAML
	tmpFile, err := os.CreateTemp("", "discovery-*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString("invalid: yaml: [")
	require.NoError(t, err)
	err = tmpFile.Close()
	require.NoError(t, err)

	peers, err := LoadDiscoveryFile(tmpFile.Name())
	assert.Error(t, err)
	assert.Nil(t, peers)
	assert.Contains(t, err.Error(), "failed to parse discovery.yml")
}
