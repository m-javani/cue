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
	"errors"
	"sync"
	"testing"

	"github.com/m-javani/cue/internal/testutils"
	"github.com/m-javani/cue/internal/utils"
	"go.uber.org/zap"
)

// mockAddressResolver implements discovery.AddressResolver for testing
type mockAddressResolver struct {
	resolveFunc func(ctx context.Context, node string) (string, error)
}

func (m *mockAddressResolver) Resolve(ctx context.Context, node string) (string, error) {
	if m.resolveFunc != nil {
		return m.resolveFunc(ctx, node)
	}
	return "", nil
}

// mockAddressResolverWithError returns an error for specific nodes
type mockAddressResolverWithError struct {
	errNodes map[string]bool
}

func (m *mockAddressResolverWithError) Resolve(ctx context.Context, node string) (string, error) {
	if m.errNodes[node] {
		return "", errors.New("resolve error")
	}
	return "192.168.1.1:8080", nil
}

func TestNewServiceDiscovery(t *testing.T) {
	logger := zap.NewNop()
	quicPort := uint16(8080)
	addressResolver := &mockAddressResolver{}

	tests := []struct {
		name          string
		seed          []string
		selfNodeID    string
		expectedPeers []string
	}{
		{
			name:          "self node not in seed",
			seed:          []string{"node2", "node3"},
			selfNodeID:    "node1",
			expectedPeers: []string{"node2", "node3", "node1"},
		},
		{
			name:          "self node already in seed",
			seed:          []string{"node1", "node2"},
			selfNodeID:    "node1",
			expectedPeers: []string{"node1", "node2"},
		},
		{
			name:          "empty seed",
			seed:          []string{},
			selfNodeID:    "node1",
			expectedPeers: []string{"node1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sd, err := NewServiceDiscovery(logger, tt.seed, quicPort, tt.selfNodeID, addressResolver)
			if err != nil {
				t.Fatalf("NewServiceDiscovery failed: %v", err)
			}

			// Check peers list
			peers := sd.ListPeers()
			if len(peers) != len(tt.expectedPeers) {
				t.Errorf("expected %d peers, got %d", len(tt.expectedPeers), len(peers))
			}

			// Check nodeToRaft map
			for _, peer := range tt.expectedPeers {
				raftID, ok := sd.GetRaftIDFromNode(peer)
				if !ok {
					t.Errorf("peer %s not found in nodeToRaft map", peer)
				}
				expectedRaftID := utils.StringToUint64(peer)
				if raftID != expectedRaftID {
					t.Errorf("expected raftID %d for node %s, got %d", expectedRaftID, peer, raftID)
				}
			}

			// Check raftToNode map
			for _, peer := range tt.expectedPeers {
				raftID := utils.StringToUint64(peer)
				node, ok := sd.GetNodeIDFromRaftID(raftID)
				if !ok {
					t.Errorf("raftID %d not found in raftToNode map", raftID)
				}
				if node != peer {
					t.Errorf("expected node %s for raftID %d, got %s", peer, raftID, node)
				}
			}
		})
	}
}

func TestGetNodeIDFromRaftID(t *testing.T) {
	logger := zap.NewNop()
	seed := []string{"node1", "node2"}
	sd, _ := NewServiceDiscovery(logger, seed, 8080, "node1", &mockAddressResolver{})

	tests := []struct {
		name          string
		raftID        uint64
		expectedNode  string
		expectedFound bool
	}{
		{
			name:          "existing raft ID",
			raftID:        utils.StringToUint64("node1"),
			expectedNode:  "node1",
			expectedFound: true,
		},
		{
			name:          "non-existing raft ID",
			raftID:        utils.StringToUint64("node3"),
			expectedNode:  "",
			expectedFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, found := sd.GetNodeIDFromRaftID(tt.raftID)
			if found != tt.expectedFound {
				t.Errorf("expected found %v, got %v", tt.expectedFound, found)
			}
			if node != tt.expectedNode {
				t.Errorf("expected node %s, got %s", tt.expectedNode, node)
			}
		})
	}
}

func TestGetRaftIDFromNode(t *testing.T) {
	logger := zap.NewNop()
	seed := []string{"node1", "node2"}
	sd, _ := NewServiceDiscovery(logger, seed, 8080, "node1", &mockAddressResolver{})

	tests := []struct {
		name          string
		node          string
		expectedID    uint64
		expectedFound bool
	}{
		{
			name:          "existing node",
			node:          "node1",
			expectedID:    utils.StringToUint64("node1"),
			expectedFound: true,
		},
		{
			name:          "non-existing node",
			node:          "node3",
			expectedID:    0,
			expectedFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raftID, found := sd.GetRaftIDFromNode(tt.node)
			if found != tt.expectedFound {
				t.Errorf("expected found %v, got %v", tt.expectedFound, found)
			}
			if raftID != tt.expectedID {
				t.Errorf("expected raftID %d, got %d", tt.expectedID, raftID)
			}
		})
	}
}

func TestListPeers(t *testing.T) {
	logger := zap.NewNop()
	seed := []string{"node1", "node2"}
	sd, _ := NewServiceDiscovery(logger, seed, 8080, "node1", &mockAddressResolver{})

	peers := sd.ListPeers()
	expected := []string{"node1", "node2"}

	if len(peers) != len(expected) {
		t.Errorf("expected %d peers, got %d", len(expected), len(peers))
	}

	for i, peer := range expected {
		if peers[i] != peer {
			t.Errorf("expected peer %s, got %s", peer, peers[i])
		}
	}

	// Test that it returns a copy (modifying returned slice shouldn't affect original)
	peers[0] = "modified"
	originalPeers := sd.ListPeers()
	if originalPeers[0] == "modified" {
		t.Error("ListPeers returned a reference to internal slice instead of a copy")
	}
}

func TestListPeersRaftIDs(t *testing.T) {
	logger := zap.NewNop()
	seed := []string{"node1", "node2"}
	sd, _ := NewServiceDiscovery(logger, seed, 8080, "node1", &mockAddressResolver{})

	raftIDs := sd.ListPeersRaftIDs()
	expected := []uint64{
		utils.StringToUint64("node1"),
		utils.StringToUint64("node2"),
	}

	if len(raftIDs) != len(expected) {
		t.Errorf("expected %d raft IDs, got %d", len(expected), len(raftIDs))
	}

	for i, id := range expected {
		if raftIDs[i] != id {
			t.Errorf("expected raftID %d, got %d", id, raftIDs[i])
		}
	}

	// Test that it returns a copy
	raftIDs[0] = 999
	originalIDs := sd.ListPeersRaftIDs()
	if originalIDs[0] == 999 {
		t.Error("ListPeersRaftIDs returned a reference to internal slice instead of a copy")
	}
}

func TestUpdatePeers(t *testing.T) {
	logger := zap.NewNop()
	sd, _ := NewServiceDiscovery(logger, []string{"node1", "node2"}, 8080, "node1", &mockAddressResolver{})

	newPeers := []string{"node3", "node4", "node5"}
	sd.UpdatePeers(newPeers)

	// Check peers list
	peers := sd.ListPeers()
	if len(peers) != len(newPeers) {
		t.Errorf("expected %d peers, got %d", len(newPeers), len(peers))
	}
	for i, peer := range newPeers {
		if peers[i] != peer {
			t.Errorf("expected peer %s, got %s", peer, peers[i])
		}
	}

	// Check raft IDs list
	raftIDs := sd.ListPeersRaftIDs()
	if len(raftIDs) != len(newPeers) {
		t.Errorf("expected %d raft IDs, got %d", len(newPeers), len(raftIDs))
	}
	for i, peer := range newPeers {
		expectedID := utils.StringToUint64(peer)
		if raftIDs[i] != expectedID {
			t.Errorf("expected raftID %d for peer %s, got %d", expectedID, peer, raftIDs[i])
		}
	}

	// Check maps
	for _, peer := range newPeers {
		raftID, ok := sd.GetRaftIDFromNode(peer)
		if !ok {
			t.Errorf("peer %s not found in nodeToRaft map", peer)
		}
		expectedID := utils.StringToUint64(peer)
		if raftID != expectedID {
			t.Errorf("expected raftID %d for node %s, got %d", expectedID, peer, raftID)
		}
	}
}

func TestMergePeers(t *testing.T) {
	tests := []struct {
		name          string
		initialPeers  []string
		newPeers      []string
		expectedPeers []string
	}{
		{
			name:          "add new peers",
			initialPeers:  []string{"node1", "node2"},
			newPeers:      []string{"node3", "node4"},
			expectedPeers: []string{"node1", "node2", "node3", "node4"},
		},
		{
			name:          "add existing and new peers",
			initialPeers:  []string{"node1", "node2"},
			newPeers:      []string{"node2", "node3"},
			expectedPeers: []string{"node1", "node2", "node3"},
		},
		{
			name:          "add only existing peers",
			initialPeers:  []string{"node1", "node2"},
			newPeers:      []string{"node1", "node2"},
			expectedPeers: []string{"node1", "node2"},
		},
		{
			name:          "add empty list",
			initialPeers:  []string{"node1", "node2"},
			newPeers:      []string{},
			expectedPeers: []string{"node1", "node2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			sd, _ := NewServiceDiscovery(logger, tt.initialPeers, 8080, "node1", &mockAddressResolver{})

			sd.MergePeers(tt.newPeers)

			peers := sd.ListPeers()
			if len(peers) != len(tt.expectedPeers) {
				t.Errorf("expected %d peers, got %d", len(tt.expectedPeers), len(peers))
			}

			// Check that all expected peers are present
			peerSet := make(map[string]bool)
			for _, p := range peers {
				peerSet[p] = true
			}
			for _, expected := range tt.expectedPeers {
				if !peerSet[expected] {
					t.Errorf("expected peer %s not found", expected)
				}
			}

			// Verify maps are consistent
			if !sd.ValidateConsistency() {
				t.Error("data structures are inconsistent after MergePeers")
			}
		})
	}
}

func TestListPeersAddrServerName(t *testing.T) {
	t.Run("with test address resolver", func(t *testing.T) {
		logger := zap.NewNop()
		seed := []string{"NC78YT-49217", "ABC123-8080"}
		sd, _ := NewServiceDiscovery(logger, seed, 0, "node1", &testutils.TestAddressResolver{})

		resolved := sd.ListPeersAddrServerName()

		if len(resolved) != 2 {
			t.Errorf("expected 2 resolved peers, got %d", len(resolved))
		}

		expected := []PeerResolvedInfo{
			{
				NodeId:     "NC78YT-49217",
				Addr:       "127.0.0.1:49217",
				ServerName: "NC78YT-49217",
			},
			{
				NodeId:     "ABC123-8080",
				Addr:       "127.0.0.1:8080",
				ServerName: "ABC123-8080",
			},
		}

		for i, exp := range expected {
			if i >= len(resolved) {
				t.Errorf("expected %d resolved peers but got %d", len(expected), len(resolved))
				break
			}
			if resolved[i].NodeId != exp.NodeId {
				t.Errorf("expected NodeId %s, got %s", exp.NodeId, resolved[i].NodeId)
			}
			if resolved[i].Addr != exp.Addr {
				t.Errorf("expected Addr %s, got %s", exp.Addr, resolved[i].Addr)
			}
			if resolved[i].ServerName != exp.ServerName {
				t.Errorf("expected ServerName %s, got %s", exp.ServerName, resolved[i].ServerName)
			}
		}
	})

	t.Run("with mock address resolver", func(t *testing.T) {
		logger := zap.NewNop()
		addressResolver := &mockAddressResolver{
			resolveFunc: func(ctx context.Context, node string) (string, error) {
				return "192.168.1.1:8080", nil
			},
		}
		seed := []string{"node1", "node2"}
		sd, _ := NewServiceDiscovery(logger, seed, 8080, "node1", addressResolver)

		resolved := sd.ListPeersAddrServerName()

		if len(resolved) != 2 {
			t.Errorf("expected 2 resolved peers, got %d", len(resolved))
		}

		for _, peer := range resolved {
			if peer.Addr != "192.168.1.1:8080" {
				t.Errorf("expected Addr 192.168.1.1:8080, got %s", peer.Addr)
			}
			if peer.ServerName != peer.NodeId {
				t.Errorf("expected ServerName %s, got %s", peer.NodeId, peer.ServerName)
			}
		}
	})

	t.Run("with invalid peer format", func(t *testing.T) {
		logger := zap.NewNop()
		seed := []string{"invalid", "node1-abc", "node2-123"}
		sd, _ := NewServiceDiscovery(logger, seed, 0, "node1", &testutils.TestAddressResolver{})

		resolved := sd.ListPeersAddrServerName()

		// Only valid format "node2-123" should be included
		if len(resolved) != 1 {
			t.Errorf("expected 1 resolved peer, got %d", len(resolved))
		} else if resolved[0].NodeId != "node2-123" {
			t.Errorf("expected NodeId node2-123, got %s", resolved[0].NodeId)
		}
	})

	t.Run("with empty peers", func(t *testing.T) {
		logger := zap.NewNop()
		sd, _ := NewServiceDiscovery(logger, []string{}, 0, "node1", &testutils.TestAddressResolver{})

		resolved := sd.ListPeersAddrServerName()
		if len(resolved) != 0 {
			t.Errorf("expected 0 resolved peers, got %d", len(resolved))
		}
	})
}

func TestListPeersAddrServerName_ErrorCases(t *testing.T) {
	t.Run("resolution errors", func(t *testing.T) {
		logger := zap.NewNop()
		addressResolver := &mockAddressResolverWithError{
			errNodes: map[string]bool{
				"node1": true,
			},
		}
		seed := []string{"node1", "node2"}
		sd, _ := NewServiceDiscovery(logger, seed, 8080, "node1", addressResolver)

		resolved := sd.ListPeersAddrServerName()

		// Only node2 should be resolved successfully
		if len(resolved) != 1 {
			t.Errorf("expected 1 resolved peer, got %d", len(resolved))
		} else if resolved[0].NodeId != "node2" {
			t.Errorf("expected NodeId node2, got %s", resolved[0].NodeId)
		}
	})

	t.Run("all resolution errors", func(t *testing.T) {
		logger := zap.NewNop()
		addressResolver := &mockAddressResolverWithError{
			errNodes: map[string]bool{
				"node1": true,
				"node2": true,
			},
		}
		seed := []string{"node1", "node2"}
		sd, _ := NewServiceDiscovery(logger, seed, 8080, "node1", addressResolver)

		resolved := sd.ListPeersAddrServerName()
		if len(resolved) != 0 {
			t.Errorf("expected 0 resolved peers, got %d", len(resolved))
		}
	})

	t.Run("empty peers list", func(t *testing.T) {
		logger := zap.NewNop()
		addressResolver := &mockAddressResolver{
			resolveFunc: func(ctx context.Context, node string) (string, error) {
				return "192.168.1.1:8080", nil
			},
		}
		sd, _ := NewServiceDiscovery(logger, []string{"temp"}, 8080, "node1", addressResolver)

		// Manually clear the peers list to test empty case
		sd.peers.Lock()
		sd.peersList = []string{}
		sd.peers.Unlock()

		// Rebuild maps to maintain consistency
		sd.rebuildMapsFromPeers()

		resolved := sd.ListPeersAddrServerName()
		if len(resolved) != 0 {
			t.Errorf("expected 0 resolved peers, got %d", len(resolved))
		}
	})
}

func TestValidateConsistency(t *testing.T) {
	logger := zap.NewNop()
	sd, _ := NewServiceDiscovery(logger, []string{"node1", "node2"}, 8080, "node1", &mockAddressResolver{})

	// Should be consistent initially
	if !sd.ValidateConsistency() {
		t.Error("ValidateConsistency returned false initially")
	}

	// Manually corrupt the data structures and test
	sd.peersList = []string{"node1"} // Different length from peersRaftIDsList
	if sd.ValidateConsistency() {
		t.Error("ValidateConsistency should return false when peers and raft IDs lengths don't match")
	}
	sd.peersList = []string{"node1", "node2"} // Restore

	// Corrupt nodeToRaft map
	sd.nodeToRaft.Lock()
	sd.nodeToRaftMap = map[string]uint64{"node1": utils.StringToUint64("node1")}
	sd.nodeToRaft.Unlock()
	if sd.ValidateConsistency() {
		t.Error("ValidateConsistency should return false when nodeToRaft map is missing a peer")
	}
	sd.nodeToRaft.Lock()
	sd.nodeToRaftMap = map[string]uint64{
		"node1": utils.StringToUint64("node1"),
		"node2": utils.StringToUint64("node2"),
	}
	sd.nodeToRaft.Unlock()

	// Corrupt raftToNode map
	sd.raftToNode.Lock()
	sd.raftToNodeMap = map[uint64]string{
		utils.StringToUint64("node1"): "node1",
	}
	sd.raftToNode.Unlock()
	if sd.ValidateConsistency() {
		t.Error("ValidateConsistency should return false when raftToNode map is missing a peer")
	}
}

func TestServiceDiscovery_Concurrency(t *testing.T) {
	logger := zap.NewNop()
	sd, _ := NewServiceDiscovery(logger, []string{"node1", "node2"}, 8080, "node1", &mockAddressResolver{})

	// Test concurrent reads and writes
	const numGoroutines = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines * 4) // 4 operations per goroutine

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			sd.ListPeers()
			sd.ListPeersRaftIDs()
			sd.GetRaftIDFromNode("node1")
			sd.GetNodeIDFromRaftID(utils.StringToUint64("node1"))
		}()

		go func() {
			defer wg.Done()
			sd.UpdatePeers([]string{"node3", "node4", "node5"})
		}()

		go func() {
			defer wg.Done()
			sd.MergePeers([]string{"node6", "node7"})
		}()

		go func() {
			defer wg.Done()
			sd.ValidateConsistency()
		}()
	}

	wg.Wait()
}

func TestRebuildMapsFromPeers(t *testing.T) {
	logger := zap.NewNop()
	sd, _ := NewServiceDiscovery(logger, []string{"node1", "node2"}, 8080, "node1", &mockAddressResolver{})

	// Modify peers list directly
	sd.peers.Lock()
	sd.peersList = []string{"node3", "node4", "node5"}
	sd.peers.Unlock()

	// Rebuild maps
	sd.rebuildMapsFromPeers()

	// Verify consistency
	if !sd.ValidateConsistency() {
		t.Error("ValidateConsistency returned false after rebuildMapsFromPeers")
	}

	// Verify all peers are in maps
	peers := sd.ListPeers()
	expected := []string{"node3", "node4", "node5"}
	if len(peers) != len(expected) {
		t.Errorf("expected %d peers, got %d", len(expected), len(peers))
	}
	for i, peer := range expected {
		if peers[i] != peer {
			t.Errorf("expected peer %s, got %s", peer, peers[i])
		}
	}

	// Verify raft IDs
	raftIDs := sd.ListPeersRaftIDs()
	if len(raftIDs) != len(expected) {
		t.Errorf("expected %d raft IDs, got %d", len(expected), len(raftIDs))
	}
	for i, peer := range expected {
		expectedID := utils.StringToUint64(peer)
		if raftIDs[i] != expectedID {
			t.Errorf("expected raftID %d for peer %s, got %d", expectedID, peer, raftIDs[i])
		}
	}
}
