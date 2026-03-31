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
	"slices"
	"sync"
	"time"

	"github.com/m-javani/cue/internal/utils"
	"github.com/m-javani/cue/pkg/discovery"
	"go.uber.org/zap"
)

// ServiceDiscovery manages peer discovery and RAFT ID mappings
type ServiceDiscovery struct {
	selfNodeID       string
	peers            *sync.RWMutex
	peersList        []string
	peersRaftIDs     *sync.RWMutex
	peersRaftIDsList []uint64
	nodeToRaft       *sync.RWMutex
	nodeToRaftMap    map[string]uint64
	raftToNode       *sync.RWMutex
	raftToNodeMap    map[uint64]string
	quicPort         uint16
	addressResolver  discovery.AddressResolver
	logger           *zap.Logger
}

// New creates a new ServiceDiscovery instance
func NewServiceDiscovery(logger *zap.Logger, seed []string, quicPort uint16, selfNodeID string, addressResolver discovery.AddressResolver) (*ServiceDiscovery, error) {
	found := slices.Contains(seed, selfNodeID)
	if !found {
		seed = append(seed, selfNodeID)
	}

	logger.Sugar().Debugf("service discovery-  initial seed: %v", seed)

	// Build initial data structures
	peersRaftIDs := make([]uint64, len(seed))
	nodeToRaftMap := make(map[string]uint64)
	raftToNodeMap := make(map[uint64]string)

	for i, peer := range seed {
		raftID := utils.StringToUint64(peer)
		peersRaftIDs[i] = raftID
		nodeToRaftMap[peer] = raftID
		raftToNodeMap[raftID] = peer
	}

	return &ServiceDiscovery{
		selfNodeID:       selfNodeID,
		peers:            &sync.RWMutex{},
		peersList:        seed,
		peersRaftIDs:     &sync.RWMutex{},
		peersRaftIDsList: peersRaftIDs,
		nodeToRaft:       &sync.RWMutex{},
		nodeToRaftMap:    nodeToRaftMap,
		raftToNode:       &sync.RWMutex{},
		raftToNodeMap:    raftToNodeMap,
		quicPort:         quicPort,
		addressResolver:  addressResolver,
		logger:           logger,
	}, nil
}

// GetNodeIDFromRaftID performs fast lookup from raft_id to node string
func (sd *ServiceDiscovery) GetNodeIDFromRaftID(raftID uint64) (string, bool) {
	sd.raftToNode.RLock()
	defer sd.raftToNode.RUnlock()
	node, ok := sd.raftToNodeMap[raftID]
	return node, ok
}

// GetRaftIDFromNode performs fast lookup from node string to raft_id
func (sd *ServiceDiscovery) GetRaftIDFromNode(node string) (uint64, bool) {
	sd.nodeToRaft.RLock()
	defer sd.nodeToRaft.RUnlock()
	raftID, ok := sd.nodeToRaftMap[node]
	return raftID, ok
}

// ListPeersAddrServerName resolves and returns the list of peers and their IP:port strings
func (sd *ServiceDiscovery) ListPeersAddrServerName() []PeerResolvedInfo {
	sd.peers.RLock()
	peers := make([]string, len(sd.peersList))
	copy(peers, sd.peersList)
	sd.peers.RUnlock()

	resolved := make([]PeerResolvedInfo, 0, len(peers))

	for _, peer := range peers {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		addr, err := sd.addressResolver.Resolve(ctx, peer)
		cancel()
		if err != nil {
			continue
		}
		resolved = append(resolved, PeerResolvedInfo{
			NodeId:     peer,
			Addr:       addr,
			ServerName: peer,
		})
	}

	if len(resolved) == 0 {
		sd.logger.Sugar().Warnf("%s: the resolved peers list is 0/%d", sd.selfNodeID, len(peers))
	}

	return resolved
}

// ListPeers returns list of peer strings
func (sd *ServiceDiscovery) ListPeers() []string {
	sd.peers.RLock()
	defer sd.peers.RUnlock()
	peers := make([]string, len(sd.peersList))
	copy(peers, sd.peersList)
	return peers
}

// ListPeersRaftIDs returns list of raft IDs
func (sd *ServiceDiscovery) ListPeersRaftIDs() []uint64 {
	sd.peersRaftIDs.RLock()
	defer sd.peersRaftIDs.RUnlock()
	ids := make([]uint64, len(sd.peersRaftIDsList))
	copy(ids, sd.peersRaftIDsList)
	return ids
}

// UpdatePeers replaces the entire peer list with new values and syncs all data structures
func (sd *ServiceDiscovery) UpdatePeers(updatePeers []string) {
	raftIDs := make([]uint64, len(updatePeers))
	for i, peer := range updatePeers {
		raftIDs[i] = utils.StringToUint64(peer)
	}

	// Build new maps
	nodeToRaftMap := make(map[string]uint64)
	raftToNodeMap := make(map[uint64]string)

	for i, peer := range updatePeers {
		raftID := raftIDs[i]
		nodeToRaftMap[peer] = raftID
		raftToNodeMap[raftID] = peer
	}

	// Update all data structures
	sd.peers.Lock()
	sd.peersList = updatePeers
	sd.peers.Unlock()

	sd.peersRaftIDs.Lock()
	sd.peersRaftIDsList = raftIDs
	sd.peersRaftIDs.Unlock()

	sd.nodeToRaft.Lock()
	sd.nodeToRaftMap = nodeToRaftMap
	sd.nodeToRaft.Unlock()

	sd.raftToNode.Lock()
	sd.raftToNodeMap = raftToNodeMap
	sd.raftToNode.Unlock()
}

// MergePeers adds new peers that aren't already present
func (sd *ServiceDiscovery) MergePeers(newPeers []string) {
	sd.peers.Lock()
	changed := false
	for _, peer := range newPeers {
		if !slices.Contains(sd.peersList, peer) {
			sd.peersList = append(sd.peersList, peer)
			changed = true
		}
	}
	sd.peers.Unlock()

	if changed {
		sd.rebuildMapsFromPeers()
	}
}

// rebuildMapsFromPeers rebuilds lookup maps from current peer list
func (sd *ServiceDiscovery) rebuildMapsFromPeers() {
	sd.peers.RLock()
	peers := make([]string, len(sd.peersList))
	copy(peers, sd.peersList)
	sd.peers.RUnlock()

	raftIDs := make([]uint64, len(peers))
	for i, peer := range peers {
		raftIDs[i] = utils.StringToUint64(peer)
	}

	nodeToRaftMap := make(map[string]uint64)
	raftToNodeMap := make(map[uint64]string)

	for i, peer := range peers {
		raftID := raftIDs[i]
		nodeToRaftMap[peer] = raftID
		raftToNodeMap[raftID] = peer
	}

	sd.peersRaftIDs.Lock()
	sd.peersRaftIDsList = raftIDs
	sd.peersRaftIDs.Unlock()

	sd.nodeToRaft.Lock()
	sd.nodeToRaftMap = nodeToRaftMap
	sd.nodeToRaft.Unlock()

	sd.raftToNode.Lock()
	sd.raftToNodeMap = raftToNodeMap
	sd.raftToNode.Unlock()
}

// ValidateConsistency checks that all data structures are consistent
func (sd *ServiceDiscovery) ValidateConsistency() bool {
	sd.peers.RLock()
	peers := make([]string, len(sd.peersList))
	copy(peers, sd.peersList)
	sd.peers.RUnlock()

	sd.peersRaftIDs.RLock()
	peersRaftIDs := make([]uint64, len(sd.peersRaftIDsList))
	copy(peersRaftIDs, sd.peersRaftIDsList)
	sd.peersRaftIDs.RUnlock()

	sd.nodeToRaft.RLock()
	nodeToRaftMap := sd.nodeToRaftMap
	sd.nodeToRaft.RUnlock()

	sd.raftToNode.RLock()
	raftToNodeMap := sd.raftToNodeMap
	sd.raftToNode.RUnlock()

	// Check vector lengths match
	if len(peers) != len(peersRaftIDs) {
		return false
	}

	// Check all peers are in nodeToRaft map
	for _, peer := range peers {
		if _, ok := nodeToRaftMap[peer]; !ok {
			return false
		}
	}

	// Check all raft_ids are in raftToNode map
	for _, raftID := range peersRaftIDs {
		if _, ok := raftToNodeMap[raftID]; !ok {
			return false
		}
	}

	return true
}
