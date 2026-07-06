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
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
	"github.com/m-javani/cue/internal/utils"
	"github.com/stretchr/testify/assert/yaml"
	"go.uber.org/zap"
)

// httpPeersResponse for versioned HTTP discovery protocol
type httpPeersResponse struct {
	Version int              `json:"version"`
	Peers   []model.PeerInfo `json:"peers"`
}

// ServiceDiscovery manages peer discovery and RAFT ID mappings
type ServiceDiscovery struct {
	selfNodeID        string
	discoveryKind     internal.DiscoveryKind
	discoveryHTTPHost string
	refreshInterval   time.Duration

	peers     *sync.RWMutex
	peersList []model.PeerInfo

	nodeToPeer    *sync.RWMutex
	nodeToPeerMap map[string]model.PeerInfo

	nodeToRaft    *sync.RWMutex
	nodeToRaftMap map[string]uint64

	raftToNode    *sync.RWMutex
	raftToNodeMap map[uint64]string

	logger *zap.Logger
}

// NewServiceDiscovery creates a new ServiceDiscovery instance
func NewServiceDiscovery(
	logger *zap.Logger,
	seed []model.PeerInfo,
	selfNodeID string,
	discoveryKind internal.DiscoveryKind,
	discoveryHTTPHost string,
	refreshInterval time.Duration,
) (*ServiceDiscovery, error) {
	if discoveryKind == internal.DiscoveryKindHttp && discoveryHTTPHost == "" {
		return nil, fmt.Errorf("http endpoint required for HTTP discovery")
	}

	// logger.Sugar().Debugf("service discovery- initial seed: %v", seed)

	sd := &ServiceDiscovery{
		selfNodeID:        selfNodeID,
		discoveryKind:     discoveryKind,
		discoveryHTTPHost: discoveryHTTPHost,
		refreshInterval:   refreshInterval,
		peers:             &sync.RWMutex{},
		nodeToPeer:        &sync.RWMutex{},
		nodeToRaft:        &sync.RWMutex{},
		raftToNode:        &sync.RWMutex{},
		logger:            logger,
	}

	sd.updateInternalState(seed) // initial build

	return sd, nil
}

// updateInternalState rebuilds all internal structures from model.PeerInfo list (shared logic)
func (sd *ServiceDiscovery) updateInternalState(peers []model.PeerInfo) {
	peersCopy := make([]model.PeerInfo, len(peers))
	copy(peersCopy, peers)

	nodeToPeerMap := make(map[string]model.PeerInfo, len(peers))
	nodeToRaftMap := make(map[string]uint64, len(peers))
	raftToNodeMap := make(map[uint64]string, len(peers))

	for _, peer := range peers {
		raftID := utils.StringToUint64(peer.NodeID)
		nodeToPeerMap[peer.NodeID] = peer
		nodeToRaftMap[peer.NodeID] = raftID
		raftToNodeMap[raftID] = peer.NodeID
	}

	sd.peers.Lock()
	sd.peersList = peersCopy
	sd.peers.Unlock()

	sd.nodeToPeer.Lock()
	sd.nodeToPeerMap = nodeToPeerMap
	sd.nodeToPeer.Unlock()

	sd.nodeToRaft.Lock()
	sd.nodeToRaftMap = nodeToRaftMap
	sd.nodeToRaft.Unlock()

	sd.raftToNode.Lock()
	sd.raftToNodeMap = raftToNodeMap
	sd.raftToNode.Unlock()
}

// refreshFromHTTP fetches latest peers (private)
func (sd *ServiceDiscovery) refreshFromHTTP(ctx context.Context) ([]model.PeerInfo, error) {
	if sd.discoveryHTTPHost == "" {
		return nil, fmt.Errorf("no HTTP endpoint configured")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sd.discoveryHTTPHost, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status not OK: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Versioned first
	var httpResp httpPeersResponse
	if err := json.Unmarshal(body, &httpResp); err == nil && httpResp.Version > 0 {
		if httpResp.Version != 1 {
			return nil, fmt.Errorf("unsupported version: %d", httpResp.Version)
		}
		return httpResp.Peers, nil
	}

	// Fallback bare array
	var peers []model.PeerInfo
	if err := json.Unmarshal(body, &peers); err != nil {
		return nil, fmt.Errorf("failed to unmarshal peers: %w", err)
	}
	return peers, nil
}

// Run starts background polling (HTTP only; no-op for static)
func (sd *ServiceDiscovery) Run(ctx context.Context) {
	if sd.discoveryKind != internal.DiscoveryKindHttp {
		return
	}

	ticker := time.NewTicker(sd.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sd.syncFromHTTP(ctx)
		}
	}
}

// syncFromHTTP refreshes and updates (internal)
func (sd *ServiceDiscovery) syncFromHTTP(ctx context.Context) {
	peers, err := sd.refreshFromHTTP(ctx)
	if err != nil {
		sd.logger.Error("failed to refresh peers from HTTP", zap.Error(err))
		return
	}
	if len(peers) == 0 {
		sd.logger.Warn("received empty peers list from HTTP")
		return
	}
	sd.UpdatePeers(peers)
}

// Lookup returns model.PeerInfo for nodeID
func (sd *ServiceDiscovery) Lookup(nodeID string) (model.PeerInfo, bool) {
	sd.nodeToPeer.RLock()
	defer sd.nodeToPeer.RUnlock()
	peer, ok := sd.nodeToPeerMap[nodeID]
	return peer, ok
}

// ListPeers returns copy of peers
func (sd *ServiceDiscovery) ListPeers() []model.PeerInfo {
	sd.peers.RLock()
	defer sd.peers.RUnlock()
	peers := make([]model.PeerInfo, len(sd.peersList))
	copy(peers, sd.peersList)
	return peers
}

// ListPeersRaftIDs (preserved/adapted)
func (sd *ServiceDiscovery) ListPeersRaftIDs() []uint64 {
	sd.nodeToRaft.RLock()
	defer sd.nodeToRaft.RUnlock()
	ids := make([]uint64, 0, len(sd.nodeToRaftMap))
	for _, id := range sd.nodeToRaftMap {
		ids = append(ids, id)
	}
	return ids
}

// GetNodeIDFromRaftID (preserved)
func (sd *ServiceDiscovery) GetNodeIDFromRaftID(raftID uint64) (string, bool) {
	sd.raftToNode.RLock()
	defer sd.raftToNode.RUnlock()
	node, ok := sd.raftToNodeMap[raftID]
	return node, ok
}

// GetRaftIDFromNode (preserved)
func (sd *ServiceDiscovery) GetRaftIDFromNode(node string) (uint64, bool) {
	sd.nodeToRaft.RLock()
	defer sd.nodeToRaft.RUnlock()
	raftID, ok := sd.nodeToRaftMap[node]
	return raftID, ok
}

// UpdatePeers replaces entire list
func (sd *ServiceDiscovery) UpdatePeers(updatePeers []model.PeerInfo) {
	sd.updateInternalState(updatePeers)
}

// MergePeers adds missing peers
func (sd *ServiceDiscovery) MergePeers(newPeers []model.PeerInfo) {
	sd.peers.Lock()
	changed := false
	existing := make(map[string]bool, len(sd.peersList))
	for _, p := range sd.peersList {
		existing[p.NodeID] = true
	}
	for _, peer := range newPeers {
		if !existing[peer.NodeID] {
			sd.peersList = append(sd.peersList, peer)
			changed = true
		}
	}
	sd.peers.Unlock()

	if changed {
		sd.peers.RLock()
		current := make([]model.PeerInfo, len(sd.peersList))
		copy(current, sd.peersList)
		sd.peers.RUnlock()
		sd.updateInternalState(current)
	}
}

// ValidateConsistency updated for new structures
func (sd *ServiceDiscovery) ValidateConsistency() bool {
	sd.peers.RLock()
	peers := make([]model.PeerInfo, len(sd.peersList))
	copy(peers, sd.peersList)
	sd.peers.RUnlock()

	sd.nodeToPeer.RLock()
	nodeToPeerMap := make(map[string]model.PeerInfo, len(sd.nodeToPeerMap))
	for k, v := range sd.nodeToPeerMap {
		nodeToPeerMap[k] = v
	}
	sd.nodeToPeer.RUnlock()

	sd.nodeToRaft.RLock()
	nodeToRaftMap := make(map[string]uint64, len(sd.nodeToRaftMap))
	for k, v := range sd.nodeToRaftMap {
		nodeToRaftMap[k] = v
	}
	sd.nodeToRaft.RUnlock()

	sd.raftToNode.RLock()
	raftToNodeMap := make(map[uint64]string, len(sd.raftToNodeMap))
	for k, v := range sd.raftToNodeMap {
		raftToNodeMap[k] = v
	}
	sd.raftToNode.RUnlock()

	if len(peers) != len(nodeToPeerMap) || len(peers) != len(nodeToRaftMap) || len(peers) != len(raftToNodeMap) {
		return false
	}

	for _, peer := range peers {
		if p, ok := nodeToPeerMap[peer.NodeID]; !ok || p.NodeID != peer.NodeID {
			return false
		}
		raftID := utils.StringToUint64(peer.NodeID)
		if id, ok := nodeToRaftMap[peer.NodeID]; !ok || id != raftID {
			return false
		}
		if n, ok := raftToNodeMap[raftID]; !ok || n != peer.NodeID {
			return false
		}
	}
	return true
}

// LoadDiscoveryFile loads and validates peers from YAML
func LoadDiscoveryFile(pathStr string) ([]model.PeerInfo, error) {
	data, err := os.ReadFile(pathStr)
	if err != nil {
		return nil, fmt.Errorf("failed to read discovery file %s: %w", pathStr, err)
	}

	var config struct {
		Nodes []model.PeerInfo `yaml:"nodes"`
	}

	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse discovery.yml: %w", err)
	}

	if len(config.Nodes) == 0 {
		return nil, fmt.Errorf("no nodes defined in discovery file")
	}

	seen := make(map[string]bool)
	validated := make([]model.PeerInfo, 0, len(config.Nodes))

	for i, node := range config.Nodes {
		if err := node.Validate(); err != nil {
			return nil, fmt.Errorf("validation failed for node at index %d (%s): %w", i, node.NodeID, err)
		}

		if seen[node.NodeID] {
			return nil, fmt.Errorf("duplicate node_id: %s", node.NodeID)
		}
		seen[node.NodeID] = true

		validated = append(validated, node)
	}

	return validated, nil
}
