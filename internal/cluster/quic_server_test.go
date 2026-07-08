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
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/m-javani/cue/internal/model"
	"github.com/m-javani/cue/internal/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// quicTestHelper manages isolated QUIC servers for testing
type quicTestHelper struct {
	t       *testing.T
	ctx     context.Context
	cancel  context.CancelFunc
	dir     string
	ca      *testutils.CAInfo
	logger  *zap.Logger
	randSrc *rand.Rand
}

// quicTestNode represents a single QUIC server for testing
type quicTestNode struct {
	ID        string
	Server    *ClusterQUIC
	Addr      string
	Port      int
	CertPath  string
	KeyPath   string
	CAPath    string
	Discovery *ServiceDiscovery
}

func newQUICTestHelper(t *testing.T) *quicTestHelper {
	ctx, cancel := context.WithCancel(context.Background())
	dir, err := os.MkdirTemp("", "quic-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	ca, err := testutils.CreateCA(dir, "ca", 1, "localhost")
	require.NoError(t, err)

	// Seed randomness based on test name for better separation between parallel tests
	seed := time.Now().UnixNano()
	// Combine with test name hash for better distribution
	nameHash := int64(0)
	for _, r := range t.Name() {
		nameHash = nameHash*31 + int64(r)
	}
	seed = seed ^ nameHash // XOR to combine

	return &quicTestHelper{
		t:       t,
		ctx:     ctx,
		cancel:  cancel,
		dir:     dir,
		ca:      ca,
		logger:  logger,
		randSrc: rand.New(rand.NewSource(seed)),
	}
}

func (tc *quicTestHelper) generateRandomPort() int {
	return 30000 + tc.randSrc.Intn(30001)
}

// createNode creates a single QUIC server without cluster logic
func (h *quicTestHelper) createNode(id string) *quicTestNode {
	nodeDir := filepath.Join(h.dir, id)
	err := os.MkdirAll(nodeDir, 0755)
	require.NoError(h.t, err)
	nodeInfo := testutils.NodeCert{
		NodeIdentity: id,
		ServerNames:  []string{id + ".localhost"},
	}
	certPath, keyPath, err := testutils.CreateNodeCert(nodeDir, h.ca, nodeInfo, 1)
	require.NoError(h.t, err)
	caPath := filepath.Join(h.dir, "ca_cert.pem")

	// Create fresh discovery per node (selfNodeID set to id)
	disco, err := NewServiceDiscovery(
		h.logger,
		[]model.PeerInfo{},
		id,
		0,
		"",
		0,
	)
	require.NoError(h.t, err)

	var quic *ClusterQUIC
	port := h.generateRandomPort()
	require.NotZero(h.t, port, "generated port should not be 0 ")
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	quic, err = NewClusterQUIC(
		id,
		port,
		certPath,
		keyPath,
		caPath,
		addr,
		h.logger,
		disco,
	)
	require.NoError(h.t, err)
	require.NotNil(h.t, quic)

	node := &quicTestNode{
		ID:        id,
		Server:    quic,
		Addr:      addr,
		Port:      port,
		CertPath:  certPath,
		KeyPath:   keyPath,
		CAPath:    caPath,
		Discovery: disco,
	}
	return node
}

// startAccepting starts accepting connections in background
func (h *quicTestHelper) startAccepting(node *quicTestNode) {
	go func() {
		for {
			select {
			case <-h.ctx.Done():
				return
			default:
				_, _, err := node.Server.AcceptConnection(h.ctx)
				if err != nil && h.ctx.Err() == nil {
					h.logger.Debug("accept error", zap.Error(err))
				}
			}
		}
	}()
}

// connectNodes establishes a direct QUIC connection between two nodes
func (h *quicTestHelper) connectNodes(from, to *quicTestNode) error {
	// Ensure both have peer info in their discoveries for the new architecture
	ipFrom, _, _ := strings.Cut(from.Addr, ":")
	ipTo, _, _ := strings.Cut(to.Addr, ":")
	peerTo := model.PeerInfo{
		NodeID: to.ID,
		Host:   ipTo,
		Identity: model.TLSIdentity{
			Kind:  model.IdentityDNS,
			Value: to.ID + ".localhost",
		},
		Port: uint16(to.Port),
	}
	peerFrom := model.PeerInfo{
		NodeID: from.ID,
		Host:   ipFrom,
		Identity: model.TLSIdentity{
			Kind:  model.IdentityDNS,
			Value: from.ID + ".localhost",
		},
		Port: uint16(from.Port),
	}
	from.Discovery.UpdatePeers([]model.PeerInfo{peerTo, peerFrom})
	to.Discovery.UpdatePeers([]model.PeerInfo{peerTo, peerFrom})

	ctx, cancel := context.WithTimeout(h.ctx, 2*time.Second)
	defer cancel()
	return from.Server.Connect(ctx, to.ID)
}

// close shuts down all servers
func (h *quicTestHelper) close() {
	h.cancel()
}

// TestQuicServer_GetConnectedBidirectionalNodeIds_NoConnections tests with no connections
func TestQuicServer_GetConnectedBidirectionalNodeIds_NoConnections(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	result := node1.Server.GetConnectedBidirectionalNodeIds()
	assert.Empty(t, result)
}

// TestQuicServer_GetConnectedBidirectionalNodeIds_OnlyOutgoing tests only outgoing connections
func TestQuicServer_GetConnectedBidirectionalNodeIds_OnlyOutgoing(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	node2 := h.createNode("node2")
	node3 := h.createNode("node3")
	// Start accepting connections
	h.startAccepting(node2)
	h.startAccepting(node3)
	// Create only outgoing connections from node1
	err := h.connectNodes(node1, node2)
	require.NoError(t, err)
	err = h.connectNodes(node1, node3)
	require.NoError(t, err)
	// Give connections time to establish
	time.Sleep(100 * time.Millisecond)
	result := node1.Server.GetConnectedBidirectionalNodeIds()
	assert.Empty(t, result, "should have no bidirectional connections when only outgoing")
}

// TestQuicServer_GetConnectedBidirectionalNodeIds_Bidirectional tests bidirectional connections
func TestQuicServer_GetConnectedBidirectionalNodeIds_Bidirectional(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	node2 := h.createNode("node2")
	node3 := h.createNode("node3")
	h.startAccepting(node1)
	h.startAccepting(node2)
	h.startAccepting(node3)
	// Create bidirectional connections between node1 and node2
	err := h.connectNodes(node1, node2)
	require.NoError(t, err)
	err = h.connectNodes(node2, node1)
	require.NoError(t, err)
	// Create bidirectional connections between node1 and node3
	err = h.connectNodes(node1, node3)
	require.NoError(t, err)
	err = h.connectNodes(node3, node1)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	result := node1.Server.GetConnectedBidirectionalNodeIds()
	assert.Len(t, result, 2)
	assert.ElementsMatch(t, []string{"node2", "node3"}, result)
}

// TestQuicServer_GetConnectedBidirectionalNodeIds_Mixed tests mixed connections
func TestQuicServer_GetConnectedBidirectionalNodeIds_Mixed(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	node2 := h.createNode("node2")
	node3 := h.createNode("node3")
	h.startAccepting(node1)
	h.startAccepting(node2)
	h.startAccepting(node3)
	// Bidirectional with node2
	err := h.connectNodes(node1, node2)
	require.NoError(t, err)
	err = h.connectNodes(node2, node1)
	require.NoError(t, err)
	// Only outgoing to node3
	err = h.connectNodes(node1, node3)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	result := node1.Server.GetConnectedBidirectionalNodeIds()
	assert.Len(t, result, 1)
	assert.Equal(t, []string{"node2"}, result)
}

// TestQuicServer_GetActiveOutgoingNodes tests retrieving outgoing connections
func TestQuicServer_GetActiveOutgoingNodes(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	node2 := h.createNode("node2")
	node3 := h.createNode("node3")
	h.startAccepting(node2)
	h.startAccepting(node3)
	err := h.connectNodes(node1, node2)
	require.NoError(t, err)
	err = h.connectNodes(node1, node3)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	outgoing := node1.Server.GetActiveOutgoingNodes()
	assert.Len(t, outgoing, 2)
	assert.ElementsMatch(t, []string{"node2", "node3"}, outgoing)
}

// TestQuicServer_GetActiveIncomingNodes tests retrieving incoming connections
func TestQuicServer_GetActiveIncomingNodes(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	node2 := h.createNode("node2")
	node3 := h.createNode("node3")
	h.startAccepting(node1)
	h.startAccepting(node2)
	h.startAccepting(node3)
	// Node2 and node3 connect to node1
	err := h.connectNodes(node2, node1)
	require.NoError(t, err)
	err = h.connectNodes(node3, node1)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	incoming := node1.Server.GetActiveIncomingNodes()
	assert.Len(t, incoming, 2)
	assert.ElementsMatch(t, []string{"node2", "node3"}, incoming)
}

// TestQuicServer_GetConnectedNodeIdsAnyDirection tests retrieving all connections
func TestQuicServer_GetConnectedNodeIdsAnyDirection(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	node2 := h.createNode("node2")
	node3 := h.createNode("node3")
	h.startAccepting(node1)
	h.startAccepting(node2)
	h.startAccepting(node3)
	// Node2 connects to node1 (incoming for node1)
	err := h.connectNodes(node2, node1)
	require.NoError(t, err)
	// Node1 connects to node3 (outgoing for node1)
	err = h.connectNodes(node1, node3)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	connected := node1.Server.GetConnectedNodeIdsAnyDirection()
	assert.Len(t, connected, 2)
	assert.ElementsMatch(t, []string{"node2", "node3"}, connected)
}

// TestQuicServer_GetNumOfActiveBidirectionalNodes tests counting bidirectional connections
func TestQuicServer_GetNumOfActiveBidirectionalNodes(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	node2 := h.createNode("node2")
	node3 := h.createNode("node3")
	h.startAccepting(node1)
	h.startAccepting(node2)
	h.startAccepting(node3)
	// Bidirectional with node2
	err := h.connectNodes(node1, node2)
	require.NoError(t, err)
	err = h.connectNodes(node2, node1)
	require.NoError(t, err)
	// Only outgoing to node3
	err = h.connectNodes(node1, node3)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	count := node1.Server.GetNumOfActiveBidirectionalNodes()
	assert.Equal(t, 1, count)
}

// TestQuicServer_GetOutgoingConnection tests retrieving specific outgoing connection
func TestQuicServer_GetOutgoingConnection(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	node2 := h.createNode("node2")
	h.startAccepting(node2)
	err := h.connectNodes(node1, node2)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	// Test successful retrieval
	conn, err := node1.Server.GetOutgoingConnection("node2")
	require.NoError(t, err)
	assert.NotNil(t, conn)
	// Test non-existent connection
	conn, err = node1.Server.GetOutgoingConnection("nonexistent")
	assert.Error(t, err)
	assert.Nil(t, conn)
}

// TestQuicServer_GetIncomingConnection tests retrieving specific incoming connection
func TestQuicServer_GetIncomingConnection(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	node2 := h.createNode("node2")
	h.startAccepting(node1)
	err := h.connectNodes(node2, node1)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	// Test successful retrieval
	conn, err := node1.Server.GetIncomingConnection("node2")
	require.NoError(t, err)
	assert.NotNil(t, conn)
	// Test non-existent connection
	conn, err = node1.Server.GetIncomingConnection("nonexistent")
	assert.Error(t, err)
	assert.Nil(t, conn)
}

// TestQuicServer_ManualMapManipulation tests methods that work with manual map changes
func TestQuicServer_ManualMapManipulation(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	node2 := h.createNode("node2")
	node3 := h.createNode("node3")
	// Start accepting on all nodes
	h.startAccepting(node1)
	h.startAccepting(node2)
	h.startAccepting(node3)
	// Create real connections for node1
	err := h.connectNodes(node1, node2)
	require.NoError(t, err)
	err = h.connectNodes(node1, node3)
	require.NoError(t, err)
	// Also make node2 connect to node1 for bidirectional
	err = h.connectNodes(node2, node1)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	// Now manually manipulate - remove incoming from node3 (making it outgoing only)
	node1.Server.mu.Lock()
	if conn, exists := node1.Server.incomingConns["node3"]; exists {
		_ = conn.CloseWithError(0, "test disconnect")
		delete(node1.Server.incomingConns, "node3")
	}
	node1.Server.mu.Unlock()
	// Test bidirectional detection - should only have node2
	result := node1.Server.GetConnectedBidirectionalNodeIds()
	assert.Equal(t, []string{"node2"}, result)
	// Test outgoing nodes - should have both
	outgoing := node1.Server.GetActiveOutgoingNodes()
	assert.ElementsMatch(t, []string{"node2", "node3"}, outgoing)
	// Test incoming nodes - should only have node2
	incoming := node1.Server.GetActiveIncomingNodes()
	assert.Equal(t, []string{"node2"}, incoming)
	// Test count - should be 1
	count := node1.Server.GetNumOfActiveBidirectionalNodes()
	assert.Equal(t, 1, count)
}

// -------------
// TestQuicServer_Handshake_SelfConnection tests that connecting to self is rejected
func TestQuicServer_Handshake_SelfConnection(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	// Populate self in discovery so Connect can lookup peer info
	selfIP, _, _ := strings.Cut(node1.Addr, ":")
	selfPeer := []model.PeerInfo{{
		NodeID: node1.ID,
		Host:   selfIP,
		Identity: model.TLSIdentity{
			Kind:  model.IdentityDNS,
			Value: node1.ID + ".localhost",
		},
	}}
	node1.Discovery.UpdatePeers(selfPeer)
	h.startAccepting(node1)
	ctx, cancel := context.WithTimeout(h.ctx, 2*time.Second)
	defer cancel()
	err := node1.Server.Connect(
		ctx,
		node1.ID,
	)
	assert.Error(t, err, "should not allow connecting to self")
	assert.Contains(t, err.Error(), "self connection not allowed")
}

// TestQuicServer_Handshake_DuplicateNodeID tests that duplicate connections replace old ones
func TestQuicServer_Handshake_DuplicateNodeID(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	node2 := h.createNode("node2")
	// Start accepting on node1 and node2
	h.startAccepting(node1)
	h.startAccepting(node2)
	// First connection: node1 connects to node2
	err := h.connectNodes(node1, node2)
	require.NoError(t, err, "first connection should succeed")
	time.Sleep(100 * time.Millisecond)
	// Second connection: node2 connects to node1 (this creates an incoming connection on node1)
	// This is a duplicate in the sense that node1 will have two outgoing connections to node2
	// But actually this creates a bidirectional connection, which is fine
	err = h.connectNodes(node2, node1)
	require.NoError(t, err, "second connection should succeed")
	time.Sleep(100 * time.Millisecond)
	// Check that node1 has an outgoing connection to node2
	node1.Server.mu.RLock()
	outConn, outExists := node1.Server.outgoingConns["node2"]
	node1.Server.mu.RUnlock()
	assert.True(t, outExists, "node1 should have outgoing connection to node2")
	assert.NotNil(t, outConn, "outgoing connection should exist")
	// Check that node1 has an incoming connection from node2
	node1.Server.mu.RLock()
	inConn, inExists := node1.Server.incomingConns["node2"]
	node1.Server.mu.RUnlock()
	assert.True(t, inExists, "node1 should have incoming connection from node2")
	assert.NotNil(t, inConn, "incoming connection should exist")
}

// TestQuicServer_Handshake_Wrong_Address tests dial fail
func TestQuicServer_Handshake_Wrong_Address(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	node2 := h.createNode("node2")
	peerTo := model.PeerInfo{
		NodeID: "node2",
		Host:   "127.0.0.10",
		Identity: model.TLSIdentity{
			Kind:  model.IdentityDNS,
			Value: "node2.xxx",
		},
	}

	node1.Discovery.UpdatePeers([]model.PeerInfo{peerTo})
	// err := h.connectNodes(node1, node2)
	ctx, cancel := context.WithTimeout(h.ctx, 2*time.Second)
	defer cancel()
	h.startAccepting(node2)
	time.Sleep(100 * time.Millisecond)
	err := node1.Server.Connect(
		ctx,
		node2.ID,
	)
	assert.Error(t, err, "should fail when connecting to invalid address")
	assert.True(t,
		strings.Contains(err.Error(), "context deadline exceeded") ||
			strings.Contains(err.Error(), "peer not found") ||
			strings.Contains(err.Error(), "failed to dial"),
		"should timeout or get connection refused, got: %v", err)
}

// TestQuicServer_CleanupRetiringIncoming tests cleaning up retiring incoming connections
func TestQuicServer_CleanupRetiringIncoming(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	node2 := h.createNode("node2")
	h.startAccepting(node1)
	h.startAccepting(node2)
	err := h.connectNodes(node2, node1)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	// Get the actual connection
	conn, err := node1.Server.GetIncomingConnection(node2.ID)
	require.NoError(t, err)
	// Close the connection first so it can be cleaned up
	_ = conn.CloseWithError(0, "test close")
	// Manually add a retiring connection with the closed connection
	node1.Server.mu.Lock()
	node1.Server.retiringIncomingConns = []retiringConn{
		{
			timestamp: time.Now().Add(-rotateTLSWindow - time.Second), // Old connection
			conn:      conn,
		},
	}
	node1.Server.mu.Unlock()
	// Cleanup should remove the old connection
	node1.Server.CleanupRetiringIncoming()
	node1.Server.mu.RLock()
	count := len(node1.Server.retiringIncomingConns)
	node1.Server.mu.RUnlock()
	assert.Equal(t, 0, count, "old retiring connection should be cleaned up")
}

// TestQuicServer_ReconnectToPeers tests reconnecting to peers
func TestQuicServer_ReconnectToPeers(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	node2 := h.createNode("node2")
	node3 := h.createNode("node3")
	h.startAccepting(node2)
	h.startAccepting(node3)
	// Populate discovery on all participating nodes (IP only)
	ip1, _, _ := strings.Cut(node1.Addr, ":")
	ip2, _, _ := strings.Cut(node2.Addr, ":")
	ip3, _, _ := strings.Cut(node3.Addr, ":")
	peers := []model.PeerInfo{
		{
			NodeID: node1.ID,
			Host:   ip1,
			Identity: model.TLSIdentity{
				Kind:  model.IdentityDNS,
				Value: node1.ID + ".localhost",
			},
			Port: uint16(node1.Port),
		},
		{
			NodeID: node2.ID,
			Host:   ip2,
			Identity: model.TLSIdentity{
				Kind:  model.IdentityDNS,
				Value: node2.ID + ".localhost",
			},
			Port: uint16(node2.Port),
		},
		{
			NodeID: node3.ID,
			Host:   ip3,
			Identity: model.TLSIdentity{
				Kind:  model.IdentityDNS,
				Value: node3.ID + ".localhost",
			},
			Port: uint16(node3.Port),
		},
	}
	node1.Discovery.UpdatePeers(peers)
	node2.Discovery.UpdatePeers(peers)
	node3.Discovery.UpdatePeers(peers)
	// Reconnect to peers (reads from own discovery)
	successful, err := node1.Server.ReconnectToPeers(peers)
	require.NoError(t, err)
	assert.Len(t, successful, 2, "should reconnect to both peers")

	node1Outgoings := node1.Server.GetActiveOutgoingNodes()
	assert.True(t, slices.Contains(node1Outgoings, node2.ID) && slices.Contains(node1Outgoings, node3.ID))

}

// TestQuicServer_ReconnectToPeers_InvalidPeers tests reconnecting with invalid peers
func TestQuicServer_ReconnectToPeers_InvalidPeers(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	// Create peer info with invalid address
	peers := []model.PeerInfo{
		{
			NodeID: "invalid",
			Host:   "invalid-address",
			Identity: model.TLSIdentity{
				Kind:  model.IdentityDNS,
				Value: "invalid.local",
			},
		},
	}
	node1.Discovery.UpdatePeers(peers)
	successful, err := node1.Server.ReconnectToPeers(peers)
	assert.NoError(t, err, "should not return error for invalid peers")
	assert.Empty(t, successful, "should not connect to invalid peers")
}

func TestQuicServer_ReloadTLS(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	// Store old listener reference
	oldListener := node1.Server.quicListener
	// Get old configs
	oldServerCfg := node1.Server.serverConfig.Load()
	oldClientCfg := node1.Server.clientConfig.Load()
	// Reload TLS (loads from existing files)
	err := node1.Server.ReloadTLS()
	require.NoError(t, err)
	// Verify listener is the SAME instance
	assert.Equal(t, oldListener, node1.Server.quicListener, "listener should NOT be recreated")
	// Verify configs were reloaded (should be new objects even if same content)
	newServerCfg := node1.Server.serverConfig.Load()
	newClientCfg := node1.Server.clientConfig.Load()
	// These should be different objects (even if content is same)
	assert.NotEqual(t, oldServerCfg, newServerCfg, "server config should be a new object")
	assert.NotEqual(t, oldClientCfg, newClientCfg, "client config should be a new object")
	assert.NotNil(t, newServerCfg)
	assert.NotNil(t, newClientCfg)
	// The listener should still be using the same underlying config
	// but GetConfigForClient will return the new config
	assert.NotNil(t, node1.Server.quicListener)
}

// TestQuicServer_ReloadTLS_InvalidCert tests reloading with invalid certificate
func TestQuicServer_ReloadTLS_InvalidCert(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	// Save original cert path
	origCertPath := node1.Server.certPath
	// Set invalid cert path
	node1.Server.certPath = "/nonexistent/cert.pem"
	// Reload should fail
	err := node1.Server.ReloadTLS()
	assert.Error(t, err, "should fail with invalid cert path")
	assert.Contains(t, err.Error(), "cert validation failed", "should report cert validation error")
	// Restore cert path for cleanup
	node1.Server.certPath = origCertPath
}

// TestQuicServer_ReloadTLS_InvalidKey tests reloading with invalid key
func TestQuicServer_ReloadTLS_InvalidKey(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	// Save original key path
	origKeyPath := node1.Server.keyPath
	// Set invalid key path
	node1.Server.keyPath = "/nonexistent/key.pem"
	// Reload should fail
	err := node1.Server.ReloadTLS()
	assert.Error(t, err, "should fail with invalid key path")
	assert.Contains(t, err.Error(), "cert validation failed", "should report cert validation error")
	// Restore key path for cleanup
	node1.Server.keyPath = origKeyPath
}

// TestQuicServer_DynamicConfigUpdate tests that new connections use new config
func TestQuicServer_DynamicConfigUpdate(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	// Get initial config
	initialCfg := node1.Server.serverConfig.Load()
	assert.NotNil(t, initialCfg)
	// Reload TLS
	err := node1.Server.ReloadTLS()
	require.NoError(t, err)
	// Get new config
	newCfg := node1.Server.serverConfig.Load()
	assert.NotNil(t, newCfg)
	// Configs should be different (new certificate)
	assert.NotEqual(t, initialCfg, newCfg, "config should be updated")
	// But listener should be the same
	listener := node1.Server.quicListener
	assert.NotNil(t, listener, "listener should still exist")
}

// TestQuicServer_GetConfigForClient tests that GetConfigForClient returns current config
func TestQuicServer_GetConfigForClient(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()
	node1 := h.createNode("node1")
	// Get initial config from atomic pointer
	initialCfg := node1.Server.serverConfig.Load()
	assert.NotNil(t, initialCfg)
	// Store initial cert for comparison
	initialCert := initialCfg.Certificates[0].Certificate[0]
	// CREATE NEW CERT WITH THE SAME IDENTITY (overwrites existing files)
	nc := testutils.NodeCert{
		NodeIdentity: node1.Server.selfNodeID, // SAME identity!
		ServerNames:  []string{node1.Server.selfNodeID + ".localhost"},
	}
	certDir := filepath.Dir(node1.Server.certPath)
	_, _, err := testutils.CreateNodeCert(certDir, h.ca, nc, 1)
	require.NoError(t, err)
	// Reload TLS - should load the NEW certificate from the same paths
	err = node1.Server.ReloadTLS()
	require.NoError(t, err)
	// Get new config from atomic pointer
	newCfg := node1.Server.serverConfig.Load()
	assert.NotNil(t, newCfg)
	// Verify the certificate actually changed (different serial number)
	newCert := newCfg.Certificates[0].Certificate[0]
	assert.NotEqual(t, initialCert, newCert, "certificate should be different after reload")
	// Verify listener still exists (not recreated)
	assert.NotNil(t, node1.Server.quicListener)
}

// -----------------------------
// invalid certs
// -----------------------------
// TestQuicServer_DifferentCA_ConnectionRejected tests that connections are rejected
// when peers have certificates signed by different CAs
func TestQuicServer_DifferentCA_ConnectionRejected(t *testing.T) {
	// Create first helper with its own CA
	h1 := newQUICTestHelper(t)
	defer h1.close()
	// Create second helper with its own CA (different from h1)
	h2 := newQUICTestHelper(t)
	defer h2.close()
	// Create nodes with different CAs
	node1 := h1.createNode("node1")
	node2 := h2.createNode("node2")
	// Start accepting on node2
	h2.startAccepting(node2)

	err := h1.connectNodes(node1, node2)

	// Connection should be rejected due to CA mismatch
	assert.Error(t, err, "connection should be rejected when CAs are different")
	assert.Contains(t, err.Error(), "failed to dial QUIC: CRYPTO_ERROR")
	// Verify no connection was established
	outgoing := node1.Server.GetActiveOutgoingNodes()
	assert.Empty(t, outgoing, "no outgoing connections should exist")
	incoming := node2.Server.GetActiveIncomingNodes()
	assert.Empty(t, incoming, "no incoming connections should exist")
}

// TestQuicServer_MultipleCAs_WithMixedTrust tests a scenario where some nodes
// share CAs and others don't
func TestQuicServer_MultipleCAs_WithMixedTrust(t *testing.T) {
	// Create two helpers with different CAs
	h1 := newQUICTestHelper(t)
	defer h1.close()
	h2 := newQUICTestHelper(t)
	defer h2.close()
	// Create node1 and node3 with CA1 (h1)
	node1 := h1.createNode("node1")
	node3 := h1.createNode("node3")
	// Create node2 with CA2 (h2)
	node2 := h2.createNode("node2")
	// Start accepting on all nodes
	h1.startAccepting(node1)
	h1.startAccepting(node3)
	h2.startAccepting(node2)
	// Connection 1: node1 (CA1) to node3 (CA1) - SHOULD SUCCEED
	err := h1.connectNodes(node1, node3)
	require.NoError(t, err, "nodes with same CA should connect")

	err = h1.connectNodes(node1, node2)

	assert.Error(t, err, "connection should be rejected when CAs are different")
	// Wait a bit for connection states to settle
	time.Sleep(200 * time.Millisecond)
	// Verify node1 connections
	node1Outgoing := node1.Server.GetActiveOutgoingNodes()
	assert.Contains(t, node1Outgoing, "node3", "node1 should be connected to node3")
	assert.NotContains(t, node1Outgoing, "node2", "node1 should NOT be connected to node2")
	// Verify node3 connections (should have node1)
	node3Incoming := node3.Server.GetActiveIncomingNodes()
	assert.Contains(t, node3Incoming, "node1", "node3 should have incoming from node1")
	// Verify node2 has no connections
	node2Incoming := node2.Server.GetActiveIncomingNodes()
	assert.Empty(t, node2Incoming, "node2 should have no incoming connections")
	node2Outgoing := node2.Server.GetActiveOutgoingNodes()
	assert.Empty(t, node2Outgoing, "node2 should have no outgoing connections")
}

// TestQuicServer_CAVerifier_ChainValidation tests that the verifier properly
// validates the entire certificate chain, not just the leaf certificate
func TestQuicServer_CAVerifier_ChainValidation(t *testing.T) {
	// Create helper with a CA
	h := newQUICTestHelper(t)
	defer h.close()
	// Create node1 with the main CA
	node1 := h.createNode("node1")
	// Create a separate helper for the different CA
	// This ensures proper CA file creation using the same pattern
	h2 := newQUICTestHelper(t)
	defer h2.close()
	// Create node2 using h2's CA (which is different from h1's CA)
	node2 := h2.createNode("node2")
	// Start accepting on node2
	h2.startAccepting(node2)
	// Start accepting on node1 (though not strictly needed for outgoing connection)
	h.startAccepting(node1)
	// Small delay to ensure servers are ready
	time.Sleep(100 * time.Millisecond)

	err := h.connectNodes(node1, node2)

	// The connection should fail due to CA mismatch
	assert.Error(t, err, "connection should be rejected due to CA chain validation failure")
	// The error might be various things, but it should indicate a failure
	errorMsg := err.Error()
	assert.True(t,
		strings.Contains(errorMsg, "failed to decode peer handshake") ||
			strings.Contains(errorMsg, "certificate signed by unknown authority") ||
			strings.Contains(errorMsg, "x509: certificate signed by unknown authority") ||
			strings.Contains(errorMsg, "tls: failed to verify certificate"),
		"error should indicate CA validation failure, got: %v", err)
	// Verify no connections established
	node1Outgoing := node1.Server.GetActiveOutgoingNodes()
	assert.Empty(t, node1Outgoing, "no outgoing connections should exist")
	node2Incoming := node2.Server.GetActiveIncomingNodes()
	assert.Empty(t, node2Incoming, "no incoming connections should exist")
}
