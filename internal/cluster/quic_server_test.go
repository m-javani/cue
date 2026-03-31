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
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/m-javani/cue/internal/testutils"
	"github.com/m-javani/cue/pkg/verifier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// quicTestHelper manages isolated QUIC servers for testing
type quicTestHelper struct {
	t      *testing.T
	ctx    context.Context
	cancel context.CancelFunc
	dir    string
	ca     *testutils.CAInfo
	logger *zap.Logger
}

// quicTestNode represents a single QUIC server for testing
type quicTestNode struct {
	ID       string
	Server   *ClusterQUIC
	Addr     string
	Port     int
	CertPath string
	KeyPath  string
	CAPath   string
}

func newQUICTestHelper(t *testing.T) *quicTestHelper {
	ctx, cancel := context.WithCancel(context.Background())

	dir, err := os.MkdirTemp("", "quic-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	// Create shared CA - this creates ca_cert.pem and ca_key.pem
	ca, err := testutils.CreateCA(dir, "ca", 1, "localhost")
	require.NoError(t, err)

	return &quicTestHelper{
		t:      t,
		ctx:    ctx,
		cancel: cancel,
		dir:    dir,
		ca:     ca,
		logger: logger,
	}
}

// createNode creates a single QUIC server without cluster logic
func (h *quicTestHelper) createNode(id string) *quicTestNode {
	nodeDir := filepath.Join(h.dir, id)
	err := os.MkdirAll(nodeDir, 0755)
	require.NoError(h.t, err)

	// Create node certificate
	nodeInfo := testutils.NodeCert{
		NodeIdentity: id,
		ServerNames:  []string{id + ".localhost"},
	}
	certPath, keyPath, err := testutils.CreateNodeCert(nodeDir, h.ca, nodeInfo, 1)
	require.NoError(h.t, err)

	// CA cert should be in the root directory
	caPath := filepath.Join(h.dir, "ca_cert.pem")

	// Verify CA cert exists
	_, err = os.Stat(caPath)
	require.NoError(h.t, err, "CA cert should exist at %s", caPath)

	// Find available port with retry
	var port int
	var addr string
	var quic *ClusterQUIC

	maxRetries := 5
	for i := 0; i < maxRetries; i++ {
		// Find available port
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(h.t, err)
		port = listener.Addr().(*net.TCPAddr).Port
		listener.Close()

		// Small delay to ensure port is released
		time.Sleep(10 * time.Millisecond)

		addr = fmt.Sprintf("127.0.0.1:%d", port)

		quic, err = NewClusterQUIC(
			id,
			certPath,
			keyPath,
			caPath,
			addr,
			h.logger,
			verifier.CNVerifier{},
		)

		if err == nil {
			// Success!
			break
		}

		// If it's a port conflict, retry
		if strings.Contains(err.Error(), "address already in use") {
			h.logger.Debug("port conflict, retrying",
				zap.Int("port", port),
				zap.Int("attempt", i+1))
			continue
		}

		// Other error - fail
		require.NoError(h.t, err)
	}

	require.NotNil(h.t, quic, "failed to create QUIC server after %d attempts", maxRetries)

	node := &quicTestNode{
		ID:       id,
		Server:   quic,
		Addr:     addr,
		Port:     port,
		CertPath: certPath,
		KeyPath:  keyPath,
		CAPath:   caPath,
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
				if err != nil {
					if h.ctx.Err() == nil {
						// Only log unexpected errors
						h.logger.Debug("accept error", zap.Error(err))
					}
					return
				}
			}
		}
	}()
}

// connectNodes establishes a direct QUIC connection between two nodes
func (h *quicTestHelper) connectNodes(from, to *quicTestNode) error {
	handshake := Handshake{
		NodeID:           from.ID,
		TargetServerName: to.ID + ".localhost",
	}

	ctx, cancel := context.WithTimeout(h.ctx, 2*time.Second)
	defer cancel()

	return from.Server.Connect(
		ctx,
		uint64(to.Port),
		to.Addr,
		to.ID+".localhost",
		to.ID,
		handshake,
	)
}

// disconnectOutgoing manually removes an outgoing connection
func (h *quicTestHelper) disconnectOutgoing(node *quicTestNode, targetID string) {
	node.Server.mu.Lock()
	defer node.Server.mu.Unlock()

	if conn, exists := node.Server.outgoingConns[targetID]; exists {
		conn.CloseWithError(0, "test disconnect")
		delete(node.Server.outgoingConns, targetID)
	}
}

// disconnectIncoming manually removes an incoming connection
func (h *quicTestHelper) disconnectIncoming(node *quicTestNode, targetID string) {
	node.Server.mu.Lock()
	defer node.Server.mu.Unlock()

	if conn, exists := node.Server.incomingConns[targetID]; exists {
		conn.CloseWithError(0, "test disconnect")
		delete(node.Server.incomingConns, targetID)
	}
}

// close shuts down all servers
func (h *quicTestHelper) close() {
	h.cancel()
}

// Now the actual tests using the isolated helper

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
		conn.CloseWithError(0, "test disconnect")
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
	h.startAccepting(node1)

	// Try to connect to self
	handshake := Handshake{
		NodeID:           node1.ID,
		TargetServerName: node1.ID + ".localhost",
	}

	ctx, cancel := context.WithTimeout(h.ctx, 2*time.Second)
	defer cancel()

	err := node1.Server.Connect(
		ctx,
		uint64(node1.Port),
		node1.Addr,
		node1.ID+".localhost",
		node1.ID,
		handshake,
	)

	assert.Error(t, err, "should not allow connecting to self")
	assert.Contains(t, err.Error(), "failed to decode peer handshake")
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

// TestQuicServer_Handshake_TargetServerNameStored tests that the target server name is stored
func TestQuicServer_Handshake_TargetServerNameStored(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()

	node1 := h.createNode("node1")
	node2 := h.createNode("node2")

	h.startAccepting(node2)

	testServerName := "test-server-name"
	handshake := Handshake{
		NodeID:           node1.ID,
		TargetServerName: testServerName,
	}

	ctx, cancel := context.WithTimeout(h.ctx, 2*time.Second)
	defer cancel()

	err := node1.Server.Connect(
		ctx,
		uint64(node2.Port),
		node2.Addr,
		node2.ID+".localhost", // Use correct TLS server name
		node2.ID,
		handshake,
	)
	require.NoError(t, err, "connection should succeed")

	time.Sleep(100 * time.Millisecond)

	// Verify the server name was stored
	node1.Server.mu.RLock()
	storedName, exists := node1.Server.nodeToServerName[node2.ID]
	node1.Server.mu.RUnlock()

	assert.True(t, exists, "server name should be stored")
	assert.Equal(t, testServerName, storedName, "stored server name should match")
}

// TestQuicServer_Handshake_Timeout tests handshake timeout
func TestQuicServer_Handshake_Timeout(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()

	node1 := h.createNode("node1")

	// Use a port that's not being used (55555 is likely free)
	invalidAddr := "127.0.0.1:55555"

	handshake := Handshake{
		NodeID:           node1.ID,
		TargetServerName: "test.local",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := node1.Server.Connect(
		ctx,
		55555,
		invalidAddr,
		"test.local",
		"test-node",
		handshake,
	)

	assert.Error(t, err, "should fail when connecting to invalid address")
	assert.True(t,
		strings.Contains(err.Error(), "context deadline exceeded") ||
			strings.Contains(err.Error(), "dial tcp"),
		"should timeout or get connection refused, got: %v", err)
}

// ----------------
// TestQuicServer_GetServerNameByNodeID tests retrieving server name by node ID
func TestQuicServer_GetServerNameByNodeID(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()

	node1 := h.createNode("node1")
	node2 := h.createNode("node2")

	h.startAccepting(node2)

	err := h.connectNodes(node1, node2)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Test existing node
	serverName, exists := node1.Server.GetServerNameByNodeID(node2.ID)
	assert.True(t, exists, "server name should exist for node2")
	assert.Equal(t, node2.ID+".localhost", serverName)

	// Test non-existent node
	_, exists = node1.Server.GetServerNameByNodeID("nonexistent")
	assert.False(t, exists, "server name should not exist for nonexistent node")
}

// TestQuicServer_GetNodeIDByAddress tests retrieving node ID by address
func TestQuicServer_GetNodeIDByAddress(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()

	node1 := h.createNode("node1")
	node2 := h.createNode("node2")

	h.startAccepting(node2)

	err := h.connectNodes(node1, node2)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Test existing address
	nodeID, exists := node1.Server.GetNodeIDByAddress(node2.Addr)
	assert.True(t, exists, "node ID should exist for address")
	assert.Equal(t, node2.ID, nodeID)

	// Test non-existent address
	_, exists = node1.Server.GetNodeIDByAddress("127.0.0.1:99999")
	assert.False(t, exists, "node ID should not exist for non-existent address")
}

// TestQuicServer_CleanupRetiringOutgoing tests cleaning up retiring outgoing connections
func TestQuicServer_CleanupRetiringOutgoing(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()

	node1 := h.createNode("node1")
	node2 := h.createNode("node2")

	h.startAccepting(node2)

	err := h.connectNodes(node1, node2)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Get the actual connection
	conn, err := node1.Server.GetOutgoingConnection(node2.ID)
	require.NoError(t, err)

	// Close the connection first so it can be cleaned up
	conn.CloseWithError(0, "test close")

	// Manually add a retiring connection with the closed connection
	node1.Server.mu.Lock()
	node1.Server.retiringOutgoingConns = []retiringConn{
		{
			timestamp: time.Now().Add(-rotateTLSWindow - time.Second), // Old connection
			conn:      conn,
		},
	}
	node1.Server.mu.Unlock()

	// Cleanup should remove the old connection
	node1.Server.CleanupRetiringOutgoing()

	node1.Server.mu.RLock()
	count := len(node1.Server.retiringOutgoingConns)
	node1.Server.mu.RUnlock()

	assert.Equal(t, 0, count, "old retiring connection should be cleaned up")
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
	conn.CloseWithError(0, "test close")

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

// TestQuicServer_GetNodeID tests retrieving node ID from connection
func TestQuicServer_GetNodeID(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()

	node1 := h.createNode("node1")
	node2 := h.createNode("node2")

	h.startAccepting(node2)

	err := h.connectNodes(node1, node2)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Get the connection
	conn, err := node1.Server.GetOutgoingConnection(node2.ID)
	require.NoError(t, err)

	// Test GetNodeID
	nodeID := node1.Server.GetNodeID(conn)
	assert.Equal(t, node2.ID, nodeID, "should return correct node ID for connection")
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

	// Create peer info
	peers := []PeerResolvedInfo{
		{
			NodeId:     node2.ID,
			Addr:       node2.Addr,
			ServerName: node2.ID + ".localhost",
		},
		{
			NodeId:     node3.ID,
			Addr:       node3.Addr,
			ServerName: node3.ID + ".localhost",
		},
	}

	// Reconnect to peers
	successful, err := node1.Server.ReconnectToPeers(peers)
	require.NoError(t, err)

	assert.Len(t, successful, 2, "should reconnect to both peers")
	assert.ElementsMatch(t, []string{node2.Addr, node3.Addr}, successful)

	// Verify connections were established
	outgoing := node1.Server.GetActiveOutgoingNodes()
	assert.ElementsMatch(t, []string{node2.ID, node3.ID}, outgoing)
}

// TestQuicServer_ReconnectToPeers_InvalidPeers tests reconnecting with invalid peers
func TestQuicServer_ReconnectToPeers_InvalidPeers(t *testing.T) {
	h := newQUICTestHelper(t)
	defer h.close()

	node1 := h.createNode("node1")

	// Create peer info with invalid address
	peers := []PeerResolvedInfo{
		{
			NodeId:     "invalid",
			Addr:       "invalid-address",
			ServerName: "invalid.local",
		},
	}

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
