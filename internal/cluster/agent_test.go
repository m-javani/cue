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
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
	"github.com/m-javani/cue/internal/state"
	"github.com/m-javani/cue/internal/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type FakeHandler struct {
	cmdRouter *state.CommandRouter
	logger    *zap.Logger
}

func NewFakeHandler(logger *zap.Logger, cmdRouter *state.CommandRouter) state.Handler {
	return &FakeHandler{
		cmdRouter: cmdRouter,
		logger:    logger,
	}
}

func (h *FakeHandler) ProcessCommand(ctx context.Context, topic string, cmd *model.Command, index uint64) error {
	if cmd.RespInfo == nil || cmd.RespInfo.RespCh == nil {
		return nil
	}
	select {
	case cmd.RespInfo.RespCh <- model.ToProducerResponse{
		RequestID: cmd.RespInfo.RequestID,
		Status:    "success",
		Error:     "",
	}:
	case <-ctx.Done():
		return ctx.Err()
	default:
		// h.logger.Error("command channel full, dropping command")
	}
	return nil
}

// === NEW: Node name mapping helpers ===

type nodeMapping struct {
	nodeID     string
	internalID string
	quicPort   uint16
	peerInfo   model.PeerInfo
}

// TestCluster manages test nodes for integration tests
type TestCluster struct {
	nodes         map[string]*testNode    // key = userNodeID
	nodeMapping   map[string]*nodeMapping // userNodeID -> mapping
	testDirs      map[string]string       // userNodeID -> data directory path
	baseDir       string
	testBaseDir   string
	t             testing.TB
	mu            sync.RWMutex
	logger        *zap.Logger
	cleanedUp     bool
	ca            *testutils.CAInfo
	peers         []model.PeerInfo // user node IDs
	randSrc       *rand.Rand
	certBasePath  string
	initialVoters []string
}

// testNode holds everything needed for one node (internalID is used for transport)
type testNode struct {
	agent      *ClusterAgent
	quicServer *ClusterQUIC
	commandCh  chan model.Command
	cancel     context.CancelFunc
	dataDir    string
	quicPort   uint16
	internalID string // e.g. "NC78YT-49217"
	nodeID     string
	started    bool
}

// NewTestCluster creates a new test cluster with persistent disk state
func NewTestCluster(t testing.TB) (*TestCluster, error) {
	logger, err := testutils.NewDevLogger()
	if err != nil {
		return nil, err
	}

	baseDir := testutils.GetTesDataPath()
	testBaseDir := filepath.Join(baseDir, fmt.Sprintf("test-%s", t.Name()))

	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create testdata directory: %w", err)
	}
	if err := os.MkdirAll(testBaseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create testBaseDir directory: %w", err)
	}

	certBasePath := testutils.GetCertsPath() + "/" + t.Name()
	ca, err := testutils.CreateCA(certBasePath, "ca", 1, "localhost")
	if err != nil {
		return nil, err
	}

	// Seed randomness based on test name for better separation between parallel tests
	seed := int64(0)
	for _, r := range t.Name() {
		seed = seed*31 + int64(r)
	}

	tc := &TestCluster{
		nodes:        make(map[string]*testNode),
		nodeMapping:  make(map[string]*nodeMapping),
		testDirs:     make(map[string]string),
		baseDir:      baseDir,
		testBaseDir:  testBaseDir,
		t:            t,
		logger:       logger,
		cleanedUp:    false,
		ca:           ca,
		randSrc:      rand.New(rand.NewSource(seed)),
		certBasePath: certBasePath,
	}

	t.Cleanup(func() {
		if err := tc.Cleanup(); err != nil {
			tc.logger.Error("failed to cleanup test cluster", zap.Error(err))
		}
	})

	return tc, nil
}

func (tc *TestCluster) UpsertTestClusterPeers(peers []string) {
	// Build peers list with internal IDs
	internalPeers := make([]model.PeerInfo, len(tc.peers))
	for _, p := range peers {
		if pm, ok := tc.nodeMapping[p]; ok {
			internalPeers = append(internalPeers, pm.peerInfo)
		} else {
			mapping := tc.getOrCreateMapping(p)
			internalPeers = append(internalPeers, mapping.peerInfo)
		}
	}
	tc.peers = internalPeers // user node IDs
}

// generateRandomPort returns a random port in 30000-60000 range (no availability check)
func (tc *TestCluster) generateRandomPort() uint16 {
	return 30000 + uint16(tc.randSrc.Intn(30001))
}

// getOrCreateMapping returns (or creates) the internal mapping for a node ID
func (tc *TestCluster) getOrCreateMapping(nodeID string) *nodeMapping {
	if m, exists := tc.nodeMapping[nodeID]; exists {
		return m
	}

	port := tc.generateRandomPort()
	internalID := fmt.Sprintf("%s-%d", nodeID, port)

	m := &nodeMapping{
		nodeID:     nodeID,
		internalID: internalID,
		quicPort:   port,
		peerInfo: model.PeerInfo{
			NodeID: internalID,
			Host:   "127.0.0.1",
			Identity: model.TLSIdentity{
				Kind:  model.IdentityDNS,
				Value: internalID + ".localhost",
			},
			Port: port,
		},
	}
	tc.nodeMapping[nodeID] = m
	return m
}

// AddNode adds a new node (does NOT start it automatically)
// If customPeers is nil, uses tc.peers (default behavior)
func (tc *TestCluster) AddNode(
	nodeID string,
	useExisting bool,
	snapshotIntervalSec uint64,
	snapshotTriggerCount uint64,
	walFlushThreshold int,
	customPeers ...[]string, // optional: custom initial peers
) error {

	tc.mu.Lock()
	defer tc.mu.Unlock()

	if snapshotIntervalSec == 0 {
		snapshotIntervalSec = 30
	}
	if snapshotTriggerCount == 0 {
		snapshotTriggerCount = 1000
	}
	if walFlushThreshold == 0 {
		walFlushThreshold = 100
	}

	if _, exists := tc.nodes[nodeID]; exists {
		return fmt.Errorf("node %s already exists", nodeID)
	}

	mapping := tc.getOrCreateMapping(nodeID)
	// Determine peers: use custom if provided, otherwise use tc.peers
	var peers []model.PeerInfo
	if len(customPeers) > 0 && customPeers[0] != nil {
		// Convert user IDs to internal IDs
		for _, userID := range customPeers[0] {
			if pm, exists := tc.nodeMapping[userID]; exists {
				peers = append(peers, pm.peerInfo)
			} else {
				peers = append(peers, mapping.peerInfo)
			}
		}
	} else {
		// Use default peers (tc.peers are already internal IDs)
		peers = tc.peers
	}

	// Data directory is always based on userNodeID (stable across restarts)
	dataDir := filepath.Join(tc.testBaseDir, nodeID)
	tc.testDirs[nodeID] = dataDir

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create testdata directory: %w", err)
	}

	if !useExisting {
		if err := os.RemoveAll(dataDir); err != nil {
			return fmt.Errorf("failed to remove existing data dir: %w", err)
		}
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return fmt.Errorf("failed to create data dir: %w", err)
		}
	}

	commandCh := make(chan model.Command, 64)

	logger, err := testutils.NewDevLogger()
	if err != nil {
		return err
	}

	// Certs are created using userNodeID (stable paths)
	certDir := tc.certBasePath
	nc := testutils.NodeCert{
		NodeIdentity: mapping.internalID, // user name for cert
		ServerNames:  []string{mapping.internalID + ".localhost"},
	}
	certPath, keyPath, err := testutils.CreateNodeCert(certDir, tc.ca, nc, 1)
	if err != nil {
		return err
	}
	caPath := certDir + "/ca_cert.pem"

	// Use internalID + port for QUIC
	listenAddr := fmt.Sprintf("127.0.0.1:%d", mapping.quicPort)

	discovery, err := NewServiceDiscovery(logger, peers, mapping.internalID, internal.DiscoveryKindStatic, "", time.Duration(1*time.Second))
	if err != nil {
		return err
	}
	quicServer, err := NewClusterQUIC(
		mapping.internalID, // internal name for transport layer
		int(mapping.quicPort),
		certPath, keyPath, caPath,
		listenAddr,
		logger,
		discovery,
	)
	if err != nil {
		return fmt.Errorf("failed to create QUIC server for %s: %w", nodeID, err)
	}

	cfg := internal.ClusterConfig{
		InitialVoters:        tc.initialVoters, // Use custom peers or default
		ListenAddr:           listenAddr,
		QUICPort:             mapping.quicPort,
		SnapshotIntervalSec:  snapshotIntervalSec,
		SnapshotTriggerCount: snapshotTriggerCount,
		WALFlushThreshold:    walFlushThreshold,
		CertPath:             certPath,
		KeyPath:              keyPath,
		CACertPath:           caPath,
		RaftTickMs:           100,
		RaftHeartbeatTick:    2,
		RaftElectionTick:     10,
		DLQMaxSizeBytes:      1024,
	}

	handler := NewFakeHandler(logger, nil)
	ctx, cancel := context.WithCancel(context.Background())
	var status atomic.Uint32
	status.Store(model.NodeStatusUnavailable.ToUin32())
	currentTerm := atomic.Uint64{}
	currentTerm.Store(0)
	members := &model.Members{
		Voters:   []string{},
		Learners: []string{},
	}
	peerStore := &model.PeerStore{Peers: make(map[string]model.PeerInfo)}
	leaderID := &atomic.Value{}
	leaderID.Store("")

	agent, err := NewClusterAgent(
		ctx,
		cancel,
		mapping.internalID,
		cfg,
		dataDir,
		commandCh,
		handler,
		quicServer,
		&status,
		&currentTerm,
		members,
		peerStore,
		leaderID,
		discovery,
		logger)
	if err != nil {
		return fmt.Errorf("failed to create ClusterAgent for %s: %w", nodeID, err)
	}

	node := &testNode{
		agent:      agent,
		quicServer: quicServer,
		commandCh:  commandCh,
		cancel:     cancel,
		dataDir:    dataDir,
		quicPort:   mapping.quicPort,
		internalID: mapping.internalID,
		nodeID:     nodeID,
		started:    false,
	}

	tc.nodes[nodeID] = node

	return nil
}

func (tc *TestCluster) SetInitialVoters(initial []string) {
	var initialVoters []string
	for _, v := range initial {
		mapping := tc.getOrCreateMapping(v)
		initialVoters = append(initialVoters, mapping.internalID)

	}
	tc.initialVoters = initialVoters
}

// In TestCluster, add a method:
func (tc *TestCluster) UpdateAllNodesDiscoveries() {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	// Build full peer list from all mappings
	allPeers := make([]model.PeerInfo, 0, len(tc.nodeMapping))
	for _, m := range tc.nodeMapping {
		allPeers = append(allPeers, m.peerInfo)
	}

	// Update all nodes' discoveries
	for _, node := range tc.nodes {
		if node.agent != nil && node.agent.discovery != nil {
			node.agent.discovery.UpdatePeers(allPeers)
		}
	}
}

func (tc *TestCluster) UpdatePeerDiscoveryByNodeID(nodeID string) error {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	// Build full peer list from all mappings
	allPeers := make([]model.PeerInfo, 0, len(tc.nodeMapping))
	for _, m := range tc.nodeMapping {
		allPeers = append(allPeers, m.peerInfo)
	}

	agent, err := tc.GetAgent(nodeID)
	if err != nil {
		return err
	}
	// Update nodes' discovery
	if agent != nil && agent.discovery != nil {
		agent.discovery.UpdatePeers(allPeers)
	}

	return nil
}

// StartNode starts a node (userNodeID)
func (tc *TestCluster) StartNode(nodeID string) error {
	tc.mu.RLock()
	node, exists := tc.nodes[nodeID]
	tc.mu.RUnlock()
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}
	if node.started {
		return nil
	}

	go func() {
		if err := node.agent.Start(); err != nil {
			tc.logger.Error("agent exited with error", zap.Error(err))
		}
	}()
	node.started = true
	return nil
}

// RemoveNode stops and removes node from memory (preserves disk state)
func (tc *TestCluster) RemoveNode(nodeID string) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	node, exists := tc.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	if node.started {
		node.cancel()
	}
	time.Sleep(1 * time.Second)

	delete(tc.nodes, nodeID)
	// Note: we keep the mapping so port stays stable on re-AddNode in same test

	node = nil
	runtime.GC()
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	return nil
}

// GetAgent returns the ClusterAgent for probing (userNodeID)
func (tc *TestCluster) GetAgent(nodeID string) (*ClusterAgent, error) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	node, exists := tc.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	return node.agent, nil
}

// GetNode returns the testNode for probing
func (tc *TestCluster) GetNode(nodeID string) (*testNode, error) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	node, exists := tc.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	return node, nil
}

// GetLeaderID returns current leader userNodeID
func (tc *TestCluster) GetLeaderID() (string, error) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	for id, node := range tc.nodes {
		if node.agent.IsLeader() {
			return id, nil // return userID
		}
	}
	return "", fmt.Errorf("no leader found")
}
func (tc *TestCluster) AssertAllNodesKnowLeader() bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	for _, node := range tc.nodes {
		if node.agent.GetLeaderID() == "" {
			return false
		}
	}
	return true
}

// GetFollowerIDs returns all follower userNodeIDs
func (tc *TestCluster) GetFollowerIDs() []string {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	var followers []string
	for id, node := range tc.nodes {
		if !node.agent.IsLeader() && node.agent.IsActive() {
			followers = append(followers, id) // userID
		}
	}
	return followers
}

// WaitForClusterFormed, WaitForNodeCatchUp, Propose remain unchanged (they already use userNodeID)

func (tc *TestCluster) WaitForClusterFormed(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		tc.mu.RLock()
		activeCount := 0
		for _, node := range tc.nodes {
			if node.agent.IsActive() {
				activeCount++
			}
		}
		tc.mu.RUnlock()
		if activeCount == len(tc.nodes) {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("cluster failed to form within %v", timeout)
}

func (tc *TestCluster) WaitForNodeCatchUp(nodeID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	agent, err := tc.GetAgent(nodeID)
	for time.Now().Before(deadline) {
		if err == nil && agent.IsActive() && !agent.IsLeader() {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("node %s failed to catch up within %v", nodeID, timeout)
}

func (tc *TestCluster) Propose(ctx context.Context, cmd model.Command) error {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	for _, node := range tc.nodes {
		if node.started && node.agent.IsLeader() {
			select {
			case node.commandCh <- cmd:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			default:
				tc.logger.Error("couldnt propose cmd")
			}
		}
	}
	return fmt.Errorf("no leader found")
}

// Cleanup stops all nodes without deleting disk state
func (tc *TestCluster) Cleanup() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for id, node := range tc.nodes {
		if node.started {
			node.cancel()
		}
		delete(tc.nodes, id)
	}

	if err := os.RemoveAll(tc.certBasePath); err != nil {
		return fmt.Errorf("failed to remove cert dir: %w", err)
	}

	if err := os.RemoveAll(tc.testBaseDir); err != nil {
		return fmt.Errorf("failed to remove test dir: %w", err)
	}

	return nil
}

// ReplaceNodeCerts replaces the TLS certificates for one or all nodes with new certificates
// signed by the same CA. If nodeID is empty, replaces certs for all nodes.
// Returns a map of nodeID -> error for any failures.
func (tc *TestCluster) ReplaceNodeCerts(nodeID string) (map[string]error, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	results := make(map[string]error)

	var targetNodes []string
	if nodeID != "" {
		if _, exists := tc.nodes[nodeID]; !exists {
			return nil, fmt.Errorf("node %s not found", nodeID)
		}
		targetNodes = []string{nodeID}
	} else {
		for id := range tc.nodes {
			targetNodes = append(targetNodes, id)
		}
	}

	for _, id := range targetNodes {
		mapping := tc.nodeMapping[id]

		// Create new cert with same identity - this overwrites existing files
		nc := testutils.NodeCert{
			NodeIdentity: mapping.internalID,
			ServerNames:  []string{mapping.internalID + ".localhost"},
		}

		// Just write the new certs - the watcher will detect and trigger reload
		_, _, err := testutils.CreateNodeCert(tc.certBasePath, tc.ca, nc, 1)
		if err != nil {
			results[id] = fmt.Errorf("failed to create new cert: %w", err)
			continue
		}

		// No manual reload - let the watcher do its job!
		results[id] = nil
	}

	return results, nil
}

func (a *ClusterAgent) GetQUICServer() *ClusterQUIC {
	return a.quicServer
}

// ------------------------------------------------
// Cluster Formation
// ------------------------------------------------
func TestClusterFormationAndQuorum(t *testing.T) {
	// Create test cluster with 3 nodes
	cl, err := NewTestCluster(t)
	require.NoError(t, err)

	// Add 3 nodes with proper peer and voter configuration
	voters := []string{"node1", "node2", "node3"}
	cl.UpsertTestClusterPeers(voters)
	cl.SetInitialVoters(voters)

	for _, nodeID := range voters {
		err := cl.AddNode(nodeID, false, 0, 0, 0)
		require.NoError(t, err)
	}
	cl.UpdateAllNodesDiscoveries()
	// Start all nodes
	for _, nodeID := range voters {
		err := cl.StartNode(nodeID)
		require.NoError(t, err)
	}

	// Wait for cluster to form
	err = cl.WaitForClusterFormed(5 * time.Second)
	require.NoError(t, err, "cluster should form within timeout")

	// Verify leader is elected
	leaderID, err := cl.GetLeaderID()
	require.NoError(t, err, "should have a leader")
	t.Logf("Leader elected: %s", leaderID)

	allKnowLeader := cl.AssertAllNodesKnowLeader()
	require.True(t, allKnowLeader, "should have a leader")

	// Verify leader status
	leaderAgent, err := cl.GetAgent(leaderID)
	require.NoError(t, err)
	assert.Equal(t, model.NodeStatusLeaderActive, leaderAgent.GetStatus(),
		"leader should have LeaderActive status")

	// Verify followers
	followers := cl.GetFollowerIDs()
	assert.Len(t, followers, 2, "should have 2 followers")

	for _, followerID := range followers {
		followerAgent, err := cl.GetAgent(followerID)
		require.NoError(t, err)
		assert.Equal(t, model.NodeStatusFollowerActive, followerAgent.GetStatus(),
			"follower %s should have FollowerActive status", followerID)
	}

	// Test quorum maintenance - stop one follower (still have 2/3 nodes)
	t.Log("Stopping one follower - cluster should maintain quorum")
	followerToStop := followers[0]
	err = cl.RemoveNode(followerToStop)
	require.NoError(t, err)

	// Wait for cluster to detect node loss
	time.Sleep(3 * time.Second)

	// With 2 out of 3 nodes, should still have a leader
	leaderID2, err := cl.GetLeaderID()
	require.NoError(t, err, "should still have leader with 2/3 nodes")
	t.Logf("Leader after stopping follower: %s", leaderID2)

	// Test quorum loss - stop second follower (only 1 node left)
	t.Log("Stopping second follower - cluster should lose quorum")
	secondFollower := followers[1]
	err = cl.RemoveNode(secondFollower)
	require.NoError(t, err)

	// Wait for leader to detect loss of quorum
	time.Sleep(4 * time.Second)

	// The last remaining node (formerly leader) should be UNAVAILABLE
	remainingNodeAgent, err := cl.GetAgent(leaderID2)
	require.NoError(t, err, "last remaining node should still be accessible")
	finalStatus := remainingNodeAgent.GetStatus()

	// After quorum loss, the remaining node should be Unavailable
	assert.Equal(t, model.NodeStatusUnavailable, finalStatus,
		"remaining node should be Unavailable after quorum loss, got %v", finalStatus)

	// Verify no leader is reported
	leaderID3, err := cl.GetLeaderID()
	assert.Error(t, err, "should not find leader after quorum loss")
	assert.Empty(t, leaderID3, "leader ID should be empty")

	t.Log("✓ Quorum loss correctly detected - last node is Unavailable, no leader elected")
}

// ------------------------------------------------
// Leader Crash
// ------------------------------------------------
func TestLeaderCrashAndReelection(t *testing.T) {
	// Create test cluster with 3 nodes
	cl, err := NewTestCluster(t)
	require.NoError(t, err)

	// Add 3 nodes
	voters := []string{"node1", "node2", "node3"}
	cl.UpsertTestClusterPeers(voters)
	cl.SetInitialVoters(voters)

	for _, nodeID := range voters {
		err := cl.AddNode(nodeID, false, 0, 0, 0)
		require.NoError(t, err)
	}
	cl.UpdateAllNodesDiscoveries()
	// Start all nodes
	for _, nodeID := range voters {
		err := cl.StartNode(nodeID)
		require.NoError(t, err)
	}

	// Wait for cluster to form
	err = cl.WaitForClusterFormed(5 * time.Second)
	require.NoError(t, err, "cluster should form within timeout")

	// Get initial leader
	originalLeader, err := cl.GetLeaderID()
	require.NoError(t, err, "should have a leader")
	t.Logf("Original leader: %s", originalLeader)

	// Get the followers
	var followers []string
	for _, nodeID := range voters {
		if nodeID != originalLeader {
			followers = append(followers, nodeID)
		}
	}
	t.Logf("Followers: %v", followers)

	// Remove the leader (simulates crash)
	t.Logf("Removing leader node: %s", originalLeader)
	err = cl.RemoveNode(originalLeader)
	require.NoError(t, err)

	// Wait for election timeout and new leader election
	t.Log("Waiting for new leader election...")
	time.Sleep(6 * time.Second)

	// Get the new leader - should be one of the followers
	newLeader, err := cl.GetLeaderID()
	require.NoError(t, err, "should elect a new leader after leader crash")
	t.Logf("New leader elected: %s", newLeader)

	// Verify new leader is not the crashed leader
	assert.NotEqual(t, originalLeader, newLeader,
		"new leader should be different from crashed leader")

	// Verify new leader is one of the remaining nodes
	assert.Contains(t, followers, newLeader,
		"new leader should be one of the remaining nodes (followers: %v, got: %s)", followers, newLeader)

	// Verify new leader status
	newLeaderAgent, err := cl.GetAgent(newLeader)
	require.NoError(t, err)
	assert.Equal(t, model.NodeStatusLeaderActive, newLeaderAgent.GetStatus(),
		"new leader should have LeaderActive status")

	// Verify the other remaining node is a follower
	for _, nodeID := range followers {
		if nodeID != newLeader {
			otherNodeAgent, err := cl.GetAgent(nodeID)
			require.NoError(t, err)
			assert.Equal(t, model.NodeStatusFollowerActive, otherNodeAgent.GetStatus(),
				"remaining node %s should have FollowerActive status", nodeID)
			t.Logf("Node %s is follower", nodeID)
		}
	}

	// Verify cluster still has quorum (2 out of 3 nodes, but one is removed)
	activeCount := 0
	for _, nodeID := range []string{"node1", "node2", "node3"} {
		agent, err := cl.GetAgent(nodeID)
		if err == nil && agent.IsActive() {
			activeCount++
		}
	}
	assert.Equal(t, 2, activeCount, "should have 2 active nodes after leader crash")
	t.Logf("Active nodes after crash: %d", activeCount)

	t.Log("✓ Leader crash test passed - new leader successfully elected")
}

// ------------------------------------------------
// Replication
// ------------------------------------------------
func TestReplication(t *testing.T) {
	// Create test cluster with 3 nodes
	cl, err := NewTestCluster(t)
	require.NoError(t, err)

	// Add 3 nodes
	voters := []string{"node1", "node2", "node3"}
	cl.UpsertTestClusterPeers(voters)
	cl.SetInitialVoters(voters)

	for _, nodeID := range voters {
		err := cl.AddNode(nodeID, false, 0, 0, 0)
		require.NoError(t, err)
	}
	cl.UpdateAllNodesDiscoveries()
	// Start all nodes
	for _, nodeID := range voters {
		err := cl.StartNode(nodeID)
		require.NoError(t, err)
	}

	// Wait for cluster to form
	err = cl.WaitForClusterFormed(10 * time.Second)
	require.NoError(t, err, "cluster should form within timeout")

	// Get leader
	leaderID, err := cl.GetLeaderID()
	require.NoError(t, err, "should have a leader")
	t.Logf("Leader: %s", leaderID)

	// Get initial last applied indexes
	initialIndexes := make(map[string]uint64)
	for _, nodeID := range voters {
		agent, err := cl.GetAgent(nodeID)
		require.NoError(t, err)
		initialIndexes[nodeID] = agent.GetLastAppliedIndex()
		t.Logf("Node %s initial last applied index: %d", nodeID, initialIndexes[nodeID])
	}

	// Send 10 commands
	numCommands := 2
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Logf("Sending %d commands to leader %s...", numCommands, leaderID)

	var successCount atomic.Int32
	successCount.Store(0)
	for i := 1; i <= numCommands; i++ {
		respCh := make(chan model.ToProducerResponse, 1)

		cmd := model.Command{
			Type: model.CmdAddJob,
			AddJob: &model.AddJobPayload{
				Job: model.Job{
					ID:    fmt.Sprintf("job-%d", i),
					Topic: "test-topic",
					Data:  fmt.Appendf(nil, "payload-%d", i),
				},
			},
			RespInfo: &model.RespInfo{
				RequestID: "a-request-id",
				RespCh:    respCh,
			},
		}

		wg.Add(1)
		go func(cmd model.Command, idx int) {
			defer wg.Done()

			// Propose command to leader
			err := cl.Propose(ctx, cmd)
			if err != nil {
				t.Logf("Failed to propose command %d: %v", idx, err)
				return
			}

			// Wait for response
			select {
			case respInfo := <-respCh:
				if respInfo.Status != "success" {
					t.Logf("Command %d failed: %v", idx, err)
				} else {
					t.Logf("Command %d committed successfully", idx)
					successCount.Add(1)
				}
			case <-ctx.Done():
				t.Logf("Command %d timed out", idx)
			}
		}(cmd, i)

		// Small delay between commands to avoid overwhelming
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for all commands to complete
	wg.Wait()
	sc := successCount.Load()
	t.Logf("All commands sent. Successful commits: %d/%d", sc, numCommands)
	assert.Equal(t, numCommands, int(sc), "all commands should succeed")

	// Wait for replication to all nodes
	t.Log("Waiting for replication to all nodes...")
	time.Sleep(3 * time.Second)

	// Get last applied index from all nodes
	type nodeInfo struct {
		id        string
		lastIndex uint64
		term      uint64
		isActive  bool
	}

	var nodesInfo []nodeInfo
	var finalIndexes []uint64

	for _, nodeID := range voters {
		agent, err := cl.GetAgent(nodeID)
		if err != nil {
			t.Logf("Node %s not accessible: %v", nodeID, err)
			continue
		}

		lastIndex := agent.GetLastAppliedIndex()
		currentTerm := agent.GetCurrentTerm()
		isActive := agent.IsActive()

		nodesInfo = append(nodesInfo, nodeInfo{
			id:        nodeID,
			lastIndex: lastIndex,
			term:      currentTerm,
			isActive:  isActive,
		})
		finalIndexes = append(finalIndexes, lastIndex)

		t.Logf("Node %s - lastIndex: %d, term: %d, active: %v",
			nodeID, lastIndex, currentTerm, isActive)
	}

	// Verify all nodes are active
	for _, info := range nodesInfo {
		assert.True(t, info.isActive, "node %s should be active", info.id)
	}

	// Verify all nodes have the same last applied index
	assert.Len(t, finalIndexes, 3, "should have all 3 nodes")
	for i := 1; i < len(finalIndexes); i++ {
		assert.Equal(t, finalIndexes[0], finalIndexes[i],
			"all nodes should have same last applied index, got %v", finalIndexes)
	}

	t.Logf("All nodes have same last applied index: %d", finalIndexes[0])

	// Verify we applied at least the 10 commands
	// Initial snapshot has index 1, so 10 commands should bring us to index 11
	expectedMinIndex := initialIndexes[leaderID] + uint64(numCommands)
	assert.GreaterOrEqual(t, finalIndexes[0], expectedMinIndex,
		"should have applied at least %d commands (from %d to %d), got index %d",
		numCommands, initialIndexes[leaderID], expectedMinIndex, finalIndexes[0])

	// Verify each node's last index increased by at least the number of commands
	for nodeID, initialIdx := range initialIndexes {
		for _, info := range nodesInfo {
			if info.id == nodeID {
				increase := info.lastIndex - initialIdx
				t.Logf("Node %s increased by %d indexes", nodeID, increase)
				assert.GreaterOrEqual(t, increase, uint64(numCommands),
					"node %s should have applied at least %d commands", nodeID, numCommands)
				break
			}
		}
	}

	t.Log("✓ Replication test passed - all 3 nodes synced and consistent")
}

// ------------------------------------------------
// Catchup
// ------------------------------------------------
func TestNewNodeCatchesUp(t *testing.T) {
	// Start with 3-node cluster
	cl, err := NewTestCluster(t)
	require.NoError(t, err)

	// Create and start initial 3 nodes
	voters := []string{"node1", "node2", "node3"}
	cl.UpsertTestClusterPeers(voters)
	cl.SetInitialVoters(voters)

	for _, nodeID := range voters {
		err := cl.AddNode(nodeID, false, 0, 0, 0)
		require.NoError(t, err)
	}
	cl.UpdateAllNodesDiscoveries()
	for _, nodeID := range voters {
		err = cl.StartNode(nodeID)
		require.NoError(t, err)
	}
	// Wait for cluster to form
	err = cl.WaitForClusterFormed(10 * time.Second)
	require.NoError(t, err)

	leaderID, err := cl.GetLeaderID()
	require.NoError(t, err)

	// Send 20 commands to create state
	numCommands := 5
	ctx := context.Background()

	for i := 1; i <= numCommands; i++ {
		respCh := make(chan model.ToProducerResponse, 1)
		cmd := model.Command{
			Type: model.CmdAddJob,
			AddJob: &model.AddJobPayload{
				Job: model.Job{
					ID:    fmt.Sprintf("job-%d", i),
					Topic: "test-topic",
					Data:  fmt.Appendf(nil, "payload-%d", i),
				},
			},
			RespInfo: &model.RespInfo{
				RequestID: fmt.Sprintf("job-%d", i),
				RespCh:    respCh,
			},
		}

		err := cl.Propose(ctx, cmd)
		require.NoError(t, err)

		select {
		case respInfo := <-respCh:
			require.Equal(t, "success", string(respInfo.Status))
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for command")
		}
	}

	// Add and start new node (node4)
	newNodeID := "node4"
	err = cl.AddNode(newNodeID, false, 0, 0, 0)
	require.NoError(t, err)

	cl.UpdateAllNodesDiscoveries()

	// Start the new node - it should catch up
	startTime := time.Now()
	err = cl.StartNode(newNodeID)
	require.NoError(t, err)

	// Wait for new node to catch up
	err = cl.WaitForNodeCatchUp(newNodeID, 10*time.Second)
	require.NoError(t, err)

	catchupDuration := time.Since(startTime)

	// Verify new node has same last index
	newAgent, err := cl.GetAgent(newNodeID)
	require.NoError(t, err)

	// Get current state of initial nodes
	initialAgent, _ := cl.GetAgent(leaderID)
	expectedLastIndex := initialAgent.GetLastAppliedIndex()

	finalIndex := newAgent.GetLastAppliedIndex()
	assert.Equal(t, expectedLastIndex, finalIndex,
		"new node should have caught up to index %d, got %d", expectedLastIndex, finalIndex)

	// Verify new node is active
	assert.True(t, newAgent.IsActive(), "new node should be active")
	assert.False(t, newAgent.IsLeader(), "new node should not be leader")

	// Try sending more commands after join
	respCh := make(chan model.ToProducerResponse, 1)
	cmd := model.Command{
		Type: model.CmdAddJob,
		AddJob: &model.AddJobPayload{
			Job: model.Job{
				ID:    "job-after-join",
				Topic: "test-topic",
				Data:  []byte("post-join-payload"),
			},
		},
		RespInfo: &model.RespInfo{
			RequestID: "a-request-id",
			RespCh:    respCh,
		},
	}

	err = cl.Propose(ctx, cmd)
	require.NoError(t, err)

	select {
	case respInfo := <-respCh:
		require.Equal(t, "success", string(respInfo.Status))
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for post-join command")
	}

	// Verify new node applied the new command
	time.Sleep(2 * time.Second) // Allow replication

	// Get current state of initial nodes
	expectedLastIndex = initialAgent.GetLastAppliedIndex()

	newFinalIndex := newAgent.GetLastAppliedIndex()
	assert.Equal(t, expectedLastIndex, newFinalIndex,
		"new node should have applied new command")

	t.Logf("New node caught up in %v and applied %d commands",
		catchupDuration, newFinalIndex)
}

// ------------------------------------------------
// Restart
// ------------------------------------------------

func TestNodeCatchesUpAfterRestart(t *testing.T) {
	// Create 3-node cluster
	cl, err := NewTestCluster(t)
	require.NoError(t, err)

	voters := []string{"node1", "node2", "node3"}
	cl.UpsertTestClusterPeers(voters)
	cl.SetInitialVoters(voters)

	for _, nodeID := range voters {
		err := cl.AddNode(nodeID, false, 0, 0, 0)
		require.NoError(t, err)
	}
	cl.UpdateAllNodesDiscoveries()
	for _, nodeID := range voters {
		err = cl.StartNode(nodeID)
		require.NoError(t, err)
	}

	err = cl.WaitForClusterFormed(10 * time.Second)
	require.NoError(t, err)

	// Send initial commands
	ctx := context.Background()
	for i := 1; i <= 15; i++ {
		respCh := make(chan model.ToProducerResponse, 1)
		cmd := model.Command{
			Type: model.CmdAddJob,
			AddJob: &model.AddJobPayload{
				Job: model.Job{
					ID:    fmt.Sprintf("job-%d", i),
					Topic: "test-topic",
					Data:  []byte(fmt.Sprintf("payload-%d", i)),
				},
			},
			RespInfo: &model.RespInfo{
				RequestID: "a-request-id",
				RespCh:    respCh,
			},
		}
		err := cl.Propose(ctx, cmd)
		require.NoError(t, err)
	}

	// Get current state
	leaderID, _ := cl.GetLeaderID()
	leaderAgent, _ := cl.GetAgent(leaderID)
	time.Sleep(2 * time.Second)
	curIndex := leaderAgent.GetLastAppliedIndex()

	// Stop a follower
	followerIDs := cl.GetFollowerIDs()
	require.NotEmpty(t, followerIDs, "should have at least one follower")
	stoppedNode := followerIDs[0]

	fmt.Printf("Stopping node %s at index %d", stoppedNode, curIndex)
	err = cl.RemoveNode(stoppedNode)
	require.NoError(t, err)

	// Send more commands while node is down
	for i := 16; i <= 25; i++ {
		respCh := make(chan model.ToProducerResponse, 1)
		cmd := model.Command{
			Type: model.CmdAddJob,
			AddJob: &model.AddJobPayload{
				Job: model.Job{
					ID:    fmt.Sprintf("job-%d", i),
					Topic: "test-topic",
					Data:  fmt.Appendf(nil, "payload-%d", i),
				},
			},
			RespInfo: &model.RespInfo{
				RequestID: "a-request-id",
				RespCh:    respCh,
			},
		}
		err := cl.Propose(ctx, cmd)
		require.NoError(t, err)
	}

	time.Sleep(2 * time.Second)
	// Restart the stopped node
	useExistingFiles := true
	err = cl.AddNode(stoppedNode, useExistingFiles, 0, 0, 0)
	require.NoError(t, err)
	cl.UpdateAllNodesDiscoveries()
	err = cl.StartNode(stoppedNode)
	require.NoError(t, err)

	// Wait for it to catch up
	err = cl.WaitForNodeCatchUp(stoppedNode, 15*time.Second)
	require.NoError(t, err)

	// Get final expected index after more commands
	finalExpectedIndex := leaderAgent.GetLastAppliedIndex()

	// Verify it caught up
	restartedAgent, err := cl.GetAgent(stoppedNode)
	require.NoError(t, err)
	restoredIndex := restartedAgent.GetLastAppliedIndex()

	assert.Equal(t, finalExpectedIndex, restoredIndex,
		"restarted node should have caught up to index %d, got %d",
		finalExpectedIndex, restoredIndex)

}

// ------------------------------------------------
// Membershipt
// ------------------------------------------------
func TestClusterMembershipChanges(t *testing.T) {
	// Create 3-node cluster
	cl, err := NewTestCluster(t)
	require.NoError(t, err)

	voters := []string{"node1", "node2", "node3"}
	cl.UpsertTestClusterPeers(voters)
	cl.SetInitialVoters(voters)

	for _, nodeID := range voters {
		err := cl.AddNode(nodeID, false, 0, 0, 0)
		require.NoError(t, err)
	}
	cl.UpdateAllNodesDiscoveries()
	for _, nodeID := range voters {
		err = cl.StartNode(nodeID)
		require.NoError(t, err)
	}

	err = cl.WaitForClusterFormed(10 * time.Second)
	require.NoError(t, err)

	// Verify all 3 nodes are voters
	for _, nodeID := range voters {
		agent, err := cl.GetAgent(nodeID)
		require.NoError(t, err)
		require.True(t, agent.IsVoter(), "node %s should be a voter", nodeID)
	}

	// Send 5 AddJob commands
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		respCh := make(chan model.ToProducerResponse, 1)
		cmd := model.Command{
			Type: model.CmdAddJob,
			AddJob: &model.AddJobPayload{
				Job: model.Job{
					ID:    fmt.Sprintf("job-%d", i),
					Topic: "test-topic",
					Data:  []byte(fmt.Sprintf("payload-%d", i)),
				},
			},
			RespInfo: &model.RespInfo{
				RequestID: fmt.Sprintf("job-%d", i),
				RespCh:    respCh,
			},
		}
		err := cl.Propose(ctx, cmd)
		require.NoError(t, err)
	}

	// Add node4 as learner
	err = cl.AddNode("node4", false, 0, 0, 0)
	require.NoError(t, err)
	cl.UpdateAllNodesDiscoveries()
	err = cl.StartNode("node4")
	require.NoError(t, err)

	// Wait for node4 to catch up
	err = cl.WaitForNodeCatchUp("node4", 10*time.Second)
	require.NoError(t, err)

	// Verify node4 is a learner (not a voter)
	node4Agent, err := cl.GetAgent("node4")
	require.NoError(t, err)
	require.False(t, node4Agent.IsVoter(), "node4 should be a learner initially")

	// Send AddNode command to make node4 a voter
	addNodeRespCh := make(chan model.ToProducerResponse, 1)
	addNodeCmd := model.Command{
		Type: model.CmdAddNode,
		AddNode: &model.AddNodePayload{
			NodeID: node4Agent.GetNodeID(),
		},
		RespInfo: &model.RespInfo{
			RequestID: "a-sample-id",
			RespCh:    addNodeRespCh,
		},
	}
	err = cl.Propose(ctx, addNodeCmd)
	require.NoError(t, err)

	// Wait for membership change to propagate
	time.Sleep(2 * time.Second)

	// Verify node4 becomes a voter
	node4Agent, err = cl.GetAgent("node4")
	require.NoError(t, err)
	require.True(t, node4Agent.IsVoter(), "node4 should become a voter after AddNode command")

	// Send RemoveNode command to demote node4 back to learner
	removeNodeRespCh := make(chan model.ToProducerResponse, 1)
	removeNodeCmd := model.Command{
		Type: model.CmdRemoveNode,
		RemoveNode: &model.RemoveNodePayload{
			NodeID: node4Agent.GetNodeID(),
		},
		RespInfo: &model.RespInfo{
			RequestID: "a-sample-id",
			RespCh:    removeNodeRespCh,
		},
	}
	err = cl.Propose(ctx, removeNodeCmd)
	require.NoError(t, err)

	// Wait for membership change to propagate
	time.Sleep(2 * time.Second)

	// Verify node4 becomes a learner again
	node4Agent, err = cl.GetAgent("node4")
	require.NoError(t, err)
	require.False(t, node4Agent.IsVoter(), "node4 should be a learner again after RemoveNode command")
}

// ------------------------------------------------
// Snapshot
// ------------------------------------------------
func TestSnapshotCompactionAndRestart(t *testing.T) {
	cl, err := NewTestCluster(t)
	assert.NoError(t, err)
	defer func() { _ = cl.Cleanup() }()

	const (
		snapshotTriggerCount = uint64(20)
		snapshotIntervalSec  = uint64(60)
		walFlushThreshold    = 100
	)

	voters := []string{"node1", "node2", "node3"}
	cl.UpsertTestClusterPeers(voters)
	cl.SetInitialVoters(voters)

	// Setup cluster
	for _, id := range voters {
		err := cl.AddNode(id, false, snapshotIntervalSec, snapshotTriggerCount, walFlushThreshold)
		assert.NoError(t, err)
	}
	cl.UpdateAllNodesDiscoveries()
	for _, id := range voters {
		err = cl.StartNode(id)
		assert.NoError(t, err)
	}
	assert.NoError(t, cl.WaitForClusterFormed(10*time.Second))

	leaderID, err := cl.GetLeaderID()
	assert.NoError(t, err)
	leader, err := cl.GetAgent(leaderID)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	t.Log("Sending jobs and completions to trigger snapshot...")

	// Phase 1: Add 15 jobs
	for i := 1; i <= 15; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			respCh := make(chan model.ToProducerResponse, 1)
			cmd := model.Command{
				Type: model.CmdAddJob,
				AddJob: &model.AddJobPayload{
					Job: model.Job{
						ID:    fmt.Sprintf("job-%d", i),
						Topic: "test-topic",
						Data:  []byte(fmt.Sprintf("data-%d", i)),
					},
				},
				RespInfo: &model.RespInfo{
					RequestID: "a-request-id",
					RespCh:    respCh,
				},
			}
			assert.NoError(t, cl.Propose(ctx, cmd))
			<-respCh // wait for response
		}(i)
	}
	wg.Wait()

	// Phase 2: Complete first 8 jobs (this allows compaction later)
	for i := 1; i <= 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			respCh := make(chan model.ToProducerResponse, 1)
			cmd := model.Command{
				Type: model.CmdDone,
				Done: &model.DonePayload{
					Topic:  "test-topic",
					JobIDs: []string{fmt.Sprintf("job-%d", i)},
				},
				RespInfo: &model.RespInfo{
					RequestID: "a-request-id",
					RespCh:    respCh,
				},
			}
			assert.NoError(t, cl.Propose(ctx, cmd))
			<-respCh
		}(i)
	}
	wg.Wait()

	// Phase 3: Add more jobs to cross snapshot threshold
	for i := 16; i <= 45; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			respCh := make(chan model.ToProducerResponse, 1)
			cmd := model.Command{
				Type: model.CmdAddJob,
				AddJob: &model.AddJobPayload{
					Job: model.Job{
						ID:    fmt.Sprintf("job-%d", i),
						Topic: "test-topic",
						Data:  []byte(fmt.Sprintf("data-%d", i)),
					},
				},
				RespInfo: &model.RespInfo{
					RequestID: "a-request-id",
					RespCh:    respCh,
				},
			}
			assert.NoError(t, cl.Propose(ctx, cmd))
			<-respCh
		}(i)
	}
	wg.Wait()

	// Wait for background snapshot trigger
	time.Sleep(1 * time.Second)

	// Verify snapshot was created
	snapshotIndex := leader.GetSnapshotIndex()
	assert.Greater(t, snapshotIndex, uint64(1), "Snapshot should have been triggered")
	t.Logf("Snapshot triggered at index: %d", snapshotIndex)

	// === Restart one follower to test snapshot transfer ===
	followerID := cl.GetFollowerIDs()[0]
	t.Logf("Restarting follower: %s", followerID)

	assert.NoError(t, cl.RemoveNode(followerID))

	assert.NoError(t, cl.AddNode(followerID, true, snapshotIntervalSec, snapshotTriggerCount, walFlushThreshold))
	cl.UpdateAllNodesDiscoveries()
	assert.NoError(t, cl.StartNode(followerID))

	assert.NoError(t, cl.WaitForNodeCatchUp(followerID, 15*time.Second))

	// Final consistency check
	leaderIndex := leader.GetLastAppliedIndex()
	for _, id := range voters {
		agent, _ := cl.GetAgent(id)
		assert.Equal(t, leaderIndex, agent.GetLastAppliedIndex(), "Node %s not caught up", id)
	}

	t.Log("Snapshot compaction and restart test passed successfully")
}

// ------------------------------------------------
// TLS Rotate
// ------------------------------------------------
func TestTlRotateAndRetringConnections(t *testing.T) {
	// Create test cluster with 3 nodes
	cl, err := NewTestCluster(t)
	require.NoError(t, err)

	// Add 3 nodes with proper peer and voter configuration
	voters := []string{"node1", "node2", "node3"}
	cl.UpsertTestClusterPeers(voters)
	cl.SetInitialVoters(voters)

	for _, nodeID := range voters {
		err := cl.AddNode(nodeID, false, 0, 0, 0)
		require.NoError(t, err)
	}
	cl.UpdateAllNodesDiscoveries()
	// Start all nodes
	for _, nodeID := range voters {
		err := cl.StartNode(nodeID)
		require.NoError(t, err)
	}

	// Wait for cluster to form
	err = cl.WaitForClusterFormed(5 * time.Second)
	require.NoError(t, err, "cluster should form within timeout")

	// Verify leader is elected
	leaderID, err := cl.GetLeaderID()
	require.NoError(t, err, "should have a leader")
	t.Logf("Leader elected: %s", leaderID)

	allKnowLeader := cl.AssertAllNodesKnowLeader()
	require.True(t, allKnowLeader, "should have a leader")

	// Verify leader status
	leaderAgent, err := cl.GetAgent(leaderID)
	require.NoError(t, err)
	assert.Equal(t, model.NodeStatusLeaderActive, leaderAgent.GetStatus(),
		"leader should have LeaderActive status")

	// Verify followers
	followers := cl.GetFollowerIDs()
	assert.Len(t, followers, 2, "should have 2 followers")

	for _, followerID := range followers {
		followerAgent, err := cl.GetAgent(followerID)
		require.NoError(t, err)
		assert.Equal(t, model.NodeStatusFollowerActive, followerAgent.GetStatus(),
			"follower %s should have FollowerActive status", followerID)
	}

	// create new certs for all nodes and check retrying connections
	results, err := cl.ReplaceNodeCerts("") // empty string updates all nodes
	require.NoError(t, err)

	// Check all nodes updated successfully
	for nodeID, err := range results {
		assert.NoError(t, err, "node %s cert replacement failed", nodeID)
	}

	// Wait for connection retries
	time.Sleep(2 * time.Second)

	// Now check the retiring connections - they should be populated
	for _, nodeID := range voters {
		agent, err := cl.GetAgent(nodeID)
		require.NoError(t, err)

		// The QUIC server should have retiring connections now
		quicServer := agent.GetQUICServer()
		outgoingCount := len(quicServer.GetRetiringOutgoingConnections())
		incomingCount := len(quicServer.GetRetiringIncomingConnections())

		// Should see connections being retired
		assert.Greater(t, outgoingCount+incomingCount, 0,
			"node %s should have retiring connections after cert rotation", nodeID)
	}
}

// ------------------------------------------------
// Transfer Leader Command
// ------------------------------------------------
func TestTransferLeaderCommand(t *testing.T) {
	// Create test cluster with 3 nodes
	cl, err := NewTestCluster(t)
	require.NoError(t, err)

	// Add 3 nodes with proper peer and voter configuration
	voters := []string{"node1", "node2", "node3"}
	cl.UpsertTestClusterPeers(voters)
	cl.SetInitialVoters(voters)

	for _, nodeID := range voters {
		err := cl.AddNode(nodeID, false, 0, 0, 0)
		require.NoError(t, err)
	}
	cl.UpdateAllNodesDiscoveries()
	// Start all nodes
	for _, nodeID := range voters {
		err := cl.StartNode(nodeID)
		require.NoError(t, err)
	}

	// Wait for cluster to form
	err = cl.WaitForClusterFormed(5 * time.Second)
	require.NoError(t, err, "cluster should form within timeout")

	// Verify leader is elected
	leaderID, err := cl.GetLeaderID()
	require.NoError(t, err, "should have a leader")
	t.Logf("Leader elected: %s", leaderID)

	allKnowLeader := cl.AssertAllNodesKnowLeader()
	require.True(t, allKnowLeader, "should have a leader")

	// Verify leader status
	leaderAgent, err := cl.GetAgent(leaderID)
	require.NoError(t, err)
	assert.Equal(t, model.NodeStatusLeaderActive, leaderAgent.GetStatus(),
		"leader should have LeaderActive status")

	leaderTestNode, _ := cl.GetNode(leaderID)

	// Verify followers
	followers := cl.GetFollowerIDs()
	assert.Len(t, followers, 2, "should have 2 followers")

	candidate, err := cl.GetNode(followers[0])
	require.NoError(t, err)

	select {
	case leaderTestNode.commandCh <- model.Command{
		Type:      model.CmdTransferLeader,
		ProposeID: 1,
		Transfer: &model.TransferLeaderPayload{
			TargetNodeID: candidate.internalID,
		},
	}:
	default:
		t.Fatal("failed to send CmdTransferLeader command to leader channel")
	}

	time.Sleep(2 * time.Second)

	leaderID, err = cl.GetLeaderID()
	require.NoError(t, err, "should have a leader")
	require.Equal(t, candidate.nodeID, leaderID, "candidate follower should be new leader")

	for _, node := range cl.nodes {
		agent, err := cl.GetAgent(node.nodeID)
		require.NoError(t, err)
		lid := agent.GetLeaderID()
		assert.Equal(t, candidate.internalID, lid,
			"follower %s should acknoledge new leader", node.nodeID)
		status := agent.GetStatus()
		assert.True(t, status == model.NodeStatusFollowerActive || status == model.NodeStatusLeaderActive,
			"follower %s should have FollowerActive status", node.nodeID)
	}

}

// ------------------------------------------------
// Update Peers List Command
// ------------------------------------------------
func TestUpdatePeersListCommand(t *testing.T) {
	// Create test cluster with 3 nodes
	cl, err := NewTestCluster(t)
	require.NoError(t, err)

	// Add 3 nodes with proper peer and voter configuration
	voters := []string{"node1", "node2", "node3"}
	cl.UpsertTestClusterPeers(voters)
	cl.SetInitialVoters(voters)

	for _, nodeID := range voters {
		err := cl.AddNode(nodeID, false, 0, 0, 0)
		require.NoError(t, err)
	}
	cl.UpdateAllNodesDiscoveries()
	// Start all nodes
	for _, nodeID := range voters {
		err := cl.StartNode(nodeID)
		require.NoError(t, err)
	}

	// Wait for cluster to form
	err = cl.WaitForClusterFormed(5 * time.Second)
	require.NoError(t, err, "cluster should form within timeout")

	// Verify leader is elected
	leaderID, err := cl.GetLeaderID()
	require.NoError(t, err, "should have a leader")
	t.Logf("Leader elected: %s", leaderID)

	allKnowLeader := cl.AssertAllNodesKnowLeader()
	require.True(t, allKnowLeader, "should have a leader")

	// Verify leader status
	leaderAgent, err := cl.GetAgent(leaderID)
	require.NoError(t, err)
	assert.Equal(t, model.NodeStatusLeaderActive, leaderAgent.GetStatus(),
		"leader should have LeaderActive status")

	leaderTestNode, _ := cl.GetNode(leaderID)
	require.NoError(t, err)

	node4 := cl.getOrCreateMapping("node4")
	var updateList []model.PeerInfo
	updateList = append(updateList, node4.peerInfo)
	for _, mapping := range cl.nodeMapping {
		updateList = append(updateList, mapping.peerInfo)
	}

	select {
	case leaderTestNode.commandCh <- model.Command{
		Type:      model.CmdUpdatePeersList,
		ProposeID: 0,
		Peers: &model.PeersListPayload{
			Peers: updateList,
		},
	}:
	default:
		t.Fatal("failed to send CmdUpdatePeersList command to leader channel")
	}

	time.Sleep(2 * time.Second)

	// Verify followers
	followers := cl.GetFollowerIDs()
	assert.Len(t, followers, 2, "should have 2 followers")

	for _, followerID := range followers {
		followerAgent, err := cl.GetAgent(followerID)
		require.NoError(t, err)
		assert.Equal(t, model.NodeStatusFollowerActive, followerAgent.GetStatus(),
			"follower %s should have FollowerActive status", followerID)

		ul := followerAgent.discovery.ListPeers()
		slices.SortFunc(ul, func(a, b model.PeerInfo) int { return strings.Compare(a.NodeID, b.NodeID) })
		slices.SortFunc(updateList, func(a, b model.PeerInfo) int { return strings.Compare(a.NodeID, b.NodeID) })
		require.True(t, reflect.DeepEqual(updateList, ul))
	}

}

// ------------------------------------------------
// Sync Peers
// ------------------------------------------------
func TestSyncPeers(t *testing.T) {
	// Create test cluster with 3 nodes
	cl, err := NewTestCluster(t)
	require.NoError(t, err)

	voters := []string{"node1", "node2", "node3"}
	cl.UpsertTestClusterPeers(voters[:2])
	cl.SetInitialVoters(voters[:2])

	for _, nodeID := range voters[:2] {
		err := cl.AddNode(nodeID, false, 0, 0, 0)
		require.NoError(t, err)
	}
	cl.UpdateAllNodesDiscoveries()
	// Start all nodes
	for _, nodeID := range voters[:2] {
		err := cl.StartNode(nodeID)
		require.NoError(t, err)
	}

	// Wait for cluster to form
	err = cl.WaitForClusterFormed(5 * time.Second)
	require.NoError(t, err, "cluster should form within timeout")

	// Verify leader is elected
	leaderID, err := cl.GetLeaderID()
	require.NoError(t, err, "should have a leader")
	t.Logf("Leader elected: %s", leaderID)

	allKnowLeader := cl.AssertAllNodesKnowLeader()
	require.True(t, allKnowLeader, "should have a leader")

	// Verify leader status
	leaderAgent, err := cl.GetAgent(leaderID)
	require.NoError(t, err)
	assert.Equal(t, model.NodeStatusLeaderActive, leaderAgent.GetStatus(),
		"leader should have LeaderActive status")

	// up to this point we have a cluster of two nodes that do not know third node

	err = cl.AddNode(voters[2], false, 0, 0, 0, voters) // node3 knows others
	require.NoError(t, err)

	err = cl.StartNode(voters[2])
	require.NoError(t, err)

	// Wait for sync connection
	time.Sleep(5 * time.Second)

	// node3 should have 2 outgoing connection to other nodes
	node3, err := cl.GetAgent(voters[2])
	require.NoError(t, err)
	syncedPeers := node3.discovery.ListPeers()
	require.Equal(t, len(voters), len(syncedPeers))
	ougoing := node3.quicServer.GetActiveOutgoingNodes()
	require.Equal(t, 2, len(ougoing))

	// other nodes should have incoming connection from node3
	for _, v := range voters[:2] {
		n, err := cl.GetAgent(v)
		require.NoError(t, err)
		in := n.quicServer.GetActiveIncomingNodes()
		require.Equal(t, 2, len(in))
	}

	// other node should find node3
	for _, nodeID := range voters[:2] {
		n, err := cl.GetAgent(nodeID)
		require.NoError(t, err)
		ougoing := n.quicServer.GetActiveOutgoingNodes()
		require.Equal(t, 2, len(ougoing))
	}

	// Verify all nodes have the same peer store snapshot
	var expectedPeers map[string]model.PeerInfo
	for i, nodeID := range voters {
		n, err := cl.GetAgent(nodeID)
		require.NoError(t, err)
		peerMap := n.peerStore.Get()

		// All nodes should know about all peers
		for _, p := range cl.peers {
			_, exists := peerMap[p.NodeID]
			require.True(t, exists, "node %s missing peer %s", nodeID, p.NodeID)
		}

		if i == 0 {
			expectedPeers = peerMap
		} else {
			require.Equal(t, expectedPeers, peerMap, "node %s peer store inconsistent", nodeID)
		}
	}

}

// ------------------------------------------------
// Peers List Query - nodes with len(outgoing) < len(incoming)
// ------------------------------------------------
func TestPeersListQuery(t *testing.T) {
	// Create test cluster with 3 nodes
	cl, err := NewTestCluster(t)
	require.NoError(t, err)

	voters := []string{"node1", "node2", "node3"}
	cl.UpsertTestClusterPeers(voters)
	cl.SetInitialVoters(voters)

	for _, nodeID := range voters {
		err := cl.AddNode(nodeID, false, 0, 0, 0)
		require.NoError(t, err)
	}
	cl.UpdateAllNodesDiscoveries()
	// Start all nodes
	for _, nodeID := range voters {
		err := cl.StartNode(nodeID)
		require.NoError(t, err)
	}

	// Wait for cluster to form
	err = cl.WaitForClusterFormed(5 * time.Second)
	require.NoError(t, err, "cluster should form within timeout")

	// Verify leader is elected
	leaderID, err := cl.GetLeaderID()
	require.NoError(t, err, "should have a leader")
	t.Logf("Leader elected: %s", leaderID)

	allKnowLeader := cl.AssertAllNodesKnowLeader()
	require.True(t, allKnowLeader, "should have a leader")

	// Verify leader status
	leaderAgent, err := cl.GetAgent(leaderID)
	require.NoError(t, err)
	assert.Equal(t, model.NodeStatusLeaderActive, leaderAgent.GetStatus(),
		"leader should have LeaderActive status")

	followers := cl.GetFollowerIDs()
	assert.Len(t, followers, 2, "should have 2 followers")

	leaderPeers := leaderAgent.discovery.ListPeers()
	var peers []model.PeerInfo
	followerA, err := cl.GetAgent(followers[0])
	require.NoError(t, err)
	for _, pi := range leaderPeers {
		if pi.NodeID == followerA.nodeID {
			continue
		}
		peers = append(peers, pi)
	}
	followerB, err := cl.GetAgent(followers[1])
	require.NoError(t, err)
	followerB.discovery.UpdatePeers(peers)
	con, ok := followerB.quicServer.outgoingConns[followerA.nodeID]
	require.True(t, ok)
	err = con.CloseWithError(0, "for test")
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	p := followerB.discovery.ListPeers()
	require.Equal(t, 3, len(p))

	_, err = followerB.quicServer.outgoingConns[followerA.nodeID].OpenStream()
	require.NoError(t, err)
}
