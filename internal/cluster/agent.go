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
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m-javani/cue/internal"

	"github.com/m-javani/cue/internal/model"
	"github.com/m-javani/cue/internal/state"
	"github.com/m-javani/cue/internal/utils"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

// ClusterAgent owns channels for Raft communication but not the Raft node itself
type ClusterAgent struct {
	// Identity
	nodeID        string
	raftNodeID    uint64
	initialVoters []string

	raftTickMs        int
	raftHeartbeatTick int
	raftElectionTick  int

	// Channel to get commands from gateway
	commandCh <-chan model.Command

	proposalID       atomic.Uint64
	muPndPr          sync.RWMutex
	pendingProposals map[uint64]model.RespInfo

	// Channels for Raft communication
	proposeCh  chan ProposeRequest
	stepCh     chan raftpb.Message
	commitCh   chan CommittedEntry
	outgoingCh chan raftpb.Message
	ctrlCh     chan ControlCmd
	notifyCh   chan NotifyEvent

	// State (using the proper type from model.go)
	isLeader atomic.Bool
	leaderID *atomic.Value
	status   *atomic.Uint32
	isVoter  atomic.Int32

	// Discovery
	discovery *ServiceDiscovery

	// Quic server
	quicServer *ClusterQUIC

	// Handler
	handler state.Handler

	// Context
	ctx    context.Context
	cancel context.CancelFunc

	lastAppliedIndex atomic.Uint64
	currentTerm      *atomic.Uint64
	snapshotIndex    atomic.Uint64
	lastSnapshotSec  atomic.Int64
	// to avoid replaying done/dropped jobs
	deadJobs map[string]bool

	// Configuration
	dataDir              string
	lastSnapshotTrySec   atomic.Int64
	snapshotIntervalSec  uint64
	snapshotTriggerCount uint64
	walFlushThreshold    int
	certPath             string
	keyPath              string
	caCertPath           string
	lastCertFingerprint  string

	dlqManager *DLQFileManager

	metrics *internal.ClusterMetrics

	members *model.Members

	logger *zap.Logger

	peerSyncOutgoingCoolDown atomic.Int64
	peerUpdateNodesCoolDown  atomic.Int64
}

// New creates a new ClusterAgent
func NewClusterAgent(
	ctx context.Context,
	cancel context.CancelFunc,
	nodeID string,
	cfg internal.ClusterConfig,
	dataDir string,
	commandCh <-chan model.Command,
	handler state.Handler,
	quicServer *ClusterQUIC,
	status *atomic.Uint32,
	currentTerm *atomic.Uint64,
	members *model.Members,
	leaderID *atomic.Value,
	discovery *ServiceDiscovery,
	logger *zap.Logger) (*ClusterAgent, error) {

	if handler == nil {
		return nil, fmt.Errorf("handler is required")
	}
	dlqManager, err := NewDLQFileManager(dataDir, cfg.DLQMaxSizeBytes)
	if err != nil {
		return nil, err
	}

	agent := &ClusterAgent{
		nodeID:                   nodeID,
		raftNodeID:               utils.StringToUint64(nodeID),
		initialVoters:            cfg.InitialVoters,
		dataDir:                  dataDir,
		snapshotIntervalSec:      cfg.SnapshotIntervalSec,
		snapshotTriggerCount:     cfg.SnapshotTriggerCount,
		walFlushThreshold:        cfg.WALFlushThreshold,
		certPath:                 cfg.CertPath,
		keyPath:                  cfg.KeyPath,
		caCertPath:               cfg.CACertPath,
		ctx:                      ctx,
		cancel:                   cancel,
		commandCh:                commandCh,
		handler:                  handler,
		dlqManager:               dlqManager,
		metrics:                  internal.GetClusterMetrics(),
		logger:                   logger,
		quicServer:               quicServer,
		status:                   status,
		currentTerm:              currentTerm,
		pendingProposals:         make(map[uint64]model.RespInfo),
		proposalID:               atomic.Uint64{},
		leaderID:                 leaderID,
		muPndPr:                  sync.RWMutex{},
		proposeCh:                make(chan ProposeRequest, 1024),
		stepCh:                   make(chan raftpb.Message, 1024),
		commitCh:                 make(chan CommittedEntry, 1024),
		outgoingCh:               make(chan raftpb.Message, 1024),
		ctrlCh:                   make(chan ControlCmd, 1024),
		notifyCh:                 make(chan NotifyEvent, 1024),
		isLeader:                 atomic.Bool{},
		isVoter:                  atomic.Int32{},
		discovery:                discovery,
		lastAppliedIndex:         atomic.Uint64{},
		snapshotIndex:            atomic.Uint64{},
		lastSnapshotSec:          atomic.Int64{},
		deadJobs:                 map[string]bool{},
		lastSnapshotTrySec:       atomic.Int64{},
		lastCertFingerprint:      "",
		members:                  members,
		raftTickMs:               cfg.RaftTickMs,
		raftHeartbeatTick:        cfg.RaftHeartbeatTick,
		raftElectionTick:         cfg.RaftElectionTick,
		peerSyncOutgoingCoolDown: atomic.Int64{},
		peerUpdateNodesCoolDown:  atomic.Int64{},
	}

	// Set initial status
	agent.status.Store(model.NodeStatusUnavailable.ToUin32())

	return agent, nil
}

// Start launches the Raft node and all background goroutines
func (a *ClusterAgent) Start() error {
	defer func() {
		if r := recover(); r != nil {
			a.logger.Error("agent panicked",
				zap.String("node_id", a.nodeID),
				zap.Any("panic", r),
				zap.Stack("stacktrace"))
			// Re-panic to ensure the test knows about it
			panic(r)
		}
	}()

	// 1. Create storage (will be owned by Raft)
	storage, err := NewRaftStorage(
		filepath.Join(a.dataDir, "wal"),
		filepath.Join(a.dataDir, "snapshot"),
		a.walFlushThreshold,
		a.logger,
		a.metrics,
	)
	if err != nil {
		return fmt.Errorf("failed to create storage: %w", err)
	}

	// Build dead jobs list from storage's secondary index
	a.deadJobs = storage.GetCompletedJobIDs()

	// 2. Ensure dummy snapshot if needed
	if err := a.ensureDummySnapshot(storage); err != nil {
		return fmt.Errorf("failed to ensure dummy snapshot: %w", err)
	}

	// 3. Start connection handlers (they don't depend on Raft)
	go a.handleConnections()
	go a.syncConnections()
	go a.sendConnectionHeartbeats()
	go a.sendHeartbeatToRetiringConnections()

	// 4. Wait for cluster readiness (learners wait for leader, voters wait for majority)
	if err := a.waitForClusterReadiness(); err != nil {
		return fmt.Errorf("cluster readiness failed: %w", err)
	}

	// 5. Build and start Raft node (passes storage ownership)
	if err := a.buildAndStartRaftNode(storage); err != nil {
		return fmt.Errorf("failed to build raft node: %w", err)
	}

	// 6. Start Raft-dependent goroutines (must come after Raft is running)
	go a.commandProcessor()
	go a.handleCommittedEntries()
	go a.handleOutgoingMessages()
	go a.handleRaftNotifications()
	go a.startTLSWatcher()
	go a.runSnapshotMaintenance()

	a.logger.Info("cluster agent started",
		zap.String("node_id", a.nodeID),
		zap.Uint64("raft_node_id", a.raftNodeID),
		zap.Strings("initial_voters", a.initialVoters))

	// 7. Wait for shutdown signal
	<-a.ctx.Done()
	a.Stop()

	a.logger.Sugar().Infof("%s shutting down cluster agent", a.nodeID)

	// Give goroutines time to clean up
	time.Sleep(1 * time.Second)

	return nil
}

// Stop gracefully shuts down the agent
func (a *ClusterAgent) Stop() {
	if a.quicServer != nil {
		_ = a.quicServer.Close()
	}
}

// IsLeader returns true if this node is the Raft leader
func (a *ClusterAgent) IsLeader() bool {
	return a.isLeader.Load()
}

// GetLeaderID returns the current leader's node ID
func (a *ClusterAgent) GetLeaderID() string {
	if val := a.leaderID.Load(); val != nil {
		if s, ok := val.(string); ok {
			return s
		}
	}
	return ""
}

func (a *ClusterAgent) GetNodeID() string {
	return a.nodeID
}

func (a *ClusterAgent) GetRaftID() uint64 {
	return a.raftNodeID
}

// GetStatus returns the current node status as ClusterNodeStatus type
func (a *ClusterAgent) GetStatus() model.ClusterNodeStatus {
	if a.status == nil {
		return model.NodeStatusUnavailable
	}
	val := a.status.Load()
	if val == 0 {
		return model.NodeStatusUnavailable
	}
	return model.ClusterNodeStatusFromUint32(val)
}

// IsActive returns true if node is either leader or follower active
func (a *ClusterAgent) IsActive() bool {
	s := a.status.Load()
	return s == model.NodeStatusLeaderActive.ToUin32() || s == model.NodeStatusFollowerActive.ToUin32()
}

// IsVoter returns true if this node is currently a voting member of the raft cluster
func (a *ClusterAgent) IsVoter() bool {
	return a.isVoter.Load() == 1
}

// DataDir returns the data directory path
func (a *ClusterAgent) DataDir() string {
	return a.dataDir
}

// StorageDir returns the storage subdirectory
func (a *ClusterAgent) StorageDir() string {
	return a.dataDir + "/storage"
}

// GetLastAppliedIndex returns the last applied index
func (a *ClusterAgent) GetLastAppliedIndex() uint64 {
	return a.lastAppliedIndex.Load()
}

// GetCurrentTerm returns the current term
func (a *ClusterAgent) GetCurrentTerm() uint64 {
	return a.currentTerm.Load()
}

func (a *ClusterAgent) GetSnapshotIndex() uint64 {
	return a.snapshotIndex.Load()
}

// waitForClusterReadiness waits for cluster to be ready based on node type
func (a *ClusterAgent) waitForClusterReadiness() error {
	isInitialVoter := slices.Contains(a.initialVoters, a.nodeID)
	if !isInitialVoter {
		// Learner: wait for trusted leader
		return a.waitForLeader()
	}
	// Voter: wait for majority of initial voters
	return a.waitForMajority(a.initialVoters)
}

// waitForLeader waits for a trusted leader (learners only)
func (a *ClusterAgent) waitForLeader() error {
	const maxAttempts = 10
	const initialBackoffMs = 100

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		activeNodes := a.quicServer.GetActiveOutgoingNodes()

		for _, targetNodeID := range activeNodes {
			req := &ClusterRequest{Type: ReqClusterInfo, ClusterInfo: &ClusterInfoPayload{}}
			resp, err := a.sendRequest(targetNodeID, req)
			if err != nil {
				continue
			}

			if resp.Type != ResClusterInfo || resp.ClusterInfo == nil {
				continue
			}

			if resp.ClusterInfo.LeaderID == "" {
				continue // No leader
			}

			leaderNodeID := resp.ClusterInfo.LeaderID
			verifyReq := &ClusterRequest{Type: ReqClusterInfo, ClusterInfo: &ClusterInfoPayload{}}
			verifyResp, err := a.sendRequest(leaderNodeID, verifyReq)
			if err != nil {
				continue
			}

			if verifyResp.Type != ResClusterInfo || verifyResp.ClusterInfo == nil {
				continue
			}

			if verifyResp.ClusterInfo.Status == model.NodeStatusLeaderActive {
				a.logger.Info("learner found trusted leader",
					zap.String("leader_node_id", leaderNodeID))
				return nil
			}
		}

		sleepMs := a.backoffDuration(attempt, initialBackoffMs)
		a.logger.Debug("waiting for leader",
			zap.Int("attempt", attempt),
			zap.Int("max_attempts", maxAttempts),
			zap.Int64("sleep_ms", int64(sleepMs)))

		select {
		case <-time.After(time.Duration(sleepMs) * time.Millisecond):
		case <-a.ctx.Done():
			return a.ctx.Err()
		}
	}

	a.markUnavailable()
	return fmt.Errorf("node failed to find trusted leader after %d attempts", maxAttempts)
}

// waitForMajority waits for majority of initial voters to be connected
func (a *ClusterAgent) waitForMajority(initialVoters []string) error {
	majorityThreshold := (len(initialVoters) / 2) + 1
	isInitialVoter := slices.Contains(initialVoters, a.nodeID)
	const maxAttempts = 100
	const initialBackoffMs = 500

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		activeNodes := a.quicServer.GetActiveOutgoingNodes()

		connected := 0
		for _, nodeID := range activeNodes {
			if slices.Contains(initialVoters, nodeID) {
				connected++
			}
		}
		if isInitialVoter {
			connected++ // count self
		}

		if attempt%10 == 0 {
			a.logger.Warn("waiting for majority of initial voters",
				zap.Int("connected", connected),
				zap.Int("need", majorityThreshold),
				zap.Int("total", len(initialVoters)),
				zap.Int("attempt", attempt))
		}

		if connected >= majorityThreshold {
			a.logger.Info("majority of initial voters connected",
				zap.Int("connected", connected),
				zap.Int("total", len(initialVoters)))
			return nil
		}

		sleepMs := a.backoffDuration(attempt, initialBackoffMs)
		select {
		case <-time.After(time.Duration(sleepMs) * time.Millisecond):
		case <-a.ctx.Done():
			return a.ctx.Err()
		}
	}

	a.markUnavailable()
	return fmt.Errorf("failed to establish majority connection to initial voters after %d attempts", maxAttempts)
}

// backoffDuration calculates exponential backoff with jitter
func (a *ClusterAgent) backoffDuration(attempt int, initialMs uint64) uint64 {
	backoff := initialMs << (attempt - 1) // exponential: initial * 2^(attempt-1)
	if backoff > 5000 {
		backoff = 5000 // cap at 5 seconds
	}
	// Add jitter: +/- 25%
	jitter := uint64(rand.Int63n(int64(backoff / 4)))
	return backoff + jitter
}

// markUnavailable marks node as unavailable and cancels context
func (a *ClusterAgent) markUnavailable() {
	a.status.Store(model.NodeStatusUnavailable.ToUin32())
	a.cancel()
	a.logger.Error("node marked as unavailable")
}
