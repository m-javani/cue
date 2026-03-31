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
	"slices"
	"sync"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/utils"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

// ========== Configuration ==========

type Config struct {
	ID              uint64
	Peers           []uint64
	RaftTickMs      int
	ElectionTick    int
	HeartbeatTick   int
	MaxSizePerMsg   uint64
	MaxInflightMsgs int
}

// ========== Proposal ==========

type ProposeRequest struct {
	Data []byte
}

// ========== Committed Entry ==========

type CommittedEntry struct {
	Data     []byte
	Index    uint64
	Term     uint64
	Type     raftpb.EntryType
	Voters   []uint64
	Learners []uint64
}

// ========== Control Commands ==========

type ControlType int

const (
	CmdAddNode ControlType = iota
	CmdAddLearner
	CmdRemoveNode
	CmdTransferLeader
	CmdSnapshot
	CmdSyncLearnerNodes
)

type ControlCmd struct {
	Type     ControlType
	NodeID   uint64
	TargetID uint64
	NodeIDs  []uint64 // For CmdSyncLearnerNodes
	Index    uint64
	Timeout  time.Duration
}

// ========== Notify Events ==========

type NotifyEventType int

const (
	EventRoleChange NotifyEventType = iota
	RoleIsVoter
	RoleIsNonVoter
	WalCompact
)

type NotifyEvent struct {
	Type          NotifyEventType
	IsLeader      bool
	LeaderID      uint64
	Role          raft.StateType
	Term          uint64
	SnapshotIndex uint64
}

// ========== CRaft ==========

type CRaft struct {
	nodeID     string
	node       *raft.RawNode // Changed from *raft.Node to *raft.RawNode for more control
	cfg        *raft.Config
	raftTickMs int

	// Channels (provided by ClusterAgent)
	proposeCh  <-chan ProposeRequest
	stepCh     <-chan raftpb.Message
	commitCh   chan<- CommittedEntry
	outgoingCh chan<- raftpb.Message
	ctrlCh     <-chan ControlCmd
	notifyCh   chan<- NotifyEvent

	// Storage
	storage *RaftStorage

	// Leadership tracking
	prevRole     raft.StateType
	prevLeaderID uint64

	metrics  *internal.ClusterMetrics
	logger   *zap.Logger
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	stopOnce sync.Once

	raftMu sync.RWMutex
}

// ========== Constructor ==========

func NewCRaft(
	nodeID string,
	cfg Config,
	storage *RaftStorage,
	proposeCh <-chan ProposeRequest,
	stepCh <-chan raftpb.Message,
	commitCh chan<- CommittedEntry,
	outgoingCh chan<- raftpb.Message,
	ctrlCh <-chan ControlCmd,
	notifyCh chan<- NotifyEvent,
	logger *zap.Logger,
	metrics *internal.ClusterMetrics,
) (*CRaft, error) {

	raftConfig := &raft.Config{
		ID:              cfg.ID,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   cfg.MaxSizePerMsg,
		MaxInflightMsgs: cfg.MaxInflightMsgs,
		PreVote:         true,
		CheckQuorum:     true,
		Logger:          utils.NewRaftZapLogger(logger),
	}

	// Create RawNode (gives you the Ready() API )
	rawNode, err := raft.NewRawNode(raftConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create raw node: %w", err)
	}

	// Bootstrap with initial peers if needed
	if len(cfg.Peers) > 0 {
		peers := make([]raft.Peer, len(cfg.Peers))
		for i, id := range cfg.Peers {
			peers[i] = raft.Peer{ID: id}
		}
		rawNode.Bootstrap(peers)
	}

	return &CRaft{
		nodeID:     nodeID,
		node:       rawNode,
		cfg:        raftConfig,
		raftTickMs: cfg.RaftTickMs,
		storage:    storage,
		proposeCh:  proposeCh,
		stepCh:     stepCh,
		commitCh:   commitCh,
		outgoingCh: outgoingCh,
		ctrlCh:     ctrlCh,
		notifyCh:   notifyCh,
		logger:     logger,
		metrics:    metrics,
	}, nil
}

// ========== Run Main Loop ==========

func (c *CRaft) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.wg.Add(1)
	c.runLoop(ctx)
}

func (c *CRaft) runLoop(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Duration(c.raftTickMs) * time.Millisecond)
	defer ticker.Stop()

	logger := c.logger.With(zap.Uint64("node_id", c.cfg.ID))
	// logger.Info("CRaft node started")

	for {
		select {
		case <-ctx.Done():
			// logger.Info("CRaft node stopping",
			// 	zap.String("node_id", c.nodeID))
			c.Stop()
			return

		case <-ticker.C:
			c.node.Tick()

		case msg, ok := <-c.stepCh:
			if !ok {
				return
			}
			c.raftMu.Lock()
			if err := c.node.Step(msg); err != nil {
				logger.Debug("step error", zap.Error(err))
			}
			c.raftMu.Unlock()

		case req, ok := <-c.proposeCh:
			if !ok {
				return
			}
			c.handlePropose(req)

		case cmd, ok := <-c.ctrlCh:
			if !ok {
				return
			}
			c.handleControl(cmd)
		}

		// Process ready state
		if !c.node.HasReady() {
			continue
		}

		if err := c.handleReady(ctx); err != nil && err != context.Canceled {
			logger.Error("failed to handle ready", zap.Error(err))
		}
	}
}

// ========== Handle Ready State ==========

func (c *CRaft) handleReady(ctx context.Context) error {
	rd := c.node.Ready()

	// 1. Check role change
	c.raftMu.RLock()
	currentStatus := c.node.Status()
	c.raftMu.RUnlock()
	currentRole := currentStatus.RaftState
	currentLeaderID := currentStatus.Lead
	if currentRole != c.prevRole || currentLeaderID != c.prevLeaderID {
		currentTerm := currentStatus.Term
		select {
		case c.notifyCh <- NotifyEvent{
			Type:     EventRoleChange,
			IsLeader: currentRole == raft.StateLeader,
			LeaderID: currentLeaderID,
			Role:     currentRole,
			Term:     currentTerm,
		}:
		case <-ctx.Done():
			return ctx.Err()
		default:
			c.logger.Warn("notify channel full, dropping role change event")
		}
		c.prevRole = currentRole
		c.prevLeaderID = currentLeaderID
	}

	// 2. Handle snapshot - metadata-only, apply directly
	if !raft.IsEmptySnap(rd.Snapshot) {
		snap := rd.Snapshot

		// Apply snapshot to storage (updates metadata, truncates WAL)
		if err := c.storage.InstallSnapshot(snap.Metadata); err != nil {
			c.raftMu.Lock()
			c.node.ReportSnapshot(snap.Metadata.Index, raft.SnapshotFailure)
			c.raftMu.Unlock()
			return fmt.Errorf("failed to apply snapshot: %w", err)
		}
		c.raftMu.Lock()
		c.node.ReportSnapshot(snap.Metadata.Index, raft.SnapshotFinish)
		c.raftMu.Unlock()
	}

	// 3. Persist hard state
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := c.storage.SetHardState(rd.HardState); err != nil {
			return fmt.Errorf("failed to save hard state: %w", err)
		}
	}

	// 4. Append entries to log
	if len(rd.Entries) > 0 {
		if err := c.storage.Append(rd.Entries); err != nil {
			return fmt.Errorf("failed to append entries: %w", err)
		}
	}

	// 5. Send messages to peers (via ClusterAgent's QUIC)
	for _, msg := range rd.Messages {
		select {
		case c.outgoingCh <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// 6. Apply committed entries to state machine
	for _, entry := range rd.CommittedEntries {
		c.applyCommittedEntry(entry)
	}

	// 7. Advance the raft node
	c.node.Advance(rd)

	return nil
}

// ========== Apply Committed Entry ==========
func (c *CRaft) applyCommittedEntry(entry raftpb.Entry) {
	confUpdated := false
	switch entry.Type {
	case raftpb.EntryConfChangeV2:
		confUpdated = true
		var cc raftpb.ConfChangeV2
		if err := cc.Unmarshal(entry.Data); err != nil {
			c.logger.Error("unmarshal v2 failed", zap.Error(err))
			return
		}
		c.handleConfChange(cc.Changes)

	case raftpb.EntryConfChange:
		confUpdated = true
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			c.logger.Error("unmarshal v1 failed", zap.Error(err))
			return
		}
		c.handleConfChange([]raftpb.ConfChangeSingle{{
			Type:   cc.Type,
			NodeID: cc.NodeID,
		}})
	}

	if err := c.storage.AppendCommitted(entry); err != nil {
		c.logger.Error("failed to persist commited entry",
			zap.Uint64("index", entry.Index),
			zap.Uint64("term", entry.Term))
		c.cancel()
	}

	var voterIDs []uint64
	var learnerIDs []uint64

	if confUpdated {
		c.raftMu.RLock()
		status := c.node.Status()
		c.raftMu.RUnlock()
		voterIDs = make([]uint64, 0, len(status.Config.Voters))
		for id := range status.Config.Voters.IDs() {
			voterIDs = append(voterIDs, id)
		}

		learnerIDs = make([]uint64, 0, len(status.Config.Learners))
		for id := range status.Config.Learners {
			learnerIDs = append(learnerIDs, id)
		}
	}

	select {
	case c.commitCh <- CommittedEntry{
		Data:     entry.Data,
		Index:    entry.Index,
		Term:     entry.Term,
		Type:     entry.Type,
		Voters:   voterIDs,
		Learners: learnerIDs,
	}:
	default:
		c.logger.Warn("commit channel full, dropping entry",
			zap.Uint64("index", entry.Index),
			zap.Uint64("term", entry.Term))
	}
}

func (c *CRaft) handleConfChange(changes []raftpb.ConfChangeSingle) {
	// Apply first
	var cc raftpb.ConfChangeV2
	cc.Changes = changes
	c.raftMu.Lock()
	cs := c.node.ApplyConfChange(cc)
	c.raftMu.Unlock()

	c.logger.Info("raft conf changed",
		zap.Any("conf_state", cs),
		zap.String("changes", raftpb.ConfChangesToString(changes)))

	if err := c.storage.core.setConfState(*cs); err != nil {
		c.logger.Error("failed to persist conf state", zap.Error(err))
	}

	// Check current role from actual ConfState
	c.raftMu.RLock()
	raftID := c.node.Status().ID
	c.raftMu.RUnlock()
	isVoter := slices.Contains(cs.Voters, raftID)

	notifyType := RoleIsNonVoter
	if isVoter {
		notifyType = RoleIsVoter
	}

	select {
	case c.notifyCh <- NotifyEvent{
		Type: notifyType,
	}:
	default:
		c.logger.Warn("notify channel full, dropping notification",
			zap.Int("type", int(notifyType)),
			zap.Uint64("raft_id", raftID))
	}
}

// ========== Handle Proposals ==========
func (c *CRaft) handlePropose(req ProposeRequest) {
	c.raftMu.Lock()
	if err := c.node.Propose(req.Data); err != nil {
		// c.logger.Debug("propose failed", zap.Error(err))
	}
	c.raftMu.Unlock()
}

// ========== Handle Control Commands ==========

func (c *CRaft) handleControl(cmd ControlCmd) {
	switch cmd.Type {
	case CmdAddNode:
		cc := raftpb.ConfChangeV2{
			Transition: raftpb.ConfChangeTransitionAuto,
			Changes: []raftpb.ConfChangeSingle{
				{
					Type:   raftpb.ConfChangeAddNode,
					NodeID: cmd.NodeID,
				},
			},
		}
		c.raftMu.Lock()
		if err := c.node.ProposeConfChange(cc); err != nil {
			c.logger.Error("failed to add node", zap.Uint64("node_id", cmd.NodeID), zap.Error(err))
		}
		c.raftMu.Unlock()

	case CmdAddLearner:
		cc := raftpb.ConfChangeV2{
			Transition: raftpb.ConfChangeTransitionAuto,
			Changes: []raftpb.ConfChangeSingle{
				{
					Type:   raftpb.ConfChangeAddLearnerNode,
					NodeID: cmd.NodeID,
				},
			},
		}
		c.raftMu.Lock()
		if err := c.node.ProposeConfChange(cc); err != nil {
			c.logger.Error("failed to add learner", zap.Uint64("node_id", cmd.NodeID), zap.Error(err))
		}
		c.raftMu.Unlock()

	case CmdRemoveNode:
		cc := raftpb.ConfChangeV2{
			Transition: raftpb.ConfChangeTransitionAuto,
			Changes: []raftpb.ConfChangeSingle{
				{
					Type:   raftpb.ConfChangeRemoveNode,
					NodeID: cmd.NodeID,
				},
			},
		}
		c.raftMu.Lock()
		if err := c.node.ProposeConfChange(cc); err != nil {
			c.logger.Error("failed to remove node", zap.Uint64("node_id", cmd.NodeID), zap.Error(err))
		}
		c.raftMu.Unlock()

	case CmdTransferLeader:
		c.raftMu.Lock()
		c.node.TransferLeader(cmd.NodeID)
		c.raftMu.Unlock()
		c.logger.Info("leader transfer initiated", zap.Uint64("target", cmd.NodeID))

	case CmdSnapshot:
		// Compact the wal (this updates snapshotMeta in storage)
		if err := c.storage.Compact(); err != nil {
			c.logger.Error("failed to save snapshot", zap.Error(err))
			return
		}
		select {
		case c.notifyCh <- NotifyEvent{
			Type:          WalCompact,
			SnapshotIndex: c.storage.core.snapshotMeta.Index,
		}:
		default:
			c.logger.Warn("notify channel full, dropping notification",
				zap.Int("type", int(WalCompact)),
				zap.Uint64("WalCompactIndex", c.storage.core.snapshotMeta.Index))
		}

		c.logger.Info("STORAGE INDEXES",
			zap.Uint64("firstIndex", c.storage.core.getFirstIndex()),
			zap.Uint64("lastIndex", c.storage.core.getLastIndex()))

	case CmdSyncLearnerNodes:
		c.raftMu.RLock()
		status := c.node.Status()
		if status.RaftState != raft.StateLeader {
			c.raftMu.RUnlock()
			return
		}
		c.raftMu.RUnlock()

		hs, confState, err := c.storage.InitialState()
		if err != nil {
			c.logger.Error("failed to get initial state", zap.Error(err))
			return
		}
		_ = hs

		existing := make(map[uint64]bool)
		for _, id := range confState.Voters {
			existing[id] = true
		}
		for _, id := range confState.Learners {
			existing[id] = true
		}

		for _, nodeID := range cmd.NodeIDs {
			if existing[nodeID] {
				continue
			}

			cc := raftpb.ConfChangeV2{
				Transition: raftpb.ConfChangeTransitionAuto,
				Changes: []raftpb.ConfChangeSingle{
					{
						Type:   raftpb.ConfChangeAddLearnerNode,
						NodeID: nodeID,
					},
				},
			}
			c.raftMu.Lock()
			if err := c.node.ProposeConfChange(cc); err != nil {
				c.logger.Error("failed to add learner node",
					zap.Uint64("node_id", nodeID),
					zap.Error(err))
			} else {
				c.logger.Sugar().Debugf("proposed new learner: %d", nodeID)
				existing[nodeID] = true
			}
			c.raftMu.Unlock()
		}
	}
}

// ========== Lifecycle Methods ==========

func (c *CRaft) Stop() {
	c.stopOnce.Do(func() {
		c.storage.Close()
		if c.cancel != nil {
			c.cancel()
		}
		c.wg.Wait()
	})
}

func (c *CRaft) Wait() {
	c.wg.Wait()
}

// ========== Status Methods ==========

func (c *CRaft) GetStatus() raft.Status {
	c.raftMu.RLock()
	st := c.node.Status()
	c.raftMu.RUnlock()
	return st
}
