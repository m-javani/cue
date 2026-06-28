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
	"fmt"
	"time"

	"github.com/m-javani/cue/internal"

	"github.com/m-javani/cue/internal/model"
	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

// handleRaftNotifications processes notifications from Raft node
func (a *ClusterAgent) handleRaftNotifications() {
	for {
		select {
		case <-a.ctx.Done():
			return

		case notif, ok := <-a.notifyCh:
			if !ok {
				return
			}

			switch notif.Type {
			case RoleIsVoter:
				a.isVoter.Store(1)

			case RoleIsNonVoter:
				a.isVoter.Store(0)

			case WalCompact:
				a.snapshotIndex.Store(notif.SnapshotIndex)

			case EventRoleChange:
				// Leadership or term changes - handle separately if needed
				a.applyRoleChange(notif.IsLeader, notif.LeaderID, notif.Role, notif.Term)
			}
		}
	}
}

// applyRoleChange updates agent state based on Raft role change
func (a *ClusterAgent) applyRoleChange(isLeader bool, leaderRaftID uint64, role raft.StateType, term uint64) {
	// Get leader node ID from raft ID
	leaderNodeID, _ := a.discovery.GetNodeIDFromRaftID(leaderRaftID)

	// Check if this is a new leader (compare node IDs, not raft IDs)
	currentLeaderNodeID := a.GetLeaderID()

	isNewLeader := (currentLeaderNodeID == "" && leaderNodeID != "") ||
		(currentLeaderNodeID != "" && leaderNodeID != "" && currentLeaderNodeID != leaderNodeID)

	if isNewLeader && leaderNodeID != "" {

		a.metrics.LeaderChanged()

		a.logger.Info("new leader detected",
			zap.String("leader_node_id", leaderNodeID),
			zap.Uint64("leader_raft_id", leaderRaftID))
	}

	// Update state
	a.isLeader.Store(isLeader)
	if leaderNodeID != "" {
		a.leaderID.Store(leaderNodeID)
	} else {
		a.leaderID.Store("")
	}
	a.currentTerm.Store(term)

	// Update status
	var status model.ClusterNodeStatus
	switch role {
	case 2: // raft.StateLeader = 2
		status = model.NodeStatusLeaderActive
		a.isLeader.Store(true)
	case 0: // raft.StateFollower = 0
		status = model.NodeStatusFollowerActive
		a.isLeader.Store(false)
	default:
		status = model.NodeStatusUnavailable
		a.isLeader.Store(false)
	}
	a.status.Store(status.ToUin32())

	a.logger.Info("role update",
		zap.String("node_id", a.nodeID),
		zap.Uint8("status", uint8(status)),
		zap.Bool("is_leader", isLeader),
		zap.String("leader_node_id", leaderNodeID))
}

// handleCommittedEntries processes committed entries from Raft and sends to Handler
func (a *ClusterAgent) handleCommittedEntries() {
	defer func() {
		if r := recover(); r != nil {
			a.logger.Error("handleCommittedEntries panicked",
				zap.String("node_id", a.nodeID),
				zap.Any("panic", r),
				zap.Stack("stacktrace"))
		}
	}()

	for {
		select {
		case <-a.ctx.Done():
			return
		case committed, ok := <-a.commitCh:
			if !ok {
				return
			}

			if err := a.processCommittedEntry(committed); err != nil {
				a.logger.Error("Raft state machine error - immediate panic for consistency",
					zap.Uint64("index", committed.Index),
					zap.Error(err))

				_ = a.logger.Sync() // Best effort to flush the logs

				panic("raft consistency violated")
			}
		}
	}
}

// processCommittedEntry routes the entry to the appropriate handler
func (a *ClusterAgent) processCommittedEntry(committed CommittedEntry) error {
	a.currentTerm.Store(committed.Term)

	switch committed.Type {
	case raftpb.EntryConfChange, raftpb.EntryConfChangeV2:
		return a.handleConfigChange(committed)

	case raftpb.EntryNormal:
		return a.handleNormalCommand(committed)

	default:
		a.logger.Warn("unknown entry type",
			zap.Uint64("type", uint64(committed.Type)))
		return nil
	}
}

// handleConfigChange processes membership changes
func (a *ClusterAgent) handleConfigChange(committed CommittedEntry) error {
	voters := make([]string, 0, len(committed.Voters))
	for _, raftID := range committed.Voters {
		if nodeIDStr, ok := a.discovery.GetNodeIDFromRaftID(raftID); ok {
			voters = append(voters, nodeIDStr)
		}
	}
	learners := make([]string, 0, len(committed.Learners))
	for _, raftID := range committed.Learners {
		if nodeIDStr, ok := a.discovery.GetNodeIDFromRaftID(raftID); ok {
			learners = append(learners, nodeIDStr)
		}
	}

	a.members.Update(voters, learners)

	switch committed.Type {
	case raftpb.EntryConfChange:
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(committed.Data); err != nil {
			return fmt.Errorf("unmarshal ConfChange: %w", err)
		}
		return a.handleConfChangeSingle(cc.NodeID, cc.Type)

	case raftpb.EntryConfChangeV2:
		var cc raftpb.ConfChangeV2
		if err := cc.Unmarshal(committed.Data); err != nil {
			return fmt.Errorf("unmarshal ConfChangeV2: %w", err)
		}
		// V2 can have multiple changes in one entry
		for _, change := range cc.Changes {
			if err := a.handleConfChangeSingle(change.NodeID, change.Type); err != nil {
				return err
			}
		}
		return nil

	default:
		return fmt.Errorf("unexpected entry type: %v", committed.Type)
	}
}

func (a *ClusterAgent) handleConfChangeSingle(nodeID uint64, changeType raftpb.ConfChangeType) error {
	nodeIDStr, ok := a.discovery.GetNodeIDFromRaftID(nodeID)
	if !ok {
		a.logger.Warn("unknown raft node ID", zap.Uint64("raft_id", nodeID))
		return nil
	}

	switch changeType {
	case raftpb.ConfChangeAddNode:
		a.logger.Debug("new member added",
			zap.String("node_id", a.nodeID),
			zap.String("peer_node_id", nodeIDStr),
			zap.Uint64("raft_id", nodeID))
	case raftpb.ConfChangeAddLearnerNode:
		a.logger.Debug("new learner added",
			zap.String("node_id", a.nodeID),
			zap.String("peer_node_id", nodeIDStr),
			zap.Uint64("raft_id", nodeID))
	case raftpb.ConfChangeRemoveNode:
		a.logger.Debug("node removed",
			zap.String("node_id", nodeIDStr),
			zap.Uint64("raft_id", nodeID))
		if nodeIDStr == a.nodeID {
			a.logger.Warn("this node was removed from cluster")
		}
	}

	return nil
}

// handleNormalCommand deserializes and executes normal commands
func (a *ClusterAgent) handleNormalCommand(committed CommittedEntry) error {
	// Update applied index and metrics
	a.lastAppliedIndex.Store(committed.Index)
	a.metrics.SetLastAppliedWalIndex(committed.Index)
	if committed.Data == nil {
		return nil
	}
	var cmd model.Command
	if err := msgpack.Unmarshal(committed.Data, &cmd); err != nil {
		return fmt.Errorf("unmarshal Command: %w", err)
	}

	// Reattach response channel from pending proposals
	ri, exist := a.getAndRemovePendingResponse(cmd.ProposeID)
	if exist {
		cmd.RespInfo = &ri
	}

	switch cmd.Type {
	case model.CmdAddJob:
		if cmd.AddJob == nil {
			return fmt.Errorf("add_job payload missing")
		}
		if a.deadJobs[cmd.AddJob.Job.ID] {
			return nil
		}
		return a.handler.ProcessCommand(a.ctx, cmd.AddJob.Job.Topic, &cmd, committed.Index)

	case model.CmdDone:
		if cmd.Done == nil {
			return fmt.Errorf("done payload missing")
		}
		for _, id := range cmd.Done.JobIDs {
			delete(a.deadJobs, id)
		}
		return a.handler.ProcessCommand(a.ctx, cmd.Done.Topic, &cmd, committed.Index)

	case model.CmdDrop:
		if cmd.Drop == nil {
			return fmt.Errorf("drop payload missing")
		}
		for _, jobID := range cmd.Drop.JobIDs {
			delete(a.deadJobs, jobID)
		}
		_ = a.handler.ProcessCommand(a.ctx, cmd.Drop.Topic, &cmd, committed.Index)
		// persist do dlq
		if a.dlqManager != nil {
			a.dlqManager.AppendBatch(time.Now().UnixMilli(), cmd.Drop.Topic, cmd.Drop.JobIDs)
		}

	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}

	return nil
}

// getAndRemovePendingResponse retrieves and removes a pending response channel
func (a *ClusterAgent) getAndRemovePendingResponse(id uint64) (model.RespInfo, bool) {
	a.muPndPr.Lock()
	resp, ok := a.pendingProposals[id]
	delete(a.pendingProposals, id)
	a.muPndPr.Unlock()
	return resp, ok
}

// handleOutgoingMessages processes outgoing Raft messages and sends to peers
func (a *ClusterAgent) handleOutgoingMessages() {
	for {
		select {
		case <-a.ctx.Done():
			return

		case msg, ok := <-a.outgoingCh:
			if !ok {
				return
			}

			if err := a.sendRaftMessage(msg); err != nil {
				a.logger.Debug("failed to send Raft message",
					zap.Uint64("to", msg.To),
					zap.Error(err))
			}
		}
	}
}

// sendRaftMessage sends a Raft message to the target node
func (a *ClusterAgent) sendRaftMessage(msg raftpb.Message) error {
	targetNodeID, ok := a.discovery.GetNodeIDFromRaftID(msg.To)
	if !ok {
		return internal.ErrUnknownNodeID
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	request := &ClusterRequest{
		Type:        ReqRaftMessage,
		RaftMessage: &RaftMessagePayload{Data: data},
	}

	_, err = a.sendRequest(targetNodeID, request)
	return err
}
