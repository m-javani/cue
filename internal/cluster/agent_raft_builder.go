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
	"slices"

	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// buildAndStartRaftNode creates and starts the Raft node
func (a *ClusterAgent) buildAndStartRaftNode(storage *RaftStorage) error {
	voterRaftIDs, err := a.resolvePeerRaftIDs(a.initialVoters)
	if err != nil {
		return err
	}
	var peers []uint64
	if a.containsRaftID(voterRaftIDs, a.raftNodeID) {
		peers = voterRaftIDs
	}

	cRaft, err := NewCRaft(
		a.nodeID,
		Config{
			ID:              a.raftNodeID,
			Peers:           peers,
			RaftTickMs:      a.raftTickMs,
			ElectionTick:    a.raftElectionTick,
			HeartbeatTick:   a.raftHeartbeatTick,
			MaxSizePerMsg:   4 * 1024 * 1024,
			MaxInflightMsgs: 2048,
		},
		storage, // Pass storage ownership to Raft
		a.proposeCh,
		a.stepCh,
		a.commitCh,
		a.outgoingCh,
		a.ctrlCh,
		a.notifyCh,
		a.logger,
		a.metrics,
	)
	if err != nil {
		return err
	}

	_, confState, err := storage.InitialState()
	if err != nil {
		a.logger.Error("failed to get initial state", zap.Error(err))
		return err
	}

	if slices.Contains(confState.Voters, a.raftNodeID) {
		a.isVoter.Store(1)
	}

	// Start Raft in its own goroutine
	go cRaft.Run(a.ctx)

	status := cRaft.GetStatus()
	voters := make([]string, 0, len(status.Config.Voters))
	for raftID := range status.Config.Voters.IDs() {
		if nodeIDStr, ok := a.discovery.GetNodeIDFromRaftID(raftID); ok {
			voters = append(voters, nodeIDStr)
		}
	}

	learners := make([]string, 0, len(status.Config.Learners))
	for raftID := range status.Config.Learners {
		if nodeIDStr, ok := a.discovery.GetNodeIDFromRaftID(raftID); ok {
			learners = append(learners, nodeIDStr)
		}
	}

	a.members.Update(voters, learners)

	return nil
}

// resolvePeerRaftIDs converts node ID strings to raft IDs
func (a *ClusterAgent) resolvePeerRaftIDs(nodeIDs []string) ([]uint64, error) {
	var raftIDs []uint64
	for _, nodeID := range nodeIDs {
		raftID, ok := a.discovery.GetRaftIDFromNode(nodeID)
		if !ok || raftID == 0 {
			return nil, fmt.Errorf("failed to resolve raft ID for node: %s", nodeID)
		}
		raftIDs = append(raftIDs, raftID)
	}
	return raftIDs, nil
}

// ensureDummySnapshot creates a dummy snapshot if storage is empty
func (a *ClusterAgent) ensureDummySnapshot(storage *RaftStorage) error {
	lastIdx, err := storage.LastIndex()
	if err != nil {
		return err
	}

	if lastIdx > 1 {
		return nil // Already has data
	}

	// isInitialVoter := a.containsNodeID(a.initialVoters, a.nodeID)
	// if !isInitialVoter {
	// 	return nil // Only initial voters create dummy snapshot
	// }

	// Resolve voter raft IDs
	voterRaftIDs, err := a.resolvePeerRaftIDs(a.initialVoters)
	if err != nil {
		return err
	}

	// Create dummy snapshot
	confState := &raftpb.ConfState{Voters: voterRaftIDs}
	metadata := &raftpb.SnapshotMetadata{
		Index:     proto.Uint64(1),
		Term:      proto.Uint64(1),
		ConfState: confState,
	}
	a.currentTerm.Store(1)

	return storage.InstallSnapshot(metadata)
}

// containsRaftID checks if a slice contains a raft ID
func (a *ClusterAgent) containsRaftID(ids []uint64, target uint64) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}
