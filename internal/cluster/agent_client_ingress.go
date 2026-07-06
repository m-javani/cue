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
	"time"

	"github.com/m-javani/cue/internal"

	"github.com/m-javani/cue/internal/model"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
)

// commandProcessor runs as a goroutine and processes commands from the gateway
func (a *ClusterAgent) commandProcessor() {
	for {
		select {
		case <-a.ctx.Done():
			return

		case cmd, ok := <-a.commandCh:
			if !ok {
				return
			}

			a.processCommand(cmd)
		}
	}
}

// processCommand handles a single command
func (a *ClusterAgent) processCommand(cmd model.Command) {
	if !a.IsLeader() {
		a.logger.Error("ignoring command in follower agent")
		return
	}
	if !a.IsActive() {
		a.logger.Error("ignoring command - leader is not active")
		return
	}
	switch cmd.Type {
	case model.CmdUpdatePeersList:
		if cmd.Peers == nil {
			a.logger.Error("peers payload missing")
			return
		}
		if err := a.processUpdatePeersList(cmd.Peers.Peers); err != nil {
			a.logger.Error("failed to update peers list", zap.Error(err))
		}

	case model.CmdAddNode:
		if cmd.AddNode == nil {
			a.logger.Error("add_node payload missing")
			return
		}
		raftID, ok := a.discovery.GetRaftIDFromNode(cmd.AddNode.NodeID)
		if ok && raftID != 0 {
			select {
			case a.ctrlCh <- ControlCmd{
				Type:   CmdAddNode,
				NodeID: raftID,
			}:
			case <-a.ctx.Done():
				return
			}
		}

	case model.CmdRemoveNode:
		if cmd.RemoveNode == nil {
			a.logger.Error("remove_node payload missing")
			return
		}
		raftID, ok := a.discovery.GetRaftIDFromNode(cmd.RemoveNode.NodeID)
		if ok && raftID != 0 {
			select {
			case a.ctrlCh <- ControlCmd{
				Type:   CmdRemoveNode,
				NodeID: raftID,
			}:
			case <-a.ctx.Done():
				return
			}
		}

	case model.CmdTransferLeader:
		if cmd.Transfer == nil {
			a.logger.Error("transfer_leader payload missing")
			return
		}
		raftID, ok := a.discovery.GetRaftIDFromNode(cmd.Transfer.TargetNodeID)
		if ok && raftID != 0 {
			select {
			case a.ctrlCh <- ControlCmd{
				Type:   CmdTransferLeader,
				NodeID: raftID,
			}:
			case <-a.ctx.Done():
				return
			}
		}

	case model.CmdAddJob, model.CmdDone, model.CmdDrop:
		// Check if node is active (leader or follower)
		if !a.IsActive() {
			if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
				res := model.ToProducerResponse{
					RequestID: cmd.RespInfo.RequestID,
					Status:    "error",
					Error:     internal.ErrServiceUnavailable.Error(),
				}
				cmd.RespInfo.RespCh <- res
			}
			return
		}

		id := a.proposalID.Add(1)
		cmd.ProposeID = id
		a.muPndPr.Lock()
		a.pendingProposals[id] = *cmd.RespInfo
		a.muPndPr.Unlock()

		// Serialize command directly — custom marshaler handles Type, ProposeID, and active payload
		data, err := msgpack.Marshal(cmd)
		if err != nil {
			if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
				res := model.ToProducerResponse{
					RequestID: cmd.RespInfo.RequestID,
					Status:    "error",
					Error:     err.Error(),
				}
				cmd.RespInfo.RespCh <- res
			}
			return
		}

		// Send to Raft propose channel
		select {
		case a.proposeCh <- ProposeRequest{Data: data}:
		case <-a.ctx.Done():
		}

	default:
		if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
			res := model.ToProducerResponse{
				RequestID: cmd.RespInfo.RequestID,
				Status:    "error",
				Error:     internal.ErrUnknownCommand.Error(),
			}
			cmd.RespInfo.RespCh <- res
		}
	}
}

// processUpdatePeersList handles the UpdatePeersList command from gateway or Raft
func (a *ClusterAgent) processUpdatePeersList(peers []model.PeerInfo) error {
	// Update local service discovery
	a.discovery.UpdatePeers(peers)

	// Broadcast the new peer list to all other nodes
	request := &ClusterRequest{
		Type:        ReqUpdatePeersList,
		UpdatePeers: &UpdatePeersPayload{Peers: peers},
	}

	// Broadcast with 500ms timeout (fire and forget - don't care about responses)
	go a.broadcast(request, 2*time.Second)

	a.logger.Info("updated peers list",
		zap.Any("peers", peers),
		zap.Int("peer_count", len(peers)))

	return nil
}
