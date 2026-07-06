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

	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
)

// ProcessRequest handles incoming cluster requests from other nodes via QUIC
func (a *ClusterAgent) ProcessRequest(request *ClusterRequest, nodeID string) (*ClusterResponse, error) {
	defer func() {
		if r := recover(); r != nil {
			a.logger.Error("commandProcessor panicked",
				zap.String("node_id", a.nodeID),
				zap.Any("panic", r),
				zap.Stack("stacktrace"))
		}
	}()

	switch request.Type {
	case ReqConnectionHeartbeat:
		return &ClusterResponse{Type: ResAck, Ack: &AckPayload{}}, nil

	case ReqClusterInfo:
		return &ClusterResponse{
			Type: ResClusterInfo,
			ClusterInfo: &ClusterInfoRespPayload{
				LeaderID: a.GetLeaderID(),
				Status:   a.GetStatus(),
			},
		}, nil

	case ReqUpdatePeersList:
		if request.UpdatePeers == nil {
			a.logger.Sugar().Errorf("update_peers payload missing")
			return &ClusterResponse{Type: ResNegative, Negative: &NegativePayload{}}, nil
		}
		return a.handleUpdatePeersList(request.UpdatePeers.Peers)

	case ReqAddMissingPeers:
		if request.AddMissing == nil {
			a.logger.Sugar().Errorf("add_missing payload missing")
			return &ClusterResponse{Type: ResNegative, Negative: &NegativePayload{}}, nil
		}
		return a.handleAddMissingPeers(request.AddMissing.Peers)

	case ReqPeersListQuery:
		return a.handlePeersListQuery(nodeID)

	case ReqRaftMessage:
		if request.RaftMessage == nil {
			return nil, internal.ErrInvalidRaftMessage
		}

		var msg raftpb.Message
		if err := msg.Unmarshal(request.RaftMessage.Data); err != nil {
			return nil, internal.ErrInvalidRaftMessage
		}

		select {
		case a.stepCh <- msg:
		case <-a.ctx.Done():
			return &ClusterResponse{Type: ResNegative, Negative: &NegativePayload{}}, nil
		}

		return &ClusterResponse{Type: ResAck, Ack: &AckPayload{}}, nil

	default:
		return nil, internal.ErrUnknownRequestType
	}
}

func (a *ClusterAgent) handleUpdatePeersList(peers []model.PeerInfo) (*ClusterResponse, error) {
	// Update service discovery
	a.discovery.UpdatePeers(peers)

	return &ClusterResponse{Type: ResAck, Ack: &AckPayload{}}, nil
}

func (a *ClusterAgent) handleAddMissingPeers(peers []model.PeerInfo) (*ClusterResponse, error) {
	a.discovery.MergePeers(peers)
	a.peerSyncOutgoingCoolDown.Store(time.Now().Local().UnixMilli())
	return &ClusterResponse{Type: ResAck, Ack: &AckPayload{}}, nil
}

func (a *ClusterAgent) handlePeersListQuery(nodeID string) (*ClusterResponse, error) {
	peers := a.discovery.ListPeers()

	go func(tid string) {
		request := &ClusterRequest{
			Type:       ReqAddMissingPeers,
			AddMissing: &AddMissingPayload{Peers: peers},
		}
		_, err := a.sendRequest(nodeID, request)
		if err != nil {
			a.logger.Debug("failed to share peers with node",
				zap.String("target", nodeID),
				zap.Error(err))
		}
	}(nodeID)

	return &ClusterResponse{Type: ResAck, Ack: &AckPayload{}}, nil
}
