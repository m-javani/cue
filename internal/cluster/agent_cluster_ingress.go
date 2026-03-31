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
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/m-javani/cue/internal"
)

// ProcessRequest handles incoming cluster requests from other nodes via QUIC
func (a *ClusterAgent) ProcessRequest(request *ClusterRequest) (*ClusterResponse, error) {
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

	case ReqSharedPeersList:
		if request.SharedPeers == nil {
			a.logger.Sugar().Errorf("shared_peers payload missing")
			return &ClusterResponse{Type: ResNegative, Negative: &NegativePayload{}}, nil
		}
		return a.handleSharedPeersList(request.SharedPeers.Peers)

	case ReqPeersListQuery:
		return &ClusterResponse{
			Type:      ResPeersList,
			PeersList: &PeersListRespPayload{Peers: a.discovery.ListPeers()},
		}, nil

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

func (a *ClusterAgent) handleUpdatePeersList(peers []string) (*ClusterResponse, error) {
	// Update service discovery
	a.discovery.UpdatePeers(peers)

	// Sync connections asynchronously
	go func() {
		peerAddrs := a.discovery.ListPeersAddrServerName()
		if err := a.quicServer.SyncConnections(peerAddrs); err != nil {
			a.logger.Warn("failed to sync connections after peer list update",
				zap.Error(err))
		}
	}()

	return &ClusterResponse{Type: ResAck, Ack: &AckPayload{}}, nil
}

func (a *ClusterAgent) handleSharedPeersList(peers []string) (*ClusterResponse, error) {
	a.discovery.MergePeers(peers)

	return &ClusterResponse{Type: ResAck, Ack: &AckPayload{}}, nil
}
