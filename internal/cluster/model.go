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

	"github.com/m-javani/cue/internal/model"
	"github.com/vmihailenco/msgpack/v5"
)

// ========== Cluster Request ==========

type ClusterRequestType uint8

const (
	ReqConnectionHeartbeat ClusterRequestType = iota
	ReqPeersListQuery
	ReqUpdatePeersList
	ReqAddMissingPeers
	ReqRaftMessage
	ReqClusterInfo
)

type ClusterRequest struct {
	Type ClusterRequestType `msgpack:"type"`

	// Fixed union: exactly one pointer is non-nil, matching Type
	Heartbeat   *HeartbeatPayload
	PeersList   *PeersListRespPayload
	UpdatePeers *UpdatePeersPayload
	AddMissing  *AddMissingPayload
	RaftMessage *RaftMessagePayload
	ClusterInfo *ClusterInfoPayload
}

type HeartbeatPayload struct {
	Timestamp int64 `msgpack:"timestamp"`
}

type UpdatePeersPayload struct {
	Peers []model.PeerInfo `msgpack:"peers"`
}

type AddMissingPayload struct {
	Peers []model.PeerInfo `msgpack:"peers"`
}

// RaftMessagePayload wraps the raw protobuf bytes
type RaftMessagePayload struct {
	Data []byte `msgpack:"data"`
}

type ClusterInfoPayload struct{} // empty for now

// Custom marshaler
// MarshalMsgpack encodes ClusterRequest as [Type, Payload]
func (r ClusterRequest) MarshalMsgpack() ([]byte, error) {
	var payload any
	switch r.Type {
	case ReqConnectionHeartbeat:
		if r.Heartbeat == nil {
			return nil, fmt.Errorf("heartbeat payload missing")
		}
		payload = r.Heartbeat
	case ReqPeersListQuery:
		payload = nil
	case ReqUpdatePeersList:
		if r.UpdatePeers == nil {
			return nil, fmt.Errorf("update_peers payload missing")
		}
		payload = r.UpdatePeers
	case ReqAddMissingPeers:
		if r.AddMissing == nil {
			return nil, fmt.Errorf("add_missing payload missing")
		}
		payload = r.AddMissing
	case ReqRaftMessage:
		if r.RaftMessage == nil {
			return nil, fmt.Errorf("raft_message payload missing")
		}
		payload = r.RaftMessage
	case ReqClusterInfo:
		if r.ClusterInfo == nil {
			return nil, fmt.Errorf("cluster_info payload missing")
		}
		payload = r.ClusterInfo
	default:
		return nil, fmt.Errorf("unknown request type: %d", r.Type)
	}
	return msgpack.Marshal([2]any{uint8(r.Type), payload})
}

// UnmarshalMsgpack decodes [Type, Payload] into ClusterRequest
func (r *ClusterRequest) UnmarshalMsgpack(data []byte) error {
	var arr [2]msgpack.RawMessage
	if err := msgpack.Unmarshal(data, &arr); err != nil {
		return fmt.Errorf("unmarshal array: %w", err)
	}

	// Unmarshal Type
	var typeVal uint8
	if err := msgpack.Unmarshal(arr[0], &typeVal); err != nil {
		return fmt.Errorf("unmarshal type: %w", err)
	}
	r.Type = ClusterRequestType(typeVal)

	// Unmarshal payload based on type
	switch r.Type {
	case ReqConnectionHeartbeat:
		r.Heartbeat = new(HeartbeatPayload)
		if err := msgpack.Unmarshal(arr[1], r.Heartbeat); err != nil {
			return fmt.Errorf("unmarshal heartbeat: %w", err)
		}
	case ReqPeersListQuery:
		r.PeersList = nil
	case ReqUpdatePeersList:
		r.UpdatePeers = new(UpdatePeersPayload)
		if err := msgpack.Unmarshal(arr[1], r.UpdatePeers); err != nil {
			return fmt.Errorf("unmarshal update_peers: %w", err)
		}
	case ReqAddMissingPeers:
		r.AddMissing = new(AddMissingPayload)
		if err := msgpack.Unmarshal(arr[1], r.AddMissing); err != nil {
			return fmt.Errorf("unmarshal add_missing: %w", err)
		}
	case ReqRaftMessage:
		r.RaftMessage = new(RaftMessagePayload)
		if err := msgpack.Unmarshal(arr[1], r.RaftMessage); err != nil {
			return fmt.Errorf("unmarshal raft_message: %w", err)
		}
	case ReqClusterInfo:
		r.ClusterInfo = new(ClusterInfoPayload)
		if err := msgpack.Unmarshal(arr[1], r.ClusterInfo); err != nil {
			return fmt.Errorf("unmarshal cluster_info: %w", err)
		}
	default:
		return fmt.Errorf("unknown request type: %d", r.Type)
	}
	return nil
}

// String returns a human-readable representation of ClusterRequestType
func (t ClusterRequestType) String() string {
	switch t {
	case ReqConnectionHeartbeat:
		return "ReqConnectionHeartbeat"
	case ReqPeersListQuery:
		return "ReqPeersListQuery"
	case ReqUpdatePeersList:
		return "ReqUpdatePeersList"
	case ReqAddMissingPeers:
		return "ReqAddMissingPeers"
	case ReqRaftMessage:
		return "ReqRaftMessage"
	case ReqClusterInfo:
		return "ReqClusterInfo"
	default:
		return fmt.Sprintf("UnknownRequestType(%d)", t)
	}
}

// String returns a human-readable representation of ClusterRequest
func (r ClusterRequest) String() string {
	var payloadStr string
	switch r.Type {
	case ReqConnectionHeartbeat:
		if r.Heartbeat != nil {
			payloadStr = fmt.Sprintf("Timestamp=%d", r.Heartbeat.Timestamp)
		} else {
			payloadStr = "nil"
		}
	case ReqPeersListQuery:
		payloadStr = "nil"
	case ReqUpdatePeersList:
		if r.UpdatePeers != nil {
			payloadStr = fmt.Sprintf("Peers=%v", r.UpdatePeers.Peers)
		} else {
			payloadStr = "nil"
		}
	case ReqAddMissingPeers:
		if r.AddMissing != nil {
			payloadStr = fmt.Sprintf("Peers=%v", r.AddMissing.Peers)
		} else {
			payloadStr = "nil"
		}
	case ReqRaftMessage:
		if r.RaftMessage != nil {
			payloadStr = fmt.Sprintf("DataLen=%d", len(r.RaftMessage.Data))
			// Show first 50 bytes if not empty
			if len(r.RaftMessage.Data) > 0 {
				preview := r.RaftMessage.Data
				if len(preview) > 50 {
					preview = preview[:50]
				}
				payloadStr += fmt.Sprintf(" Preview=%q", preview)
			}
		} else {
			payloadStr = "nil"
		}
	case ReqClusterInfo:
		payloadStr = "{}"
	default:
		payloadStr = "unknown"
	}

	return fmt.Sprintf("ClusterRequest{Type=%s, Payload=%s}",
		r.Type.String(), payloadStr)
}

// ========== Cluster Response ==========

type ClusterResponseType uint8

const (
	ResAck ClusterResponseType = iota
	ResNegative
	ResPeersList
	ResUnavailable
	ResError
	ResClusterInfo
)

type ClusterResponse struct {
	Type ClusterResponseType `msgpack:"type"`

	// Fixed union
	Ack         *AckPayload
	Negative    *NegativePayload
	PeersList   *PeersListRespPayload
	Unavailable *UnavailablePayload
	Error       *ErrorPayload
	ClusterInfo *ClusterInfoRespPayload
}

type AckPayload struct{}
type NegativePayload struct{}
type UnavailablePayload struct{}
type ErrorPayload struct {
	Message string `msgpack:"message"`
}

type PeersListRespPayload struct {
	Peers []model.PeerInfo `msgpack:"peers"`
}

type ClusterInfoRespPayload struct {
	LeaderID string                  `msgpack:"leader_id"`
	Status   model.ClusterNodeStatus `msgpack:"status"`
}

// Custom marshaler
// MarshalMsgpack encodes ClusterResponse as [Type, Payload]
func (r ClusterResponse) MarshalMsgpack() ([]byte, error) {
	var payload any
	switch r.Type {
	case ResAck:
		if r.Ack == nil {
			return nil, fmt.Errorf("ack payload missing")
		}
		payload = r.Ack
	case ResNegative:
		if r.Negative == nil {
			return nil, fmt.Errorf("negative payload missing")
		}
		payload = r.Negative
	case ResPeersList:
		if r.PeersList == nil {
			return nil, fmt.Errorf("peers_list payload missing")
		}
		payload = r.PeersList
	case ResUnavailable:
		if r.Unavailable == nil {
			return nil, fmt.Errorf("unavailable payload missing")
		}
		payload = r.Unavailable
	case ResError:
		if r.Error == nil {
			return nil, fmt.Errorf("error payload missing")
		}
		payload = r.Error
	case ResClusterInfo:
		if r.ClusterInfo == nil {
			return nil, fmt.Errorf("cluster_info payload missing")
		}
		payload = r.ClusterInfo
	default:
		return nil, fmt.Errorf("unknown response type: %d", r.Type)
	}
	return msgpack.Marshal([2]any{uint8(r.Type), payload})
}

// UnmarshalMsgpack decodes [Type, Payload] into ClusterResponse
func (r *ClusterResponse) UnmarshalMsgpack(data []byte) error {
	var arr [2]msgpack.RawMessage
	if err := msgpack.Unmarshal(data, &arr); err != nil {
		return fmt.Errorf("unmarshal array: %w", err)
	}

	// Unmarshal Type
	var typeVal uint8
	if err := msgpack.Unmarshal(arr[0], &typeVal); err != nil {
		return fmt.Errorf("unmarshal type: %w", err)
	}
	r.Type = ClusterResponseType(typeVal)

	// Unmarshal payload based on type
	switch r.Type {
	case ResAck:
		r.Ack = new(AckPayload)
		if err := msgpack.Unmarshal(arr[1], r.Ack); err != nil {
			return fmt.Errorf("unmarshal ack: %w", err)
		}
	case ResNegative:
		r.Negative = new(NegativePayload)
		if err := msgpack.Unmarshal(arr[1], r.Negative); err != nil {
			return fmt.Errorf("unmarshal negative: %w", err)
		}
	case ResPeersList:
		r.PeersList = new(PeersListRespPayload)
		if err := msgpack.Unmarshal(arr[1], r.PeersList); err != nil {
			return fmt.Errorf("unmarshal peers_list: %w", err)
		}
	case ResUnavailable:
		r.Unavailable = new(UnavailablePayload)
		if err := msgpack.Unmarshal(arr[1], r.Unavailable); err != nil {
			return fmt.Errorf("unmarshal unavailable: %w", err)
		}
	case ResError:
		r.Error = new(ErrorPayload)
		if err := msgpack.Unmarshal(arr[1], r.Error); err != nil {
			return fmt.Errorf("unmarshal error: %w", err)
		}
	case ResClusterInfo:
		r.ClusterInfo = new(ClusterInfoRespPayload)
		if err := msgpack.Unmarshal(arr[1], r.ClusterInfo); err != nil {
			return fmt.Errorf("unmarshal cluster_info: %w", err)
		}
	default:
		return fmt.Errorf("unknown response type: %d", r.Type)
	}
	return nil
}

// String returns a human-readable representation of ClusterResponseType
func (t ClusterResponseType) String() string {
	switch t {
	case ResAck:
		return "ResAck"
	case ResNegative:
		return "ResNegative"
	case ResPeersList:
		return "ResPeersList"
	case ResUnavailable:
		return "ResUnavailable"
	case ResError:
		return "ResError"
	case ResClusterInfo:
		return "ResClusterInfo"
	default:
		return fmt.Sprintf("UnknownResponseType(%d)", t)
	}
}

// String returns a human-readable representation of ClusterResponse
func (r ClusterResponse) String() string {
	var payloadStr string
	switch r.Type {
	case ResAck:
		payloadStr = "{}"
	case ResNegative:
		payloadStr = "{}"
	case ResPeersList:
		if r.PeersList != nil {
			payloadStr = fmt.Sprintf("Peers=%v", r.PeersList.Peers)
		} else {
			payloadStr = "nil"
		}
	case ResUnavailable:
		payloadStr = "{}"
	case ResError:
		if r.Error != nil {
			payloadStr = fmt.Sprintf("Message=%q", r.Error.Message)
		} else {
			payloadStr = "nil"
		}
	case ResClusterInfo:
		if r.ClusterInfo != nil {
			payloadStr = fmt.Sprintf("LeaderID=%q, Status=%v",
				r.ClusterInfo.LeaderID, r.ClusterInfo.Status)
		} else {
			payloadStr = "nil"
		}
	default:
		payloadStr = "unknown"
	}

	return fmt.Sprintf("ClusterResponse{Type=%s, Payload=%s}",
		r.Type.String(), payloadStr)
}
