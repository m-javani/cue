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
	"bytes"
	"testing"

	"github.com/m-javani/cue/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestClusterRequestType_String(t *testing.T) {
	tests := []struct {
		name     string
		rt       ClusterRequestType
		expected string
	}{
		{"Heartbeat", ReqConnectionHeartbeat, "ReqConnectionHeartbeat"},
		{"PeersListQuery", ReqPeersListQuery, "ReqPeersListQuery"},
		{"UpdatePeersList", ReqUpdatePeersList, "ReqUpdatePeersList"},
		{"AddMissingPeers", ReqAddMissingPeers, "ReqAddMissingPeers"},
		{"RaftMessage", ReqRaftMessage, "ReqRaftMessage"},
		{"ClusterInfo", ReqClusterInfo, "ReqClusterInfo"},
		{"Unknown", ClusterRequestType(99), "UnknownRequestType(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.rt.String())
		})
	}
}

func TestClusterResponseType_String(t *testing.T) {
	tests := []struct {
		name     string
		rt       ClusterResponseType
		expected string
	}{
		{"Ack", ResAck, "ResAck"},
		{"Negative", ResNegative, "ResNegative"},
		{"PeersList", ResPeersList, "ResPeersList"},
		{"Unavailable", ResUnavailable, "ResUnavailable"},
		{"Error", ResError, "ResError"},
		{"ClusterInfo", ResClusterInfo, "ResClusterInfo"},
		{"Unknown", ClusterResponseType(99), "UnknownResponseType(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.rt.String())
		})
	}
}

// TestClusterRequest_String
func TestClusterRequest_String(t *testing.T) {
	tests := []struct {
		name     string
		req      ClusterRequest
		contains []string // substrings we expect to find
	}{
		{
			name: "Heartbeat",
			req: ClusterRequest{
				Type:      ReqConnectionHeartbeat,
				Heartbeat: &HeartbeatPayload{Timestamp: 12345},
			},
			contains: []string{"ReqConnectionHeartbeat", "Timestamp=12345"},
		},
		{
			name: "Heartbeat nil",
			req: ClusterRequest{
				Type: ReqConnectionHeartbeat,
			},
			contains: []string{"ReqConnectionHeartbeat", "nil"},
		},
		{
			name: "PeersListQuery",
			req: ClusterRequest{
				Type: ReqPeersListQuery,
				// PeersListQuery should NOT have a PeersList payload - it's a query
				// The PeersList field is for responses
			},
			contains: []string{"ReqPeersListQuery", "nil"},
		},
		{
			name: "UpdatePeersList",
			req: ClusterRequest{
				Type: ReqUpdatePeersList,
				UpdatePeers: &UpdatePeersPayload{
					Peers: []model.PeerInfo{
						{
							NodeID: "peer1",
							IP:     "192.168.1.1:8080",
							Identity: model.TLSIdentity{
								Kind:  model.IdentityDNS,
								Value: "peer1.example.com",
							},
						},
						{
							NodeID: "peer2",
							IP:     "192.168.1.2:8080",
							Identity: model.TLSIdentity{
								Kind:  model.IdentityDNS,
								Value: "peer2.example.com",
							},
						},
					},
				},
			},
			contains: []string{"ReqUpdatePeersList", "NodeID:peer1", "NodeID:peer2"},
		},
		{
			name: "UpdatePeersList nil",
			req: ClusterRequest{
				Type: ReqUpdatePeersList,
			},
			contains: []string{"ReqUpdatePeersList", "nil"},
		},
		{
			name: "AddMissingPeers",
			req: ClusterRequest{
				Type: ReqAddMissingPeers,
				AddMissing: &AddMissingPayload{
					Peers: []model.PeerInfo{
						{
							NodeID: "peer1",
							IP:     "192.168.1.1:8080",
							Identity: model.TLSIdentity{
								Kind:  model.IdentityDNS,
								Value: "peer1.example.com",
							},
						},
						{
							NodeID: "peer2",
							IP:     "192.168.1.2:8080",
							Identity: model.TLSIdentity{
								Kind:  model.IdentityDNS,
								Value: "peer2.example.com",
							},
						},
					},
				},
			},
			contains: []string{"ReqAddMissingPeers", "NodeID:peer1", "NodeID:peer2"},
		},
		{
			name: "AddMissingPeers nil",
			req: ClusterRequest{
				Type: ReqAddMissingPeers,
			},
			contains: []string{"ReqAddMissingPeers", "nil"},
		},
		{
			name: "RaftMessage with data",
			req: ClusterRequest{
				Type: ReqRaftMessage,
				RaftMessage: &RaftMessagePayload{
					Data: []byte("test message data that is longer than 50 bytes to test truncation feature of the string method"),
				},
			},
			contains: []string{"ReqRaftMessage", "DataLen=", "Preview="},
		},
		{
			name: "RaftMessage empty",
			req: ClusterRequest{
				Type:        ReqRaftMessage,
				RaftMessage: &RaftMessagePayload{Data: []byte{}},
			},
			contains: []string{"ReqRaftMessage", "DataLen=0"},
		},
		{
			name: "RaftMessage nil",
			req: ClusterRequest{
				Type: ReqRaftMessage,
			},
			contains: []string{"ReqRaftMessage", "nil"},
		},
		{
			name: "ClusterInfo",
			req: ClusterRequest{
				Type:        ReqClusterInfo,
				ClusterInfo: &ClusterInfoPayload{},
			},
			contains: []string{"ReqClusterInfo", "{}"},
		},
		{
			name: "Unknown type",
			req: ClusterRequest{
				Type: ClusterRequestType(99),
			},
			contains: []string{"UnknownRequestType(99)", "unknown"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str := tt.req.String()
			for _, substr := range tt.contains {
				assert.Contains(t, str, substr)
			}
		})
	}
}

func TestClusterResponse_String(t *testing.T) {
	tests := []struct {
		name     string
		resp     ClusterResponse
		contains []string
	}{
		{
			name: "Ack",
			resp: ClusterResponse{
				Type: ResAck,
				Ack:  &AckPayload{},
			},
			contains: []string{"ResAck", "{}"},
		},
		{
			name: "Negative",
			resp: ClusterResponse{
				Type:     ResNegative,
				Negative: &NegativePayload{},
			},
			contains: []string{"ResNegative", "{}"},
		},
		{
			name: "PeersList",
			resp: ClusterResponse{
				Type: ResPeersList,
				PeersList: &PeersListRespPayload{
					Peers: []model.PeerInfo{
						{
							NodeID: "peer1",
							IP:     "192.168.1.1:8080",
							Identity: model.TLSIdentity{
								Kind:  model.IdentityDNS,
								Value: "peer1.example.com",
							},
						},
						{
							NodeID: "peer2",
							IP:     "192.168.1.2:8080",
							Identity: model.TLSIdentity{
								Kind:  model.IdentityDNS,
								Value: "peer2.example.com",
							},
						},
					},
				},
			},
			contains: []string{"ResPeersList", "NodeID:peer1", "NodeID:peer2"},
		},
		{
			name: "PeersList nil",
			resp: ClusterResponse{
				Type: ResPeersList,
			},
			contains: []string{"ResPeersList", "nil"},
		},
		{
			name: "Unavailable",
			resp: ClusterResponse{
				Type:        ResUnavailable,
				Unavailable: &UnavailablePayload{},
			},
			contains: []string{"ResUnavailable", "{}"},
		},
		{
			name: "Error",
			resp: ClusterResponse{
				Type: ResError,
				Error: &ErrorPayload{
					Message: "something went wrong",
				},
			},
			contains: []string{"ResError", `Message="something went wrong"`},
		},
		{
			name: "Error nil",
			resp: ClusterResponse{
				Type: ResError,
			},
			contains: []string{"ResError", "nil"},
		},
		{
			name: "ClusterInfo Leader",
			resp: ClusterResponse{
				Type: ResClusterInfo,
				ClusterInfo: &ClusterInfoRespPayload{
					LeaderID: "leader-1",
					Status:   model.NodeStatusLeaderActive,
				},
			},
			contains: []string{"ResClusterInfo", `LeaderID="leader-1"`, "Status=leader"},
		},
		{
			name: "ClusterInfo Follower",
			resp: ClusterResponse{
				Type: ResClusterInfo,
				ClusterInfo: &ClusterInfoRespPayload{
					LeaderID: "follower-1",
					Status:   model.NodeStatusFollowerActive,
				},
			},
			contains: []string{"ResClusterInfo", `LeaderID="follower-1"`, "Status=follower"},
		},
		{
			name: "ClusterInfo Unavailable",
			resp: ClusterResponse{
				Type: ResClusterInfo,
				ClusterInfo: &ClusterInfoRespPayload{
					LeaderID: "",
					Status:   model.NodeStatusUnavailable,
				},
			},
			contains: []string{"ResClusterInfo", `LeaderID=""`, "Status=unavailable"},
		},
		{
			name: "ClusterInfo nil",
			resp: ClusterResponse{
				Type: ResClusterInfo,
			},
			contains: []string{"ResClusterInfo", "nil"},
		},
		{
			name: "Unknown type",
			resp: ClusterResponse{
				Type: ClusterResponseType(99),
			},
			contains: []string{"UnknownResponseType(99)", "unknown"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str := tt.resp.String()
			for _, substr := range tt.contains {
				assert.Contains(t, str, substr)
			}
		})
	}
}

// TestClusterRequest_MarshalUnmarshal
func TestClusterRequest_MarshalUnmarshal(t *testing.T) {
	tests := []struct {
		name string
		req  ClusterRequest
	}{
		{
			name: "Heartbeat",
			req: ClusterRequest{
				Type:      ReqConnectionHeartbeat,
				Heartbeat: &HeartbeatPayload{Timestamp: 12345},
			},
		},
		{
			name: "PeersListQuery",
			req: ClusterRequest{
				Type: ReqPeersListQuery,
				// PeersListQuery is a request - it should NOT have a PeersList payload
				// The PeersList field is for responses
			},
		},
		{
			name: "UpdatePeersList",
			req: ClusterRequest{
				Type: ReqUpdatePeersList,
				UpdatePeers: &UpdatePeersPayload{
					Peers: []model.PeerInfo{
						{
							NodeID: "peer1",
							IP:     "192.168.1.1:8080",
							Identity: model.TLSIdentity{
								Kind:  model.IdentityDNS,
								Value: "peer1.example.com",
							},
							Port: 0,
						},
						{
							NodeID: "peer2",
							IP:     "192.168.1.2:8080",
							Identity: model.TLSIdentity{
								Kind:  model.IdentityDNS,
								Value: "peer2.example.com",
							},
							Port: 0,
						},
					},
				},
			},
		},
		{
			name: "AddMissingPeers",
			req: ClusterRequest{
				Type: ReqAddMissingPeers,
				AddMissing: &AddMissingPayload{
					Peers: []model.PeerInfo{
						{
							NodeID: "peer1",
							IP:     "192.168.1.1:8080",
							Identity: model.TLSIdentity{
								Kind:  model.IdentityDNS,
								Value: "peer1.example.com",
							},
							Port: 0,
						},
						{
							NodeID: "peer2",
							IP:     "192.168.1.2:8080",
							Identity: model.TLSIdentity{
								Kind:  model.IdentityDNS,
								Value: "peer2.example.com",
							},
							Port: 0,
						},
					},
				},
			},
		},
		{
			name: "RaftMessage",
			req: ClusterRequest{
				Type: ReqRaftMessage,
				RaftMessage: &RaftMessagePayload{
					Data: []byte("test raft message"),
				},
			},
		},
		{
			name: "ClusterInfo",
			req: ClusterRequest{
				Type:        ReqClusterInfo,
				ClusterInfo: &ClusterInfoPayload{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			data, err := tt.req.MarshalMsgpack()
			require.NoError(t, err)

			// Unmarshal
			var got ClusterRequest
			err = got.UnmarshalMsgpack(data)
			require.NoError(t, err)

			// Verify
			assert.Equal(t, tt.req.Type, got.Type)
			assert.Equal(t, tt.req, got)
		})
	}
}

func TestClusterResponse_MarshalUnmarshal(t *testing.T) {
	tests := []struct {
		name string
		resp ClusterResponse
	}{
		{
			name: "Ack",
			resp: ClusterResponse{
				Type: ResAck,
				Ack:  &AckPayload{},
			},
		},
		{
			name: "Negative",
			resp: ClusterResponse{
				Type:     ResNegative,
				Negative: &NegativePayload{},
			},
		},
		{
			name: "PeersList",
			resp: ClusterResponse{
				Type: ResPeersList,
				PeersList: &PeersListRespPayload{
					Peers: []model.PeerInfo{
						{
							NodeID: "peer1",
							IP:     "192.168.1.1:8080",
							Identity: model.TLSIdentity{
								Kind:  model.IdentityDNS,
								Value: "peer1.example.com",
							},
						},
						{
							NodeID: "peer2",
							IP:     "192.168.1.2:8080",
							Identity: model.TLSIdentity{
								Kind:  model.IdentityDNS,
								Value: "peer2.example.com",
							},
						},
					},
				},
			},
		},
		{
			name: "Unavailable",
			resp: ClusterResponse{
				Type:        ResUnavailable,
				Unavailable: &UnavailablePayload{},
			},
		},
		{
			name: "Error",
			resp: ClusterResponse{
				Type: ResError,
				Error: &ErrorPayload{
					Message: "test error message",
				},
			},
		},
		{
			name: "ClusterInfo Leader",
			resp: ClusterResponse{
				Type: ResClusterInfo,
				ClusterInfo: &ClusterInfoRespPayload{
					LeaderID: "leader-1",
					Status:   model.NodeStatusLeaderActive,
				},
			},
		},
		{
			name: "ClusterInfo Follower",
			resp: ClusterResponse{
				Type: ResClusterInfo,
				ClusterInfo: &ClusterInfoRespPayload{
					LeaderID: "follower-1",
					Status:   model.NodeStatusFollowerActive,
				},
			},
		},
		{
			name: "ClusterInfo Unavailable",
			resp: ClusterResponse{
				Type: ResClusterInfo,
				ClusterInfo: &ClusterInfoRespPayload{
					LeaderID: "",
					Status:   model.NodeStatusUnavailable,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			data, err := tt.resp.MarshalMsgpack()
			require.NoError(t, err)

			// Unmarshal
			var got ClusterResponse
			err = got.UnmarshalMsgpack(data)
			require.NoError(t, err)

			// Verify
			assert.Equal(t, tt.resp.Type, got.Type)
			assert.Equal(t, tt.resp, got)
		})
	}
}

// TestClusterRequest_MarshalErrors
func TestClusterRequest_MarshalErrors(t *testing.T) {
	tests := []struct {
		name    string
		req     ClusterRequest
		wantErr string
	}{
		{
			name: "Missing Heartbeat",
			req: ClusterRequest{
				Type: ReqConnectionHeartbeat,
			},
			wantErr: "heartbeat payload missing",
		},
		// ReqPeersListQuery should NOT have a payload, so this test is invalid
		// {
		// 	name: "Missing PeersList",
		// 	req: ClusterRequest{
		// 		Type: ReqPeersListQuery,
		// 	},
		// 	wantErr: "peers_list payload missing",
		// },
		{
			name: "Missing UpdatePeers",
			req: ClusterRequest{
				Type: ReqUpdatePeersList,
			},
			wantErr: "update_peers payload missing",
		},
		{
			name: "Missing AddMissing",
			req: ClusterRequest{
				Type: ReqAddMissingPeers,
			},
			wantErr: "add_missing payload missing",
		},
		{
			name: "Missing RaftMessage",
			req: ClusterRequest{
				Type: ReqRaftMessage,
			},
			wantErr: "raft_message payload missing",
		},
		{
			name: "Missing ClusterInfo",
			req: ClusterRequest{
				Type: ReqClusterInfo,
			},
			wantErr: "cluster_info payload missing",
		},
		{
			name: "Unknown Type",
			req: ClusterRequest{
				Type: ClusterRequestType(99),
			},
			wantErr: "unknown request type: 99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.req.MarshalMsgpack()
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestClusterResponse_MarshalErrors(t *testing.T) {
	tests := []struct {
		name    string
		resp    ClusterResponse
		wantErr string
	}{
		{
			name: "Missing Ack",
			resp: ClusterResponse{
				Type: ResAck,
			},
			wantErr: "ack payload missing",
		},
		{
			name: "Missing Negative",
			resp: ClusterResponse{
				Type: ResNegative,
			},
			wantErr: "negative payload missing",
		},
		{
			name: "Missing PeersList",
			resp: ClusterResponse{
				Type: ResPeersList,
			},
			wantErr: "peers_list payload missing",
		},
		{
			name: "Missing Unavailable",
			resp: ClusterResponse{
				Type: ResUnavailable,
			},
			wantErr: "unavailable payload missing",
		},
		{
			name: "Missing Error",
			resp: ClusterResponse{
				Type: ResError,
			},
			wantErr: "error payload missing",
		},
		{
			name: "Missing ClusterInfo",
			resp: ClusterResponse{
				Type: ResClusterInfo,
			},
			wantErr: "cluster_info payload missing",
		},
		{
			name: "Unknown Type",
			resp: ClusterResponse{
				Type: ClusterResponseType(99),
			},
			wantErr: "unknown response type: 99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.resp.MarshalMsgpack()
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestClusterRequest_UnmarshalErrors(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{
			name:    "Invalid array format",
			data:    []byte("invalid"),
			wantErr: "unmarshal array:",
		},
		{
			name:    "Invalid type",
			data:    mustMarshal([2]any{"not a uint8", &HeartbeatPayload{}}),
			wantErr: "unmarshal type:",
		},
		{
			name:    "Unknown type",
			data:    mustMarshal([2]any{uint8(99), &HeartbeatPayload{}}),
			wantErr: "unknown request type: 99",
		},
		{
			name:    "Invalid heartbeat payload",
			data:    mustMarshal([2]any{uint8(ReqConnectionHeartbeat), "invalid"}),
			wantErr: "unmarshal heartbeat:",
		},
		{
			name:    "Invalid add_missing payload",
			data:    mustMarshal([2]any{uint8(ReqAddMissingPeers), "invalid"}),
			wantErr: "unmarshal add_missing:",
		},
		{
			name:    "Invalid raft_message payload",
			data:    mustMarshal([2]any{uint8(ReqRaftMessage), "invalid"}),
			wantErr: "unmarshal raft_message:",
		},
		{
			name:    "Invalid cluster_info payload",
			data:    mustMarshal([2]any{uint8(ReqClusterInfo), "invalid"}),
			wantErr: "unmarshal cluster_info:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req ClusterRequest
			err := req.UnmarshalMsgpack(tt.data)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestClusterResponse_UnmarshalErrors(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{
			name:    "Invalid array format",
			data:    []byte("invalid"),
			wantErr: "unmarshal array:",
		},
		{
			name:    "Invalid type",
			data:    mustMarshal([2]any{"not a uint8", &AckPayload{}}),
			wantErr: "unmarshal type:",
		},
		{
			name:    "Unknown type",
			data:    mustMarshal([2]any{uint8(99), &AckPayload{}}),
			wantErr: "unknown response type: 99",
		},
		{
			name:    "Invalid ack payload",
			data:    mustMarshal([2]any{uint8(ResAck), "invalid"}),
			wantErr: "unmarshal ack:",
		},
		{
			name:    "Invalid negative payload",
			data:    mustMarshal([2]any{uint8(ResNegative), "invalid"}),
			wantErr: "unmarshal negative:",
		},
		{
			name:    "Invalid peers_list payload",
			data:    mustMarshal([2]any{uint8(ResPeersList), "invalid"}),
			wantErr: "unmarshal peers_list:",
		},
		{
			name:    "Invalid unavailable payload",
			data:    mustMarshal([2]any{uint8(ResUnavailable), "invalid"}),
			wantErr: "unmarshal unavailable:",
		},
		{
			name:    "Invalid error payload",
			data:    mustMarshal([2]any{uint8(ResError), "invalid"}),
			wantErr: "unmarshal error:",
		},
		{
			name:    "Invalid cluster_info payload",
			data:    mustMarshal([2]any{uint8(ResClusterInfo), "invalid"}),
			wantErr: "unmarshal cluster_info:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resp ClusterResponse
			err := resp.UnmarshalMsgpack(tt.data)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestPeerResolvedInfo_MarshalUnmarshal(t *testing.T) {
	original := model.PeerInfo{
		NodeID: "node-123",
		IP:     "192.168.1.100:8080",
		Identity: model.TLSIdentity{
			Kind:  model.IdentityDNS,
			Value: "server.example.com",
		},
	}

	data, err := msgpack.Marshal(original)
	require.NoError(t, err)

	var decoded model.PeerInfo
	err = msgpack.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original, decoded)
}

func TestClusterRequest_RoundTripWithMsgpack(t *testing.T) {
	// Test that the custom marshaler works with standard msgpack
	original := ClusterRequest{
		Type: ReqRaftMessage,
		RaftMessage: &RaftMessagePayload{
			Data: []byte("test data"),
		},
	}

	data, err := original.MarshalMsgpack()
	require.NoError(t, err)

	// Verify it's a valid msgpack array
	var arr []any
	err = msgpack.Unmarshal(data, &arr)
	require.NoError(t, err)
	assert.Len(t, arr, 2)
	assert.Equal(t, uint8(ReqRaftMessage), arr[0])

	// Decode back
	var decoded ClusterRequest
	err = decoded.UnmarshalMsgpack(data)
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}

func TestClusterResponse_RoundTripWithMsgpack(t *testing.T) {
	// Test that the custom marshaler works with standard msgpack
	original := ClusterResponse{
		Type: ResClusterInfo,
		ClusterInfo: &ClusterInfoRespPayload{
			LeaderID: "leader-1",
			Status:   model.NodeStatusLeaderActive,
		},
	}

	data, err := original.MarshalMsgpack()
	require.NoError(t, err)

	// Verify it's a valid msgpack array
	var arr []any
	err = msgpack.Unmarshal(data, &arr)
	require.NoError(t, err)
	assert.Len(t, arr, 2)
	assert.Equal(t, uint8(ResClusterInfo), arr[0])

	// Decode back
	var decoded ClusterResponse
	err = decoded.UnmarshalMsgpack(data)
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}

func TestClusterRequest_NilPayloads(t *testing.T) {
	// Test that nil payloads in String() don't panic
	req := ClusterRequest{
		Type:        ReqConnectionHeartbeat,
		Heartbeat:   nil,
		PeersList:   nil,
		UpdatePeers: nil,
		AddMissing:  nil,
		RaftMessage: nil,
		ClusterInfo: nil,
	}

	// Should not panic
	str := req.String()
	assert.Contains(t, str, "ReqConnectionHeartbeat")
	assert.Contains(t, str, "nil")
}

func TestClusterResponse_NilPayloads(t *testing.T) {
	// Test that nil payloads in String() don't panic
	resp := ClusterResponse{
		Type:        ResAck,
		Ack:         nil,
		Negative:    nil,
		PeersList:   nil,
		Unavailable: nil,
		Error:       nil,
		ClusterInfo: nil,
	}

	// Should not panic
	str := resp.String()
	assert.Contains(t, str, "ResAck")
}

// Helper function to marshal data or panic
func mustMarshal(v any) []byte {
	data, err := msgpack.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// Additional test for edge case: very large data in RaftMessage
func TestClusterRequest_RaftMessageLargeData(t *testing.T) {
	largeData := bytes.Repeat([]byte("x"), 10000)
	req := ClusterRequest{
		Type: ReqRaftMessage,
		RaftMessage: &RaftMessagePayload{
			Data: largeData,
		},
	}

	// Marshal
	data, err := req.MarshalMsgpack()
	require.NoError(t, err)

	// Unmarshal
	var decoded ClusterRequest
	err = decoded.UnmarshalMsgpack(data)
	require.NoError(t, err)

	assert.Equal(t, req, decoded)
	assert.Equal(t, largeData, decoded.RaftMessage.Data)
}

// Test all cluster status values from the model package
func TestClusterResponse_AllStatuses(t *testing.T) {
	statuses := []model.ClusterNodeStatus{
		model.NodeStatusFollowerActive,
		model.NodeStatusLeaderActive,
		model.NodeStatusUnavailable,
	}

	for _, status := range statuses {
		t.Run(status.String(), func(t *testing.T) {
			resp := ClusterResponse{
				Type: ResClusterInfo,
				ClusterInfo: &ClusterInfoRespPayload{
					LeaderID: "test-leader",
					Status:   status,
				},
			}

			data, err := resp.MarshalMsgpack()
			require.NoError(t, err)

			var decoded ClusterResponse
			err = decoded.UnmarshalMsgpack(data)
			require.NoError(t, err)

			assert.Equal(t, resp, decoded)
			assert.Equal(t, status, decoded.ClusterInfo.Status)
		})
	}
}

// Test that ClusterNodeStatus values marshal/unmarshal correctly through msgpack
func TestClusterNodeStatus_MsgpackSerialization(t *testing.T) {
	tests := []struct {
		name   string
		status model.ClusterNodeStatus
	}{
		{"Follower", model.NodeStatusFollowerActive},
		{"Leader", model.NodeStatusLeaderActive},
		{"Unavailable", model.NodeStatusUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test direct serialization of ClusterInfoRespPayload
			payload := &ClusterInfoRespPayload{
				LeaderID: "test-node",
				Status:   tt.status,
			}

			data, err := msgpack.Marshal(payload)
			require.NoError(t, err)

			var decoded ClusterInfoRespPayload
			err = msgpack.Unmarshal(data, &decoded)
			require.NoError(t, err)

			assert.Equal(t, tt.status, decoded.Status)
		})
	}
}

// TestClusterRequest_AllTypes
func TestClusterRequest_AllTypes(t *testing.T) {
	types := []ClusterRequestType{
		ReqConnectionHeartbeat,
		ReqPeersListQuery,
		ReqUpdatePeersList,
		ReqAddMissingPeers,
		ReqRaftMessage,
		ReqClusterInfo,
	}

	for _, rt := range types {
		t.Run(rt.String(), func(t *testing.T) {
			var req ClusterRequest
			switch rt {
			case ReqConnectionHeartbeat:
				req = ClusterRequest{Type: rt, Heartbeat: &HeartbeatPayload{Timestamp: 123}}
			case ReqPeersListQuery:
				// PeersListQuery should have nil payload
				req = ClusterRequest{Type: rt}
			case ReqUpdatePeersList:
				req = ClusterRequest{
					Type: rt,
					UpdatePeers: &UpdatePeersPayload{
						Peers: []model.PeerInfo{
							{
								NodeID: "peer1",
								IP:     "192.168.1.1:8080",
								Identity: model.TLSIdentity{
									Kind:  model.IdentityDNS,
									Value: "peer1.example.com",
								},
							},
						},
					},
				}
			case ReqAddMissingPeers:
				req = ClusterRequest{
					Type: rt,
					AddMissing: &AddMissingPayload{
						Peers: []model.PeerInfo{
							{
								NodeID: "peer1",
								IP:     "192.168.1.1:8080",
								Identity: model.TLSIdentity{
									Kind:  model.IdentityDNS,
									Value: "peer1.example.com",
								},
							},
						},
					},
				}
			case ReqRaftMessage:
				req = ClusterRequest{Type: rt, RaftMessage: &RaftMessagePayload{Data: []byte("test")}}
			case ReqClusterInfo:
				req = ClusterRequest{Type: rt, ClusterInfo: &ClusterInfoPayload{}}
			}

			data, err := req.MarshalMsgpack()
			require.NoError(t, err)

			var decoded ClusterRequest
			err = decoded.UnmarshalMsgpack(data)
			require.NoError(t, err)

			assert.Equal(t, req, decoded)
		})
	}
}
