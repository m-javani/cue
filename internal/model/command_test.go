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

package model

import (
	"bytes"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// ... (keep all existing tests from previous file)

// ============ NEW TESTS FOR BETTER COVERAGE ============

func TestCommandType_String(t *testing.T) {
	tests := []struct {
		name string
		ct   CommandType
		want string
	}{
		{
			name: "CmdUpdatePeersList",
			ct:   CmdUpdatePeersList,
			want: "CmdUpdatePeersList",
		},
		{
			name: "CmdAddNode",
			ct:   CmdAddNode,
			want: "CmdAddNode",
		},
		{
			name: "CmdRemoveNode",
			ct:   CmdRemoveNode,
			want: "CmdRemoveNode",
		},
		{
			name: "CmdTransferLeader",
			ct:   CmdTransferLeader,
			want: "CmdTransferLeader",
		},
		{
			name: "CmdAddJob",
			ct:   CmdAddJobs,
			want: "CmdAddJob",
		},
		{
			name: "CmdDone",
			ct:   CmdDone,
			want: "CmdDone",
		},
		{
			name: "CmdDrop",
			ct:   CmdDrop,
			want: "CmdDrop",
		},
		{
			name: "Unknown",
			ct:   CommandType(99),
			want: "Unknown(99)",
		},
		{
			name: "Unknown with max uint8",
			ct:   CommandType(255),
			want: "Unknown(255)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ct.String(); got != tt.want {
				t.Errorf("CommandType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCommandConstants(t *testing.T) {
	// Verify iota values
	if CmdUpdatePeersList != 0 {
		t.Errorf("CmdUpdatePeersList = %v, want 0", CmdUpdatePeersList)
	}
	if CmdAddNode != 1 {
		t.Errorf("CmdAddNode = %v, want 1", CmdAddNode)
	}
	if CmdRemoveNode != 2 {
		t.Errorf("CmdRemoveNode = %v, want 2", CmdRemoveNode)
	}
	if CmdTransferLeader != 3 {
		t.Errorf("CmdTransferLeader = %v, want 3", CmdTransferLeader)
	}
	if CmdAddJobs != 4 {
		t.Errorf("CmdAddJob = %v, want 4", CmdAddJobs)
	}
	if CmdDone != 5 {
		t.Errorf("CmdDone = %v, want 5", CmdDone)
	}
	if CmdDrop != 6 {
		t.Errorf("CmdDrop = %v, want 6", CmdDrop)
	}
}

func TestJobFields(t *testing.T) {
	job := Job{
		ID:        "job-123",
		Topic:     "test-topic",
		Data:      []byte("test-data"),
		Done:      true,
		CreatedAt: 1234567890,
	}

	if job.ID != "job-123" {
		t.Errorf("Job.ID = %v, want job-123", job.ID)
	}
	if job.Topic != "test-topic" {
		t.Errorf("Job.Topic = %v, want test-topic", job.Topic)
	}
	if !bytes.Equal(job.Data, []byte("test-data")) {
		t.Errorf("Job.Data = %v, want test-data", job.Data)
	}
	if !job.Done {
		t.Errorf("Job.Done = %v, want true", job.Done)
	}
	if job.CreatedAt != 1234567890 {
		t.Errorf("Job.CreatedAt = %v, want 1234567890", job.CreatedAt)
	}
}

func TestRespInfo(t *testing.T) {
	ch := make(chan ToProducerResponse, 1)
	respInfo := &RespInfo{
		RequestID: "req-123",
		RespCh:    ch,
	}

	if respInfo.RequestID != "req-123" {
		t.Errorf("RespInfo.RequestID = %v, want req-123", respInfo.RequestID)
	}
	if respInfo.RespCh == nil {
		t.Error("RespInfo.RespCh should not be nil")
	}
}

func TestCommandMarshalMsgpack_ErrorCases(t *testing.T) {
	tests := []struct {
		name    string
		command Command
		wantErr bool
		errMsg  string
	}{
		{
			name: "Unknown command type",
			command: Command{
				Type:      CommandType(99),
				ProposeID: 1,
			},
			wantErr: true,
			errMsg:  "unknown command type: 99",
		},
		{
			name: "CmdUpdatePeersList with nil payload",
			command: Command{
				Type:      CmdUpdatePeersList,
				ProposeID: 1,
				Peers:     nil,
			},
			wantErr: true,
			errMsg:  "peers payload missing",
		},
		{
			name: "CmdAddNode with nil payload",
			command: Command{
				Type:      CmdAddNode,
				ProposeID: 1,
				AddNode:   nil,
			},
			wantErr: true,
			errMsg:  "add_node payload missing",
		},
		{
			name: "CmdRemoveNode with nil payload",
			command: Command{
				Type:       CmdRemoveNode,
				ProposeID:  1,
				RemoveNode: nil,
			},
			wantErr: true,
			errMsg:  "remove_node payload missing",
		},
		{
			name: "CmdTransferLeader with nil payload",
			command: Command{
				Type:      CmdTransferLeader,
				ProposeID: 1,
				Transfer:  nil,
			},
			wantErr: true,
			errMsg:  "transfer_leader payload missing",
		},
		{
			name: "CmdAddJob with nil payload",
			command: Command{
				Type:      CmdAddJobs,
				ProposeID: 1,
				AddJobs:   nil,
			},
			wantErr: true,
			errMsg:  "add_job payload missing",
		},
		{
			name: "CmdDone with nil payload",
			command: Command{
				Type:      CmdDone,
				ProposeID: 1,
				Done:      nil,
			},
			wantErr: true,
			errMsg:  "done payload missing",
		},
		{
			name: "CmdDrop with nil payload",
			command: Command{
				Type:      CmdDrop,
				ProposeID: 1,
				Drop:      nil,
			},
			wantErr: true,
			errMsg:  "drop payload missing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.command.MarshalMsgpack()
			if (err != nil) != tt.wantErr {
				t.Errorf("MarshalMsgpack() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err.Error() != tt.errMsg {
				t.Errorf("MarshalMsgpack() error message = %v, want %v", err.Error(), tt.errMsg)
			}
		})
	}
}

func TestCommandUnmarshalMsgpack_ErrorCases(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "Invalid data - not an array",
			data:    []byte(`{"type": 1}`),
			wantErr: true,
		},
		{
			name:    "Invalid data - wrong array size",
			data:    []byte(`[1, 2]`),
			wantErr: true,
		},
		{
			name:    "Empty data",
			data:    []byte{},
			wantErr: true,
		},
		{
			name:    "Invalid type field",
			data:    mustMarshal([3]any{"invalid", uint64(1), []byte{}}),
			wantErr: true,
		},
		{
			name:    "Invalid propose_id field",
			data:    mustMarshal([3]any{uint8(CmdDrop), "invalid", []byte{}}),
			wantErr: true,
		},
		{
			name:    "Invalid payload for CmdDrop",
			data:    mustMarshal([3]any{uint8(CmdDrop), uint64(1), "invalid"}),
			wantErr: true,
		},
		{
			name:    "Invalid payload for CmdDone",
			data:    mustMarshal([3]any{uint8(CmdDone), uint64(1), "invalid"}),
			wantErr: true,
		},
		{
			name:    "Invalid payload for CmdAddJob",
			data:    mustMarshal([3]any{uint8(CmdAddJobs), uint64(1), "invalid"}),
			wantErr: true,
		},
		{
			name:    "Invalid payload for CmdUpdatePeersList",
			data:    mustMarshal([3]any{uint8(CmdUpdatePeersList), uint64(1), "invalid"}),
			wantErr: true,
		},
		{
			name:    "Invalid payload for CmdAddNode",
			data:    mustMarshal([3]any{uint8(CmdAddNode), uint64(1), "invalid"}),
			wantErr: true,
		},
		{
			name:    "Invalid payload for CmdRemoveNode",
			data:    mustMarshal([3]any{uint8(CmdRemoveNode), uint64(1), "invalid"}),
			wantErr: true,
		},
		{
			name:    "Invalid payload for CmdTransferLeader",
			data:    mustMarshal([3]any{uint8(CmdTransferLeader), uint64(1), "invalid"}),
			wantErr: true,
		},
		{
			name:    "Unknown command type",
			data:    mustMarshal([3]any{uint8(99), uint64(1), []byte{}}),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cmd Command
			err := cmd.UnmarshalMsgpack(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalMsgpack() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Helper to marshal data for tests
func mustMarshal(v any) []byte {
	data, err := msgpack.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func TestCommandMarshalUnmarshal_Integration(t *testing.T) {
	// Test that commands with all fields marshal and unmarshal correctly
	commands := []struct {
		name string
		cmd  Command
	}{
		{
			name: "Full CmdUpdatePeersList",
			cmd: Command{
				Type:      CmdUpdatePeersList,
				ProposeID: 100,
				Peers: &PeersListPayload{
					Peers: []PeerInfo{
						{
							NodeID: "node-a",
							Host:   "192.168.1.1:8080",
							Identity: TLSIdentity{
								Kind:  IdentityDNS,
								Value: "node-a.example.com",
							},
						},
						{
							NodeID: "node-b",
							Host:   "192.168.1.2:8080",
							Identity: TLSIdentity{
								Kind:  IdentityDNS,
								Value: "node-b.example.com",
							},
						},
						{
							NodeID: "node-c",
							Host:   "192.168.1.3:8080",
							Identity: TLSIdentity{
								Kind:  IdentityDNS,
								Value: "node-c.example.com",
							},
						},
						{
							NodeID: "node-d",
							Host:   "192.168.1.4:8080",
							Identity: TLSIdentity{
								Kind:  IdentityDNS,
								Value: "node-d.example.com",
							},
						},
						{
							NodeID: "node-e",
							Host:   "192.168.1.5:8080",
							Identity: TLSIdentity{
								Kind:  IdentityDNS,
								Value: "node-e.example.com",
							},
						},
					},
				},
			},
		},
		{
			name: "Full CmdAddNode",
			cmd: Command{
				Type:      CmdAddNode,
				ProposeID: 200,
				AddNode: &AddNodePayload{
					NodeID: "new-node-123",
				},
			},
		},
		{
			name: "Full CmdRemoveNode",
			cmd: Command{
				Type:      CmdRemoveNode,
				ProposeID: 300,
				RemoveNode: &RemoveNodePayload{
					NodeID: "old-node-456",
				},
			},
		},
		{
			name: "Full CmdTransferLeader",
			cmd: Command{
				Type:      CmdTransferLeader,
				ProposeID: 400,
				Transfer: &TransferLeaderPayload{
					TargetNodeID: "leader-node-789",
				},
			},
		},
		{
			name: "Full CmdAddJob",
			cmd: Command{
				Type:      CmdAddJobs,
				ProposeID: 500,
				AddJobs: &AddJobsPayload{
					Topic: "production-topic",
					Jobs: []Job{{
						ID:    "job-2026-001",
						Topic: "production-topic",
						Data:  []byte("{\"key\":\"value\",\"timestamp\":1700000000}"),
					}},
				},
			},
		},
		{
			name: "Full CmdDone",
			cmd: Command{
				Type:      CmdDone,
				ProposeID: 600,
				Done: &DonePayload{
					Topic:  "completed-topic",
					JobIDs: []string{"job-001", "job-002", "job-003", "job-004", "job-005"},
				},
			},
		},
		{
			name: "Full CmdDrop",
			cmd: Command{
				Type:      CmdDrop,
				ProposeID: 700,
				Drop: &DropPayload{
					Topic:  "dropped-topic",
					JobIDs: []string{"job-999", "job-998"},
				},
			},
		},
	}

	for _, tt := range commands {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			data, err := tt.cmd.MarshalMsgpack()
			if err != nil {
				t.Fatalf("MarshalMsgpack failed: %v", err)
			}

			// Unmarshal
			var restored Command
			if err := restored.UnmarshalMsgpack(data); err != nil {
				t.Fatalf("UnmarshalMsgpack failed: %v", err)
			}

			// Verify
			verifyCommand(t, tt.cmd, restored)

			// Verify RespInfo is nil (should be skipped in marshaling)
			if restored.RespInfo != nil {
				t.Errorf("RespInfo should be nil after unmarshal, got %v", restored.RespInfo)
			}
		})
	}
}

func TestCommandUnmarshalMsgpack_InvalidArrayStructure(t *testing.T) {
	// Test various invalid array structures
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "First element missing",
			data: mustMarshal([]any{uint64(1), []byte{}}),
		},
		{
			name: "Second element missing",
			data: mustMarshal([]any{uint8(1)}),
		},
		{
			name: "Nil array",
			data: mustMarshal(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cmd Command
			err := cmd.UnmarshalMsgpack(tt.data)
			if err == nil {
				t.Errorf("Expected error for invalid data, got nil")
			}
		})
	}
}

func TestCommand_MsgpackCompatibility(t *testing.T) {
	// Test that the custom marshaler works with standard msgpack
	original := Command{
		Type:      CmdAddJobs,
		ProposeID: 888,
		AddJobs: &AddJobsPayload{
			Topic: "compat-topic",
			Jobs: []Job{{
				ID:    "compat-job",
				Topic: "compat-topic",
				Data:  []byte("compat-data"),
			}},
		},
	}

	// Marshal using standard msgpack (which will call our custom marshaler)
	data, err := msgpack.Marshal(&original)
	if err != nil {
		t.Fatalf("msgpack.Marshal failed: %v", err)
	}

	// Unmarshal using standard msgpack (which will call our custom unmarshaler)
	var restored Command
	if err := msgpack.Unmarshal(data, &restored); err != nil {
		t.Fatalf("msgpack.Unmarshal failed: %v", err)
	}

	// Verify
	verifyCommand(t, original, restored)
}

func TestCommandMarshalMsgpack_WithRespInfo(t *testing.T) {
	// RespInfo should be ignored in marshaling
	ch := make(chan ToProducerResponse, 1)
	cmd := Command{
		Type:      CmdDone,
		ProposeID: 123,
		Done: &DonePayload{
			Topic:  "test",
			JobIDs: []string{"job1"},
		},
		RespInfo: &RespInfo{
			RequestID: "req-123",
			RespCh:    ch,
		},
	}

	data, err := cmd.MarshalMsgpack()
	if err != nil {
		t.Fatalf("MarshalMsgpack failed: %v", err)
	}

	var restored Command
	if err := restored.UnmarshalMsgpack(data); err != nil {
		t.Fatalf("UnmarshalMsgpack failed: %v", err)
	}

	// RespInfo should be nil after unmarshal
	if restored.RespInfo != nil {
		t.Errorf("RespInfo should be nil after unmarshal, got %v", restored.RespInfo)
	}

	// But other fields should match
	if restored.Type != cmd.Type {
		t.Errorf("Type mismatch after marshal/unmarshal")
	}
	if restored.ProposeID != cmd.ProposeID {
		t.Errorf("ProposeID mismatch after marshal/unmarshal")
	}
	if restored.Done.Topic != cmd.Done.Topic {
		t.Errorf("Topic mismatch after marshal/unmarshal")
	}
}

func TestCommandMarshalUnmarshal_Concurrent(t *testing.T) {
	// Test concurrent marshaling/unmarshaling
	cmd := Command{
		Type:      CmdDrop,
		ProposeID: 999,
		Drop: &DropPayload{
			Topic:  "concurrent-topic",
			JobIDs: []string{"job-a", "job-b", "job-c"},
		},
	}

	// Marshal once
	data, err := cmd.MarshalMsgpack()
	if err != nil {
		t.Fatalf("MarshalMsgpack failed: %v", err)
	}

	// Unmarshal concurrently
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func() {
			defer func() { done <- true }()
			var restored Command
			if err := restored.UnmarshalMsgpack(data); err != nil {
				t.Errorf("UnmarshalMsgpack failed: %v", err)
			}
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 100; i++ {
		<-done
	}
}

func TestCommandMarshalMsgpack_NilCommand(t *testing.T) {
	// Test marshaling a nil pointer
	var cmd *Command
	_, err := msgpack.Marshal(cmd)
	if err != nil {
		t.Errorf("Marshaling nil command should not error, got %v", err)
	}
}

func BenchmarkCommandMarshalMsgpack(b *testing.B) {
	cmd := Command{
		Type:      CmdAddJobs,
		ProposeID: 12345,
		AddJobs: &AddJobsPayload{
			Topic: "bench-topic",
			Jobs: []Job{{
				ID:    "bench-job",
				Topic: "bench-topic",
				Data:  []byte("bench-data"),
			}},
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cmd.MarshalMsgpack()
	}
}

func BenchmarkCommandUnmarshalMsgpack(b *testing.B) {
	cmd := Command{
		Type:      CmdAddJobs,
		ProposeID: 12345,
		AddJobs: &AddJobsPayload{
			Topic: "bench-topic",
			Jobs: []Job{{
				ID:    "bench-job",
				Topic: "bench-topic",
				Data:  []byte("bench-data"),
			}},
		},
	}
	data, _ := cmd.MarshalMsgpack()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var restored Command
		_ = restored.UnmarshalMsgpack(data)
	}
}

func BenchmarkCommandType_String(b *testing.B) {
	ct := CmdAddJobs
	for i := 0; i < b.N; i++ {
		_ = ct.String()
	}
}

func BenchmarkJobFields(b *testing.B) {
	job := Job{
		ID:        "bench-job",
		Topic:     "bench-topic",
		Data:      []byte("bench-data"),
		Done:      true,
		CreatedAt: 1234567890,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = job.ID
		_ = job.Topic
		_ = job.Data
		_ = job.Done
		_ = job.CreatedAt
	}
}

// Helper function to verify two commands are equal
func verifyCommand(t *testing.T, expected, actual Command) {
	t.Helper()

	if expected.Type != actual.Type {
		t.Errorf("Type mismatch: got %v, want %v", actual.Type, expected.Type)
	}

	if expected.ProposeID != actual.ProposeID {
		t.Errorf("ProposeID mismatch: got %d, want %d", actual.ProposeID, expected.ProposeID)
	}

	// Verify the correct payload is set based on type
	switch expected.Type {
	case CmdUpdatePeersList:
		if actual.Peers == nil {
			t.Error("Peers is nil")
		} else if !equalPeerInfos(expected.Peers.Peers, actual.Peers.Peers) {
			t.Errorf("Peers mismatch: got %v, want %v", actual.Peers.Peers, expected.Peers.Peers)
		}

	case CmdAddNode:
		if actual.AddNode == nil {
			t.Error("AddNode is nil")
		} else if expected.AddNode.NodeID != actual.AddNode.NodeID {
			t.Errorf("NodeID mismatch: got %s, want %s", actual.AddNode.NodeID, expected.AddNode.NodeID)
		}

	case CmdRemoveNode:
		if actual.RemoveNode == nil {
			t.Error("RemoveNode is nil")
		} else if expected.RemoveNode.NodeID != actual.RemoveNode.NodeID {
			t.Errorf("NodeID mismatch: got %s, want %s", actual.RemoveNode.NodeID, expected.RemoveNode.NodeID)
		}

	case CmdTransferLeader:
		if actual.Transfer == nil {
			t.Error("Transfer is nil")
		} else if expected.Transfer.TargetNodeID != actual.Transfer.TargetNodeID {
			t.Errorf("TargetNodeID mismatch: got %s, want %s", actual.Transfer.TargetNodeID, expected.Transfer.TargetNodeID)
		}

	case CmdAddJobs:
		if actual.AddJobs == nil {
			t.Error("AddJob is nil")
		} else {
			expectedJob := expected.AddJobs.Jobs[0]
			actualJob := actual.AddJobs.Jobs[0]
			if expectedJob.ID != actualJob.ID {
				t.Errorf("Job.ID mismatch: got %s, want %s", actualJob.ID, expectedJob.ID)
			}
			if expectedJob.Topic != actualJob.Topic {
				t.Errorf("Job.Topic mismatch: got %s, want %s", actualJob.Topic, expectedJob.Topic)
			}
			if !bytes.Equal(expectedJob.Data, actualJob.Data) {
				t.Errorf("Job.Data mismatch: got %v, want %v", actualJob.Data, expectedJob.Data)
			}
			// Done field should be false by default (not marshaled)
			if actualJob.Done != false {
				t.Errorf("Job.Done should be false after unmarshal, got %v", actualJob.Done)
			}
		}

	case CmdDone:
		if actual.Done == nil {
			t.Error("Done is nil")
		} else {
			if expected.Done.Topic != actual.Done.Topic {
				t.Errorf("Topic mismatch: got %s, want %s", actual.Done.Topic, expected.Done.Topic)
			}
			if !equalSlices(expected.Done.JobIDs, actual.Done.JobIDs) {
				t.Errorf("JobIDs mismatch: got %v, want %v", actual.Done.JobIDs, expected.Done.JobIDs)
			}
		}

	case CmdDrop:
		if actual.Drop == nil {
			t.Error("Drop is nil")
		} else {
			if expected.Drop.Topic != actual.Drop.Topic {
				t.Errorf("Topic mismatch: got %s, want %s", actual.Drop.Topic, expected.Drop.Topic)
			}
			if !equalSlices(expected.Drop.JobIDs, actual.Drop.JobIDs) {
				t.Errorf("JobIDs mismatch: got %v, want %v", actual.Drop.JobIDs, expected.Drop.JobIDs)
			}
		}

	default:
		t.Errorf("Unknown command type: %d", expected.Type)
	}
}

// Helper function to compare PeerInfo slices
func equalPeerInfos(a, b []PeerInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].NodeID != b[i].NodeID {
			return false
		}
		if a[i].Host != b[i].Host {
			return false
		}
		if a[i].Identity.Kind != b[i].Identity.Kind {
			return false
		}
		if a[i].Identity.Value != b[i].Identity.Value {
			return false
		}
	}
	return true
}

// Helper function to compare string slices
func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
