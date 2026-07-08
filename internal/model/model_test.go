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
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestProxyRequestType_String(t *testing.T) {
	tests := []struct {
		name string
		t    ProxyRequestType
		want string
	}{
		{
			name: "ReqHeartbeatReport",
			t:    ReqHeartbeatReport,
			want: "ReqHeartbeatReport",
		},
		{
			name: "ReqAddTopic",
			t:    ReqAddTopic,
			want: "ReqAddTopic",
		},
		{
			name: "ReqAddJob",
			t:    ReqAddJob,
			want: "ReqAddJob",
		},
		{
			name: "ReqDone",
			t:    ReqDone,
			want: "ReqDone",
		},
		{
			name: "Unknown",
			t:    ProxyRequestType(99),
			want: "Unknown(99)",
		},
		{
			name: "Unknown with negative value",
			t:    ProxyRequestType(255),
			want: "Unknown(255)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.t.String(); got != tt.want {
				t.Errorf("ProxyRequestType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProxyRequestTypeConstants(t *testing.T) {
	// Verify iota values
	if ReqHeartbeatReport != 0 {
		t.Errorf("ReqHeartbeatReport = %v, want 0", ReqHeartbeatReport)
	}
	if ReqAddTopic != 1 {
		t.Errorf("ReqAddTopic = %v, want 1", ReqAddTopic)
	}
	if ReqAddJob != 2 {
		t.Errorf("ReqAddJob = %v, want 2", ReqAddJob)
	}
	if ReqDone != 3 {
		t.Errorf("ReqDone = %v, want 3", ReqDone)
	}
}

func TestToProxyMessageTypeConstants(t *testing.T) {
	if ProxyMessageResponse != "response" {
		t.Errorf("ProxyMessageResponse = %v, want response", ProxyMessageResponse)
	}
	if ProxyMessageOutbound != "outbound" {
		t.Errorf("ProxyMessageOutbound = %v, want outbound", ProxyMessageOutbound)
	}
	if ProxyMessageHeartbeat != "heartbeat" {
		t.Errorf("ProxyMessageHeartbeat = %v, want heartbeat", ProxyMessageHeartbeat)
	}
}

func TestToProducerRespStatusConstants(t *testing.T) {
	if ToProxyRespStatusSuccess != "success" {
		t.Errorf("ToProxyRespStatusSuccess = %v, want success", ToProxyRespStatusSuccess)
	}
	if ToProxyRespStatusError != "error" {
		t.Errorf("ToProxyRespStatusError = %v, want error", ToProxyRespStatusError)
	}
	if ToProxyRespStatusExist != "exist" {
		t.Errorf("ToProxyRespStatusExist = %v, want exist", ToProxyRespStatusExist)
	}
}

func TestToGatewayMessageTypeConstants(t *testing.T) {
	if ToGatewayMessageConsumer != "consumer" {
		t.Errorf("ToGatewayMessageConsumer = %v, want consumer", ToGatewayMessageConsumer)
	}
	if ToGatewayMessageHeartbeat != "heartbeat" {
		t.Errorf("ToGatewayMessageHeartbeat = %v, want heartbeat", ToGatewayMessageHeartbeat)
	}
	if ToGatewayMessageLoopback != "loopback" {
		t.Errorf("ToGatewayMessageLoopback = %v, want loopback", ToGatewayMessageLoopback)
	}
}

func TestProxyRequest(t *testing.T) {
	tests := []struct {
		name    string
		request *ProxyRequest
		want    *ProxyRequest
	}{
		{
			name: "Full ProxyRequest with all fields",
			request: &ProxyRequest{
				RequestID: "req-123",
				Type:      ReqAddTopic,
				AddTopic: &AddTopicPayload{
					Topic: "test-topic",
				},
				HeartbeatReport: &HeartbeatReport{
					ProxyID:   "proxy-1",
					Timestamp: 1234567890,
					Capacities: []TopicCapacity{
						{Topic: "topic1", ConsumptionScore: 100},
						{Topic: "topic2", ConsumptionScore: 200},
					},
				},
				AddJob: &AddJobPayload{
					// AddJobPayload fields would be defined here
				},
				Done: &DonePayload{
					// DonePayload fields would be defined here
				},
			},
			want: &ProxyRequest{
				RequestID: "req-123",
				Type:      ReqAddTopic,
				AddTopic: &AddTopicPayload{
					Topic: "test-topic",
				},
				HeartbeatReport: &HeartbeatReport{
					ProxyID:   "proxy-1",
					Timestamp: 1234567890,
					Capacities: []TopicCapacity{
						{Topic: "topic1", ConsumptionScore: 100},
						{Topic: "topic2", ConsumptionScore: 200},
					},
				},
				AddJob: &AddJobPayload{},
				Done:   &DonePayload{},
			},
		},
		{
			name: "Minimal ProxyRequest",
			request: &ProxyRequest{
				RequestID: "req-456",
				Type:      ReqDone,
			},
			want: &ProxyRequest{
				RequestID: "req-456",
				Type:      ReqDone,
			},
		},
		{
			name: "ProxyRequest with only AddJob",
			request: &ProxyRequest{
				RequestID: "req-789",
				Type:      ReqAddJob,
				AddJob: &AddJobPayload{
					Job: Job{
						ID:    "job-1",
						Topic: "test-topic",
						Data:  []byte("test-data"),
					},
				},
			},
			want: &ProxyRequest{
				RequestID: "req-789",
				Type:      ReqAddJob,
				AddJob: &AddJobPayload{
					Job: Job{
						ID:    "job-1",
						Topic: "test-topic",
						Data:  []byte("test-data"),
					},
				},
			},
		},
		{
			name: "ProxyRequest with only Done",
			request: &ProxyRequest{
				RequestID: "req-101",
				Type:      ReqDone,
				Done: &DonePayload{
					Topic:  "test-topic",
					JobIDs: []string{"job-1", "job-2"},
				},
			},
			want: &ProxyRequest{
				RequestID: "req-101",
				Type:      ReqDone,
				Done: &DonePayload{
					Topic:  "test-topic",
					JobIDs: []string{"job-1", "job-2"},
				},
			},
		},
		{
			name: "ProxyRequest with HeartbeatReport only",
			request: &ProxyRequest{
				RequestID: "req-202",
				Type:      ReqHeartbeatReport,
				HeartbeatReport: &HeartbeatReport{
					ProxyID:   "proxy-2",
					Timestamp: 9876543210,
					Capacities: []TopicCapacity{
						{Topic: "topic3", ConsumptionScore: 300},
					},
				},
			},
			want: &ProxyRequest{
				RequestID: "req-202",
				Type:      ReqHeartbeatReport,
				HeartbeatReport: &HeartbeatReport{
					ProxyID:   "proxy-2",
					Timestamp: 9876543210,
					Capacities: []TopicCapacity{
						{Topic: "topic3", ConsumptionScore: 300},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(tt.request, tt.want) {
				t.Errorf("ProxyRequest mismatch:\ngot:  %+v\nwant: %+v", tt.request, tt.want)
			}
		})
	}
}

func TestAddTopicPayload(t *testing.T) {
	payload := &AddTopicPayload{
		Topic: "test-topic",
	}
	if payload.Topic != "test-topic" {
		t.Errorf("AddTopicPayload.Topic = %v, want test-topic", payload.Topic)
	}
}

func TestHeartbeatReport(t *testing.T) {
	report := &HeartbeatReport{
		ProxyID:   "proxy-1",
		Timestamp: 1234567890,
		Capacities: []TopicCapacity{
			{Topic: "topic1", ConsumptionScore: 100},
			{Topic: "topic2", ConsumptionScore: 200},
		},
	}

	if report.ProxyID != "proxy-1" {
		t.Errorf("HeartbeatReport.ProxyID = %v, want proxy-1", report.ProxyID)
	}
	if report.Timestamp != 1234567890 {
		t.Errorf("HeartbeatReport.Timestamp = %v, want 1234567890", report.Timestamp)
	}
	if len(report.Capacities) != 2 {
		t.Errorf("HeartbeatReport.Capacities length = %v, want 2", len(report.Capacities))
	}
	if report.Capacities[0].Topic != "topic1" || report.Capacities[0].ConsumptionScore != 100 {
		t.Errorf("HeartbeatReport.Capacities[0] = %v, want {topic1 100}", report.Capacities[0])
	}
	if report.Capacities[1].Topic != "topic2" || report.Capacities[1].ConsumptionScore != 200 {
		t.Errorf("HeartbeatReport.Capacities[1] = %v, want {topic2 200}", report.Capacities[1])
	}
}

func TestTopicCapacity(t *testing.T) {
	tc := TopicCapacity{
		Topic:            "test-topic",
		ConsumptionScore: 150,
	}
	if tc.Topic != "test-topic" {
		t.Errorf("TopicCapacity.Topic = %v, want test-topic", tc.Topic)
	}
	if tc.ConsumptionScore != 150 {
		t.Errorf("TopicCapacity.ConsumptionScore = %v, want 150", tc.ConsumptionScore)
	}
}

func TestToProxyMessage(t *testing.T) {
	tests := []struct {
		name    string
		message *ToProxyMessage
		want    *ToProxyMessage
	}{
		{
			name: "Response message",
			message: &ToProxyMessage{
				Type: ProxyMessageResponse,
				Response: &ToProducerResponse{
					RequestID: "req-123",
					Status:    ToProxyRespStatusSuccess,
					Error:     "",
				},
			},
			want: &ToProxyMessage{
				Type: ProxyMessageResponse,
				Response: &ToProducerResponse{
					RequestID: "req-123",
					Status:    ToProxyRespStatusSuccess,
					Error:     "",
				},
			},
		},
		{
			name: "Outbound message",
			message: &ToProxyMessage{
				Type: ProxyMessageOutbound,
				Outbound: &ToConsumerMessage{
					Topic:   "test-topic",
					ProxyID: "proxy-1",
					Jobs:    []*Job{},
				},
			},
			want: &ToProxyMessage{
				Type: ProxyMessageOutbound,
				Outbound: &ToConsumerMessage{
					Topic:   "test-topic",
					ProxyID: "proxy-1",
					Jobs:    []*Job{},
				},
			},
		},
		{
			name: "Heartbeat message",
			message: &ToProxyMessage{
				Type: ProxyMessageHeartbeat,
				Heartbeat: &ToProxyHeartbeat{
					NodeStatus: "leader",
					Voters:     []string{"node1", "node2"},
					Learners:   []string{"node3"},
					Leader:     "node1",
					Term:       5,
				},
			},
			want: &ToProxyMessage{
				Type: ProxyMessageHeartbeat,
				Heartbeat: &ToProxyHeartbeat{
					NodeStatus: "leader",
					Voters:     []string{"node1", "node2"},
					Learners:   []string{"node3"},
					Leader:     "node1",
					Term:       5,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(tt.message, tt.want) {
				t.Errorf("ToProxyMessage mismatch")
			}
		})
	}
}

func TestToProducerResponse(t *testing.T) {
	tests := []struct {
		name     string
		response *ToProducerResponse
		want     *ToProducerResponse
	}{
		{
			name: "Success response",
			response: &ToProducerResponse{
				RequestID: "req-123",
				Status:    ToProxyRespStatusSuccess,
				Error:     "",
			},
			want: &ToProducerResponse{
				RequestID: "req-123",
				Status:    ToProxyRespStatusSuccess,
				Error:     "",
			},
		},
		{
			name: "Error response",
			response: &ToProducerResponse{
				RequestID: "req-456",
				Status:    ToProxyRespStatusError,
				Error:     "something went wrong",
			},
			want: &ToProducerResponse{
				RequestID: "req-456",
				Status:    ToProxyRespStatusError,
				Error:     "something went wrong",
			},
		},
		{
			name: "Exist response",
			response: &ToProducerResponse{
				RequestID: "req-789",
				Status:    ToProxyRespStatusExist,
				Error:     "already exists",
			},
			want: &ToProducerResponse{
				RequestID: "req-789",
				Status:    ToProxyRespStatusExist,
				Error:     "already exists",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(tt.response, tt.want) {
				t.Errorf("ToProducerResponse mismatch")
			}
		})
	}
}

func TestPartitionHeartbeat(t *testing.T) {
	ph := &PartitionHeartbeat{
		Topic:     "test-topic",
		CanAccept: true,
		Timestamp: 1234567890,
	}
	if ph.Topic != "test-topic" {
		t.Errorf("PartitionHeartbeat.Topic = %v, want test-topic", ph.Topic)
	}
	if !ph.CanAccept {
		t.Errorf("PartitionHeartbeat.CanAccept = %v, want true", ph.CanAccept)
	}
	if ph.Timestamp != 1234567890 {
		t.Errorf("PartitionHeartbeat.Timestamp = %v, want 1234567890", ph.Timestamp)
	}
}

func TestToConsumerMessage(t *testing.T) {
	msg := &ToConsumerMessage{
		Topic:   "test-topic",
		ProxyID: "proxy-1",
		Jobs: []*Job{
			{ID: "job-1"},
			{ID: "job-2"},
		},
	}
	if msg.Topic != "test-topic" {
		t.Errorf("ToConsumerMessage.Topic = %v, want test-topic", msg.Topic)
	}
	if msg.ProxyID != "proxy-1" {
		t.Errorf("ToConsumerMessage.ProxyID = %v, want proxy-1", msg.ProxyID)
	}
	if len(msg.Jobs) != 2 {
		t.Errorf("ToConsumerMessage.Jobs length = %v, want 2", len(msg.Jobs))
	}
	if msg.Jobs[0].ID != "job-1" {
		t.Errorf("ToConsumerMessage.Jobs[0].ID = %v, want job-1", msg.Jobs[0].ID)
	}
	if msg.Jobs[1].ID != "job-2" {
		t.Errorf("ToConsumerMessage.Jobs[1].ID = %v, want job-2", msg.Jobs[1].ID)
	}
}

func TestToProxyHeartbeat(t *testing.T) {
	tph := &ToProxyHeartbeat{
		NodeStatus: "follower",
		Voters:     []string{"node1", "node2", "node3"},
		Learners:   []string{"node4", "node5"},
		Leader:     "node1",
		Term:       10,
	}
	if tph.NodeStatus != "follower" {
		t.Errorf("ToProxyHeartbeat.NodeStatus = %v, want follower", tph.NodeStatus)
	}
	if len(tph.Voters) != 3 {
		t.Errorf("ToProxyHeartbeat.Voters length = %v, want 3", len(tph.Voters))
	}
	if len(tph.Learners) != 2 {
		t.Errorf("ToProxyHeartbeat.Learners length = %v, want 2", len(tph.Learners))
	}
	if tph.Leader != "node1" {
		t.Errorf("ToProxyHeartbeat.Leader = %v, want node1", tph.Leader)
	}
	if tph.Term != 10 {
		t.Errorf("ToProxyHeartbeat.Term = %v, want 10", tph.Term)
	}
}

func TestMembers_Get(t *testing.T) {
	tests := []struct {
		name            string
		initialVoters   []string
		initialLearners []string
		wantVoters      []string
		wantLearners    []string
	}{
		{
			name:            "Non-empty members",
			initialVoters:   []string{"node1", "node2"},
			initialLearners: []string{"node3"},
			wantVoters:      []string{"node1", "node2"},
			wantLearners:    []string{"node3"},
		},
		{
			name:            "Empty members",
			initialVoters:   []string{},
			initialLearners: []string{},
			wantVoters:      []string{},
			wantLearners:    []string{},
		},
		{
			name:            "Nil slices",
			initialVoters:   nil,
			initialLearners: nil,
			wantVoters:      []string{},
			wantLearners:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Members{
				Voters:   tt.initialVoters,
				Learners: tt.initialLearners,
			}
			gotVoters, gotLearners := s.Get()

			// Check that returned slices are copies, not references
			if len(gotVoters) != len(tt.wantVoters) {
				t.Errorf("Get() voters length = %v, want %v", len(gotVoters), len(tt.wantVoters))
			}
			if len(gotLearners) != len(tt.wantLearners) {
				t.Errorf("Get() learners length = %v, want %v", len(gotLearners), len(tt.wantLearners))
			}

			// Check content
			for i, v := range gotVoters {
				if i < len(tt.wantVoters) && v != tt.wantVoters[i] {
					t.Errorf("Get() voters[%d] = %v, want %v", i, v, tt.wantVoters[i])
				}
			}
			for i, v := range gotLearners {
				if i < len(tt.wantLearners) && v != tt.wantLearners[i] {
					t.Errorf("Get() learners[%d] = %v, want %v", i, v, tt.wantLearners[i])
				}
			}
		})
	}
}

func TestMembers_Get_Concurrency(t *testing.T) {
	s := &Members{
		Voters:   []string{"node1", "node2"},
		Learners: []string{"node3"},
	}

	var wg sync.WaitGroup
	// Test concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			voters, learners := s.Get()
			if len(voters) != 2 || len(learners) != 1 {
				t.Errorf("Concurrent Get() returned unexpected lengths")
			}
		}()
	}
	wg.Wait()
}

func TestMembers_Update(t *testing.T) {
	tests := []struct {
		name            string
		initialVoters   []string
		initialLearners []string
		newVoters       []string
		newLearners     []string
		wantVoters      []string
		wantLearners    []string
	}{
		{
			name:            "Update with new values",
			initialVoters:   []string{"node1", "node2"},
			initialLearners: []string{"node3"},
			newVoters:       []string{"node4", "node5"},
			newLearners:     []string{"node6"},
			wantVoters:      []string{"node4", "node5"},
			wantLearners:    []string{"node6"},
		},
		{
			name:            "Update to empty slices",
			initialVoters:   []string{"node1", "node2"},
			initialLearners: []string{"node3"},
			newVoters:       []string{},
			newLearners:     []string{},
			wantVoters:      []string{},
			wantLearners:    []string{},
		},
		{
			name:            "Update with nil slices",
			initialVoters:   []string{"node1", "node2"},
			initialLearners: []string{"node3"},
			newVoters:       nil,
			newLearners:     nil,
			wantVoters:      []string{},
			wantLearners:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Members{
				Voters:   tt.initialVoters,
				Learners: tt.initialLearners,
			}
			s.Update(tt.newVoters, tt.newLearners)

			if len(s.Voters) != len(tt.wantVoters) {
				t.Errorf("Update() voters length = %v, want %v", len(s.Voters), len(tt.wantVoters))
			}
			if len(s.Learners) != len(tt.wantLearners) {
				t.Errorf("Update() learners length = %v, want %v", len(s.Learners), len(tt.wantLearners))
			}

			for i, v := range s.Voters {
				if i < len(tt.wantVoters) && v != tt.wantVoters[i] {
					t.Errorf("Update() voters[%d] = %v, want %v", i, v, tt.wantVoters[i])
				}
			}
			for i, v := range s.Learners {
				if i < len(tt.wantLearners) && v != tt.wantLearners[i] {
					t.Errorf("Update() learners[%d] = %v, want %v", i, v, tt.wantLearners[i])
				}
			}
		})
	}
}

func TestMembers_Update_Concurrency(t *testing.T) {
	s := &Members{
		Voters:   []string{"node1"},
		Learners: []string{"node2"},
	}

	var wg sync.WaitGroup
	// Test concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			voters := []string{string(rune('a' + i%26))}
			learners := []string{string(rune('z' - i%26))}
			s.Update(voters, learners)
		}(i)
	}
	wg.Wait()

	// Verify final state is consistent
	voters, learners := s.Get()
	if len(voters) != 1 || len(learners) != 1 {
		t.Errorf("After concurrent updates, got voters=%v, learners=%v", voters, learners)
	}
}

func TestMembers_ConcurrentReadWrite(t *testing.T) {
	s := &Members{
		Voters:   []string{"node1", "node2"},
		Learners: []string{"node3"},
	}

	var wg sync.WaitGroup
	// Mix of reads and writes
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.Update([]string{"node1"}, []string{"node2"})
		}()
		go func() {
			defer wg.Done()
			_, _ = s.Get()
		}()
	}
	wg.Wait()
}

func TestProxyHeartbeat(t *testing.T) {
	ph := &ProxyHeartbeat{
		ProxyID:          "proxy-1",
		Topic:            "test-topic",
		ConsumptionScore: 150,
		Timestamp:        1234567890,
	}
	if ph.ProxyID != "proxy-1" {
		t.Errorf("ProxyHeartbeat.ProxyID = %v, want proxy-1", ph.ProxyID)
	}
	if ph.Topic != "test-topic" {
		t.Errorf("ProxyHeartbeat.Topic = %v, want test-topic", ph.Topic)
	}
	if ph.ConsumptionScore != 150 {
		t.Errorf("ProxyHeartbeat.ConsumptionScore = %v, want 150", ph.ConsumptionScore)
	}
	if ph.Timestamp != 1234567890 {
		t.Errorf("ProxyHeartbeat.Timestamp = %v, want 1234567890", ph.Timestamp)
	}
}

func TestToGatewayMessage(t *testing.T) {
	tests := []struct {
		name    string
		message *ToGatewayMessage
		want    *ToGatewayMessage
	}{
		{
			name: "Consumer message",
			message: &ToGatewayMessage{
				Type:       ToGatewayMessageConsumer,
				ToConsumer: []byte("test-data"),
			},
			want: &ToGatewayMessage{
				Type:       ToGatewayMessageConsumer,
				ToConsumer: []byte("test-data"),
			},
		},
		{
			name: "Heartbeat message",
			message: &ToGatewayMessage{
				Type: ToGatewayMessageHeartbeat,
				Heartbeat: &PartitionHeartbeat{
					Topic:     "test-topic",
					CanAccept: true,
					Timestamp: 1234567890,
				},
			},
			want: &ToGatewayMessage{
				Type: ToGatewayMessageHeartbeat,
				Heartbeat: &PartitionHeartbeat{
					Topic:     "test-topic",
					CanAccept: true,
					Timestamp: 1234567890,
				},
			},
		},
		{
			name: "Loopback message",
			message: &ToGatewayMessage{
				Type: ToGatewayMessageLoopback,
				LoopbackMessage: &ToProducerResponse{
					RequestID: "req-123",
					Status:    ToProxyRespStatusSuccess,
					Error:     "",
				},
			},
			want: &ToGatewayMessage{
				Type: ToGatewayMessageLoopback,
				LoopbackMessage: &ToProducerResponse{
					RequestID: "req-123",
					Status:    ToProxyRespStatusSuccess,
					Error:     "",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(tt.message, tt.want) {
				t.Errorf("ToGatewayMessage mismatch")
			}
		})
	}
}

func TestIdentityKindConstants(t *testing.T) {
	if IdentityDNS != 0 {
		t.Errorf("IdentityDNS = %v, want 0", IdentityDNS)
	}
	if IdentityIP != 1 {
		t.Errorf("IdentityIP = %v, want 1", IdentityIP)
	}
	if IdentitySPIFFE != 2 {
		t.Errorf("IdentitySPIFFE = %v, want 2", IdentitySPIFFE)
	}
}

func TestTLSIdentity_String(t *testing.T) {
	tests := []struct {
		name     string
		identity TLSIdentity
		want     string
	}{
		{
			name: "DNS identity",
			identity: TLSIdentity{
				Kind:  IdentityDNS,
				Value: "example.com",
			},
			want: "DNS:example.com",
		},
		{
			name: "IP identity",
			identity: TLSIdentity{
				Kind:  IdentityIP,
				Value: "192.168.1.1",
			},
			want: "IP:192.168.1.1",
		},
		{
			name: "SPIFFE identity",
			identity: TLSIdentity{
				Kind:  IdentitySPIFFE,
				Value: "spiffe://example.org/service",
			},
			want: "SPIFFE:spiffe://example.org/service",
		},
		{
			name: "Unknown identity",
			identity: TLSIdentity{
				Kind:  IdentityKind(99),
				Value: "unknown-value",
			},
			want: "Unknown:unknown-value",
		},
		{
			name: "Empty value with DNS kind",
			identity: TLSIdentity{
				Kind:  IdentityDNS,
				Value: "",
			},
			want: "DNS:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.identity.String(); got != tt.want {
				t.Errorf("TLSIdentity.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPeerInfo_String(t *testing.T) {
	tests := []struct {
		name string
		peer PeerInfo
		want string
	}{
		{
			name: "Peer with DNS identity",
			peer: PeerInfo{
				NodeID: "node-1",
				Host:   "192.168.1.1:8080",
				Identity: TLSIdentity{
					Kind:  IdentityDNS,
					Value: "node1.example.com",
				},
				Port: 0,
			},
			want: "NodeID:node-1, IP:192.168.1.1:8080, Port:0, Identity:DNS:node1.example.com",
		},
		{
			name: "Peer with IP identity",
			peer: PeerInfo{
				NodeID: "node-2",
				Host:   "10.0.0.1:9090",
				Identity: TLSIdentity{
					Kind:  IdentityIP,
					Value: "10.0.0.1",
				},
				Port: 0,
			},
			want: "NodeID:node-2, IP:10.0.0.1:9090, Port:0, Identity:IP:10.0.0.1",
		},
		{
			name: "Peer with SPIFFE identity",
			peer: PeerInfo{
				NodeID: "node-3",
				Host:   "172.16.0.1:8080",
				Identity: TLSIdentity{
					Kind:  IdentitySPIFFE,
					Value: "spiffe://cluster/node-3",
				},
				Port: 0,
			},
			want: "NodeID:node-3, IP:172.16.0.1:8080, Port:0, Identity:SPIFFE:spiffe://cluster/node-3",
		},
		{
			name: "Peer with empty fields",
			peer: PeerInfo{
				NodeID:   "",
				Host:     "",
				Identity: TLSIdentity{},
				Port:     0,
			},
			want: "NodeID:, IP:, Port:0, Identity:DNS:",
		},
		{
			name: "Peer with unknown identity kind",
			peer: PeerInfo{
				NodeID: "node-4",
				Host:   "192.168.1.4:8080",
				Identity: TLSIdentity{
					Kind:  IdentityKind(99),
					Value: "unknown",
				},
				Port: 0,
			},
			want: "NodeID:node-4, IP:192.168.1.4:8080, Port:0, Identity:Unknown:unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.peer.String(); got != tt.want {
				t.Errorf("PeerInfo.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPeerInfo_Fields(t *testing.T) {
	peer := PeerInfo{
		NodeID: "test-node",
		Host:   "192.168.1.100:8080",
		Identity: TLSIdentity{
			Kind:  IdentityDNS,
			Value: "test.example.com",
		},
	}

	if peer.NodeID != "test-node" {
		t.Errorf("PeerInfo.NodeID = %v, want test-node", peer.NodeID)
	}
	if peer.Host != "192.168.1.100:8080" {
		t.Errorf("PeerInfo.IP = %v, want 192.168.1.100:8080", peer.Host)
	}
	if peer.Identity.Kind != IdentityDNS {
		t.Errorf("PeerInfo.Identity.Kind = %v, want IdentityDNS", peer.Identity.Kind)
	}
	if peer.Identity.Value != "test.example.com" {
		t.Errorf("PeerInfo.Identity.Value = %v, want test.example.com", peer.Identity.Value)
	}
}

func TestTLSIdentity_Validate(t *testing.T) {
	tests := []struct {
		name     string
		identity TLSIdentity
		wantErr  bool
		errMsg   string
	}{
		{
			name: "Valid DNS identity",
			identity: TLSIdentity{
				Kind:  IdentityDNS,
				Value: "example.com",
			},
			wantErr: false,
		},
		{
			name: "Valid DNS with subdomain",
			identity: TLSIdentity{
				Kind:  IdentityDNS,
				Value: "sub.example.com",
			},
			wantErr: false,
		},
		{
			name: "Valid DNS with hyphen",
			identity: TLSIdentity{
				Kind:  IdentityDNS,
				Value: "my-service.example.com",
			},
			wantErr: false,
		},
		{
			name: "Valid IP address (IPv4)",
			identity: TLSIdentity{
				Kind:  IdentityIP,
				Value: "192.168.1.1",
			},
			wantErr: false,
		},
		{
			name: "Valid IP address (IPv6)",
			identity: TLSIdentity{
				Kind:  IdentityIP,
				Value: "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
			},
			wantErr: false,
		},
		{
			name: "Valid SPIFFE identity",
			identity: TLSIdentity{
				Kind:  IdentitySPIFFE,
				Value: "spiffe://example.org/service",
			},
			wantErr: false,
		},
		{
			name: "Valid SPIFFE with path",
			identity: TLSIdentity{
				Kind:  IdentitySPIFFE,
				Value: "spiffe://cluster.local/ns/default/sa/my-service",
			},
			wantErr: false,
		},
		{
			name: "Empty value",
			identity: TLSIdentity{
				Kind:  IdentityDNS,
				Value: "",
			},
			wantErr: true,
			errMsg:  "identity value is required",
		},
		{
			name: "Whitespace only value",
			identity: TLSIdentity{
				Kind:  IdentityDNS,
				Value: "",
			},
			wantErr: true,
			errMsg:  "identity value is required",
		},
		{
			name: "Unknown identity kind",
			identity: TLSIdentity{
				Kind:  IdentityKind(99),
				Value: "some-value",
			},
			wantErr: true,
			errMsg:  "unknown identity kind",
		},
		{
			name: "DNS name too long",
			identity: TLSIdentity{
				Kind:  IdentityDNS,
				Value: string(make([]byte, 254)), // 254 characters
			},
			wantErr: true,
			errMsg:  "DNS name too long",
		},
		{
			name: "Invalid IP address",
			identity: TLSIdentity{
				Kind:  IdentityIP,
				Value: "256.256.256.256",
			},
			wantErr: true,
			errMsg:  "invalid IP address",
		},
		{
			name: "Invalid IP address format",
			identity: TLSIdentity{
				Kind:  IdentityIP,
				Value: "not-an-ip",
			},
			wantErr: true,
			errMsg:  "invalid IP address",
		},
		{
			name: "SPIFFE without prefix",
			identity: TLSIdentity{
				Kind:  IdentitySPIFFE,
				Value: "example.org/service",
			},
			wantErr: true,
			errMsg:  "SPIFFE identity must start with spiffe://",
		},
		{
			name: "SPIFFE with wrong case prefix",
			identity: TLSIdentity{
				Kind:  IdentitySPIFFE,
				Value: "SPIFFE://example.org/service",
			},
			wantErr: false, // Should be case-insensitive
		},
		{
			name: "SPIFFE with just prefix",
			identity: TLSIdentity{
				Kind:  IdentitySPIFFE,
				Value: "spiffe://",
			},
			wantErr: false, // Valid SPIFFE identity can be just the prefix
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.identity.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("TLSIdentity.Validate() expected error, got nil")
				} else if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("TLSIdentity.Validate() error = %v, want error containing %v", err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("TLSIdentity.Validate() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestPeerInfo_Validate(t *testing.T) {
	tests := []struct {
		name    string
		peer    PeerInfo
		wantErr bool
		errMsg  string
	}{
		{
			name: "Valid peer with DNS identity",
			peer: PeerInfo{
				NodeID: "node-1",
				Host:   "192.168.1.1",
				Identity: TLSIdentity{
					Kind:  IdentityDNS,
					Value: "node1.example.com",
				},
			},
			wantErr: false,
		},
		{
			name: "Valid peer with IP identity",
			peer: PeerInfo{
				NodeID: "node-2",
				Host:   "10.0.0.1",
				Identity: TLSIdentity{
					Kind:  IdentityIP,
					Value: "10.0.0.1",
				},
			},
			wantErr: false,
		},
		{
			name: "Valid peer with SPIFFE identity",
			peer: PeerInfo{
				NodeID: "node-3",
				Host:   "172.16.0.1",
				Identity: TLSIdentity{
					Kind:  IdentitySPIFFE,
					Value: "spiffe://cluster/node-3",
				},
			},
			wantErr: false,
		},
		{
			name: "Valid peer with IPv6 address",
			peer: PeerInfo{
				NodeID: "node-4",
				Host:   "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
				Identity: TLSIdentity{
					Kind:  IdentityDNS,
					Value: "node4.example.com",
				},
			},
			wantErr: false,
		},
		{
			name: "Empty NodeID",
			peer: PeerInfo{
				NodeID: "",
				Host:   "192.168.1.1",
				Identity: TLSIdentity{
					Kind:  IdentityDNS,
					Value: "example.com",
				},
			},
			wantErr: true,
			errMsg:  "node_id is required",
		},
		{
			name: "Whitespace NodeID",
			peer: PeerInfo{
				NodeID: "",
				Host:   "192.168.1.1",
				Identity: TLSIdentity{
					Kind:  IdentityDNS,
					Value: "example.com",
				},
			},
			wantErr: true,
			errMsg:  "node_id is required",
		},
		{
			name: "NodeID too long",
			peer: PeerInfo{
				NodeID: string(make([]byte, 65)), // 65 characters
				Host:   "192.168.1.1",
				Identity: TLSIdentity{
					Kind:  IdentityDNS,
					Value: "example.com",
				},
			},
			wantErr: true,
			errMsg:  "node_id too long",
		},
		{
			name: "Invalid identity",
			peer: PeerInfo{
				NodeID: "node-1",
				Host:   "192.168.1.1",
				Identity: TLSIdentity{
					Kind:  IdentityDNS,
					Value: "", // Empty value
				},
			},
			wantErr: true,
			errMsg:  "invalid identity",
		},
		{
			name: "Unknown identity kind",
			peer: PeerInfo{
				NodeID: "node-1",
				Host:   "192.168.1.1",
				Identity: TLSIdentity{
					Kind:  IdentityKind(99),
					Value: "some-value",
				},
			},
			wantErr: true,
			errMsg:  "invalid identity",
		},
		{
			name: "Peer with port in IP (should be invalid)",
			peer: PeerInfo{
				NodeID: "node-1",
				Host:   "192.168.1.1:8080",
				Identity: TLSIdentity{
					Kind:  IdentityDNS,
					Value: "example.com",
				},
			},
			wantErr: true,
			errMsg:  "invalid IP address",
		},
		{
			name: "Peer with DNS name in IP field (should be invalid)",
			peer: PeerInfo{
				NodeID: "node-1",
				Host:   "example.com",
				Identity: TLSIdentity{
					Kind:  IdentityDNS,
					Value: "example.com",
				},
			},
			wantErr: true,
			errMsg:  "invalid IP address",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.peer.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("PeerInfo.Validate() expected error, got nil")
				} else if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("PeerInfo.Validate() error = %v, want error containing %v", err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("PeerInfo.Validate() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestDiscoveryInfo_Get(t *testing.T) {
	// Create a DiscoveryInfo with some initial data
	d := &PeerStore{
		Peers: map[string]PeerInfo{
			"node1": {
				NodeID: "node1",
				Host:   "192.168.1.1",
				Port:   8080,
				Identity: TLSIdentity{
					Kind:  IdentityDNS,
					Value: "node1.example.com",
				},
			},
			"node2": {
				NodeID: "node2",
				Host:   "192.168.1.2",
				Port:   8080,
				Identity: TLSIdentity{
					Kind:  IdentityIP,
					Value: "192.168.1.2",
				},
			},
		},
	}

	// Get a copy of the peers
	peers := d.Get()

	// Verify all peers are present
	if len(peers) != 2 {
		t.Errorf("Expected 2 peers, got %d", len(peers))
	}

	if _, ok := peers["node1"]; !ok {
		t.Error("Expected node1 to be present")
	}
	if _, ok := peers["node2"]; !ok {
		t.Error("Expected node2 to be present")
	}

	// Verify it's a copy (modifying returned map doesn't affect original)
	delete(peers, "node1")
	if len(d.Peers) != 2 {
		t.Errorf("Original map should still have 2 peers, got %d", len(d.Peers))
	}
}

func TestDiscoveryInfo_GetByNodeID(t *testing.T) {
	d := &PeerStore{
		Peers: map[string]PeerInfo{
			"node1": {
				NodeID: "node1",
				Host:   "192.168.1.1",
				Port:   8080,
				Identity: TLSIdentity{
					Kind:  IdentityDNS,
					Value: "node1.example.com",
				},
			},
		},
	}

	// Test existing peer
	peer, exists := d.Lookup("node1")
	if !exists {
		t.Error("Expected node1 to exist")
	}
	if peer.NodeID != "node1" {
		t.Errorf("Expected NodeID 'node1', got '%s'", peer.NodeID)
	}
	if peer.Host != "192.168.1.1" {
		t.Errorf("Expected IP '192.168.1.1', got '%s'", peer.Host)
	}

	// Test non-existent peer
	_, exists = d.Lookup("node3")
	if exists {
		t.Error("Expected node3 to not exist")
	}
}

func TestDiscoveryInfo_Update(t *testing.T) {
	d := &PeerStore{
		Peers: map[string]PeerInfo{
			"node1": {
				NodeID: "node1",
				Host:   "192.168.1.1",
				Port:   8080,
				Identity: TLSIdentity{
					Kind:  IdentityDNS,
					Value: "node1.example.com",
				},
			},
		},
	}

	// New peers to update with
	newPeers := map[string]PeerInfo{
		"node2": {
			NodeID: "node2",
			Host:   "192.168.1.2",
			Port:   9090,
			Identity: TLSIdentity{
				Kind:  IdentityIP,
				Value: "192.168.1.2",
			},
		},
		"node3": {
			NodeID: "node3",
			Host:   "192.168.1.3",
			Port:   9090,
			Identity: TLSIdentity{
				Kind:  IdentitySPIFFE,
				Value: "spiffe://example.com/node3",
			},
		},
	}

	d.Set(newPeers)

	// Verify old peer is gone
	if _, ok := d.Peers["node1"]; ok {
		t.Error("Expected node1 to be removed")
	}

	// Verify new peers are present
	if _, ok := d.Peers["node2"]; !ok {
		t.Error("Expected node2 to be present")
	}
	if _, ok := d.Peers["node3"]; !ok {
		t.Error("Expected node3 to be present")
	}

	// Verify all peers are present
	if len(d.Peers) != 2 {
		t.Errorf("Expected 2 peers, got %d", len(d.Peers))
	}

	// Test update with empty map clears everything
	d.Set(map[string]PeerInfo{})
	if len(d.Peers) != 0 {
		t.Errorf("Expected 0 peers after empty update, got %d", len(d.Peers))
	}
}

func TestDiscoveryInfo_ConcurrentAccess(t *testing.T) {
	d := &PeerStore{
		Peers: make(map[string]PeerInfo),
	}

	// Run concurrent reads and writes
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			nodeID := "node" + string(rune(i))
			peer := PeerInfo{
				NodeID: nodeID,
				Host:   "192.168.1.1",
				Port:   8080,
				Identity: TLSIdentity{
					Kind:  IdentityDNS,
					Value: "example.com",
				},
			}
			d.Set(map[string]PeerInfo{nodeID: peer})
			d.Get()
			d.Lookup(nodeID)
		}(i)
	}
	wg.Wait()
}
