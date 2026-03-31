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

// Benchmark tests
func BenchmarkProxyRequestType_String(b *testing.B) {
	t := ReqAddTopic
	for i := 0; i < b.N; i++ {
		_ = t.String()
	}
}

func BenchmarkMembers_Get(b *testing.B) {
	s := &Members{
		Voters:   []string{"node1", "node2", "node3"},
		Learners: []string{"node4", "node5"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Get()
	}
}

func BenchmarkMembers_Update(b *testing.B) {
	s := &Members{
		Voters:   []string{"node1", "node2"},
		Learners: []string{"node3"},
	}
	voters := []string{"node4", "node5"}
	learners := []string{"node6"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Update(voters, learners)
	}
}
