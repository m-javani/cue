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
	"fmt"
	"sync"
)

// ========================
// from proxy to gateway
// ========================

type ProxyRequestType uint8

const (
	ReqHeartbeatReport ProxyRequestType = iota
	ReqAddTopic
	ReqAddJob
	ReqDone
)

func (t ProxyRequestType) String() string {
	switch t {
	case ReqHeartbeatReport:
		return "ReqHeartbeatReport"
	case ReqAddTopic:
		return "ReqAddTopic"
	case ReqAddJob:
		return "ReqAddJob"
	case ReqDone:
		return "ReqDone"
	default:
		return fmt.Sprintf("Unknown(%d)", t)
	}
}

// ProxyRequest from proxy (inbound)
type ProxyRequest struct {
	RequestID string           `msgpack:"request_id"`
	Type      ProxyRequestType `msgpack:"type"`

	AddTopic        *AddTopicPayload `msgpack:"add_topic,omitempty"`
	HeartbeatReport *HeartbeatReport `msgpack:"heartbeat_report,omitempty"`
	AddJob          *AddJobPayload   `msgpack:"add_job,omitempty"`
	Done            *DonePayload     `msgpack:"done,omitempty"`
}

type AddTopicPayload struct {
	Topic string `msgpack:"topic"`
}

type HeartbeatReport struct {
	ProxyID    string          `msgpack:"proxy_id"`
	Timestamp  int64           `msgpack:"timestamp"`
	Capacities []TopicCapacity `msgpack:"capacities"`
}

type TopicCapacity struct {
	Topic            string `msgpack:"topic"`
	ConsumptionScore int    `msgpack:"consumption_score"`
}

// ========================
// from gateway to proxy
// ========================
type ToProxyMessageType string

const (
	ProxyMessageResponse  ToProxyMessageType = "response"
	ProxyMessageOutbound  ToProxyMessageType = "outbound"
	ProxyMessageHeartbeat ToProxyMessageType = "heartbeat"
)

type ToProxyMessage struct {
	Type ToProxyMessageType `msgpack:"type"`

	Response  *ToProducerResponse `msgpack:"response,omitempty"`
	Outbound  *ToConsumerMessage  `msgpack:"outbound,omitempty"`
	Heartbeat *ToProxyHeartbeat   `msgpack:"heartbeat,omitempty"`
}

type ToProducerRespStatus string

const (
	ToProxyRespStatusSuccess ToProducerRespStatus = "success"
	ToProxyRespStatusError   ToProducerRespStatus = "error"
	ToProxyRespStatusExist   ToProducerRespStatus = "exist"
)

// to gateway
type ToGatewayMessageType string

const (
	ToGatewayMessageConsumer  ToGatewayMessageType = "consumer"
	ToGatewayMessageHeartbeat ToGatewayMessageType = "heartbeat"
	ToGatewayMessageLoopback  ToGatewayMessageType = "loopback"
)

type ToGatewayMessage struct {
	Type ToGatewayMessageType

	// For job dispatch (existing)
	ToConsumer []byte

	// For status reporting (new)
	Heartbeat *PartitionHeartbeat

	// Durect response from inbound connection
	LoopbackMessage *ToProducerResponse
}

// ToProducerResponse to proxy (outbound)
type ToProducerResponse struct {
	RequestID string               `msgpack:"request_id"`
	Status    ToProducerRespStatus `msgpack:"status"`
	Error     string               `msgpack:"error,omitempty"`
}

type PartitionHeartbeat struct {
	Topic     string
	CanAccept bool
	Timestamp int64
}

// ToConsumerMessage (to gateway)
type ToConsumerMessage struct {
	Topic   string `msgpack:"topic"`
	ProxyID string `msgpack:"proxy_id"`
	Jobs    []*Job `msgpack:"jobs"`
}

type ToProxyHeartbeat struct {
	NodeStatus string   `msgpack:"node_status"`
	Voters     []string `msgpack:"voters"`
	Learners   []string `msgpack:"learners"`
	Leader     string   `msgpack:"leader"`
	Term       uint64   `msgpack:"term"`
}

type Members struct {
	mu       sync.RWMutex
	Voters   []string
	Learners []string
}

func (s *Members) Get() (voters, learners []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	voters = make([]string, len(s.Voters))
	learners = make([]string, len(s.Learners))

	copy(voters, s.Voters)
	copy(learners, s.Learners)
	return voters, learners
}

func (s *Members) Update(voters, learners []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Voters = make([]string, len(voters))
	copy(s.Voters, voters)

	s.Learners = make([]string, len(learners))
	copy(s.Learners, learners)
}

// ========================
// from gateway to partitions
// ========================
// Gateway adds some fields to HeartbeatReport and sends to partitions
type ProxyHeartbeat struct {
	ProxyID          string
	Topic            string
	ConsumptionScore int // shows overall consumer speed
	Timestamp        int64
}
