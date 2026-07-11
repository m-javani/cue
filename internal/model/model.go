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
	"maps"
	"net"
	"strings"
	"sync"
)

type IdentityKind uint8

const (
	IdentityDNS IdentityKind = iota
	IdentityIP
	IdentitySPIFFE
)

var identityKindStrings = map[IdentityKind]string{
	IdentityDNS:    "dns",
	IdentityIP:     "ip",
	IdentitySPIFFE: "spiffe",
}

var stringToIdentityKind = map[string]IdentityKind{
	"dns":    IdentityDNS,
	"ip":     IdentityIP,
	"spiffe": IdentitySPIFFE,
}

func (k IdentityKind) String() string {
	if s, ok := identityKindStrings[k]; ok {
		return s
	}
	return fmt.Sprintf("unknown(%d)", k)
}

func (k IdentityKind) MarshalText() ([]byte, error) {
	return []byte(k.String()), nil
}

func (k *IdentityKind) UnmarshalText(text []byte) error {
	s := strings.ToLower(strings.TrimSpace(string(text)))
	if kind, ok := stringToIdentityKind[s]; ok {
		*k = kind
		return nil
	}
	return fmt.Errorf("unknown identity kind: %s", s)
}

type TLSIdentity struct {
	Kind  IdentityKind `msgpack:"kind" json:"kind" yaml:"kind"`
	Value string       `msgpack:"value" json:"value" yaml:"value"`
}

func (i TLSIdentity) String() string {
	switch i.Kind {
	case IdentityDNS:
		return fmt.Sprintf("DNS:%s", i.Value)
	case IdentityIP:
		return fmt.Sprintf("IP:%s", i.Value)
	case IdentitySPIFFE:
		return fmt.Sprintf("SPIFFE:%s", i.Value)
	default:
		return fmt.Sprintf("Unknown:%s", i.Value)
	}
}

func (i TLSIdentity) Validate() error {
	if strings.TrimSpace(i.Value) == "" {
		return fmt.Errorf("identity value is required")
	}

	switch i.Kind {
	case IdentityDNS, IdentityIP, IdentitySPIFFE:
		// valid
	default:
		return fmt.Errorf("unknown identity kind: %s (valid: dns, ip, spiffe)", i.Kind)
	}

	// Kind-specific validation
	switch i.Kind {
	case IdentityDNS:
		if len(i.Value) > 253 {
			return fmt.Errorf("DNS name too long (max 253 characters)")
		}
	case IdentityIP:
		if net.ParseIP(i.Value) == nil {
			return fmt.Errorf("invalid IP address in identity: %s", i.Value)
		}
	case IdentitySPIFFE:
		if !strings.HasPrefix(strings.ToLower(i.Value), "spiffe://") {
			return fmt.Errorf("SPIFFE identity must start with spiffe://")
		}
	}

	return nil
}

type PeerInfo struct {
	NodeID   string      `msgpack:"node_id" json:"node_id" yaml:"node_id"`
	Host     string      `msgpack:"host" json:"host" yaml:"host"`
	Port     uint16      `msgpack:"port" json:"port" yaml:"port"`
	Identity TLSIdentity `msgpack:"identity" json:"identity" yaml:"identity"`
}

func (p PeerInfo) String() string {
	return fmt.Sprintf("NodeID:%s, IP:%s, Port:%d, Identity:%s", p.NodeID, p.Host, p.Port, p.Identity.String())
}

func (p PeerInfo) Validate() error {
	// NodeID
	if strings.TrimSpace(p.NodeID) == "" {
		return fmt.Errorf("node_id is required")
	}
	if len(p.NodeID) > 64 {
		return fmt.Errorf("node_id too long (max 64 characters)")
	}

	// Host is optional - only validate if provided
	if strings.TrimSpace(p.Host) != "" {
		if net.ParseIP(p.Host) == nil {
			return fmt.Errorf("invalid IP address: %s", p.Host)
		}
	}

	// Identity
	if err := p.Identity.Validate(); err != nil {
		return fmt.Errorf("invalid identity: %w", err)
	}

	return nil
}

// ========================
// from proxy to gateway
// ========================

type ProxyRequestType uint8

const (
	ReqHeartbeatReport ProxyRequestType = iota
	ReqAddTopic
	ReqAddJobs
	ReqDone
)

func (t ProxyRequestType) String() string {
	switch t {
	case ReqHeartbeatReport:
		return "ReqHeartbeatReport"
	case ReqAddTopic:
		return "ReqAddTopic"
	case ReqAddJobs:
		return "ReqAddJobs"
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
	AddJobs         *AddJobsPayload  `msgpack:"add_job,omitempty"`
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

// ========================
// shared to API
// ========================

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

type PeerStore struct {
	mu    sync.RWMutex
	Peers map[string]PeerInfo // key: nodeID
}

// Get returns a copy of the peers map to avoid data races
func (s *PeerStore) Get() map[string]PeerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	peers := make(map[string]PeerInfo, len(s.Peers))
	maps.Copy(peers, s.Peers)
	return peers
}

// Lookup returns a peer info for the given nodeID
// Returns the peer info and a boolean indicating if the peer was found
func (s *PeerStore) Lookup(nodeID string) (PeerInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	peer, exists := s.Peers[nodeID]
	return peer, exists
}

// Set updates the peers map with the given peers
// If a peer already exists, it will be overwritten
func (s *PeerStore) Set(peers map[string]PeerInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Peers = peers
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
