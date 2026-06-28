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

package state

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
	"go.uber.org/zap"
)

type TopologyUpdateType string

const (
	TopologyAddProxy    TopologyUpdateType = "add"
	TopologyRemoveProxy TopologyUpdateType = "remove"
)

// ProxyTopologyUpdate is sent by Gateway when a proxy connects or disconnects
type ProxyTopologyUpdate struct {
	Type    TopologyUpdateType
	ProxyID string
	PushCh  chan model.ToGatewayMessage // only used on "add"
}

type TopicCommandType string

const (
	TopicCommandSpawn    TopicCommandType = "spawn"
	TopicCommandKill     TopicCommandType = "kill"
	TopicCommandTopology TopicCommandType = "proxy_topology"
)

// TopicCommand represents a command to manage a topic partition
type TopicCommand struct {
	Type      TopicCommandType
	Topic     string
	RequestID string
	RespCh    chan<- model.ToProducerResponse

	// For proxy topology updates
	Topology *ProxyTopologyUpdate
}

// TopicManager owns topic lifecycle management via a single goroutine
type TopicManager struct {
	cmdCh       chan TopicCommand
	stopChs     map[string]chan struct{}
	topologyChs map[string]chan<- ProxyTopologyUpdate

	cmdRouter *CommandRouter
	hbRouter  *HeartbeatRouter

	dropProposalCh chan<- model.Command
	status         *atomic.Uint32
	metrics        *internal.TopicManagerMetrics
	logger         *zap.Logger
	config         *internal.PartitionConfig

	commandBuffer   int
	heartbeatBuffer int

	currentTopology map[string]ProxyTopologyUpdate // proxyID -> topology info
	topologyMu      sync.RWMutex

	ctx context.Context

	mu       sync.RWMutex
	stopOnce sync.Once
}

// TopicManagerConfig holds configuration for TopicManager
type TopicManagerConfig struct {
	CommandBufferSize   int
	HeartbeatBufferSize int
}

// DefaultTopicManagerConfig returns default configuration
func DefaultTopicManagerConfig() *TopicManagerConfig {
	return &TopicManagerConfig{
		CommandBufferSize:   1000,
		HeartbeatBufferSize: 1000,
	}
}

// NewTopicManager creates a new TopicManager
func NewTopicManager(
	cmdCh chan TopicCommand,
	cmdRouter *CommandRouter,
	hbRouter *HeartbeatRouter,
	dropProposalCh chan<- model.Command,
	status *atomic.Uint32,
	logger *zap.Logger,
	pconfig *internal.PartitionConfig,
	mgmtConfig *TopicManagerConfig,
	ctx context.Context,
) *TopicManager {
	if mgmtConfig == nil {
		mgmtConfig = DefaultTopicManagerConfig()
	}
	return &TopicManager{
		cmdCh:           cmdCh,
		stopChs:         make(map[string]chan struct{}),
		topologyChs:     make(map[string]chan<- ProxyTopologyUpdate),
		cmdRouter:       cmdRouter,
		hbRouter:        hbRouter,
		dropProposalCh:  dropProposalCh,
		status:          status,
		metrics:         internal.GetTopicManagerMetrics(),
		logger:          logger,
		config:          pconfig,
		ctx:             ctx,
		commandBuffer:   mgmtConfig.CommandBufferSize,
		heartbeatBuffer: mgmtConfig.HeartbeatBufferSize,
		currentTopology: make(map[string]ProxyTopologyUpdate, 0),
	}
}

// Run starts the TopicManager's main loop
func (tm *TopicManager) Run() {
	tm.logger.Info("topic manager started")

	for {
		select {
		case <-tm.ctx.Done():
			// tm.logger.Info("context cancelled, stopping topic manager")
			tm.Stop()
			return

		case cmd, ok := <-tm.cmdCh:
			if !ok {
				// tm.logger.Info("command channel closed, stopping topic manager")
				tm.Stop()
				return
			}

			switch cmd.Type {
			case TopicCommandSpawn:
				tm.handleSpawn(cmd)
			case TopicCommandKill:
				tm.handleKill(cmd)
			case TopicCommandTopology:
				tm.broadcastTopologyUpdate(cmd.Topology)
			default:
				tm.logger.Warn("unknown topic command", zap.String("type", string(cmd.Type)))
				if cmd.RespCh != nil {
					resp := model.ToProducerResponse{
						RequestID: cmd.RequestID,
						Status:    "error",
						Error:     internal.ErrInvalidPayload.Error(),
					}
					select {
					case cmd.RespCh <- resp:
					default:
					}
				}
			}
		}
	}
}

// broadcastTopologyUpdate sends update to all active partitions
func (tm *TopicManager) broadcastTopologyUpdate(update *ProxyTopologyUpdate) {
	if update == nil {
		return
	}

	tm.topologyMu.Lock()
	switch update.Type {
	case TopologyAddProxy:
		tm.currentTopology[update.ProxyID] = *update
	case TopologyRemoveProxy:
		delete(tm.currentTopology, update.ProxyID)
	}
	tm.topologyMu.Unlock()

	for topic, ch := range tm.topologyChs {
		select {
		case ch <- *update:
		default:
			tm.logger.Warn("topology update channel full for topic",
				zap.String("topic", topic),
				zap.String("proxy_id", update.ProxyID))
		}
	}
}

// handleSpawn creates a new partition for a topic
func (tm *TopicManager) handleSpawn(cmd TopicCommand) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	resp := model.ToProducerResponse{RequestID: cmd.RequestID}

	if _, exists := tm.stopChs[cmd.Topic]; exists {
		tm.logger.Warn("topic already exists", zap.String("topic", cmd.Topic))
		if cmd.RespCh != nil {
			resp.Status = "error"
			resp.Error = internal.ErrTopicExists.Error()
			select {
			case cmd.RespCh <- resp:
			default:
			}
		}
		return
	}

	stopCh := make(chan struct{})
	topologyCh := make(chan ProxyTopologyUpdate, 64)

	tm.stopChs[cmd.Topic] = stopCh
	tm.topologyChs[cmd.Topic] = topologyCh

	commandCh := make(chan model.Command, tm.commandBuffer)
	heartbeatCh := make(chan model.ProxyHeartbeat, tm.heartbeatBuffer)

	tm.cmdRouter.Register(cmd.Topic, commandCh)
	tm.hbRouter.Register(cmd.Topic, heartbeatCh)

	partition := NewPartition(
		cmd.Topic,
		commandCh,
		heartbeatCh,
		topologyCh,
		tm.dropProposalCh,
		tm.status,
		tm.logger,
		tm.config,
		stopCh,
	)

	// let partition learn current topology
	for _, update := range tm.currentTopology {
		partition.HandleTopologyUpdate(update)
	}

	go partition.Run()

	tm.metrics.TopicCreated()

	tm.logger.Info("spawned partition", zap.String("topic", cmd.Topic))

	if cmd.RespCh != nil {
		resp.Status = "success"
		select {
		case cmd.RespCh <- resp:
		default:
		}
	}
}

// handleKill terminates a partition
func (tm *TopicManager) handleKill(cmd TopicCommand) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	resp := model.ToProducerResponse{RequestID: cmd.RequestID}

	stopCh, exists := tm.stopChs[cmd.Topic]
	if !exists {
		tm.logger.Warn("topic not found", zap.String("topic", cmd.Topic))
		if cmd.RespCh != nil {
			resp.Status = "error"
			resp.Error = internal.ErrTopicNotFound.Error()
			select {
			case cmd.RespCh <- resp:
			default:
			}
		}
		return
	}

	close(stopCh)

	time.Sleep(100 * time.Millisecond)

	tm.cmdRouter.Unregister(cmd.Topic)
	tm.hbRouter.Unregister(cmd.Topic)

	delete(tm.stopChs, cmd.Topic)
	delete(tm.topologyChs, cmd.Topic)

	// Clean up metrics for this topic
	internal.GetPartitionMetrics().RemoveTopic(cmd.Topic)

	tm.metrics.TopicRemoved()

	if cmd.RespCh != nil {
		resp.Status = "success"
		select {
		case cmd.RespCh <- resp:
		default:
		}
	}
}

// Stop gracefully shuts down the TopicManager
func (tm *TopicManager) Stop() {
	tm.stopOnce.Do(func() {

		tm.mu.Lock()

		for _, stopCh := range tm.stopChs {
			close(stopCh)
		}

		tm.mu.Unlock()

		// give partitions a chance to flush DLQ
		time.Sleep(50 * time.Millisecond)

		close(tm.cmdCh)
	})
}
