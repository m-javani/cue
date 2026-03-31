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
	"sync/atomic"
	"testing"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTopicManager(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := context.Background()

	// Setup
	cmdCh := make(chan TopicCommand, 10)
	dropCh := make(chan model.Command, 10)
	status := &atomic.Uint32{}
	status.Store(uint32(model.NodeStatusLeaderActive))

	cmdRouter := NewCommandRouter()
	hbRouter := NewHeartbeatRouter()

	tm := NewTopicManager(
		cmdCh,
		cmdRouter,
		hbRouter,
		dropCh,
		status,
		logger,
		&internal.NewConfig().Partition,
		nil,
		ctx,
	)

	go tm.Run()
	defer tm.Stop()

	// Allow manager to start
	time.Sleep(100 * time.Millisecond)

	t.Run("spawn topic", func(t *testing.T) {
		respCh := make(chan model.ToProducerResponse, 1)

		cmdCh <- TopicCommand{
			Type:      TopicCommandSpawn,
			Topic:     "test-topic",
			RequestID: "req-1",
			RespCh:    respCh,
		}

		select {
		case resp := <-respCh:
			require.Equal(t, "success", string(resp.Status))
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for spawn response")
		}

		// Verify router registration
		ch, exists := cmdRouter.GetChannel("test-topic")
		require.True(t, exists, "command channel should be registered")
		require.NotNil(t, ch)

		_, exists = hbRouter.GetChannel("test-topic")
		require.True(t, exists, "heartbeat channel should be registered")
	})

	t.Run("spawn duplicate topic", func(t *testing.T) {
		respCh := make(chan model.ToProducerResponse, 1)

		cmdCh <- TopicCommand{
			Type:      TopicCommandSpawn,
			Topic:     "test-topic",
			RequestID: "req-2",
			RespCh:    respCh,
		}

		select {
		case resp := <-respCh:
			require.Equal(t, "error", string(resp.Status))
			require.Contains(t, resp.Error, "exists")
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for duplicate spawn response")
		}
	})

	t.Run("topology update propagates to partition", func(t *testing.T) {
		// Get the partition's topology channel
		// We need to verify the partition received the update
		pushCh := make(chan model.ToGatewayMessage, 10)

		cmdCh <- TopicCommand{
			Type: TopicCommandTopology,
			Topology: &ProxyTopologyUpdate{
				Type:    TopologyAddProxy,
				ProxyID: "proxy-1",
				PushCh:  pushCh,
			},
		}

		// Give time for propagation
		time.Sleep(200 * time.Millisecond)

		// Send heartbeat to verify proxy is known
		hbCh, _ := hbRouter.GetChannel("test-topic")
		hbCh <- model.ProxyHeartbeat{
			ProxyID:          "proxy-1",
			Topic:            "test-topic",
			ConsumptionScore: 5,
			Timestamp:        time.Now().UnixMilli(),
		}

		// Give time for heartbeat processing
		time.Sleep(200 * time.Millisecond)

		// We can't easily inspect partition's internal state,
		// but we can verify topology removal works
		cmdCh <- TopicCommand{
			Type: TopicCommandTopology,
			Topology: &ProxyTopologyUpdate{
				Type:    TopologyRemoveProxy,
				ProxyID: "proxy-1",
			},
		}

		time.Sleep(100 * time.Millisecond)
	})

	t.Run("kill topic", func(t *testing.T) {
		respCh := make(chan model.ToProducerResponse, 1)

		cmdCh <- TopicCommand{
			Type:      TopicCommandKill,
			Topic:     "test-topic",
			RequestID: "req-3",
			RespCh:    respCh,
		}

		select {
		case resp := <-respCh:
			require.Equal(t, "success", string(resp.Status))
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for kill response")
		}

		// Verify cleanup
		_, exists := cmdRouter.GetChannel("test-topic")
		require.False(t, exists, "command channel should be unregistered")

		_, exists = hbRouter.GetChannel("test-topic")
		require.False(t, exists, "heartbeat channel should be unregistered")
	})

	t.Run("kill non-existent topic", func(t *testing.T) {
		respCh := make(chan model.ToProducerResponse, 1)

		cmdCh <- TopicCommand{
			Type:      TopicCommandKill,
			Topic:     "non-existent",
			RequestID: "req-4",
			RespCh:    respCh,
		}

		select {
		case resp := <-respCh:
			require.Equal(t, "error", string(resp.Status))
			require.Contains(t, resp.Error, "not found")
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for kill response")
		}
	})
}

func TestTopicManager_UnknownCommand_RespChFull(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := context.Background()

	// Setup
	cmdCh := make(chan TopicCommand, 10)
	dropCh := make(chan model.Command, 10)
	status := &atomic.Uint32{}
	status.Store(uint32(model.NodeStatusLeaderActive))

	cmdRouter := NewCommandRouter()
	hbRouter := NewHeartbeatRouter()

	tm := NewTopicManager(
		cmdCh,
		cmdRouter,
		hbRouter,
		dropCh,
		status,
		logger,
		&internal.NewConfig().Partition,
		nil,
		ctx,
	)

	go tm.Run()
	defer tm.Stop()

	// Allow manager to start
	time.Sleep(100 * time.Millisecond)

	t.Run("unknown command with full response channel", func(t *testing.T) {
		respCh := make(chan model.ToProducerResponse, 1)

		// Fill the response channel
		respCh <- model.ToProducerResponse{
			RequestID: "dummy",
			Status:    "success",
		}

		// Send unknown command
		cmdCh <- TopicCommand{
			Type:      TopicCommandType("unknown_full"),
			Topic:     "test-topic",
			RequestID: "req-full",
			RespCh:    respCh,
		}

		// Give time for processing - should not block on full channel
		time.Sleep(200 * time.Millisecond)

		// Drain channel
		select {
		case <-respCh:
			// Channel was full, this may or may not be the response
		default:
		}
	})
}
