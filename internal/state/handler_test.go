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
	"testing"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// TestNewHandler tests the NewHandler function
func TestNewHandler(t *testing.T) {
	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand)
	logger := zap.NewNop()

	handler := NewHandler(cmdRouter, topologyCh, logger)
	assert.NotNil(t, handler)

	// Verify it returns the correct type
	_, ok := handler.(*CHandler)
	assert.True(t, ok)
}

// TestProcessCommand_NilCommand tests ProcessCommand with nil command
func TestProcessCommand_NilCommand(t *testing.T) {
	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand)
	logger := zap.NewNop()

	handler := NewHandler(cmdRouter, topologyCh, logger)
	err := handler.ProcessCommand(context.Background(), "test-topic", nil, 0)
	assert.NoError(t, err)
}

// TestProcessCommand_ChannelExists tests ProcessCommand when channel already exists
func TestProcessCommand_ChannelExists(t *testing.T) {
	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand)
	logger := zap.NewNop()

	// Create a channel and register it with the router
	ch := make(chan model.Command, 1)
	cmdRouter.Register("test-topic", ch)

	handler := NewHandler(cmdRouter, topologyCh, logger)

	// Start consumer goroutine to drain the channel
	go func() {
		for range ch {
			// Just drain the channel
		}
	}()

	cmd := &model.Command{Type: model.CmdDone, Done: &model.DonePayload{Topic: "test-topic", JobIDs: []string{"job1"}}}

	err := handler.ProcessCommand(context.Background(), "test-topic", cmd, 1)
	assert.NoError(t, err)

	// Small delay to ensure command is processed
	time.Sleep(10 * time.Millisecond)
}

// TestProcessCommand_SpawnTopicSuccess tests ProcessCommand when topic needs to be spawned
func TestProcessCommand_SpawnTopicSuccess(t *testing.T) {
	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand)
	logger := zap.NewNop()

	handler := NewHandler(cmdRouter, topologyCh, logger)
	cmd := &model.Command{Type: model.CmdDone, Done: &model.DonePayload{Topic: "test-topic", JobIDs: []string{"job1"}}}

	// Start a goroutine to handle the spawn request
	go func() {
		select {
		case req := <-topologyCh:
			assert.Equal(t, TopicCommandSpawn, req.Type)
			assert.Equal(t, "test-topic", req.Topic)

			// Register channel after spawn
			ch := make(chan model.Command, 1)
			cmdRouter.Register("test-topic", ch)

			// Start consumer for the new channel
			go func() {
				for range ch {
					// Just drain the channel
				}
			}()

			// Send success response
			req.RespCh <- model.ToProducerResponse{
				Status: "success",
				Error:  "",
			}
		case <-time.After(time.Second):
			// t.Error("timeout waiting for spawn request")
		}
	}()

	// Small delay to ensure goroutine is ready
	time.Sleep(10 * time.Millisecond)

	err := handler.ProcessCommand(context.Background(), "test-topic", cmd, 1)
	assert.NoError(t, err)

	// Small delay to ensure command is processed
	time.Sleep(10 * time.Millisecond)
}

// TestProcessCommand_TopicAlreadyExists tests spawn when topic already exists
func TestProcessCommand_TopicAlreadyExists(t *testing.T) {
	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand)
	logger := zap.NewNop()

	// Pre-register the channel
	ch := make(chan model.Command, 1)
	cmdRouter.Register("test-topic", ch)

	// Start consumer goroutine to drain the channel
	go func() {
		for range ch {
			// Just drain the channel
		}
	}()

	handler := NewHandler(cmdRouter, topologyCh, logger)
	cmd := &model.Command{Type: model.CmdDone, Done: &model.DonePayload{Topic: "test-topic", JobIDs: []string{"job1"}}}

	// Start a goroutine to handle the spawn request
	go func() {
		select {
		case req := <-topologyCh:
			// Send error that topic already exists
			req.RespCh <- model.ToProducerResponse{
				Status: "error",
				Error:  internal.ErrTopicExists.Error(),
			}
		case <-time.After(time.Second):
			// t.Error("timeout waiting for spawn request")
		}
	}()

	time.Sleep(10 * time.Millisecond)

	err := handler.ProcessCommand(context.Background(), "test-topic", cmd, 1)
	assert.NoError(t, err)

	// Small delay to ensure command is processed
	time.Sleep(10 * time.Millisecond)
}

// TestProcessCommand_SpawnError tests spawn with a real error
func TestProcessCommand_SpawnError(t *testing.T) {
	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand)
	logger := zap.NewNop()

	handler := NewHandler(cmdRouter, topologyCh, logger)
	cmd := &model.Command{Type: model.CmdDone, Done: &model.DonePayload{Topic: "test-topic", JobIDs: []string{"job1"}}}

	// Start a goroutine to handle the spawn request
	go func() {
		select {
		case req := <-topologyCh:
			req.RespCh <- model.ToProducerResponse{
				Status: "error",
				Error:  "some real error",
			}
		case <-time.After(time.Second):
			// t.Error("timeout waiting for spawn request")
		}
	}()

	time.Sleep(10 * time.Millisecond)

	err := handler.ProcessCommand(context.Background(), "test-topic", cmd, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to spawn topic")
}

// TestProcessCommand_Timeout tests timeout waiting for spawn
func TestProcessCommand_Timeout(t *testing.T) {
	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand, 1) // Buffered to not block
	logger := zap.NewNop()

	handler := NewHandler(cmdRouter, topologyCh, logger)
	cmd := &model.Command{Type: model.CmdDone, Done: &model.DonePayload{Topic: "test-topic", JobIDs: []string{"job1"}}}

	done := make(chan error, 1)
	go func() {
		done <- handler.ProcessCommand(context.Background(), "test-topic", cmd, 1)
	}()

	select {
	case err := <-done:
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "CRITICAL: topic channel unavailable")
	case <-time.After(11 * time.Second): // Slightly more than the 10s internal timeout
		t.Fatal("test timed out - should have returned error")
	}
}

// TestProcessCommand_CancelledContext tests ProcessCommand with cancelled context
func TestProcessCommand_CancelledContext(t *testing.T) {
	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand)
	logger := zap.NewNop()

	handler := NewHandler(cmdRouter, topologyCh, logger)
	cmd := &model.Command{Type: model.CmdDone, Done: &model.DonePayload{Topic: "test-topic", JobIDs: []string{"job1"}}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := handler.ProcessCommand(ctx, "test-topic", cmd, 1)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// TestProcessCommand_CommandChannelBlocked tests when command channel is blocked
func TestProcessCommand_CommandChannelBlocked(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand, 1)
	logger := zap.NewNop()

	// Create a channel with 0 capacity (blocking)
	ch := make(chan model.Command)
	cmdRouter.Register("test-topic", ch)

	handler := NewHandler(cmdRouter, topologyCh, logger)
	cmd := &model.Command{Type: model.CmdDone, Done: &model.DonePayload{Topic: "test-topic", JobIDs: []string{"job1"}}}

	// This should panic
	defer func() {
		if r := recover(); r != nil {
			// Expected - we got a panic
			assert.Contains(t, r.(string), "system deadlocked")
			t.Log("Got panic as expected:", r)
		} else {
			t.Error("Expected panic but got none")
		}
	}()

	_ = handler.ProcessCommand(context.Background(), "test-topic", cmd, 1)
}

// TestEnsureChannel_ChannelExists tests ensureChannel when channel exists
func TestEnsureChannel_ChannelExists(t *testing.T) {
	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand, 1)
	logger := zap.NewNop()

	ch := make(chan model.Command, 1)
	cmdRouter.Register("test-topic", ch)

	// Start consumer goroutine to drain the channel
	go func() {
		for range ch {
			// Just drain the channel
		}
	}()

	handler := &CHandler{
		cmdRouter:  cmdRouter,
		topologyCh: topologyCh,
		logger:     logger,
	}

	result, err := handler.ensureChannel(context.Background(), "test-topic")
	assert.NoError(t, err)

	// Send a command to verify the channel works
	cmd := &model.Command{Type: model.CmdDone, Done: &model.DonePayload{Topic: "test-topic", JobIDs: []string{"job1"}}}
	select {
	case result <- *cmd:
		// Successfully sent
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout sending to channel")
	}

	// Verify we can receive it
	select {
	case received := <-ch:
		assert.Equal(t, *cmd, received)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for command")
	}
}

// TestEnsureChannel_CancelledContext tests ensureChannel with cancelled context
func TestEnsureChannel_CancelledContext(t *testing.T) {
	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand)
	logger := zap.NewNop()

	handler := &CHandler{
		cmdRouter:  cmdRouter,
		topologyCh: topologyCh,
		logger:     logger,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := handler.ensureChannel(ctx, "test-topic")
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// TestEnsureChannel_SpawnSuccess tests ensureChannel with successful spawn
func TestEnsureChannel_SpawnSuccess(t *testing.T) {
	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand)
	logger := zap.NewNop()

	handler := &CHandler{
		cmdRouter:  cmdRouter,
		topologyCh: topologyCh,
		logger:     logger,
	}

	// Start a goroutine to handle the spawn request
	go func() {
		select {
		case req := <-topologyCh:
			ch := make(chan model.Command, 1)
			cmdRouter.Register("test-topic", ch)

			// Start consumer for the new channel
			go func() {
				for range ch {
					// Just drain the channel
				}
			}()

			req.RespCh <- model.ToProducerResponse{
				Status: "success",
				Error:  "",
			}
		case <-time.After(time.Second):
			// t.Error("timeout waiting for spawn request")
		}
	}()

	time.Sleep(10 * time.Millisecond)

	result, err := handler.ensureChannel(context.Background(), "test-topic")
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

// TestEnsureChannel_ChannelMissingAfterSpawn tests when channel is missing after spawn
func TestEnsureChannel_ChannelMissingAfterSpawn(t *testing.T) {
	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand)
	logger := zap.NewNop()

	handler := &CHandler{
		cmdRouter:  cmdRouter,
		topologyCh: topologyCh,
		logger:     logger,
	}

	// Start a goroutine to handle the spawn request but don't create the channel
	go func() {
		select {
		case req := <-topologyCh:
			// Send success but don't register channel
			req.RespCh <- model.ToProducerResponse{
				Status: "success",
				Error:  "",
			}
		case <-time.After(time.Second):
			// t.Error("timeout waiting for spawn request")
		}
	}()

	time.Sleep(10 * time.Millisecond)

	// This should retry and eventually fail
	_, err := handler.ensureChannel(context.Background(), "test-topic")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CRITICAL: topic channel unavailable")
}

// TestEnsureChannel_RetrySuccess tests that retry works when channel appears later
func TestEnsureChannel_RetrySuccess(t *testing.T) {
	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand)
	logger := zap.NewNop()

	handler := &CHandler{
		cmdRouter:  cmdRouter,
		topologyCh: topologyCh,
		logger:     logger,
	}

	// Start a goroutine to handle the spawn request
	go func() {
		select {
		case req := <-topologyCh:
			// Send success
			req.RespCh <- model.ToProducerResponse{
				Status: "success",
				Error:  "",
			}
			// Register channel after a delay
			time.Sleep(20 * time.Millisecond)
			ch := make(chan model.Command, 1)
			cmdRouter.Register("test-topic", ch)

			// Start consumer for the new channel
			go func() {
				for range ch {
					// Just drain the channel
				}
			}()
		case <-time.After(time.Second):
			// t.Error("timeout waiting for spawn request")
		}
	}()

	time.Sleep(10 * time.Millisecond)

	result, err := handler.ensureChannel(context.Background(), "test-topic")
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

// TestEnsureChannel_TopicExistsError tests spawn with topic exists error
func TestEnsureChannel_TopicExistsError(t *testing.T) {
	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand, 1)
	logger := zap.NewNop()

	// Register the channel first so GetChannel finds it
	ch := make(chan model.Command, 1)
	cmdRouter.Register("test-topic", ch)

	// Start consumer goroutine to drain the channel
	go func() {
		for range ch {
			// Just drain the channel
		}
	}()

	handler := &CHandler{
		cmdRouter:  cmdRouter,
		topologyCh: topologyCh,
		logger:     logger,
	}

	// This should find the existing channel and return it immediately
	// No spawn request should be sent
	result, err := handler.ensureChannel(context.Background(), "test-topic")
	assert.NoError(t, err)

	// Verify it's the same channel by sending and receiving
	cmd := &model.Command{Type: model.CmdDone}
	select {
	case result <- *cmd:
		// Successfully sent
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout sending to channel")
	}

	select {
	case received := <-ch:
		assert.Equal(t, *cmd, received)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout receiving from channel")
	}
}

// TestCommandRouter_RegisterAndGet tests the CommandRouter methods
func TestCommandRouter_RegisterAndGet(t *testing.T) {
	router := NewCommandRouter()

	ch := make(chan model.Command)
	router.Register("topic1", ch)

	// Test GetChannel
	retrieved, exists := router.GetChannel("topic1")
	assert.True(t, exists)
	// Compare by checking if they point to the same underlying channel
	// by sending to one and receiving from the other
	cmd := &model.Command{Type: model.CmdDone}
	go func() {
		retrieved <- *cmd
	}()
	select {
	case received := <-ch:
		assert.Equal(t, *cmd, received)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for command")
	}

	// Test non-existent topic
	_, exists = router.GetChannel("topic2")
	assert.False(t, exists)
}

// TestCommandRouter_Unregister tests the Unregister method
func TestCommandRouter_Unregister(t *testing.T) {
	router := NewCommandRouter()

	ch := make(chan model.Command)
	router.Register("topic1", ch)

	// Verify it exists
	_, exists := router.GetChannel("topic1")
	assert.True(t, exists)

	// Unregister
	router.Unregister("topic1")

	// Verify it's gone
	_, exists = router.GetChannel("topic1")
	assert.False(t, exists)
}

// TestCommandRouter_ConcurrentAccess tests concurrent access to CommandRouter
func TestCommandRouter_ConcurrentAccess(t *testing.T) {
	router := NewCommandRouter()

	// Concurrent writes
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(i int) {
			ch := make(chan model.Command)
			router.Register("topic", ch)
			done <- true
		}(i)
	}

	// Wait for all writes
	for i := 0; i < 10; i++ {
		<-done
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		go func() {
			_, _ = router.GetChannel("topic")
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	// Final verification
	_, exists := router.GetChannel("topic")
	assert.True(t, exists)
}

// TestProcessCommand_AllCommandTypes tests ProcessCommand with different command types
func TestProcessCommand_AllCommandTypes(t *testing.T) {
	cmdRouter := NewCommandRouter()
	topologyCh := make(chan TopicCommand)
	logger := zap.NewNop()

	// Create and register channel
	ch := make(chan model.Command, 1)
	cmdRouter.Register("test-topic", ch)

	// Start consumer goroutine to drain the channel
	go func() {
		for range ch {
			// Just drain the channel
		}
	}()

	handler := NewHandler(cmdRouter, topologyCh, logger)

	// Test different command types
	commands := []*model.Command{
		{Type: model.CmdDone, Done: &model.DonePayload{Topic: "test-topic", JobIDs: []string{"job1"}}},
		{Type: model.CmdUpdatePeersList},
		{Type: model.CmdAddNode},
		{Type: model.CmdRemoveNode},
		{Type: model.CmdTransferLeader},
		{Type: model.CmdAddJob},
		{Type: model.CmdDrop},
	}

	for i, cmd := range commands {
		err := handler.ProcessCommand(context.Background(), "test-topic", cmd, uint64(i))
		assert.NoError(t, err)
	}

	// Small delay to ensure all commands are processed
	time.Sleep(10 * time.Millisecond)
}
