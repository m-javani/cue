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
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
	"go.uber.org/zap"
)

type Handler interface {
	ProcessCommand(ctx context.Context, topic string, cmd *model.Command, index uint64) error
}

type CHandler struct {
	cmdRouter  *CommandRouter
	topologyCh chan<- TopicCommand
	logger     *zap.Logger
}

// Fixed: Return the Handler interface, not *Handler
func NewHandler(cmdRouter *CommandRouter, topologyCh chan<- TopicCommand, logger *zap.Logger) Handler {
	return &CHandler{
		cmdRouter:  cmdRouter,
		topologyCh: topologyCh,
		logger:     logger,
	}
}

// ProcessCommand processes a committed command from Raft
func (h *CHandler) ProcessCommand(ctx context.Context, topic string, cmd *model.Command, index uint64) error {
	if cmd == nil {
		h.logger.Error("received nil command")
		return nil
	}

	// Find the relevant partition/channel for this topic
	ch, err := h.ensureChannel(ctx, topic)
	if err != nil {
		return err
	}

	select {
	case ch <- *cmd:
		// Success
	case <-time.After(10 * time.Second):
		// Blocked for too long - system is stuck
		h.logger.Error("topic command blocked for 10s, system deadlocked",
			zap.String("topic", topic),
			zap.Uint64("index", index))
		panic("system deadlocked: topic command blocked for 10s")
	}

	return nil
}

func (h *CHandler) ensureChannel(ctx context.Context, topic string) (chan<- model.Command, error) {
	// First attempt - fast path
	ch, exists := h.cmdRouter.GetChannel(topic)
	if exists {
		return ch, nil
	}

	// Spawn the topic
	respCh := make(chan model.ToProducerResponse, 1)

	select {
	case h.topologyCh <- TopicCommand{
		Type:      TopicCommandSpawn,
		Topic:     topic,
		RequestID: rand.Uint32N(1000),
		RespCh:    respCh,
	}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Wait for spawn completion with retries
	for retries := range 3 {
		select {
		case resp := <-respCh:
			if resp.Status == "error" && resp.Error != internal.ErrTopicExists.Error() {
				return nil, fmt.Errorf("failed to spawn topic: %s", resp.Error)
			}
			// Spawn succeeded or topic already exists

			// Now get the channel - MUST exist
			ch, exists = h.cmdRouter.GetChannel(topic)
			if exists {
				return ch, nil
			}

			// Channel missing despite successful spawn - CRITICAL ERROR
			h.logger.Error("topic channel missing after successful spawn",
				zap.String("topic", topic),
				zap.Int("retry", retries))

			// Small backoff before retry
			time.Sleep(10 * time.Millisecond)
			continue

		case <-time.After(500 * time.Millisecond):
			h.logger.Warn("timeout waiting for topic spawn",
				zap.String("topic", topic),
				zap.Int("retry", retries))

			// Check if channel appeared anyway
			ch, exists = h.cmdRouter.GetChannel(topic)
			if exists {
				return ch, nil
			}
			continue

		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// After all retries failed - this means the system is inconsistent
	h.logger.Error("CRITICAL: unable to get topic channel after spawn",
		zap.String("topic", topic))

	return nil, fmt.Errorf("CRITICAL: topic channel unavailable - shutdown required")
}
