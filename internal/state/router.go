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
	"sync"

	"github.com/m-javani/cue/internal/model"
)

// CommandRouter
type CommandRouter struct {
	mu  sync.RWMutex
	chs map[string]chan<- model.Command // topic -> command channel
}

func NewCommandRouter() *CommandRouter {
	return &CommandRouter{
		chs: make(map[string]chan<- model.Command),
	}
}

// GetChannel returns the command channel for a topic (for Cluster Agents)
func (r *CommandRouter) GetChannel(topic string) (chan<- model.Command, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ch, exists := r.chs[topic]
	return ch, exists
}

// Register adds a new command channel (Gateway only)
func (r *CommandRouter) Register(topic string, ch chan<- model.Command) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chs[topic] = ch
}

// Unregister removes a topic (Gateway only)
func (r *CommandRouter) Unregister(topic string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.chs, topic)
}

// HeartbeatRouter - only Cluster Agents can write, Gateway reads
type HeartbeatRouter struct {
	mu  sync.RWMutex
	chs map[string]chan<- model.ProxyHeartbeat // topic -> heartbeat channel
}

func NewHeartbeatRouter() *HeartbeatRouter {
	return &HeartbeatRouter{
		chs: make(map[string]chan<- model.ProxyHeartbeat),
	}
}

// GetChannel returns the heartbeat channel
func (r *HeartbeatRouter) GetChannel(topic string) (chan<- model.ProxyHeartbeat, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ch, exists := r.chs[topic]
	return ch, exists
}

// Register adds a new heartbeat channel
func (r *HeartbeatRouter) Register(topic string, ch chan<- model.ProxyHeartbeat) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chs[topic] = ch
}

// Unregister removes a topic
func (r *HeartbeatRouter) Unregister(topic string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.chs, topic)
}
