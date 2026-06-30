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
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
)

// DefaultTestPartitionConfig returns a default configuration for testing
func DefaultTestPartitionConfig() *internal.PartitionConfig {
	pcfg := &internal.NewConfig().Partition
	pcfg.ActiveQueueCapacity = 10000
	pcfg.MaxRetries = 3
	pcfg.MaxBackoffSec = 6
	pcfg.DLQMaxBytes = 1024 * 1024
	pcfg.DLQMaxAgeMs = 60000
	pcfg.DispatchBatchSize = 1024

	return pcfg
}

// PartitionTester provides a lightweight test harness for Partition
type PartitionTester struct {
	partition *Partition
	topic     string
	status    *atomic.Uint32

	// Channels for partition communication
	commandCh      chan model.Command
	heartbeatCh    chan model.ProxyHeartbeat
	topologyCh     chan ProxyTopologyUpdate
	dropProposalCh chan model.Command
	stopCh         chan struct{}

	// Proxy simulation
	proxyID string
	pushCh  chan model.ToGatewayMessage

	// Track multiple proxies
	proxies   map[string]*TestProxy
	proxiesMu sync.RWMutex

	// Atomic counters
	responsesSuccess atomic.Uint64
	responsesError   atomic.Uint64
	jobsDispatched   atomic.Uint64
	jobsDropped      atomic.Uint64
	doneSubmitted    atomic.Uint64 // counts Done commands submitted by tester

	// Channel for dispatched jobs
	dispatchedJobs chan string

	// Track dispatched IDs for inspection
	dispatchedMu  sync.Mutex
	dispatchedIDs []string

	ctx    context.Context
	cancel context.CancelFunc

	autoDoneEnabled atomic.Bool
}

// TestProxy represents a simulated proxy for testing
type TestProxy struct {
	ID     string
	PushCh chan model.ToGatewayMessage
}

// NewPartitionTester creates a new test harness
func NewPartitionTester(topic string, config *internal.PartitionConfig) *PartitionTester {
	commandCh := make(chan model.Command, 1024)
	heartbeatCh := make(chan model.ProxyHeartbeat, 100)
	topologyCh := make(chan ProxyTopologyUpdate, 10)
	dropProposalCh := make(chan model.Command, 100)
	stopCh := make(chan struct{})

	// Default proxy
	proxyID := "test-proxy"
	pushCh := make(chan model.ToGatewayMessage, 1024)

	logger, _ := zap.NewDevelopment()

	status := &atomic.Uint32{}
	status.Store(uint32(model.NodeStatusLeaderActive))

	partition := NewPartition(
		topic,
		commandCh,
		heartbeatCh,
		topologyCh,
		dropProposalCh,
		status,
		logger,
		config,
		stopCh,
	)

	ctx, cancel := context.WithCancel(context.Background())

	tester := &PartitionTester{
		partition:        partition,
		topic:            topic,
		status:           status,
		commandCh:        commandCh,
		heartbeatCh:      heartbeatCh,
		topologyCh:       topologyCh,
		dropProposalCh:   dropProposalCh,
		stopCh:           stopCh,
		proxyID:          proxyID,
		pushCh:           pushCh,
		proxies:          make(map[string]*TestProxy),
		dispatchedJobs:   make(chan string, 10000),
		ctx:              ctx,
		cancel:           cancel,
		proxiesMu:        sync.RWMutex{},
		responsesSuccess: atomic.Uint64{},
		responsesError:   atomic.Uint64{},
		jobsDispatched:   atomic.Uint64{},
		jobsDropped:      atomic.Uint64{},
		doneSubmitted:    atomic.Uint64{},
		dispatchedMu:     sync.Mutex{},
		dispatchedIDs:    []string{},
		autoDoneEnabled:  atomic.Bool{},
	}

	tester.autoDoneEnabled.Store(true)

	// Add default proxy
	tester.proxies[proxyID] = &TestProxy{
		ID:     proxyID,
		PushCh: pushCh,
	}

	// Send topology update for default proxy
	tester.topologyCh <- ProxyTopologyUpdate{
		Type:    "add",
		ProxyID: proxyID,
		PushCh:  pushCh,
	}

	go partition.Run()
	go tester.consumeProxy(pushCh)
	go tester.consumeDropProposals()
	go tester.autoDoneProcessor()

	return tester
}

// consumeDropProposals reads drop proposals and counts them
func (t *PartitionTester) consumeDropProposals() {
	for {
		select {
		case <-t.ctx.Done():
			return
		case cmd := <-t.dropProposalCh:
			if cmd.Type == model.CmdDrop && cmd.Drop != nil {
				t.jobsDropped.Add(uint64(len(cmd.Drop.JobIDs)))
			}
		}
	}
}

// autoDoneProcessor automatically sends Done for dispatched jobs
func (t *PartitionTester) autoDoneProcessor() {
	batch := make([]string, 0, 1000)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			if len(batch) > 0 {
				t.doSendDone(batch)
			}
			return
		case jobID := <-t.dispatchedJobs:
			batch = append(batch, jobID)
			if len(batch) >= 1000 {
				t.doSendDone(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				t.doSendDone(batch)
				batch = batch[:0]
			}
		}
	}
}

func (t *PartitionTester) doSendDone(jobIDs []string) {
	if len(jobIDs) == 0 {
		return
	}
	cmd := model.Command{
		Type: model.CmdDone,
		Done: &model.DonePayload{
			Topic:  t.topic,
			JobIDs: jobIDs,
		},
	}
	select {
	case t.commandCh <- cmd:
		// Count Done commands submitted (not necessarily processed yet)
		t.doneSubmitted.Add(uint64(len(jobIDs)))
	case <-t.ctx.Done():
	}
}

func (t *PartitionTester) SetStatus(s model.ClusterNodeStatus) {
	t.status.Store(uint32(s))
}

// AddJob sends an AddJob command and consumes the response synchronously
func (t *PartitionTester) AddJob(jobID string, data []byte) {
	respCh := make(chan model.ToProducerResponse, 1)
	cmd := model.Command{
		Type: model.CmdAddJob,
		AddJob: &model.AddJobPayload{
			Job: model.Job{
				ID:    jobID,
				Topic: t.topic,
				Data:  data,
			},
		},
		RespInfo: &model.RespInfo{
			RequestID: jobID,
			RespCh:    respCh,
		},
	}

	t.commandCh <- cmd

	select {
	case resp := <-respCh:
		if resp.Status == model.ToProxyRespStatusSuccess {
			t.responsesSuccess.Add(1)
		} else {
			t.responsesError.Add(1)
		}
	case <-t.ctx.Done():
	}
}

// AddJobAsync sends AddJob command without waiting for response
func (t *PartitionTester) AddJobAsync(jobID string, data []byte) {
	respCh := make(chan model.ToProducerResponse, 1)
	cmd := model.Command{
		Type: model.CmdAddJob,
		AddJob: &model.AddJobPayload{
			Job: model.Job{
				ID:    jobID,
				Topic: t.topic,
				Data:  data,
			},
		},
		RespInfo: &model.RespInfo{
			RequestID: jobID,
			RespCh:    respCh,
		},
	}

	select {
	case t.commandCh <- cmd:
		go func() {
			select {
			case resp := <-respCh:
				if resp.Status == model.ToProxyRespStatusSuccess {
					t.responsesSuccess.Add(1)
				} else {
					t.responsesError.Add(1)
				}
			case <-t.ctx.Done():
			}
		}()
	case <-t.ctx.Done():
	}
}

// Done sends a Done command
func (t *PartitionTester) Done(jobIDs []string) {
	if len(jobIDs) == 0 {
		return
	}
	cmd := model.Command{
		Type: model.CmdDone,
		Done: &model.DonePayload{
			Topic:  t.topic,
			JobIDs: jobIDs,
		},
	}
	// Use the same path as auto-done to ensure consistent accounting
	select {
	case t.commandCh <- cmd:
		// Count Done commands submitted (not necessarily processed yet)
		t.doneSubmitted.Add(uint64(len(jobIDs)))
	case <-t.ctx.Done():
	}
}

// ProxyOptions configures a proxy for testing
type ProxyOptions struct {
	Capacity     int           // Channel capacity
	Consume      bool          // Whether to consume from the channel
	ConsumeDelay time.Duration // Delay between consuming messages (simulates slow processing)
}

// DefaultProxyOptions returns default proxy options
func DefaultProxyOptions() ProxyOptions {
	return ProxyOptions{
		Capacity:     1000,
		Consume:      true,
		ConsumeDelay: 0,
	}
}

// AddProxy adds a new proxy to the partition for testing
// Options can be provided to customize the proxy behavior
func (t *PartitionTester) AddProxy(proxyID string, opts ...ProxyOptions) {
	// Use default options if none provided
	options := DefaultProxyOptions()
	if len(opts) > 0 {
		options = opts[0]
	}

	t.proxiesMu.Lock()
	defer t.proxiesMu.Unlock()

	// Check if proxy already exists
	if _, exists := t.proxies[proxyID]; exists {
		return
	}

	pushCh := make(chan model.ToGatewayMessage, options.Capacity)

	// Send topology update
	select {
	case t.topologyCh <- ProxyTopologyUpdate{
		Type:    "add",
		ProxyID: proxyID,
		PushCh:  pushCh,
	}:
	case <-t.ctx.Done():
		return
	}

	t.proxies[proxyID] = &TestProxy{
		ID:     proxyID,
		PushCh: pushCh,
	}

	if options.Consume {
		if options.ConsumeDelay > 0 {
			go t.consumeProxySlow(pushCh, options.ConsumeDelay)
		} else {
			go t.consumeProxy(pushCh)
		}
	}
}

// RemoveProxy removes a proxy from the partition
func (t *PartitionTester) RemoveProxy(proxyID string) {
	// Check if proxy exists (quick check without lock)
	t.proxiesMu.RLock()
	_, exists := t.proxies[proxyID]
	t.proxiesMu.RUnlock()
	if !exists {
		return
	}

	// Send topology update WITHOUT holding the lock
	select {
	case t.topologyCh <- ProxyTopologyUpdate{
		Type:    "remove",
		ProxyID: proxyID,
	}:
	case <-t.ctx.Done():
		return
	}

	// Now remove from map
	t.proxiesMu.Lock()
	defer t.proxiesMu.Unlock()
	delete(t.proxies, proxyID)
}

// consumeProxy reads dispatched jobs from a proxy's push channel
func (t *PartitionTester) consumeProxy(pushCh chan model.ToGatewayMessage) {
	for {
		select {
		case <-t.ctx.Done():
			return
		case msg, ok := <-pushCh:
			if !ok {
				return
			}
			t.processProxyMessage(msg)
		}
	}
}

// consumeProxySlow reads dispatched jobs from a proxy's push channel with a delay
// This simulates a slow consumer and creates backpressure
func (t *PartitionTester) consumeProxySlow(pushCh chan model.ToGatewayMessage, delay time.Duration) {
	for {
		select {
		case <-t.ctx.Done():
			return
		case msg, ok := <-pushCh:
			if !ok {
				return
			}
			if delay > 0 {
				time.Sleep(delay)
			}
			t.processProxyMessage(msg)
		}
	}
}

// processProxyMessage processes a single proxy message and updates counters
func (t *PartitionTester) processProxyMessage(msg model.ToGatewayMessage) {
	if msg.Type != model.ToGatewayMessageConsumer || msg.ToConsumer == nil {
		return
	}

	var proxyMsg model.ToProxyMessage
	if err := msgpack.Unmarshal(msg.ToConsumer, &proxyMsg); err != nil {
		return
	}

	if proxyMsg.Type != model.ProxyMessageOutbound || proxyMsg.Outbound == nil {
		return
	}

	// Increment dispatched counter
	t.jobsDispatched.Add(uint64(len(proxyMsg.Outbound.Jobs)))

	// Store dispatched IDs
	t.dispatchedMu.Lock()
	for _, job := range proxyMsg.Outbound.Jobs {
		t.dispatchedIDs = append(t.dispatchedIDs, job.ID)
	}
	t.dispatchedMu.Unlock()

	// Auto-done pipeline
	if t.autoDoneEnabled.Load() {
		for _, job := range proxyMsg.Outbound.Jobs {
			select {
			case t.dispatchedJobs <- job.ID:
			default:
			}
		}
	}
}

// SendHeartbeat sends a heartbeat to the partition for the default proxy
func (t *PartitionTester) SendHeartbeat(consumptionScore int) {
	t.heartbeatCh <- model.ProxyHeartbeat{
		ProxyID:          t.proxyID,
		Topic:            t.topic,
		ConsumptionScore: consumptionScore,
		Timestamp:        time.Now().UnixMilli(),
	}
}

// SendHeartbeatForProxy sends a heartbeat for a specific proxy
func (t *PartitionTester) SendHeartbeatForProxy(proxyID string, consumptionScore int) bool {
	t.proxiesMu.RLock()
	_, exists := t.proxies[proxyID]
	t.proxiesMu.RUnlock()

	if !exists {
		return false
	}

	select {
	case t.heartbeatCh <- model.ProxyHeartbeat{
		ProxyID:          proxyID,
		Topic:            t.topic,
		ConsumptionScore: consumptionScore,
		Timestamp:        time.Now().UnixMilli(),
	}:
		return true
	case <-t.ctx.Done():
		return false
	}
}

// SendHeartbeats sends heartbeats periodically for the default proxy
func (t *PartitionTester) SendHeartbeats(consumptionScore int, interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.SendHeartbeat(consumptionScore)
		case <-stop:
			return
		case <-t.ctx.Done():
			return
		}
	}
}

// GetDispatchedIDs returns a copy of all dispatched job IDs
func (t *PartitionTester) GetDispatchedIDs() []string {
	t.dispatchedMu.Lock()
	defer t.dispatchedMu.Unlock()

	// Return a copy to avoid external modifications
	result := make([]string, len(t.dispatchedIDs))
	copy(result, t.dispatchedIDs)
	return result
}

// GetCounts returns the current counter values
func (t *PartitionTester) GetCounts() (success uint64, errors uint64, dispatched uint64, dropped uint64, doneSubmitted uint64) {
	return t.responsesSuccess.Load(),
		t.responsesError.Load(),
		t.jobsDispatched.Load(),
		t.jobsDropped.Load(),
		t.doneSubmitted.Load()
}

// WaitForDispatched waits until at least n jobs are dispatched
func (t *PartitionTester) WaitForDispatched(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if int(t.jobsDispatched.Load()) >= n {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// WaitForDropped waits until at least n jobs are dropped
func (t *PartitionTester) WaitForDropped(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if int(t.jobsDropped.Load()) >= n {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func (t *PartitionTester) WaitForDoneSubmitted(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if int(t.doneSubmitted.Load()) >= n {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// WaitForCondition waits until the condition function returns true or timeout expires
func (t *PartitionTester) WaitForCondition(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// GetDoneSubmittedCount
func (t *PartitionTester) GetDoneSubmittedCount() uint64 {
	return t.doneSubmitted.Load()
}

// SetAutoDone enables or disables auto-done
func (t *PartitionTester) SetAutoDone(enabled bool) {
	t.autoDoneEnabled.Store(enabled)
}

// Cleanup shuts down the partition and test harness
func (t *PartitionTester) Cleanup() {
	// Cancel context first - this stops all goroutines (including consumeProxy)
	t.cancel()

	// Signal partition to stop
	close(t.stopCh)

	// Wait for partition shutdown
	time.Sleep(50 * time.Millisecond)

	// Close the main command channels
	close(t.commandCh)
	// close(t.heartbeatCh)
	close(t.topologyCh)
	close(t.dropProposalCh)

	// Close the auto-done channel
	close(t.dispatchedJobs)

	// Note: We don't close proxy push channels here.
	// The consumeProxy goroutines exit via t.ctx.Done().
	// Closing them could cause "send on closed channel" panics
	// if the partition still tries to send during shutdown.
}
