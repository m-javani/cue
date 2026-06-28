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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
)

// =============================================================================
// Global Pools (shared across all partitions)
// =============================================================================

var jobSlicePool = sync.Pool{
	New: func() any {
		return make([]*model.Job, 0, 32)
	},
}

// DropProposal is sent to cluster agent for DLQ persistence
type DropProposal struct {
	JobID     string
	Topic     string
	Timestamp int64
}

// =============================================================================
// Partition - Main Structure
// =============================================================================

type Partition struct {
	topic string
	// Injected dependencies
	commandCh      <-chan model.Command
	heartbeatCh    <-chan model.ProxyHeartbeat
	topologyCh     <-chan ProxyTopologyUpdate
	dropProposalCh chan<- model.Command // commandCh in cluster agent
	status         *atomic.Uint32
	metrics        *internal.PartitionMetrics
	logger         *zap.Logger
	Config         *internal.PartitionConfig

	// Core data structures
	jobStore      *JobStore
	dispatchQueue *DispatchQueue

	proxyMap  *ProxyMap
	dlqBuffer *DLQBuffer

	// Graceful shutdown
	stopCh chan struct{}

	proxyPushChs map[string]chan model.ToGatewayMessage // (local copy)
	proxyPushMu  sync.RWMutex

	dispatchItems []dispatchItem

	lastCleanup time.Time
}

// NewPartition creates a new partition instance
func NewPartition(
	topic string,
	commandCh <-chan model.Command,
	heartbeatCh <-chan model.ProxyHeartbeat,
	topologyCh <-chan ProxyTopologyUpdate,
	dropProposalCh chan<- model.Command,
	status *atomic.Uint32,
	logger *zap.Logger,
	config *internal.PartitionConfig,
	stopCh chan struct{},
) *Partition {
	jobStoreCapacity := config.ActiveQueueCapacity + (int(max(config.MaxBackoffMs, 1000)/1000) * config.MaxRetries)
	metrics := internal.GetPartitionMetrics()
	jobStore := NewJobStore(jobStoreCapacity)
	dlqBuffer := NewDLQBuffer(topic, config.DLQMaxBytes, config.DLQMaxAgeMs, jobStore, logger, metrics)

	numBuckets := config.MaxRetries*int(config.MaxBackoffMs/1000) + 10
	dispatchQueue := NewDispatchQueue(numBuckets, config.ActiveQueueCapacity, dlqBuffer, jobStore)

	return &Partition{
		topic:          topic,
		commandCh:      commandCh,
		heartbeatCh:    heartbeatCh,
		topologyCh:     topologyCh,
		dropProposalCh: dropProposalCh,
		status:         status,
		metrics:        metrics,
		logger:         logger,
		Config:         config,

		jobStore:      jobStore,
		dispatchQueue: dispatchQueue,
		proxyMap:      NewProxyMap(),
		dlqBuffer:     dlqBuffer,

		proxyPushChs: make(map[string]chan model.ToGatewayMessage),

		stopCh:        stopCh,
		dispatchItems: make([]dispatchItem, 0, config.DispatchBatchSize),
	}
}

// Run starts the partition main loop
func (p *Partition) Run() {
	// Single ticker for everything
	ticker := time.NewTicker(time.Duration(p.Config.PartitionTickMs) * time.Millisecond)
	defer ticker.Stop()

	// Cleanup ticker
	proxyCleanupTicker := time.NewTicker(time.Duration(p.Config.ProxyCleanupTickSec) * time.Second)
	defer proxyCleanupTicker.Stop()

	// Partition heartbeat ticker
	heartbeatTicker := time.NewTicker(time.Duration(p.Config.HeartbeatTickMs) * time.Millisecond)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-p.stopCh:
			p.flushDLQIfNeeded()
			return

		case update := <-p.topologyCh:
			p.HandleTopologyUpdate(update)

		case heartbeat := <-p.heartbeatCh:
			p.handleHeartbeat(heartbeat)

		case cmd := <-p.commandCh:
			p.handleCommand(cmd)

		case <-proxyCleanupTicker.C:
			p.cleanupProxyMap()

		case <-heartbeatTicker.C:
			p.broadcastPartitionHeartbeat()

		case <-ticker.C:
			// Everything that needs periodic checking
			p.dispatch()
			p.cleanupDispatchQueue()
			p.flushDLQIfNeeded()
		}
	}
}

// HandleTopologyUpdate updates local proxy push channel map
func (p *Partition) HandleTopologyUpdate(update ProxyTopologyUpdate) {
	p.proxyPushMu.Lock()
	defer p.proxyPushMu.Unlock()

	switch update.Type {
	case "add":
		if update.PushCh != nil {
			p.proxyPushChs[update.ProxyID] = update.PushCh
		}
	case "remove":
		delete(p.proxyPushChs, update.ProxyID)
	}
}

// getJobSlice gets a slice from the pool
func (p *Partition) getJobSlice() []*model.Job {
	return jobSlicePool.Get().([]*model.Job)[:0]
}

// putJobSlice returns the slice to the pool
func (p *Partition) putJobSlice(s []*model.Job) {
	if s == nil {
		return
	}
	// prevent extremely large slices from polluting the pool
	if cap(s) > 512 {
		return // let it be GC'd
	}
	//nolint:staticcheck // SA6002 doesn't apply to []*model.Job
	jobSlicePool.Put(s[:0])
}

// handleCommand processes incoming commands
func (p *Partition) handleCommand(cmd model.Command) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("partition panicked",
				zap.Any("panic", r),
				zap.Stack("stacktrace"))
			// Re-panic to ensure the test knows about it
			panic(r)
		}
	}()

	switch cmd.Type {
	case model.CmdAddJob:
		p.handleAddJob(cmd)
	case model.CmdDone:
		p.handleDone(cmd)
	case model.CmdDrop:
		p.handleDrop(cmd)
	default:
		if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
			res := model.ToProducerResponse{
				RequestID: cmd.RespInfo.RequestID,
				Status:    model.ToProxyRespStatusError,
				Error:     "unknown command",
			}
			select {
			case cmd.RespInfo.RespCh <- res:
			default:
			}
		}
	}
}

func (p *Partition) handleAddJob(cmd model.Command) {
	if cmd.AddJob == nil {
		if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
			res := model.ToProducerResponse{
				RequestID: cmd.RespInfo.RequestID,
				Status:    model.ToProxyRespStatusError,
				Error:     internal.ErrInvalidPayload.Error(),
			}
			select {
			case cmd.RespInfo.RespCh <- res:
			default:
			}
		}
		return
	}

	job := &cmd.AddJob.Job
	if job.CreatedAt == 0 {
		job.CreatedAt = nowMilli()
	}

	// Create job in JobStore
	idx, err := p.jobStore.Create(job)
	if err != nil {
		if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
			res := model.ToProducerResponse{
				RequestID: cmd.RespInfo.RequestID,
				Status:    model.ToProxyRespStatusError,
				Error:     err.Error(),
			}
			select {
			case cmd.RespInfo.RespCh <- res:
			default:
			}
		}
		return
	}

	// Push to DispatchQueue
	jobRef := JobRef{Index: int(idx), RetryCount: 0}
	if err := p.dispatchQueue.AddNewJob(jobRef); err != nil {
		// DispatchQueue is full - release job and return error
		p.jobStore.Release(idx)
		if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
			res := model.ToProducerResponse{
				RequestID: cmd.RespInfo.RequestID,
				Status:    model.ToProxyRespStatusError,
				Error:     fmt.Sprintf("que is full, p.dispatchQueue.aHead: %d, p.dispatchQueue.aTail:%d, len(dq.active):%d", p.dispatchQueue.aHead, p.dispatchQueue.aTail, len(p.dispatchQueue.active)), //internal.ErrQueueFull.Error(),
			}
			select {
			case cmd.RespInfo.RespCh <- res:
			default:
			}
		}
		return
	}

	// Success
	if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
		res := model.ToProducerResponse{
			RequestID: cmd.RespInfo.RequestID,
			Status:    model.ToProxyRespStatusSuccess,
			Error:     "",
		}
		select {
		case cmd.RespInfo.RespCh <- res:
		default:
		}
	}

	p.metrics.JobAdded(p.topic, 1)
	p.metrics.SetActiveDepth(p.topic, uint32(p.dispatchQueue.ActiveQueueSize()))
}

func (p *Partition) handleDone(cmd model.Command) {
	if cmd.Done == nil {
		if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
			res := model.ToProducerResponse{
				RequestID: cmd.RespInfo.RequestID,
				Status:    model.ToProxyRespStatusError,
				Error:     internal.ErrInvalidPayload.Error(),
			}
			select {
			case cmd.RespInfo.RespCh <- res:
			default:
			}
		}
		return
	}

	for _, jobID := range cmd.Done.JobIDs {
		p.jobStore.MarkDone(jobID)
	}

	if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
		res := model.ToProducerResponse{
			RequestID: cmd.RespInfo.RequestID,
			Status:    model.ToProxyRespStatusSuccess,
			Error:     "",
		}
		select {
		case cmd.RespInfo.RespCh <- res:
		default:
		}
	}

	p.metrics.JobCompleted(p.topic, 1)
}

func (p *Partition) handleDrop(cmd model.Command) {
	if cmd.Drop == nil {
		if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
			res := model.ToProducerResponse{
				RequestID: cmd.RespInfo.RequestID,
				Status:    model.ToProxyRespStatusError,
				Error:     internal.ErrInvalidPayload.Error(),
			}
			select {
			case cmd.RespInfo.RespCh <- res:
			default:
			}
		}
		return
	}

	for _, jobID := range cmd.Drop.JobIDs {
		p.jobStore.ForceRelease(jobID)
	}

	if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
		res := model.ToProducerResponse{
			RequestID: cmd.RespInfo.RequestID,
			Status:    model.ToProxyRespStatusSuccess,
			Error:     "",
		}
		select {
		case cmd.RespInfo.RespCh <- res:
		default:
		}
	}
}

// handleHeartbeat processes proxy heartbeat updates
func (p *Partition) handleHeartbeat(hb model.ProxyHeartbeat) {
	if hb.Topic != p.getTopic() {
		return
	}
	p.proxyMap.Update(hb.ProxyID, hb.ConsumptionScore, hb.Timestamp)
}

// Inside processRetryableJobs, when re-queuing after timeout
func (p *Partition) CalculateRetryDelay(retryCount int) int64 {
	base := int64(p.Config.RetryBaseDelayMs)

	// Milder backoff: 1s → 2s → 4s → 8s → 16s → 30s → 60s cap
	// power of 2
	delay := min(base<<uint(retryCount), p.Config.MaxBackoffMs)

	// Add small jitter to avoid thundering herd
	jitter := delay / 10 // ±10%
	if jitter > 0 {
		// Simple jitter
		delay += (time.Now().UnixNano() % (jitter * 2)) - jitter
	}

	return delay
}

func (p *Partition) sendToDLQ(jobRef JobRef, job *model.Job) {
	if job == nil {
		return // Already released
	}
	payloadSize := int64(len(job.Data))
	p.dlqBuffer.Add(jobRef, payloadSize, job.Done)
}

// dispatch sends jobs to available proxies and handles job lifecycle
// It processes due jobs in order: Done → Expired/MaxRetries → Send to proxy
// dispatch sends jobs to available proxies and handles job lifecycle
func (p *Partition) dispatch() {
	if p.getStatus() != model.NodeStatusLeaderActive {
		return
	}
	if len(p.proxyMap.available) == 0 {
		return
	}

	// Select one proxy
	proxyID, consumerCount, ok := p.proxyMap.GetNextAvailable()
	if !ok || consumerCount <= 0 {
		return
	}

	p.proxyPushMu.RLock()
	pushCh, exists := p.proxyPushChs[proxyID]
	p.proxyPushMu.RUnlock()
	if !exists {
		return
	}

	// Channel capacity
	chCapacity := cap(pushCh) - len(pushCh)
	if chCapacity <= 0 {
		return
	}

	// Each consumer can handle jobsPerConsumer in this cycle
	// Total batch = consumers * jobs per consumer
	jobsPerConsumer := p.Config.DispatchBatchSize // e.g., 128, 256, 1024
	totalBatch := consumerCount * jobsPerConsumer

	// But respect channel capacity and config max
	batchSize := min(totalBatch, chCapacity)

	// Also cap by a global max to prevent overwhelming
	const maxTotalBatch = 4096
	if batchSize > maxTotalBatch {
		batchSize = maxTotalBatch
	}

	// Read batch - queue manages internal cursor state
	items := p.dispatchQueue.ReadBatch(batchSize, -1, -1, -1)

	if len(items) == 0 {
		p.metrics.SetActiveDepth(p.topic, uint32(p.dispatchQueue.ActiveQueueSize()))
		return
	}

	// Filter and prepare jobs
	jobSlice := p.getJobSlice()
	jobSlice = jobSlice[:0]
	validItems := make([]dispatchItem, 0, len(items))

	for _, item := range items {
		job := p.jobStore.Get(uint32(item.JobRef.Index))
		if job == nil || job.Done {
			p.dispatchQueue.RemoveByIndex(item.IsNew, item.Bucket, item.Cell)
			continue
		}

		if item.JobRef.RetryCount >= p.Config.MaxRetries {
			p.sendToDLQ(item.JobRef, job)
			p.dispatchQueue.RemoveByIndex(item.IsNew, item.Bucket, item.Cell)
			p.metrics.JobDLQ(p.topic, 1)
			continue
		}

		jobSlice = append(jobSlice, job)
		validItems = append(validItems, item)
	}

	if len(jobSlice) == 0 {
		p.putJobSlice(jobSlice)
		p.metrics.SetActiveDepth(p.topic, uint32(p.dispatchQueue.ActiveQueueSize()))
		return
	}

	// Send to proxy
	proxyMsg := model.ToProxyMessage{
		Type: model.ProxyMessageOutbound,
		Outbound: &model.ToConsumerMessage{
			Topic:   p.topic,
			ProxyID: proxyID,
			Jobs:    jobSlice,
		},
	}
	data, err := msgpack.Marshal(proxyMsg)
	if err != nil {
		p.logger.Error("failed to marshal", zap.Error(err))
		p.putJobSlice(jobSlice)
		p.metrics.SetActiveDepth(p.topic, uint32(p.dispatchQueue.ActiveQueueSize()))
		return
	}

	gatewayMsg := model.ToGatewayMessage{
		Type:       model.ToGatewayMessageConsumer,
		ToConsumer: data,
	}

	select {
	case pushCh <- gatewayMsg:
		now := nowMilli()
		for _, item := range validItems {
			item.JobRef.RetryCount++
			delay := p.CalculateRetryDelay(item.JobRef.RetryCount)
			retryTime := now + delay

			p.dispatchQueue.MoveDispatched(
				item.JobRef,
				retryTime,
				item.IsNew,
				item.Bucket,
				item.Cell,
			)
		}

	default:
		// Channel full - try next proxy next cycle
		p.putJobSlice(jobSlice)
	}

	p.metrics.SetActiveDepth(p.topic, uint32(p.dispatchQueue.ActiveQueueSize()))
}

// flushDLQIfNeeded proposes drops when buffer thresholds are met
func (p *Partition) flushDLQIfNeeded() {
	if p.dlqBuffer.ShouldFlush() {
		p.flushDLQ()
	}
}

func (p *Partition) flushDLQ() {
	drops := p.dlqBuffer.Flush()
	if len(drops) == 0 {
		return
	}

	var jobIDs []string
	for _, drop := range drops {
		jobIDs = append(jobIDs, drop.JobID)
	}

	select {
	case p.dropProposalCh <- model.Command{
		Type: model.CmdDrop,
		Drop: &model.DropPayload{
			Topic:  p.topic,
			JobIDs: jobIDs,
		},
		RespInfo: nil,
	}:
		p.logger.Debug("proposed DLQ batch", zap.Int("count", len(drops)))
	default:
	}
}

// cleanupProxyMap removes stale proxies
func (p *Partition) cleanupProxyMap() {
	now := nowMilli()
	var defaultProxyTimeoutSeconds int64 = 5
	p.proxyMap.CleanupStale(now, defaultProxyTimeoutSeconds)
}

// getStatus returns current node status
func (p *Partition) getStatus() model.ClusterNodeStatus {
	val := p.status.Load()
	return model.ClusterNodeStatusFromUint32(val)
}

// getTopic returns the partition topic
func (p *Partition) getTopic() string {
	return p.topic
}

func nowMilli() int64 {
	return time.Now().UnixMilli()
}

func (p *Partition) broadcastPartitionHeartbeat() {
	canAccept := true
	dq := p.dispatchQueue
	activeDepth := dq.ActiveQueueSize()

	if activeDepth > dq.bucketCap-dq.bucketCap/10 { // 10% breathing space
		canAccept = false
	}

	// ----------------------------
	// 4. Heartbeat
	// ----------------------------
	heartbeat := &model.PartitionHeartbeat{
		Topic:     p.topic,
		CanAccept: canAccept,
		Timestamp: nowMilli(),
	}

	msg := model.ToGatewayMessage{
		Type:      model.ToGatewayMessageHeartbeat,
		Heartbeat: heartbeat,
	}

	for _, pushCh := range p.proxyPushChs {
		select {
		case pushCh <- msg:
		default:
			// skip if busy
		}
	}
}

func (p *Partition) cleanupDispatchQueue() {
	if time.Since(p.lastCleanup) < 1*time.Second {
		return
	}
	p.lastCleanup = time.Now()
	// Zero-allocation cleanup
	p.dispatchQueue.CleanupOneExpiredBucket()
}
