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

var (
	jobSlicePool = sync.Pool{
		New: func() any {
			return make([]*model.Job, 0, 4096)
		},
	}
	batchPool = sync.Pool{
		New: func() any {
			return make([]JobRef, 0, 4096)
		},
	}
	validRefsPool = sync.Pool{
		New: func() any {
			return make([]JobRef, 0, 4096)
		},
	}
	removeRefsPool = sync.Pool{
		New: func() any {
			return make([]JobRef, 0, 4096)
		},
	}
)

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
	jobStore     *JobStore
	processQueue *ProcessQueue

	proxyMap  *ProxyMap
	dlqBuffer *DLQBuffer

	// Graceful shutdown
	stopCh chan struct{}

	proxyPushChs map[string]chan model.ToGatewayMessage // (local copy)
	proxyPushMu  sync.RWMutex
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
	metrics := internal.GetPartitionMetrics()

	jobStoreCapacity := config.ActiveQueueCapacity + (int(config.MaxBackoffSec) * config.MaxRetries)
	jobStore := NewJobStore(jobStoreCapacity)
	dlqBuffer := NewDLQBuffer(topic, config.DLQMaxBytes, config.DLQMaxAgeMs, jobStore, logger, metrics)
	processQueue := NewProcessQueue(config.ActiveQueueCapacity)

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

		jobStore:     jobStore,
		processQueue: processQueue,
		proxyMap:     NewProxyMap(),
		dlqBuffer:    dlqBuffer,

		proxyPushChs: make(map[string]chan model.ToGatewayMessage),

		stopCh: stopCh,
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
	case model.CmdAddJobs:
		p.handleAddJobs(cmd)
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

// handleAddJobs now supports partial success + silently skips duplicates
func (p *Partition) handleAddJobs(cmd model.Command) {
	if cmd.AddJobs == nil {
		p.sendErrorResponse(cmd, "invalid payload")
		return
	}

	var failures []model.JobFailure

	for _, job := range cmd.AddJobs.Jobs {
		if job.Done {
			continue
		}
		if job.CreatedAt == 0 {
			job.CreatedAt = nowMilli()
		}

		// Create job in JobStore - silently ignore duplicates
		idx, err := p.jobStore.Create(&job)
		if err != nil {
			if err == internal.ErrDuplicateJobID {
				// Silently ignore duplicate (idempotent)
				continue
			}
			// Other error (rare)
			failures = append(failures, model.JobFailure{
				JobID:  job.ID,
				Reason: model.FailInternal,
			})
			continue // continue with other jobs
		}

		// Push to DispatchQueue
		dispatchRef := JobRef{
			Index:      int(idx),
			RetryCount: 0,
			DueTimeSec: p.processQueue.currentSec(),
		}

		if err := p.processQueue.AddNewJob(dispatchRef); err != nil {
			// Queue full - release the job we just created
			p.jobStore.Release(idx)
			failures = append(failures, model.JobFailure{
				JobID:  job.ID,
				Reason: model.FailQueueFull,
			})
			continue // continue with remaining jobs
		}

		p.metrics.JobAdded(p.topic, 1)
	}

	// Send response
	resp := model.ToProducerResponse{
		RequestID: cmd.RespInfo.RequestID,
	}

	if len(failures) == 0 {
		resp.Status = model.ToProxyRespStatusSuccess
	} else {
		resp.Status = model.ToProxyRespStatusError
		resp.Failures = failures
	}

	p.sendResponse(cmd, resp)
}

// Helper methods
func (p *Partition) sendErrorResponse(cmd model.Command, errMsg string) {
	if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
		res := model.ToProducerResponse{
			RequestID: cmd.RespInfo.RequestID,
			Status:    model.ToProxyRespStatusError,
			Error:     errMsg,
		}
		select {
		case cmd.RespInfo.RespCh <- res:
		default:
		}
	}
}

func (p *Partition) sendResponse(cmd model.Command, resp model.ToProducerResponse) {
	if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
		select {
		case cmd.RespInfo.RespCh <- resp:
		default:
		}
	}
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
	p.metrics.JobCompleted(p.topic, uint64(len(cmd.Done.JobIDs)))

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

// CalculateRetryDelaySec returns delay in milliseconds for the next retry.
// Uses mildly increasing delays. Minimum 1 second.
func (p *Partition) CalculateRetryDelaySec(retryCount int) int64 {
	if retryCount <= 0 {
		return 1
	}

	var delaySec int64

	switch retryCount {
	case 1:
		delaySec = 3
	case 2:
		delaySec = 6
	case 3:
		delaySec = 10
	case 4:
		delaySec = 15
	default: // retry 5 and above
		delaySec = 20
	}

	// Respect MaxBackoffSec
	if p.Config.MaxBackoffSec > 0 {
		if delaySec > p.Config.MaxBackoffSec {
			delaySec = p.Config.MaxBackoffSec
		}
	}

	// Enforce minimum 1 second
	if delaySec < 1 {
		delaySec = 1
	}

	return delaySec
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
	defer p.metrics.SetActiveDepth(p.topic, uint32(p.processQueue.ActiveSize()))

	if p.getStatus() != model.NodeStatusLeaderActive || len(p.proxyMap.available) == 0 {
		return
	}

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

	chCapacity := cap(pushCh) - len(pushCh)
	if chCapacity <= 0 {
		return
	}

	batchSize := min(consumerCount*p.Config.DispatchBatchSize, chCapacity)
	if batchSize > 4096 {
		batchSize = 4096
	}

	// Read ready jobs
	nowSec := time.Now().Unix()
	items := p.processQueue.ReadBatch(batchSize, nowSec, &batchPool)
	if len(items) == 0 {
		return
	}

	// Classification
	jobSlice := jobSlicePool.Get().([]*model.Job)[:0]
	validRefs := validRefsPool.Get().([]JobRef)[:0]
	removeCells := removeRefsPool.Get().([]JobRef)[:0]

	for _, item := range items {
		job := p.jobStore.Get(uint32(item.Index))
		if job == nil || job.Done {
			removeCells = append(removeCells, item)
			continue
		}

		if item.RetryCount >= p.Config.MaxRetries {
			p.sendToDLQ(item, job)
			p.metrics.JobDLQ(p.topic, 1)
			removeCells = append(removeCells, item)
			continue
		}

		jobSlice = append(jobSlice, job)
		validRefs = append(validRefs, item)
		if item.RetryCount > 0 {
			p.metrics.JobRetried(p.topic, 1)
		}
	}

	if len(jobSlice) == 0 {
		//nolint:staticcheck
		batchPool.Put(items[:0])
		//nolint:staticcheck
		jobSlicePool.Put(jobSlice[:0])
		//nolint:staticcheck
		validRefsPool.Put(validRefs[:0])
		// we still need to remove from queue if any removable jobref appended
		p.processQueue.RemoveCells(removeCells)
		//nolint:staticcheck
		removeRefsPool.Put(removeCells[:0])
		return
	}

	// Send to proxy
	data, err := msgpack.Marshal(model.ToProxyMessage{
		Type: model.ProxyMessageOutbound,
		Outbound: &model.ToConsumerMessage{
			Topic:   p.topic,
			ProxyID: proxyID,
			Jobs:    jobSlice,
		},
	})

	if err != nil {
		p.logger.Error("failed to marshal", zap.Error(err))
		//nolint:staticcheck
		batchPool.Put(items[:0])
		//nolint:staticcheck
		jobSlicePool.Put(jobSlice[:0])
		//nolint:staticcheck
		validRefsPool.Put(validRefs[:0])
		// we still need to remove from queue if any removable jobref appended
		p.processQueue.RemoveCells(removeCells)
		//nolint:staticcheck
		removeRefsPool.Put(removeCells[:0])
		return
	}

	sent := false
	select {
	case pushCh <- model.ToGatewayMessage{
		Type:       model.ToGatewayMessageConsumer,
		ToConsumer: data,
	}:
		sent = true
	default:
		// channel full - retry next cycle
	}

	// Post-send processing
	if sent {
		for i := range validRefs {
			ref := &validRefs[i]
			ref.RetryCount++
			delaySec := p.CalculateRetryDelaySec(ref.RetryCount)
			p.processQueue.UpdateRetry(ref.Cell, ref.RetryCount, delaySec)
		}
	}

	// Final cleanup - remove dead jobs
	p.processQueue.RemoveCells(removeCells)

	// Return pools
	//nolint:staticcheck
	batchPool.Put(items[:0])
	//nolint:staticcheck
	jobSlicePool.Put(jobSlice[:0])
	//nolint:staticcheck
	validRefsPool.Put(validRefs[:0])
	//nolint:staticcheck
	removeRefsPool.Put(removeCells[:0])
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
	dq := p.processQueue
	activeDepth := dq.ActiveSize()

	if activeDepth > cap(dq.records)-cap(dq.records)/10 { // 10% breathing space
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
