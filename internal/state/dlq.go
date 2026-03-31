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
	"github.com/m-javani/cue/internal"
	"go.uber.org/zap"
)

// DLQBuffer - Dead letter batch buffer
type DLQBuffer struct {
	jobs []struct {
		ref  JobRef
		size int64
	}
	bytes     int64
	lastFlush int64
	maxBytes  int64
	maxAgeMs  int64
	jobStore  *JobStore
	logger    *zap.Logger
	metrics   *internal.PartitionMetrics
	topic     string
}

func NewDLQBuffer(topic string, maxBytes int64, maxAgeMs int64, jobStore *JobStore, logger *zap.Logger, metrics *internal.PartitionMetrics) *DLQBuffer {
	return &DLQBuffer{
		topic: topic,
		jobs: make([]struct {
			ref  JobRef
			size int64
		}, 0, 10000),
		maxBytes:  maxBytes,
		maxAgeMs:  maxAgeMs,
		lastFlush: nowMilli(),
		jobStore:  jobStore,
		logger:    logger,
		metrics:   metrics,
	}
}

func (dlq *DLQBuffer) Size() int {
	return len(dlq.jobs)
}

func (dlq *DLQBuffer) ShouldFlush() bool {
	now := nowMilli()
	return dlq.bytes >= dlq.maxBytes ||
		(now-dlq.lastFlush) >= dlq.maxAgeMs
}

func (dlq *DLQBuffer) Add(jobRef JobRef, jobSize int64, alreadyDone bool) bool {
	if alreadyDone {
		dlq.jobStore.Release(uint32(jobRef.Index))
		return false
	}

	dlq.jobs = append(dlq.jobs, struct {
		ref  JobRef
		size int64
	}{jobRef, jobSize})

	dlq.bytes += jobSize
	dlq.metrics.JobDLQ(dlq.topic, 1)
	return dlq.ShouldFlush()
}

func (dlq *DLQBuffer) Flush() []DropProposal {
	if len(dlq.jobs) == 0 {
		return nil
	}

	drops := make([]DropProposal, 0, len(dlq.jobs))

	for _, item := range dlq.jobs {
		job := dlq.jobStore.Get(uint32(item.ref.Index))
		if job.Done {
			dlq.jobStore.Release(uint32(item.ref.Index))
			continue
		}

		drops = append(drops, DropProposal{
			JobID:     job.ID,
			Topic:     job.Topic,
			Timestamp: nowMilli(),
		})
	}

	// Reset
	dlq.jobs = dlq.jobs[:0]
	dlq.bytes = 0
	dlq.lastFlush = nowMilli()

	return drops
}
