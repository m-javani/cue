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
	"testing"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
	"go.uber.org/zap"
)

func TestDLQBuffer_AddAndFlush(t *testing.T) {
	jobStore := NewJobStore(100)
	metrics := internal.GetPartitionMetrics()
	logger, _ := zap.NewDevelopment()

	dlq := NewDLQBuffer("test-topic", 1024, 60000, jobStore, logger, metrics)

	// Create a job
	job := &model.Job{
		ID:    "job-1",
		Topic: "test-topic",
		Data:  []byte("test data"),
	}
	idx, err := jobStore.Create(job)
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	jobRef := JobRef{Index: int(idx), RetryCount: 0}

	// Add to DLQ
	shouldFlush := dlq.Add(jobRef, 100, false)
	if shouldFlush {
		t.Error("should not flush immediately for small buffer")
	}

	// Flush should return drop proposal
	drops := dlq.Flush()
	if len(drops) != 1 {
		t.Fatalf("expected 1 drop, got %d", len(drops))
	}
	if drops[0].JobID != "job-1" {
		t.Errorf("expected job-1, got %s", drops[0].JobID)
	}
	if drops[0].Topic != "test-topic" {
		t.Errorf("expected test-topic, got %s", drops[0].Topic)
	}

	// Job should STILL exist after Flush (release happens in handleDrop)
	if jobStore.Get(uint32(idx)) == nil {
		t.Error("job should still exist after flush (release happens in handleDrop)")
	}

	// Force release to clean up
	jobStore.ForceRelease("job-1")
	if jobStore.Get(uint32(idx)) != nil {
		t.Error("job should be released after ForceRelease")
	}
}

func TestDLQBuffer_FlushByMaxBytes(t *testing.T) {
	jobStore := NewJobStore(100)
	metrics := internal.GetPartitionMetrics()
	logger, _ := zap.NewDevelopment()

	// Small max bytes to trigger flush
	dlq := NewDLQBuffer("test-topic", 200, 60000, jobStore, logger, metrics)

	// Add first job (100 bytes)
	job1 := &model.Job{ID: "job-1", Topic: "test-topic", Data: make([]byte, 100)}
	idx1, _ := jobStore.Create(job1)
	dlq.Add(JobRef{Index: int(idx1), RetryCount: 0}, 100, false)

	// Should not flush yet
	if dlq.ShouldFlush() {
		t.Error("should not flush at 100 bytes with max 200")
	}

	// Add second job (100 bytes) - should hit max
	job2 := &model.Job{ID: "job-2", Topic: "test-topic", Data: make([]byte, 100)}
	idx2, _ := jobStore.Create(job2)
	dlq.Add(JobRef{Index: int(idx2), RetryCount: 0}, 100, false)

	// Should flush
	if !dlq.ShouldFlush() {
		t.Error("should flush at 200 bytes")
	}

	drops := dlq.Flush()
	if len(drops) != 2 {
		t.Errorf("expected 2 drops, got %d", len(drops))
	}

	// Jobs should STILL exist after flush
	if jobStore.Get(uint32(idx1)) == nil || jobStore.Get(uint32(idx2)) == nil {
		t.Error("jobs should still exist after flush (release happens in handleDrop)")
	}

	// Force release to clean up
	jobStore.ForceRelease("job-1")
	jobStore.ForceRelease("job-2")
	if jobStore.Get(uint32(idx1)) != nil || jobStore.Get(uint32(idx2)) != nil {
		t.Error("jobs should be released after ForceRelease")
	}
}

func TestDLQBuffer_FlushByMaxAge(t *testing.T) {
	jobStore := NewJobStore(100)
	metrics := internal.GetPartitionMetrics()
	logger, _ := zap.NewDevelopment()

	// Use actual time-based flush with short max age
	dlq := NewDLQBuffer("test-topic", 1024*1024, 100, jobStore, logger, metrics)

	job := &model.Job{ID: "job-1", Topic: "test-topic", Data: []byte("data")}
	idx, _ := jobStore.Create(job)

	// Add to DLQ
	dlq.Add(JobRef{Index: int(idx), RetryCount: 0}, 10, false)

	// Should not flush immediately
	if dlq.ShouldFlush() {
		t.Error("should not flush immediately")
	}

	// Wait for age threshold
	time.Sleep(150 * time.Millisecond)

	// Should flush by age
	if !dlq.ShouldFlush() {
		t.Error("should flush after max age")
	}

	drops := dlq.Flush()
	if len(drops) != 1 {
		t.Errorf("expected 1 drop, got %d", len(drops))
	}

	// Job should still exist
	if jobStore.Get(uint32(idx)) == nil {
		t.Error("job should still exist after flush (release happens in handleDrop)")
	}
}

func TestDLQBuffer_SkipAlreadyDone(t *testing.T) {
	jobStore := NewJobStore(100)
	metrics := internal.GetPartitionMetrics()
	logger, _ := zap.NewDevelopment()

	dlq := NewDLQBuffer("test-topic", 1024, 60000, jobStore, logger, metrics)

	// Create and mark done
	job := &model.Job{ID: "job-1", Topic: "test-topic", Data: []byte("data")}
	idx, _ := jobStore.Create(job)
	jobStore.MarkDone("job-1")

	jobRef := JobRef{Index: int(idx), RetryCount: 0}

	// Add to DLQ with alreadyDone=true - should release immediately
	shouldFlush := dlq.Add(jobRef, 100, true)
	if shouldFlush {
		t.Error("should not flush when skipping done job")
	}

	// Flush should return empty
	drops := dlq.Flush()
	if len(drops) != 0 {
		t.Errorf("expected 0 drops for done job, got %d", len(drops))
	}

	// Job should be released (because alreadyDone=true releases immediately)
	if jobStore.Get(uint32(idx)) != nil {
		t.Error("done job should be released immediately")
	}
}

func TestDLQBuffer_FlushSkipDoneJobs(t *testing.T) {
	jobStore := NewJobStore(100)
	metrics := internal.GetPartitionMetrics()
	logger, _ := zap.NewDevelopment()

	dlq := NewDLQBuffer("test-topic", 1024, 60000, jobStore, logger, metrics)

	// Create two jobs
	job1 := &model.Job{ID: "job-1", Topic: "test-topic", Data: []byte("data1")}
	idx1, _ := jobStore.Create(job1)

	job2 := &model.Job{ID: "job-2", Topic: "test-topic", Data: []byte("data2")}
	idx2, _ := jobStore.Create(job2)

	// Mark job1 as done
	jobStore.MarkDone("job-1")

	// Add both to DLQ
	dlq.Add(JobRef{Index: int(idx1), RetryCount: 0}, 100, false)
	dlq.Add(JobRef{Index: int(idx2), RetryCount: 0}, 100, false)

	// Flush should only drop job2 (job1 is done and should be released)
	drops := dlq.Flush()
	if len(drops) != 1 {
		t.Errorf("expected 1 drop, got %d", len(drops))
	}
	if drops[0].JobID != "job-2" {
		t.Errorf("expected job-2, got %s", drops[0].JobID)
	}

	// job1 should be released (done jobs released during flush)
	if jobStore.Get(uint32(idx1)) != nil {
		t.Error("job1 should be released (done during flush)")
	}

	// job2 should still exist (waiting for drop proposal to be processed)
	if jobStore.Get(uint32(idx2)) == nil {
		t.Error("job2 should still exist until drop proposal is processed")
	}

	// Force release job2 to clean up
	jobStore.ForceRelease("job-2")
	if jobStore.Get(uint32(idx2)) != nil {
		t.Error("job2 should be released after ForceRelease")
	}
}

func TestDLQBuffer_EmptyFlush(t *testing.T) {
	jobStore := NewJobStore(100)
	metrics := internal.GetPartitionMetrics()
	logger, _ := zap.NewDevelopment()

	dlq := NewDLQBuffer("test-topic", 1024, 60000, jobStore, logger, metrics)

	drops := dlq.Flush()
	if drops != nil {
		t.Errorf("expected nil for empty flush, got %v", drops)
	}
}

func TestDLQBuffer_AddTriggersFlush(t *testing.T) {
	jobStore := NewJobStore(100)
	metrics := internal.GetPartitionMetrics()
	logger, _ := zap.NewDevelopment()

	// Set max bytes to exactly match the job size so Add triggers flush
	dlq := NewDLQBuffer("test-topic", 100, 60000, jobStore, logger, metrics)

	job := &model.Job{ID: "job-1", Topic: "test-topic", Data: make([]byte, 100)}
	idx, _ := jobStore.Create(job)

	// Add job that exactly hits max bytes - should trigger flush
	shouldFlush := dlq.Add(JobRef{Index: int(idx), RetryCount: 0}, 100, false)
	if !shouldFlush {
		t.Error("should flush when bytes == maxBytes")
	}
}

func TestDLQBuffer_ReleaseOnAlreadyDone(t *testing.T) {
	jobStore := NewJobStore(100)
	metrics := internal.GetPartitionMetrics()
	logger, _ := zap.NewDevelopment()

	dlq := NewDLQBuffer("test-topic", 1024, 60000, jobStore, logger, metrics)

	// Create job
	job := &model.Job{ID: "job-1", Topic: "test-topic", Data: []byte("data")}
	idx, _ := jobStore.Create(job)

	// Mark as done
	jobStore.MarkDone("job-1")

	// Add with alreadyDone=true - this should call Release
	// The Release call in Add is the one we need to cover
	shouldFlush := dlq.Add(JobRef{Index: int(idx), RetryCount: 0}, 100, true)
	if shouldFlush {
		t.Error("should not flush when already done")
	}

	// Verify job was released
	if jobStore.Get(uint32(idx)) != nil {
		t.Error("job should have been released by Add when alreadyDone=true")
	}
}

func TestDLQBuffer_MultipleFlushes(t *testing.T) {
	jobStore := NewJobStore(100)
	metrics := internal.GetPartitionMetrics()
	logger, _ := zap.NewDevelopment()

	dlq := NewDLQBuffer("test-topic", 1024, 60000, jobStore, logger, metrics)

	// First batch
	job1 := &model.Job{ID: "job-1", Topic: "test-topic", Data: []byte("data1")}
	idx1, _ := jobStore.Create(job1)
	dlq.Add(JobRef{Index: int(idx1), RetryCount: 0}, 100, false)

	drops1 := dlq.Flush()
	if len(drops1) != 1 {
		t.Errorf("expected 1 drop in first flush, got %d", len(drops1))
	}

	// Second batch
	job2 := &model.Job{ID: "job-2", Topic: "test-topic", Data: []byte("data2")}
	idx2, _ := jobStore.Create(job2)
	dlq.Add(JobRef{Index: int(idx2), RetryCount: 0}, 100, false)

	drops2 := dlq.Flush()
	if len(drops2) != 1 {
		t.Errorf("expected 1 drop in second flush, got %d", len(drops2))
	}

	// Force release to clean up
	jobStore.ForceRelease("job-1")
	jobStore.ForceRelease("job-2")
}

func TestDLQBuffer_ShouldFlushLogic(t *testing.T) {
	jobStore := NewJobStore(100)
	metrics := internal.GetPartitionMetrics()
	logger, _ := zap.NewDevelopment()

	// Test max bytes condition
	dlq := NewDLQBuffer("test-topic", 100, 60000, jobStore, logger, metrics)

	// Initially should not flush
	if dlq.ShouldFlush() {
		t.Error("empty buffer should not flush")
	}

	// Add job that triggers max bytes
	job := &model.Job{ID: "job-1", Topic: "test-topic", Data: make([]byte, 100)}
	idx, _ := jobStore.Create(job)
	dlq.Add(JobRef{Index: int(idx), RetryCount: 0}, 100, false)

	if !dlq.ShouldFlush() {
		t.Error("should flush when bytes >= maxBytes")
	}

	// Test max age condition with a separate buffer
	dlq2 := NewDLQBuffer("test-topic", 1024*1024, 50, jobStore, logger, metrics)
	job2 := &model.Job{ID: "job-2", Topic: "test-topic", Data: []byte("data")}
	idx2, _ := jobStore.Create(job2)
	dlq2.Add(JobRef{Index: int(idx2), RetryCount: 0}, 10, false)

	// Should not flush immediately
	if dlq2.ShouldFlush() {
		t.Error("should not flush immediately by age")
	}

	// Wait for age
	time.Sleep(100 * time.Millisecond)

	if !dlq2.ShouldFlush() {
		t.Error("should flush when age >= maxAgeMs")
	}
}

func TestDLQBuffer_DoneJobInMiddle(t *testing.T) {
	jobStore := NewJobStore(100)
	metrics := internal.GetPartitionMetrics()
	logger, _ := zap.NewDevelopment()

	dlq := NewDLQBuffer("test-topic", 1024, 60000, jobStore, logger, metrics)

	// Create three jobs
	job1 := &model.Job{ID: "job-1", Topic: "test-topic", Data: []byte("data1")}
	idx1, _ := jobStore.Create(job1)

	job2 := &model.Job{ID: "job-2", Topic: "test-topic", Data: []byte("data2")}
	idx2, _ := jobStore.Create(job2)

	job3 := &model.Job{ID: "job-3", Topic: "test-topic", Data: []byte("data3")}
	idx3, _ := jobStore.Create(job3)

	// Mark middle job as done
	jobStore.MarkDone("job-2")

	// Add all to DLQ
	dlq.Add(JobRef{Index: int(idx1), RetryCount: 0}, 100, false)
	dlq.Add(JobRef{Index: int(idx2), RetryCount: 0}, 100, false)
	dlq.Add(JobRef{Index: int(idx3), RetryCount: 0}, 100, false)

	drops := dlq.Flush()
	if len(drops) != 2 {
		t.Errorf("expected 2 drops (done job in middle skipped), got %d", len(drops))
	}

	// Check job IDs in drops
	foundJob1 := false
	foundJob3 := false
	for _, drop := range drops {
		if drop.JobID == "job-1" {
			foundJob1 = true
		}
		if drop.JobID == "job-3" {
			foundJob3 = true
		}
	}
	if !foundJob1 || !foundJob3 {
		t.Error("expected both job-1 and job-3 in drops")
	}

	// job2 should be released
	if jobStore.Get(uint32(idx2)) != nil {
		t.Error("job2 should be released during flush")
	}

	// job1 and job3 should still exist
	if jobStore.Get(uint32(idx1)) == nil || jobStore.Get(uint32(idx3)) == nil {
		t.Error("job1 and job3 should still exist until drop proposals are processed")
	}

	// Force release to clean up
	jobStore.ForceRelease("job-1")
	jobStore.ForceRelease("job-3")
}
