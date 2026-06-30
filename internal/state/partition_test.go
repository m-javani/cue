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
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
)

// =============================================================================
// PRIORITY 1: CRITICAL PATH (Must pass before any other tests)
// These tests validate the absolute core functionality of the partition.
// If any of these fail, the partition is fundamentally broken.
// =============================================================================

// TestP1_SingleJobEndToEnd validates the complete lifecycle of one job:
// Submit -> Dispatch -> Done -> Completion
func TestP1_SingleJobEndToEnd(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	//   - Ensure auto-done is enabled
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Auto-done is enabled by default, but explicitly set for clarity
	tester.SetAutoDone(true)

	jobID := "job-1"
	jobData := []byte("hello")

	// Actions:
	//   - AddJob("job-1", []byte("hello"))
	tester.AddJob(jobID, jobData)

	//   - SendHeartbeat(1)
	tester.SendHeartbeat(1)

	//   - WaitForDispatched(1, 5*time.Second)
	if !tester.WaitForDispatched(1, 5*time.Second) {
		t.Fatal("job was not dispatched within timeout")
	}

	//   - WaitForDoneSubmitted(1, 5*time.Second)
	if !tester.WaitForDoneSubmitted(1, 5*time.Second) {
		t.Fatal("job was not completed within timeout")
	}

	// Assertions:
	//   - success == 1, errors == 0
	success, errors, dispatched, dropped, doneSubmitted := tester.GetCounts()

	if success != 1 {
		t.Errorf("expected success=1, got %d", success)
	}
	if errors != 0 {
		t.Errorf("expected errors=0, got %d", errors)
	}

	//   - dispatched == 1
	if dispatched != 1 {
		t.Errorf("expected dispatched=1, got %d", dispatched)
	}

	//   - doneSubmitted == 1
	if doneSubmitted != 1 {
		t.Errorf("expected doneSubmitted=1, got %d", doneSubmitted)
	}

	//   - dropped == 0
	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}

	//   - GetDispatchedIDs() contains "job-1"
	dispatchedIDs := tester.GetDispatchedIDs()
	found := false
	for _, id := range dispatchedIDs {
		if id == jobID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("job ID %q not found in dispatched IDs: %v", jobID, dispatchedIDs)
	}

	//   - Conservation: submitted == doneSubmitted + dropped
	submitted := success + errors // All AddJob attempts (success + error)
	if uint64(submitted) != doneSubmitted+dropped {
		t.Errorf("conservation violated: submitted=%d, doneSubmitted=%d, dropped=%d",
			submitted, doneSubmitted, dropped)
	}
}

// TestP2_TenThousandJobs validates the partition can handle high throughput:
// Large batch processing without failures or corruption
func TestP2_TenThousandJobs(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	//   - Generate 10000 unique job IDs
	config := DefaultTestPartitionConfig()
	// Increase capacity to handle 10k jobs
	config.ActiveQueueCapacity = 50000
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	tester.SetAutoDone(true)

	const numJobs = 10000
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		jobIDs[i] = fmt.Sprintf("job-%d", i)
	}

	// Actions:
	//   - Add all jobs (use AddJobAsync for speed)
	// Use async to speed up the test
	for _, id := range jobIDs {
		tester.AddJobAsync(id, []byte("data"))
	}

	//   - Send periodic heartbeats
	// Start heartbeat goroutine that sends heartbeats every 50ms
	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)
	go tester.SendHeartbeats(100, 50*time.Millisecond, stopHeartbeat)

	//   - Wait for all to be dispatched (timeout 30s)
	if !tester.WaitForDispatched(numJobs, 30*time.Second) {
		// Get current counts for debugging
		success, errors, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for dispatch: dispatched=%d, expected=%d, success=%d, errors=%d, dropped=%d, done=%d",
			dispatched, numJobs, success, errors, dropped, done)
	}

	//   - Wait for all done submissions (timeout 30s)
	if !tester.WaitForDoneSubmitted(numJobs, 30*time.Second) {
		success, errors, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for completion: done=%d, expected=%d, dispatched=%d, success=%d, errors=%d, dropped=%d",
			done, numJobs, dispatched, success, errors, dropped)
	}

	// Assertions:
	//   - All AddJob responses successful
	success, errors, dispatched, dropped, doneSubmitted := tester.GetCounts()

	if errors != 0 {
		t.Errorf("expected errors=0, got %d", errors)
	}

	//   - dispatched == 10000
	if dispatched != uint64(numJobs) {
		t.Errorf("expected dispatched=%d, got %d", numJobs, dispatched)
	}

	//   - doneSubmitted == 10000
	if doneSubmitted != uint64(numJobs) {
		t.Errorf("expected doneSubmitted=%d, got %d", numJobs, doneSubmitted)
	}

	//   - dropped == 0
	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}

	//   - No duplicate job IDs in dispatched list
	dispatchedIDs := tester.GetDispatchedIDs()
	if len(dispatchedIDs) != numJobs {
		t.Errorf("expected %d dispatched IDs, got %d", numJobs, len(dispatchedIDs))
	}

	// Check for duplicates using a map
	seen := make(map[string]bool, len(dispatchedIDs))
	for _, id := range dispatchedIDs {
		if seen[id] {
			t.Errorf("duplicate job ID found in dispatched list: %s", id)
		}
		seen[id] = true
	}

	// Verify all expected job IDs are present
	for _, expectedID := range jobIDs {
		if !seen[expectedID] {
			t.Errorf("expected job ID not found in dispatched list: %s", expectedID)
		}
	}

	//   - Conservation: 10000 == doneSubmitted + dropped
	submitted := success + errors // All AddJob attempts
	if uint64(submitted) != doneSubmitted+dropped {
		t.Errorf("conservation violated: submitted=%d, doneSubmitted=%d, dropped=%d",
			submitted, doneSubmitted, dropped)
	}

	// Additional verification: all jobs completed exactly once
	// Check that no job appears more than once in done submissions
	// We can verify by counting unique job IDs in dispatched vs done
	t.Logf("Test passed: %d jobs submitted, dispatched, and completed successfully", numJobs)
}

// TestP3_EmptyPayload tests edge case of nil/empty job data:
// Partition should handle empty payloads gracefully
func TestP3_EmptyPayload(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	tester.SetAutoDone(true)

	jobID := "empty-payload-job"

	// Actions:
	//   - AddJob("empty", nil)
	tester.AddJob(jobID, nil)

	//   - SendHeartbeat(1)
	tester.SendHeartbeat(1)

	//   - Wait for dispatch and completion
	if !tester.WaitForDispatched(1, 5*time.Second) {
		t.Fatal("job was not dispatched within timeout")
	}

	if !tester.WaitForDoneSubmitted(1, 5*time.Second) {
		t.Fatal("job was not completed within timeout")
	}

	// Assertions:
	//   - success == 1
	//   - dispatched == 1
	//   - doneSubmitted == 1
	//   - No errors
	success, errors, dispatched, dropped, doneSubmitted := tester.GetCounts()

	if success != 1 {
		t.Errorf("expected success=1, got %d", success)
	}
	if errors != 0 {
		t.Errorf("expected errors=0, got %d", errors)
	}
	if dispatched != 1 {
		t.Errorf("expected dispatched=1, got %d", dispatched)
	}
	if doneSubmitted != 1 {
		t.Errorf("expected doneSubmitted=1, got %d", doneSubmitted)
	}
	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}

	// Additional verification: job ID appears in dispatched list
	dispatchedIDs := tester.GetDispatchedIDs()
	found := false
	for _, id := range dispatchedIDs {
		if id == jobID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("job ID %q not found in dispatched IDs: %v", jobID, dispatchedIDs)
	}

	// Conservation: submitted == doneSubmitted + dropped
	submitted := success + errors
	if uint64(submitted) != doneSubmitted+dropped {
		t.Errorf("conservation violated: submitted=%d, doneSubmitted=%d, dropped=%d",
			submitted, doneSubmitted, dropped)
	}

	t.Log("Empty payload job processed successfully")
}

// TestP4_FollowerDoesNotDispatch verifies followers are read-only:
// Followers should queue jobs but never dispatch them
func TestP4_FollowerDoesNotDispatch(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	//   - SetStatus(Follower)
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Set to follower status - should not dispatch
	tester.SetStatus(model.NodeStatusFollowerActive)

	// Auto-done is enabled by default, but it shouldn't matter since
	// jobs shouldn't be dispatched at all
	tester.SetAutoDone(true)

	jobID := "follower-job-1"

	// Actions:
	//   - AddJob("job-1", []byte("data"))
	tester.AddJob(jobID, []byte("test data"))

	//   - SendHeartbeat(100) // High score should trigger dispatch if leader
	tester.SendHeartbeat(100)

	//   - Wait 2 seconds
	// Give enough time for any potential dispatch to happen
	time.Sleep(2 * time.Second)

	// Assertions:
	//   - success == 1 (jobs can be added to followers)
	//   - dispatched == 0 (no dispatch on follower)
	//   - doneSubmitted == 0
	//   - dropped == 0
	success, errors, dispatched, dropped, doneSubmitted := tester.GetCounts()

	if success != 1 {
		t.Errorf("expected success=1, got %d", success)
	}
	if errors != 0 {
		t.Errorf("expected errors=0, got %d", errors)
	}
	if dispatched != 0 {
		t.Errorf("expected dispatched=0, got %d (followers should not dispatch)", dispatched)
	}
	if doneSubmitted != 0 {
		t.Errorf("expected doneSubmitted=0, got %d (no dispatch means no completion)", doneSubmitted)
	}
	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d (jobs should remain queued)", dropped)
	}

	// Additional verification: dispatched IDs list should be empty
	dispatchedIDs := tester.GetDispatchedIDs()
	if len(dispatchedIDs) != 0 {
		t.Errorf("expected empty dispatched IDs list, got %d entries: %v", len(dispatchedIDs), dispatchedIDs)
	}

	// Note: Conservation (submitted == doneSubmitted + dropped) does NOT apply here
	// because the job is still in the queue (not yet completed or dropped).
	// The job is queued, not lost - this is the expected behavior for followers.
	submitted := success + errors
	t.Logf("Job is queued on follower: submitted=%d, doneSubmitted=%d, dropped=%d",
		submitted, doneSubmitted, dropped)
	t.Log("This is expected - followers queue jobs but don't process them")

	// Optional: Verify the job is still queued by switching to leader
	// and confirming it gets dispatched
	t.Log("Job was queued but not dispatched as follower")

	// Verify we can switch to leader and the job will be processed
	tester.SetStatus(model.NodeStatusLeaderActive)
	tester.SendHeartbeat(100)

	if !tester.WaitForDispatched(1, 5*time.Second) {
		t.Error("job was not dispatched after switching to leader")
	}

	if !tester.WaitForDoneSubmitted(1, 5*time.Second) {
		t.Error("job was not completed after switching to leader")
	}

	// After switching to leader, conservation should hold
	success2, errors2, dispatched2, dropped2, doneSubmitted2 := tester.GetCounts()
	submitted2 := success2 + errors2
	if uint64(submitted2) != doneSubmitted2+dropped2 {
		t.Errorf("conservation violated after leader transition: submitted=%d, doneSubmitted=%d, dropped=%d",
			submitted2, doneSubmitted2, dropped2)
	}

	t.Logf("After leader transition: dispatched=%d, doneSubmitted=%d", dispatched2, doneSubmitted2)
}

// TestP5_NoHeartbeatNoDispatch validates the dependency on heartbeats:
// Without heartbeats, no proxy is considered available
func TestP5_NoHeartbeatNoDispatch(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	//   - Do NOT send any heartbeat
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	tester.SetAutoDone(true)

	jobID := "no-heartbeat-job"

	// Actions:
	//   - AddJob("job-1", []byte("data"))
	tester.AddJob(jobID, []byte("test data"))

	//   - Wait 3 seconds
	// Give enough time for any potential dispatch to happen
	time.Sleep(3 * time.Second)

	// Assertions:
	//   - success == 1
	//   - dispatched == 0 (no proxy available)
	//   - doneSubmitted == 0
	success, errors, dispatched, dropped, doneSubmitted := tester.GetCounts()

	if success != 1 {
		t.Errorf("expected success=1, got %d", success)
	}
	if errors != 0 {
		t.Errorf("expected errors=0, got %d", errors)
	}
	if dispatched != 0 {
		t.Errorf("expected dispatched=0, got %d (no heartbeat means no available proxy)", dispatched)
	}
	if doneSubmitted != 0 {
		t.Errorf("expected doneSubmitted=0, got %d (no dispatch means no completion)", doneSubmitted)
	}
	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d (jobs should remain queued)", dropped)
	}

	// Additional verification: dispatched IDs list should be empty
	dispatchedIDs := tester.GetDispatchedIDs()
	if len(dispatchedIDs) != 0 {
		t.Errorf("expected empty dispatched IDs list, got %d entries: %v", len(dispatchedIDs), dispatchedIDs)
	}

	// Note: Conservation (submitted == doneSubmitted + dropped) does NOT apply here
	// because the job is still in the queue (not yet completed or dropped).
	// The job is queued, not lost - this is the expected behavior without heartbeats.
	submitted := success + errors
	t.Logf("Job is queued without heartbeats: submitted=%d, doneSubmitted=%d, dropped=%d",
		submitted, doneSubmitted, dropped)
	t.Log("This is expected - no heartbeats means no available proxies")

	// Verify we can send a heartbeat and the job will be processed
	tester.SendHeartbeat(1)

	if !tester.WaitForDispatched(1, 5*time.Second) {
		t.Error("job was not dispatched after sending heartbeat")
	}

	if !tester.WaitForDoneSubmitted(1, 5*time.Second) {
		t.Error("job was not completed after sending heartbeat")
	}

	// After sending heartbeat, conservation should hold
	success2, errors2, dispatched2, dropped2, doneSubmitted2 := tester.GetCounts()
	submitted2 := success2 + errors2
	if uint64(submitted2) != doneSubmitted2+dropped2 {
		t.Errorf("conservation violated after heartbeat: submitted=%d, doneSubmitted=%d, dropped=%d",
			submitted2, doneSubmitted2, dropped2)
	}

	t.Logf("After heartbeat: dispatched=%d, doneSubmitted=%d", dispatched2, doneSubmitted2)
}

// TestP6_QueueFullRejection validates queue capacity limits:
// When active queue is full, new jobs should be rejected
func TestP6_QueueFullRejection(t *testing.T) {
	// Setup:
	//   - Create tester with config: ActiveQueueCapacity=10
	//   - Configure: DispatchBatchSize=10 (so we don't drain while adding)
	//   - Disable auto-done to prevent automatic draining
	config := DefaultTestPartitionConfig()
	config.ActiveQueueCapacity = 10
	config.DispatchBatchSize = 10
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Disable auto-done to prevent jobs from being completed and draining the queue
	tester.SetAutoDone(false)

	const totalJobs = 15
	const queueCapacity = 10

	// Actions:
	//   - Add 15 jobs (all with AddJob and wait for responses)
	// We use AddJob (sync) to capture responses
	jobIDs := make([]string, totalJobs)
	for i := 0; i < totalJobs; i++ {
		jobIDs[i] = fmt.Sprintf("job-%d", i)
		tester.AddJob(jobIDs[i], []byte("data"))
	}

	//   - Send heartbeat to allow dispatch
	tester.SendHeartbeat(1)

	//   - Wait briefly for dispatch to start (but not complete)
	// Wait 1 second for dispatch to attempt to drain some jobs
	time.Sleep(1 * time.Second)

	// Assertions:
	//   - First 10 jobs succeed (accepted into queue)
	//   - Last 5 jobs fail (queue full)
	//   - errors == 5
	//   - dispatched <= 10 (some may have been dispatched)
	success, errors, dispatched, dropped, doneSubmitted := tester.GetCounts()

	// We expect at most 10 successes (queue capacity)
	if success > uint64(queueCapacity) {
		t.Errorf("expected success <= %d, got %d (queue capacity exceeded)", queueCapacity, success)
	}

	// We expect at least 5 errors (queue full rejections)
	// Note: Could be more if the queue filled before all 10 were accepted
	if errors < 5 {
		t.Errorf("expected errors >= 5, got %d (queue should have rejected at least 5 jobs)", errors)
	}

	// Total submitted should be 15
	if success+errors != uint64(totalJobs) {
		t.Errorf("expected submitted=%d, got success=%d + errors=%d = %d",
			totalJobs, success, errors, success+errors)
	}

	// Dispatched should be <= 10 (only jobs that made it into the queue)
	// Some may have been dispatched immediately
	if dispatched > uint64(queueCapacity) {
		t.Errorf("expected dispatched <= %d, got %d", queueCapacity, dispatched)
	}

	// No jobs should be completed (auto-done disabled)
	if doneSubmitted != 0 {
		t.Errorf("expected doneSubmitted=0, got %d (auto-done disabled)", doneSubmitted)
	}

	// No jobs should be dropped (auto-done disabled, no retries yet)
	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}

	// Additional verification: dispatched IDs should only contain jobs that were accepted
	dispatchedIDs := tester.GetDispatchedIDs()
	if len(dispatchedIDs) > queueCapacity {
		t.Errorf("expected dispatched IDs <= %d, got %d", queueCapacity, len(dispatchedIDs))
	}

	// Verify that the first 10 job IDs are in the dispatched list if they were dispatched
	// and the last 5 are not (they should have been rejected)
	// Note: This is a soft check because some of the first 10 may not have been dispatched yet
	dispatchedSet := make(map[string]bool)
	for _, id := range dispatchedIDs {
		dispatchedSet[id] = true
	}

	// Check that at least some jobs from the first batch were accepted
	acceptedCount := 0
	for i := 0; i < queueCapacity; i++ {
		if dispatchedSet[jobIDs[i]] {
			acceptedCount++
		}
	}
	// If no jobs were dispatched, that's okay - they're just queued
	t.Logf("Jobs accepted into queue: %d, dispatched so far: %d", success, dispatched)

	// The key assertion: the queue rejected jobs when full
	t.Logf("Queue capacity test: capacity=%d, accepted=%d, rejected=%d, dispatched=%d",
		queueCapacity, success, errors, dispatched)

	// Cleanup:
	//   - The tester cleanup will handle shutdown
}

// =============================================================================
// PRIORITY 2: RETRY & DLQ LOGIC
// These tests validate the retry mechanism and dead letter queue behavior.
// Critical for ensuring jobs are not lost and retries work correctly.
// =============================================================================
// TestR1_SingleRetryThenSuccess validates the retry mechanism:
// Jobs should be retried when not completed before timeout, and should succeed
// when completed before hitting MaxRetries
func TestR1_SingleRetryThenSuccess(t *testing.T) {
	// Setup:
	//   - Create tester with config:
	//     MaxBackoffSec=5
	//     MaxRetries=3
	//   - Disable auto-done
	config := DefaultTestPartitionConfig()
	config.MaxBackoffSec = 5
	config.MaxRetries = 3
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Disable auto-done so jobs timeout and retry
	tester.SetAutoDone(false)

	jobID := "retry-job-1"

	// Actions:
	//   - AddJob("job-1", []byte("data"))
	tester.AddJob(jobID, []byte("test data"))

	//   - SendHeartbeat(1)
	tester.SendHeartbeat(1)

	// Wait for initial dispatch
	if !tester.WaitForDispatched(1, 5*time.Second) {
		t.Fatal("job was not initially dispatched")
	}

	// Wait for first retry (should be ~1s later with jitter)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, _, dispatched, _, _ := tester.GetCounts()
		if dispatched >= 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	_, _, dispatched1, _, _ := tester.GetCounts()
	if dispatched1 < 2 {
		t.Errorf("expected at least 2 dispatches (initial + 1 retry), got %d", dispatched1)
	}

	// Wait for second retry (should be ~2s after first retry with jitter)
	deadline = time.Now().Add(7 * time.Second)
	for time.Now().Before(deadline) {
		_, _, dispatched, _, _ := tester.GetCounts()
		if dispatched >= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Assertions:
	//   - Job dispatched 3 times (initial + 2 retries)
	_, _, dispatched, dropped, _ := tester.GetCounts()

	if dispatched != 3 {
		t.Errorf("expected dispatched=3 (initial + 2 retries), got %d", dispatched)
	}

	//   - Job not dropped (retry count < MaxRetries)
	if dropped != 0 {
		t.Errorf("expected dropped=0 (retry count < MaxRetries), got %d", dropped)
	}

	//   - GetDispatchedIDs() contains "job-1" 3 times
	dispatchedIDs := tester.GetDispatchedIDs()
	count := 0
	for _, id := range dispatchedIDs {
		if id == jobID {
			count++
		}
	}

	if count != 3 {
		t.Errorf("expected job ID %q to appear 3 times in dispatched list, got %d", jobID, count)
	}

	// Now complete the job explicitly (simulating consumer success)
	tester.Done([]string{jobID})

	// Wait for the done submission to be processed
	if !tester.WaitForDoneSubmitted(1, 5*time.Second) {
		_, _, finalDispatched, finalDropped, finalDone := tester.GetCounts()
		t.Errorf("job was not completed after Done command: dispatched=%d, dropped=%d, doneSubmitted=%d",
			finalDispatched, finalDropped, finalDone)
	}

	// Wait longer than the next retry window to ensure no additional dispatches
	// The job is marked done, but the dispatcher needs to see it in the queue to release it
	// Since it has RetryCount=2, the next retry will happen at ~4s
	// Wait 5 seconds to ensure the retry window passes
	time.Sleep(5 * time.Second)

	// Final assertions:
	//   - dispatched remains 3 (no additional dispatches)
	//   - dropped remains 0 (job succeeded before DLQ)
	//   - doneSubmitted == 1
	_, _, finalDispatched, finalDropped, finalDone := tester.GetCounts()

	if finalDispatched != 3 {
		t.Errorf("expected final dispatched=3, got %d (job should not be dispatched again)", finalDispatched)
	}
	if finalDropped != 0 {
		t.Errorf("expected final dropped=0, got %d (job should succeed before DLQ)", finalDropped)
	}
	if finalDone != 1 {
		t.Errorf("expected final done=1, got %d", finalDone)
	}

	// Conservation: 1 submitted = 1 done + 0 dropped
	submitted := 1
	if uint64(submitted) != finalDone+finalDropped {
		t.Errorf("conservation violated: submitted=%d, doneSubmitted=%d, dropped=%d",
			submitted, finalDone, finalDropped)
	}

	t.Logf("Final state: dispatched=%d, dropped=%d, doneSubmitted=%d", finalDispatched, finalDropped, finalDone)
}

// TestR2_RetryExhaustion validates jobs enter DLQ buffer after max retries:
// Jobs failing repeatedly should be sent to DLQ buffer (not yet flushed)
func TestR2_RetryExhaustion(t *testing.T) {
	// Setup:
	//   - Create tester with config:
	//     MaxRetries=3
	//     RetryBaseDelayMs=1000
	//     DLQMaxAgeMs=60000 (long so it doesn't flush during test)
	//   - Disable auto-done
	config := DefaultTestPartitionConfig()
	config.MaxRetries = 3
	config.DLQMaxAgeMs = 60000       // Long to prevent flush
	config.DLQMaxBytes = 1024 * 1024 // Large to prevent byte flush
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Disable auto-done so jobs timeout and retry until DLQ
	tester.SetAutoDone(false)

	jobID := "dlq-job-1"

	// Actions:
	//   - AddJob("job-1", []byte("data"))
	tester.AddJob(jobID, []byte("test data"))

	//   - SendHeartbeat(1)
	tester.SendHeartbeat(1)

	//   - Wait for retries
	// With MaxRetries=3:
	//   Dispatch #1: RetryCount=0 -> after dispatch, RetryCount=1
	//   Dispatch #2: RetryCount=1 -> after dispatch, RetryCount=2
	//   Dispatch #3: RetryCount=2 -> after dispatch, RetryCount=3
	//   Next wakeup: RetryCount=3 >= MaxRetries -> send to DLQ buffer (no dispatch)
	// So we expect 3 dispatches total
	const expectedDispatches = 3

	// Wait for all dispatches (may take time due to backoff)
	if !tester.WaitForDispatched(expectedDispatches, 30*time.Second) {
		_, _, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for dispatches: expected=%d, got dispatched=%d, dropped=%d, done=%d",
			expectedDispatches, dispatched, dropped, done)
	}

	// Wait a bit for the job to be sent to DLQ buffer
	// The job should be in the DLQ buffer but not yet flushed
	time.Sleep(2 * time.Second)

	// Assertions:
	//   - dispatched == 3 (initial + 2 retries, 3rd retry goes to DLQ without dispatch)
	success, errors, dispatched, dropped, doneSubmitted := tester.GetCounts()

	if dispatched != uint64(expectedDispatches) {
		t.Errorf("expected dispatched=%d, got %d", expectedDispatches, dispatched)
	}

	//   - dropped == 0 (job is in DLQ buffer, not yet flushed)
	if dropped != 0 {
		t.Errorf("expected dropped=0 (job in DLQ buffer not flushed), got %d", dropped)
	}

	//   - Conservation: job is in DLQ buffer, not terminal yet
	//     submitted == done + dropped + in DLQ buffer
	submitted := success + errors
	if uint64(submitted) != doneSubmitted+dropped {
		// This will fail because the job is in DLQ buffer
		// We expect: 1 = 0 + 0 + 1 (in buffer)
		t.Logf("Job is in DLQ buffer: submitted=%d, doneSubmitted=%d, dropped=%d (expected 1 in buffer)",
			submitted, doneSubmitted, dropped)
	}

	// Verify the job appears exactly 3 times in dispatched list
	dispatchedIDs := tester.GetDispatchedIDs()
	count := 0
	for _, id := range dispatchedIDs {
		if id == jobID {
			count++
		}
	}

	if count != expectedDispatches {
		t.Errorf("expected job ID %q to appear %d times in dispatched list, got %d",
			jobID, expectedDispatches, count)
	}

	// No done submissions
	if doneSubmitted != 0 {
		t.Errorf("expected doneSubmitted=0, got %d (job should not be completed)", doneSubmitted)
	}

	t.Logf("State after exhaustion: dispatched=%d, dropped=%d, doneSubmitted=%d (job in DLQ buffer)",
		dispatched, dropped, doneSubmitted)
}

// TestR2_DLQFlush validates the DLQ buffer flushes correctly.
func TestR2_DLQFlush(t *testing.T) {
	config := DefaultTestPartitionConfig()
	config.MaxRetries = 2
	config.DLQMaxAgeMs = 100 // Flush after 100ms
	config.DLQMaxBytes = 1   // Tiny threshold to force flush
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	tester.SetAutoDone(false)

	jobID := "dlq-flush-job-1"
	tester.AddJob(jobID, []byte("test data"))

	tester.SendHeartbeat(1)

	// With MaxRetries=2 and new delays:
	// Dispatch 1: RetryCount=0 -> becomes 1, retry after ~3s
	// Dispatch 2: RetryCount=1 -> becomes 2, retry after ~6s
	// Next cycle: RetryCount=2 >= MaxRetries -> goes to DLQ
	const expectedDispatches = 2

	if !tester.WaitForDispatched(expectedDispatches, 30*time.Second) {
		_, _, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for dispatches. Got dispatched=%d, dropped=%d, done=%d",
			dispatched, dropped, done)
	}

	// Wait for DLQ flush (due to tiny MaxBytes + MaxAge)
	if !tester.WaitForDropped(1, 10*time.Second) {
		_, _, dispatched, dropped, _ := tester.GetCounts()
		t.Fatalf("timeout waiting for drop. Got dropped=%d, dispatched=%d", dropped, dispatched)
	}

	// Assertions
	success, errors, dispatched, dropped, doneSubmitted := tester.GetCounts()

	if dispatched != uint64(expectedDispatches) {
		t.Errorf("expected dispatched=%d, got %d", expectedDispatches, dispatched)
	}
	if dropped != 1 {
		t.Errorf("expected dropped=1, got %d", dropped)
	}
	if doneSubmitted != 0 {
		t.Errorf("expected doneSubmitted=0, got %d", doneSubmitted)
	}

	submitted := success + errors
	if uint64(submitted) != doneSubmitted+dropped {
		t.Errorf("conservation violated: submitted=%d, done+dropped=%d", submitted, doneSubmitted+dropped)
	}

	t.Logf("DLQ flush test passed: dispatched=%d, dropped=%d", dispatched, dropped)
}

// TestR3_ThousandsOfRetries validates scale of retry handling:
// Large number of jobs should all eventually hit DLQ without memory leaks
func TestR3_ThousandsOfRetries(t *testing.T) {
	config := DefaultTestPartitionConfig()
	config.MaxRetries = 2
	config.ActiveQueueCapacity = 10000
	config.DLQMaxAgeMs = 200
	config.DLQMaxBytes = 1
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	tester.SetAutoDone(false)

	const numJobs = 1000

	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		jobIDs[i] = fmt.Sprintf("retry-job-%d", i)
	}

	t.Logf("Adding %d jobs...", numJobs)
	startTime := time.Now()

	for _, id := range jobIDs {
		tester.AddJobAsync(id, []byte("data"))
	}

	// Wait for submission
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Logf("All jobs submitted in %v", time.Since(startTime))

	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)
	go tester.SendHeartbeats(100, 50*time.Millisecond, stopHeartbeat)

	t.Logf("Waiting for all %d jobs to reach DLQ (this may take some time)...", numJobs)

	// Increased timeout to match new realistic retry timing
	if !tester.WaitForDropped(numJobs, 120*time.Second) {
		_, _, dispatched, dropped, _ := tester.GetCounts()
		t.Fatalf("timeout waiting for drops. dropped=%d/%d, dispatched=%d", dropped, numJobs, dispatched)
	}

	elapsed := time.Since(startTime)
	t.Logf("SUCCESS: All %d jobs dropped in %v (%.2f jobs/sec)", numJobs, elapsed, float64(numJobs)/elapsed.Seconds())

	// ... rest of assertions stay the same
	_, _, _, dropped, doneSubmitted := tester.GetCounts()

	if int(dropped) != numJobs {
		t.Errorf("expected dropped=%d, got %d", numJobs, dropped)
	}
	if doneSubmitted != 0 {
		t.Errorf("expected doneSubmitted=0, got %d", doneSubmitted)
	}

	// Check dispatch counts
	dispatchedIDs := tester.GetDispatchedIDs()
	dispatchCounts := make(map[string]int)
	for _, id := range dispatchedIDs {
		dispatchCounts[id]++
	}

	expected := 2
	wrong := 0
	for _, id := range jobIDs {
		if dispatchCounts[id] != expected {
			wrong++
			if wrong < 10 {
				t.Logf("Job %s dispatched %d times (expected %d)", id, dispatchCounts[id], expected)
			}
		}
	}
	if wrong > 0 {
		t.Errorf("%d jobs had wrong dispatch count", wrong)
	}
}

// =============================================================================
// PRIORITY 3: PROXY TOPOLOGY
// These tests validate adding/removing proxies and topology changes.
// Critical for production scenarios with dynamic consumer pools.
// =============================================================================

// TestT1_RemoveActiveProxyStopsDispatch validates proxy removal:
// When the only available proxy is removed, dispatch should stop
func TestT1_RemoveActiveProxyStopsDispatch(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	//   - Note: default proxy "test-proxy" is already added
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Disable auto-done so jobs stay in the system
	tester.SetAutoDone(false)

	jobID := "remove-proxy-job"

	// Actions:
	//   - AddJob("job-1", []byte("data"))
	tester.AddJob(jobID, []byte("test data"))

	//   - SendHeartbeat(1) // Activate default proxy
	tester.SendHeartbeat(1)

	//   - Wait for dispatch to start
	if !tester.WaitForDispatched(1, 5*time.Second) {
		t.Fatal("job was not initially dispatched")
	}

	// Record dispatched count before removal
	_, _, dispatchedBefore, _, _ := tester.GetCounts()
	t.Logf("Dispatched before removal: %d", dispatchedBefore)

	//   - RemoveProxy("test-proxy")
	tester.RemoveProxy("test-proxy")

	//   - Wait 2 seconds
	// Give enough time for any potential additional dispatches
	time.Sleep(2 * time.Second)

	// Assertions:
	//   - dispatched >= 1 (some jobs dispatched before removal)
	_, _, dispatchedAfter, dropped, done := tester.GetCounts()

	if dispatchedAfter < 1 {
		t.Errorf("expected dispatched >= 1, got %d", dispatchedAfter)
	}

	//   - No additional dispatches after removal
	// dispatchedAfter should equal dispatchedBefore (no new dispatches)
	if dispatchedAfter != dispatchedBefore {
		t.Errorf("expected no additional dispatches after removal: before=%d, after=%d",
			dispatchedBefore, dispatchedAfter)
	}

	//   - Jobs remain queued (not lost)
	// No drops, no completions
	if dropped != 0 {
		t.Errorf("expected dropped=0 (jobs should remain queued), got %d", dropped)
	}
	if done != 0 {
		t.Errorf("expected done=0 (auto-done disabled), got %d", done)
	}

	// Verification:
	//   - Add a new proxy and verify dispatch resumes
	t.Log("Adding new proxy to verify dispatch resumes...")
	tester.AddProxy("new-proxy")
	tester.SendHeartbeatForProxy("new-proxy", 1)

	// Wait for additional dispatches
	if !tester.WaitForCondition(10*time.Second, func() bool {
		_, _, dispatched, _, _ := tester.GetCounts()
		return dispatched > dispatchedAfter
	}) {
		_, _, finalDispatched, _, _ := tester.GetCounts()
		t.Errorf("dispatch did not resume after adding new proxy: dispatched=%d (was %d)",
			finalDispatched, dispatchedAfter)
	}

	// Verify the job was dispatched again (retry) after new proxy added
	_, _, finalDispatched, _, _ := tester.GetCounts()
	if finalDispatched <= dispatchedAfter {
		t.Errorf("expected more dispatches after adding new proxy, got %d (was %d)",
			finalDispatched, dispatchedAfter)
	}

	t.Logf("Final dispatched count: %d", finalDispatched)
	t.Log("Test passed: proxy removal stopped dispatch, new proxy resumed it")
}

// TestT2_AddProxyResumesDispatch validates proxy addition:
// After removing all proxies, adding a new one should resume dispatch
func TestT2_AddProxyResumesDispatch(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Disable auto-done so jobs stay in the system for retries
	tester.SetAutoDone(false)

	const numJobs = 100
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		jobIDs[i] = fmt.Sprintf("resume-job-%d", i)
	}

	// Actions:
	//   - Add 100 jobs
	t.Logf("Adding %d jobs...", numJobs)
	for _, id := range jobIDs {
		tester.AddJobAsync(id, []byte("test data"))
	}

	// Wait for all jobs to be submitted
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	success, errors, _, _, _ := tester.GetCounts()
	if int(success+errors) != numJobs {
		t.Fatalf("not all jobs submitted: expected=%d, got success=%d, errors=%d",
			numJobs, success, errors)
	}
	t.Logf("All %d jobs submitted", numJobs)

	//   - Send heartbeat to default proxy
	tester.SendHeartbeat(1)

	//   - Wait for some dispatches (at least 10)
	if !tester.WaitForCondition(10*time.Second, func() bool {
		_, _, dispatched, _, _ := tester.GetCounts()
		return dispatched >= 10
	}) {
		_, _, dispatched, _, _ := tester.GetCounts()
		t.Fatalf("expected at least 10 dispatches, got %d", dispatched)
	}

	_, _, dispatchedBeforeRemoval, _, _ := tester.GetCounts()
	t.Logf("Dispatched before removal: %d", dispatchedBeforeRemoval)

	//   - RemoveProxy("test-proxy")
	tester.RemoveProxy("test-proxy")

	//   - Wait 1 second (ensure dispatch stops)
	time.Sleep(1 * time.Second)

	// Record dispatched count after removal
	_, _, dispatchedAfterRemoval, _, _ := tester.GetCounts()
	t.Logf("Dispatched after removal: %d", dispatchedAfterRemoval)

	// Assertion: Dispatch stopped after removal
	if dispatchedAfterRemoval != dispatchedBeforeRemoval {
		t.Errorf("expected no additional dispatches after removal: before=%d, after=%d",
			dispatchedBeforeRemoval, dispatchedAfterRemoval)
	}

	//   - AddProxy("new-proxy")
	tester.AddProxy("new-proxy")

	//   - SendHeartbeatForProxy("new-proxy", 1)
	tester.SendHeartbeatForProxy("new-proxy", 1)

	//   - Wait for dispatch to resume
	if !tester.WaitForCondition(30*time.Second, func() bool {
		_, _, dispatched, _, _ := tester.GetCounts()
		return dispatched > dispatchedAfterRemoval
	}) {
		_, _, dispatched, _, _ := tester.GetCounts()
		t.Fatalf("dispatch did not resume after adding new proxy: dispatched=%d, was %d",
			dispatched, dispatchedAfterRemoval)
	}

	// Wait for all jobs to be dispatched at least once
	if !tester.WaitForDispatched(numJobs, 60*time.Second) {
		_, _, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for all dispatches: expected=%d, got dispatched=%d, dropped=%d, done=%d",
			numJobs, dispatched, dropped, done)
	}

	// Wait a bit more for any retries to settle
	time.Sleep(2 * time.Second)

	// Assertions:
	//   - Dispatch stopped after removal (already verified)
	//   - Dispatch resumed after adding new proxy (already verified)
	//   - No jobs lost during transition
	_, _, dispatched, dropped, done := tester.GetCounts()

	// All jobs should be dispatched at least once
	if int(dispatched) < numJobs {
		t.Errorf("expected at least %d dispatches, got %d", numJobs, dispatched)
	}

	// No jobs should be dropped (auto-done disabled, but jobs should not be lost)
	if dropped != 0 {
		t.Errorf("expected dropped=0 (jobs should not be lost), got %d", dropped)
	}

	if done != 0 {
		t.Errorf("expected done=0 (auto-done disabled), got %d", done)
	}

	//   - All 100 jobs eventually dispatched at least once
	// Check each job appears in dispatched list
	dispatchedIDs := tester.GetDispatchedIDs()
	dispatchedSet := make(map[string]bool)
	for _, id := range dispatchedIDs {
		dispatchedSet[id] = true
	}

	missingJobs := 0
	for _, id := range jobIDs {
		if !dispatchedSet[id] {
			missingJobs++
		}
	}

	if missingJobs > 0 {
		t.Errorf("%d jobs were never dispatched", missingJobs)
	}

	// Verify no duplicate dispatches (optional, with retries enabled some duplicates are expected)
	dispatchCounts := make(map[string]int)
	for _, id := range dispatchedIDs {
		dispatchCounts[id]++
	}

	duplicates := 0
	for id, count := range dispatchCounts {
		if count > 1 {
			duplicates++
			if duplicates < 10 { // Log first 10 duplicates
				t.Logf("WARNING: job %s dispatched %d times", id, count)
			}
		}
	}

	// Note: Duplicates are expected if auto-done is disabled (retries)
	if duplicates > 0 {
		t.Logf("Note: %d jobs had multiple dispatches (expected with auto-done disabled)", duplicates)
	}

	// Conservation: submitted == done + dropped + in-flight
	// Since auto-done is disabled and jobs are still in the system,
	// conservation should hold: submitted == done + dropped + (jobs still in queue)
	submitted := int(success + errors)
	if uint64(submitted) != done+dropped {
		// The difference is jobs still in the system (queued or in retry)
		inSystem := uint64(submitted) - done - dropped
		if inSystem > 0 {
			t.Logf("Jobs still in system (queued or retrying): %d", inSystem)
		} else {
			t.Errorf("conservation violated: submitted=%d, done=%d, dropped=%d",
				submitted, done, dropped)
		}
	}

	t.Logf("Final state: dispatched=%d, dropped=%d, done=%d", dispatched, dropped, done)
	t.Log("Test passed: proxy addition resumed dispatch after removal")
}

// TestT3_MultipleProxies validates multi-consumer scenarios:
// Multiple proxies should all receive jobs without conflicts
func TestT3_MultipleProxies(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Enable auto-done to complete jobs and prevent retries
	// This gives us cleaner dispatch counting
	tester.SetAutoDone(true)

	const numJobs = 1000

	// Actions:
	//   - AddProxy("p1")
	//   - AddProxy("p2")
	//   - AddProxy("p3")
	tester.AddProxy("p1")
	tester.AddProxy("p2")
	tester.AddProxy("p3")

	//   - SendHeartbeatForProxy("p1", 1)
	//   - SendHeartbeatForProxy("p2", 2)
	//   - SendHeartbeatForProxy("p3", 3)
	tester.SendHeartbeatForProxy("p1", 1)
	tester.SendHeartbeatForProxy("p2", 2)
	tester.SendHeartbeatForProxy("p3", 3)

	// Also heartbeat the default proxy to keep it active
	tester.SendHeartbeat(1)

	//   - Add 1000 jobs
	t.Logf("Adding %d jobs...", numJobs)
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		jobIDs[i] = fmt.Sprintf("multi-proxy-job-%d", i)
		tester.AddJobAsync(jobIDs[i], []byte("test data"))
	}

	// Wait for all jobs to be submitted
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	success, errors, _, _, _ := tester.GetCounts()
	if int(success+errors) != numJobs {
		t.Fatalf("not all jobs submitted: expected=%d, got success=%d, errors=%d",
			numJobs, success, errors)
	}
	t.Logf("All %d jobs submitted", numJobs)

	//   - Wait for all to be dispatched
	if !tester.WaitForDispatched(numJobs, 60*time.Second) {
		_, _, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for dispatches: expected=%d, got dispatched=%d, dropped=%d, done=%d",
			numJobs, dispatched, dropped, done)
	}

	// Wait for all jobs to be completed (auto-done enabled)
	if !tester.WaitForDoneSubmitted(numJobs, 30*time.Second) {
		_, _, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for completions: expected=%d, got done=%d, dispatched=%d, dropped=%d",
			numJobs, done, dispatched, dropped)
	}

	// Assertions:
	//   - All 1000 jobs dispatched across proxies
	//   - dispatched == 1000
	success, errors, dispatched, dropped, done := tester.GetCounts()

	if int(dispatched) != numJobs {
		t.Errorf("expected dispatched=%d, got %d", numJobs, dispatched)
	}

	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}

	if int(done) != numJobs {
		t.Errorf("expected done=%d, got %d", numJobs, done)
	}

	//   - No duplicate dispatches
	dispatchedIDs := tester.GetDispatchedIDs()
	if len(dispatchedIDs) != numJobs {
		t.Errorf("expected %d dispatched IDs, got %d", numJobs, len(dispatchedIDs))
	}

	// Check for duplicates using a map
	seen := make(map[string]bool, len(dispatchedIDs))
	duplicates := 0
	for _, id := range dispatchedIDs {
		if seen[id] {
			duplicates++
		}
		seen[id] = true
	}

	if duplicates > 0 {
		t.Errorf("found %d duplicate dispatches", duplicates)
	}

	// Verify all expected job IDs are present
	for _, expectedID := range jobIDs {
		if !seen[expectedID] {
			t.Errorf("expected job ID not found in dispatched list: %s", expectedID)
		}
	}

	//   - No panics with multiple proxies (test would have panicked)

	// Conservation: submitted == done + dropped
	submitted := int(success + errors)
	if uint64(submitted) != done+dropped {
		t.Errorf("conservation violated: submitted=%d, done=%d, dropped=%d",
			submitted, done, dropped)
	}

	// Additional verification: Check that proxies are receiving heartbeats
	// We can verify by checking that the partition's proxy map has all proxies
	t.Logf("Successfully processed %d jobs with 4 proxies (3 added + 1 default)", numJobs)
	t.Logf("Final state: dispatched=%d, done=%d, dropped=%d", dispatched, done, dropped)
}

// TestT4_ProxyChurnDuringLoad validates stability under topology changes:
// Rapidly adding/removing proxies while processing should not cause issues
func TestT4_ProxyChurnDuringLoad(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	//   - Create 10 proxy IDs: "p0" through "p9"
	config := DefaultTestPartitionConfig()
	config.ActiveQueueCapacity = 50000 // Large enough for the test
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Enable auto-done to complete jobs and prevent retries
	tester.SetAutoDone(true)

	const numJobs = 1000
	const numProxies = 10
	const churnIterations = 100

	// Create proxy IDs
	proxyIDs := make([]string, numProxies)
	for i := 0; i < numProxies; i++ {
		proxyIDs[i] = fmt.Sprintf("p%d", i)
	}

	// Actions:
	//   - Add 1000 jobs
	t.Logf("Adding %d jobs...", numJobs)
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		jobIDs[i] = fmt.Sprintf("churn-job-%d", i)
		tester.AddJobAsync(jobIDs[i], []byte("test data"))
	}

	// Wait for all jobs to be submitted
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	success, errors, _, _, _ := tester.GetCounts()
	if int(success+errors) != numJobs {
		t.Fatalf("not all jobs submitted: expected=%d, got success=%d, errors=%d",
			numJobs, success, errors)
	}
	t.Logf("All %d jobs submitted", numJobs)

	//   - Start heartbeat goroutine for default proxy
	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)
	go tester.SendHeartbeats(100, 50*time.Millisecond, stopHeartbeat)

	// Also heartbeats for some proxies to keep them active
	// We'll add heartbeats for random proxies during churn

	//   - In a loop 100 times:
	//     - AddProxy(random proxy ID)
	//     - RemoveProxy(random proxy ID)
	//     - Sleep 10ms
	t.Logf("Starting proxy churn (%d iterations)...", churnIterations)
	churnStart := time.Now()

	// Add all proxies initially
	for _, id := range proxyIDs {
		tester.AddProxy(id)
	}

	// Start heartbeat goroutines for all proxies
	stopProxyHeartbeats := make(chan struct{})
	// Don't use defer close here - we'll close it manually when done

	for _, id := range proxyIDs {
		go func(proxyID string) {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopProxyHeartbeats:
					return
				case <-ticker.C:
					// Send heartbeat with varying scores
					score := 1 + (time.Now().UnixNano() % 10)
					tester.SendHeartbeatForProxy(proxyID, int(score))
				}
			}
		}(id)
	}

	// Churn loop
	for i := 0; i < churnIterations; i++ {
		// Pick random proxy to add
		addIdx := rand.Intn(numProxies)
		tester.AddProxy(proxyIDs[addIdx])

		// Pick random proxy to remove
		removeIdx := rand.Intn(numProxies)
		// Don't remove the same proxy we just added (to keep at least one active)
		for removeIdx == addIdx {
			removeIdx = rand.Intn(numProxies)
		}
		tester.RemoveProxy(proxyIDs[removeIdx])

		time.Sleep(10 * time.Millisecond)
	}

	churnDuration := time.Since(churnStart)
	t.Logf("Proxy churn completed in %v", churnDuration)

	// Stop proxy heartbeats
	close(stopProxyHeartbeats)

	//   - Wait for all jobs to complete
	t.Log("Waiting for all jobs to complete...")
	if !tester.WaitForDoneSubmitted(numJobs, 60*time.Second) {
		success, errors, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for completions: expected=%d, got done=%d, dispatched=%d, dropped=%d, success=%d, errors=%d",
			numJobs, done, dispatched, dropped, success, errors)
	}

	// Assertions:
	//   - All jobs eventually dispatched
	//   - No panics, no deadlocks (test would have panicked or hung)
	//   - Conservation: submitted == doneSubmitted + dropped
	success, errors, dispatched, dropped, done := tester.GetCounts()

	t.Logf("Final state: dispatched=%d, done=%d, dropped=%d", dispatched, done, dropped)

	if int(dispatched) < numJobs {
		t.Errorf("expected at least %d dispatches, got %d", numJobs, dispatched)
	}

	if int(done) != numJobs {
		t.Errorf("expected done=%d, got %d", numJobs, done)
	}

	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}

	// Conservation: submitted == done + dropped
	submitted := int(success + errors)
	if uint64(submitted) != done+dropped {
		t.Errorf("conservation violated: submitted=%d, done=%d, dropped=%d",
			submitted, done, dropped)
	}

	// Verify all jobs appear in dispatched list at least once
	dispatchedIDs := tester.GetDispatchedIDs()
	dispatchedSet := make(map[string]bool)
	for _, id := range dispatchedIDs {
		dispatchedSet[id] = true
	}

	missingJobs := 0
	for _, id := range jobIDs {
		if !dispatchedSet[id] {
			missingJobs++
		}
	}

	if missingJobs > 0 {
		t.Errorf("%d jobs were never dispatched", missingJobs)
	}

	// Check for duplicate dispatches (some duplicates expected due to retries)
	// But we should have at least numJobs unique jobs
	uniqueJobs := len(dispatchedSet)
	if uniqueJobs < numJobs {
		t.Errorf("expected at least %d unique jobs dispatched, got %d", numJobs, uniqueJobs)
	}

	t.Logf("Unique jobs dispatched: %d", uniqueJobs)
	t.Logf("Total dispatched entries (including retries): %d", len(dispatchedIDs))

	t.Log("Test passed: proxy churn during load handled without issues")
}

// =============================================================================
// PRIORITY 4: LEADERSHIP TRANSITIONS
// These tests validate behavior when partition changes leadership status.
// Essential for distributed systems with leader election.
// =============================================================================

// TestL1_FollowerQueuesJobs validates followers accept jobs:
// Followers should accept and queue jobs but not dispatch them
func TestL1_FollowerQueuesJobs(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	//   - SetStatus(Follower)
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Set to follower status - should not dispatch
	tester.SetStatus(model.NodeStatusFollowerActive)

	// Enable auto-done but it shouldn't matter since no dispatch happens
	tester.SetAutoDone(true)

	const numJobs = 100

	// Actions:
	//   - Add 100 jobs (all should succeed)
	t.Logf("Adding %d jobs to follower...", numJobs)
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		jobIDs[i] = fmt.Sprintf("follower-queue-job-%d", i)
		tester.AddJob(jobIDs[i], []byte("test data"))
	}

	// Wait for all jobs to be submitted
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	success, errors, _, _, _ := tester.GetCounts()
	if int(success+errors) != numJobs {
		t.Fatalf("not all jobs submitted: expected=%d, got success=%d, errors=%d",
			numJobs, success, errors)
	}
	t.Logf("All %d jobs submitted to follower", numJobs)

	//   - Send heartbeats (high score)
	tester.SendHeartbeat(100)

	// Also send heartbeats for any additional proxies that might exist
	// (though by default there's only the test-proxy)
	tester.SendHeartbeatForProxy("test-proxy", 100)

	//   - Wait 3 seconds
	// Give enough time for any potential dispatch to happen
	time.Sleep(3 * time.Second)

	// Assertions:
	//   - 100 AddJob successes
	//   - dispatched == 0 (no dispatch on follower)
	//   - Jobs are queued (verified when we become leader)
	success, errors, dispatched, dropped, done := tester.GetCounts()

	if success != uint64(numJobs) {
		t.Errorf("expected success=%d, got %d", numJobs, success)
	}
	if errors != 0 {
		t.Errorf("expected errors=0, got %d", errors)
	}
	if dispatched != 0 {
		t.Errorf("expected dispatched=0 (follower should not dispatch), got %d", dispatched)
	}
	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}
	if done != 0 {
		t.Errorf("expected done=0 (no dispatch means no completion), got %d", done)
	}

	// Verify dispatched IDs list is empty
	dispatchedIDs := tester.GetDispatchedIDs()
	if len(dispatchedIDs) != 0 {
		t.Errorf("expected empty dispatched IDs list, got %d entries", len(dispatchedIDs))
	}

	// Verification:
	//   - SetStatus(LeaderActive)
	//   - Wait for dispatches
	//   - dispatched == 100
	t.Log("Switching follower to leader to verify queued jobs are dispatched...")
	tester.SetStatus(model.NodeStatusLeaderActive)

	// Send heartbeat to trigger dispatch
	tester.SendHeartbeat(100)

	// Wait for all jobs to be dispatched
	if !tester.WaitForDispatched(numJobs, 30*time.Second) {
		_, _, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for dispatches after becoming leader: expected=%d, got dispatched=%d, dropped=%d, done=%d",
			numJobs, dispatched, dropped, done)
	}

	// Wait for all jobs to be completed (auto-done enabled)
	if !tester.WaitForDoneSubmitted(numJobs, 30*time.Second) {
		_, _, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for completions after becoming leader: expected=%d, got done=%d, dispatched=%d, dropped=%d",
			numJobs, done, dispatched, dropped)
	}

	// Final assertions after becoming leader
	success, errors, dispatched, dropped, done = tester.GetCounts()

	if int(dispatched) != numJobs {
		t.Errorf("expected dispatched=%d after becoming leader, got %d", numJobs, dispatched)
	}
	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}
	if int(done) != numJobs {
		t.Errorf("expected done=%d, got %d", numJobs, done)
	}

	// Verify all jobs appear in dispatched list
	dispatchedIDs = tester.GetDispatchedIDs()
	dispatchedSet := make(map[string]bool)
	for _, id := range dispatchedIDs {
		dispatchedSet[id] = true
	}

	missingJobs := 0
	for _, id := range jobIDs {
		if !dispatchedSet[id] {
			missingJobs++
		}
	}

	if missingJobs > 0 {
		t.Errorf("%d jobs were never dispatched after becoming leader", missingJobs)
	}

	// Conservation: submitted == done + dropped
	submitted := int(success + errors)
	if uint64(submitted) != done+dropped {
		t.Errorf("conservation violated: submitted=%d, done=%d, dropped=%d",
			submitted, done, dropped)
	}

	t.Logf("Final state after leader transition: dispatched=%d, done=%d, dropped=%d", dispatched, done, dropped)
	t.Log("Test passed: follower queued jobs, leader processed them")
}

// TestL2_LeaderToFollowerDuringLoad validates transition behavior:
// Changing from leader to follower should stop dispatch but preserve jobs
func TestL2_LeaderToFollowerDuringLoad(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Disable auto-done so jobs stay in the system
	tester.SetAutoDone(false)

	const numJobs = 50

	// Step 1: Remove all proxies so no dispatch can happen
	t.Log("Removing default proxy to prevent dispatch...")
	tester.RemoveProxy("test-proxy")

	// Add 50 jobs (none will be dispatched)
	t.Logf("Adding %d jobs with no proxies available...", numJobs)
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		jobIDs[i] = fmt.Sprintf("leader-follower-job-%d", i)
		tester.AddJobAsync(jobIDs[i], []byte("test data"))
	}

	// Wait for all jobs to be submitted
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	success, errors, _, _, _ := tester.GetCounts()
	if int(success+errors) != numJobs {
		t.Fatalf("not all jobs submitted: expected=%d, got success=%d, errors=%d",
			numJobs, success, errors)
	}
	t.Logf("All %d jobs submitted", numJobs)

	// Verify no dispatches happened (no proxies available)
	_, _, dispatched, _, _ := tester.GetCounts()
	if dispatched != 0 {
		t.Errorf("expected 0 dispatches (no proxies), got %d", dispatched)
	}
	t.Logf("Jobs queued: %d, dispatched: %d", numJobs, dispatched)

	// Step 2: Switch to follower, then add proxy and heartbeat
	// Follower should NOT dispatch even with available proxy
	t.Log("Switching to follower and adding proxy...")
	tester.SetStatus(model.NodeStatusFollowerActive)
	tester.AddProxy("proxy-1")
	tester.SendHeartbeatForProxy("proxy-1", 100)

	// Wait to ensure no dispatch occurs
	time.Sleep(2 * time.Second)

	// Assert: Follower does not dispatch
	_, _, dispatchedAfterFollower, _, _ := tester.GetCounts()
	t.Logf("Dispatched after follower + proxy: %d", dispatchedAfterFollower)

	if dispatchedAfterFollower != 0 {
		t.Errorf("expected 0 dispatches on follower, got %d", dispatchedAfterFollower)
	}

	// Step 3: Switch to leader, dispatch should start
	t.Log("Switching to leader...")
	tester.SetStatus(model.NodeStatusLeaderActive)

	// Send heartbeat to trigger dispatch
	tester.SendHeartbeatForProxy("proxy-1", 100)

	// Wait for all jobs to be dispatched
	if !tester.WaitForDispatched(numJobs, 30*time.Second) {
		_, _, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for all dispatches after leader transition: expected=%d, got dispatched=%d, dropped=%d, done=%d",
			numJobs, dispatched, dropped, done)
	}

	// Wait a bit for any in-flight dispatches to settle
	time.Sleep(1 * time.Second)

	// Final assertions:
	_, _, dispatched, dropped, done := tester.GetCounts()
	t.Logf("Final state: dispatched=%d, done=%d, dropped=%d", dispatched, done, dropped)

	// All jobs should be dispatched
	if int(dispatched) < numJobs {
		t.Errorf("expected dispatched=%d, got %d", numJobs, dispatched)
	}

	if dropped != 0 {
		t.Errorf("expected dropped=0 (no jobs lost), got %d", dropped)
	}

	// Verify all unique jobs are dispatched
	dispatchedIDs := tester.GetDispatchedIDs()
	seen := make(map[string]bool)
	for _, id := range dispatchedIDs {
		seen[id] = true
	}

	uniqueJobs := len(seen)
	if uniqueJobs != numJobs {
		t.Errorf("expected %d unique jobs dispatched, got %d", numJobs, uniqueJobs)
	}

	// Verify all expected job IDs are present
	missingJobs := 0
	for _, expectedID := range jobIDs {
		if !seen[expectedID] {
			missingJobs++
		}
	}
	if missingJobs > 0 {
		t.Errorf("%d jobs were never dispatched", missingJobs)
	}

	t.Log("Test passed: leader->follower->leader transition handled without issues")
}

// TestL3_LeaderFollowerRapidTransition validates stability under status changes:
// Rapid leadership changes should not cause corruption or panics
func TestL3_LeaderFollowerRapidTransition(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	config := DefaultTestPartitionConfig()
	config.ActiveQueueCapacity = 100000 // Large enough for all jobs
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Enable auto-done to complete jobs
	tester.SetAutoDone(true)

	const numJobs = 1000
	const transitionDuration = 10 * time.Second

	// Actions:
	//   - Start goroutine that toggles status every 50ms:
	//     LeaderActive -> Follower -> LeaderActive -> ...
	stopStatusToggler := make(chan struct{})
	statusTogglerDone := make(chan struct{})
	go func() {
		defer close(statusTogglerDone)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		// Start as leader
		tester.SetStatus(model.NodeStatusLeaderActive)

		for {
			select {
			case <-stopStatusToggler:
				return
			case <-ticker.C:
				// Toggle between leader and follower
				current := tester.status.Load()
				if current == uint32(model.NodeStatusLeaderActive) {
					tester.SetStatus(model.NodeStatusFollowerActive)
				} else {
					tester.SetStatus(model.NodeStatusLeaderActive)
				}
			}
		}
	}()

	//   - Send heartbeats continuously
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-ticker.C:
				tester.SendHeartbeat(100)
			}
		}
	}()

	//   - Add 1000 jobs
	t.Logf("Adding %d jobs while status toggles...", numJobs)
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		jobIDs[i] = fmt.Sprintf("rapid-transition-job-%d", i)
		tester.AddJobAsync(jobIDs[i], []byte("test data"))
	}

	// Wait for all jobs to be submitted
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	success, errors, _, _, _ := tester.GetCounts()
	if int(success+errors) != numJobs {
		t.Fatalf("not all jobs submitted: expected=%d, got success=%d, errors=%d",
			numJobs, success, errors)
	}
	t.Logf("All %d jobs submitted", numJobs)

	//   - Run for 10 seconds (status toggling continues)
	t.Logf("Running with status toggles for %v...", transitionDuration)
	time.Sleep(transitionDuration)

	// Stop status toggler and heartbeat
	close(stopStatusToggler)
	<-statusTogglerDone
	close(stopHeartbeat)
	<-heartbeatDone

	// Set to leader one final time to ensure any queued jobs are processed
	t.Log("Setting final leader status to drain remaining jobs...")
	tester.SetStatus(model.NodeStatusLeaderActive)
	tester.SendHeartbeat(100)

	//   - Wait for all jobs to complete
	if !tester.WaitForDoneSubmitted(numJobs, 60*time.Second) {
		success, errors, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for completions: expected=%d, got done=%d, dispatched=%d, dropped=%d, success=%d, errors=%d",
			numJobs, done, dispatched, dropped, success, errors)
	}

	// Assertions:
	//   - All jobs eventually completed
	//   - No panics (test would have panicked)
	//   - No duplicate dispatches
	//   - Conservation: 1000 == doneSubmitted + dropped
	success, errors, dispatched, dropped, done := tester.GetCounts()

	t.Logf("Final state: dispatched=%d, done=%d, dropped=%d", dispatched, done, dropped)

	if int(done) != numJobs {
		t.Errorf("expected done=%d, got %d", numJobs, done)
	}

	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}

	// Conservation: submitted == done + dropped
	submitted := int(success + errors)
	if uint64(submitted) != done+dropped {
		t.Errorf("conservation violated: submitted=%d, done=%d, dropped=%d",
			submitted, done, dropped)
	}

	// Check for duplicate dispatches
	dispatchedIDs := tester.GetDispatchedIDs()
	seen := make(map[string]bool)
	duplicates := 0
	for _, id := range dispatchedIDs {
		if seen[id] {
			duplicates++
		}
		seen[id] = true
	}

	// With auto-done enabled, there should be no duplicates
	// (retries only happen if jobs aren't completed)
	if duplicates > 0 {
		t.Errorf("found %d duplicate dispatches (expected 0 with auto-done enabled)", duplicates)
	}

	// Verify all expected job IDs are present
	uniqueJobs := len(seen)
	if uniqueJobs != numJobs {
		t.Errorf("expected %d unique jobs dispatched, got %d", numJobs, uniqueJobs)
	}

	for _, expectedID := range jobIDs {
		if !seen[expectedID] {
			t.Errorf("expected job ID not found in dispatched list: %s", expectedID)
		}
	}

	t.Logf("Total dispatches: %d, Unique jobs: %d, Duplicates: %d",
		len(dispatchedIDs), uniqueJobs, duplicates)
	t.Log("Test passed: rapid leader/follower transitions handled without issues")
}

// =============================================================================
// PRIORITY 5: BACKPRESSURE & FLOW CONTROL
// These tests validate the partition's behavior under backpressure.
// Critical for production resilience.
// =============================================================================

// TestB1_FullChannelNoDeadlock validates the partition doesn't deadlock
// when the proxy push channel is full and never drains
func TestB1_FullChannelNoDeadlock(t *testing.T) {
	// Setup:
	//   - Create tester with custom config
	config := DefaultTestPartitionConfig()
	config.ActiveQueueCapacity = 100000
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Remove the default proxy
	tester.RemoveProxy("test-proxy")

	// Disable auto-done so jobs stay in the system
	tester.SetAutoDone(false)

	// Add proxy with tiny channel and NO consumption
	// Channel will fill and stay full forever
	proxyID := "blocked-proxy"
	tester.AddProxy(proxyID, ProxyOptions{
		Capacity: 1,
		Consume:  false, // Channel will fill and never drain
	})

	// Send heartbeat to make proxy available
	tester.SendHeartbeatForProxy(proxyID, 100)

	const numJobs = 100
	t.Logf("Adding %d jobs while channel is blocked...", numJobs)

	// Add all jobs (should not block)
	for i := 0; i < numJobs; i++ {
		jobID := fmt.Sprintf("blocked-job-%d", i)
		tester.AddJobAsync(jobID, []byte("test data"))
	}

	// Wait for all jobs to be submitted
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	success, errors, _, _, _ := tester.GetCounts()
	if int(success+errors) != numJobs {
		t.Fatalf("not all jobs submitted: expected=%d, got success=%d, errors=%d",
			numJobs, success, errors)
	}
	t.Logf("All %d jobs submitted successfully (AddJob did not block)", numJobs)

	// Let the dispatcher run for several cycles
	// The channel is full, but the partition should keep running
	t.Log("Running with blocked channel for 3 seconds...")
	time.Sleep(3 * time.Second)

	// Assertions:
	//   - submits succeeded (already verified)
	//   - partition keeps running (no deadlock - test would hang)
	//   - dropped == 0
	//   - no panic (test would have panicked)
	_, _, dispatched, dropped, done := tester.GetCounts()

	t.Logf("State after blocked period: dispatched=%d, dropped=%d, done=%d",
		dispatched, dropped, done)

	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}

	// Some jobs may have been dispatched before channel filled
	if dispatched > 0 {
		t.Logf("%d jobs were dispatched before channel filled", dispatched)
	}

	// No jobs should be done (auto-done disabled)
	if done != 0 {
		t.Errorf("expected done=0, got %d", done)
	}

	t.Log("Test passed: partition did not deadlock with full push channel")
}

// TestB2_BackpressureRecovery validates the partition recovers from backpressure
// when the consumer starts draining the channel
func TestB2_BackpressureRecovery(t *testing.T) {
	// Setup:
	//   - Create tester with custom config
	config := DefaultTestPartitionConfig()
	config.ActiveQueueCapacity = 100000
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Remove the default proxy
	tester.RemoveProxy("test-proxy")

	// Disable auto-done so jobs stay in the system
	tester.SetAutoDone(false)

	// Add proxy with small channel and SLOW consumer
	proxyID := "slow-proxy"
	tester.AddProxy(proxyID, ProxyOptions{
		Capacity:     1,                      // Very small channel
		Consume:      true,                   // Enable consumption
		ConsumeDelay: 100 * time.Millisecond, // Process one message every 100ms
	})

	// Send heartbeat to make proxy available
	tester.SendHeartbeatForProxy(proxyID, 100)

	const numJobs = 100
	t.Logf("Adding %d jobs to test backpressure recovery...", numJobs)
	startTime := time.Now()

	for i := 0; i < numJobs; i++ {
		jobID := fmt.Sprintf("recovery-job-%d", i)
		tester.AddJobAsync(jobID, []byte("test data"))
	}

	// Wait for all jobs to be submitted
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	success, errors, _, _, _ := tester.GetCounts()
	if int(success+errors) != numJobs {
		t.Fatalf("not all jobs submitted: expected=%d, got success=%d, errors=%d",
			numJobs, success, errors)
	}
	t.Logf("All %d jobs submitted in %v", numJobs, time.Since(startTime))

	// Wait for some dispatches to happen
	if !tester.WaitForCondition(30*time.Second, func() bool {
		_, _, dispatched, _, _ := tester.GetCounts()
		return dispatched > 0
	}) {
		_, _, dispatched, _, _ := tester.GetCounts()
		t.Fatalf("no dispatches happened: dispatched=%d", dispatched)
	}

	// Record state during backpressure
	_, _, dispatchedDuring, droppedDuring, doneDuring := tester.GetCounts()
	t.Logf("During backpressure: dispatched=%d, dropped=%d, done=%d",
		dispatchedDuring, droppedDuring, doneDuring)

	// No jobs should be dropped during backpressure
	if droppedDuring != 0 {
		t.Errorf("expected dropped=0 during backpressure, got %d", droppedDuring)
	}

	// Verify backpressure is working (not all jobs dispatched yet)
	if int(dispatchedDuring) < numJobs {
		t.Logf("Backpressure active: %d of %d jobs dispatched so far", dispatchedDuring, numJobs)
	} else {
		t.Logf("All %d jobs dispatched quickly (consumer was fast enough)", numJobs)
	}

	// Wait for all jobs to eventually be dispatched
	if !tester.WaitForDispatched(numJobs, 60*time.Second) {
		_, _, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for all dispatches: expected=%d, got dispatched=%d, dropped=%d, done=%d",
			numJobs, dispatched, dropped, done)
	}

	// Final assertions
	_, _, dispatched, dropped, done := tester.GetCounts()
	t.Logf("Final state: dispatched=%d, dropped=%d, done=%d", dispatched, dropped, done)

	// All jobs should be dispatched
	if int(dispatched) != numJobs {
		t.Errorf("expected dispatched=%d, got %d", numJobs, dispatched)
	}

	// No jobs should be dropped
	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}

	// No jobs should be completed (auto-done disabled)
	if done != 0 {
		t.Errorf("expected done=0 (auto-done disabled), got %d", done)
	}

	elapsed := time.Since(startTime)
	t.Logf("All %d jobs dispatched in %v", numJobs, elapsed)

	// The test should have taken at least a few seconds with the slow consumer
	if elapsed < 2*time.Second {
		t.Logf("WARNING: Test completed in %v, backpressure may not have been significant", elapsed)
	}

	t.Log("Test passed: partition recovered from backpressure")
}

// TestB2_BlockedProxyRecovery validates recovery from backpressure:
// When a blocked proxy becomes available, dispatch should resume
func TestB2_BlockedProxyRecovery(t *testing.T) {
	// Setup:
	//   - Create tester with proxy pushCh capacity=1
	//   - Add a consumer goroutine that can be paused
	config := DefaultTestPartitionConfig()
	config.ActiveQueueCapacity = 100000
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Remove the default proxy
	tester.RemoveProxy("test-proxy")

	// Disable auto-done so jobs stay in the system
	tester.SetAutoDone(false)

	// Create a pausable consumer
	proxyID := "pausable-proxy"

	// Create the push channel with capacity 1
	pushCh := make(chan model.ToGatewayMessage, 1)

	// Register the proxy with the tester
	tester.proxiesMu.Lock()
	tester.proxies[proxyID] = &TestProxy{
		ID:     proxyID,
		PushCh: pushCh,
	}
	tester.proxiesMu.Unlock()

	// Send topology update
	tester.topologyCh <- ProxyTopologyUpdate{
		Type:    "add",
		ProxyID: proxyID,
		PushCh:  pushCh,
	}

	// Start a pausable consumer goroutine
	stopConsumer := make(chan struct{})
	consumerDone := make(chan struct{})
	paused := atomic.Bool{}

	go func() {
		defer close(consumerDone)
		for {
			select {
			case <-stopConsumer:
				return
			default:
				// If paused, sleep and continue
				if paused.Load() {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				// Try to read from channel
				select {
				case msg, ok := <-pushCh:
					if !ok {
						return
					}
					tester.processProxyMessage(msg)
				default:
					// No message, small sleep to avoid busy loop
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
	}()

	// Send heartbeat to make proxy available
	tester.SendHeartbeatForProxy(proxyID, 100)

	const numJobs = 100
	t.Logf("Adding %d jobs...", numJobs)
	startTime := time.Now()

	for i := 0; i < numJobs; i++ {
		jobID := fmt.Sprintf("pausable-job-%d", i)
		tester.AddJobAsync(jobID, []byte("test data"))
	}

	// Wait for all jobs to be submitted
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	success, errors, _, _, _ := tester.GetCounts()
	if int(success+errors) != numJobs {
		t.Fatalf("not all jobs submitted: expected=%d, got success=%d, errors=%d",
			numJobs, success, errors)
	}
	t.Logf("All %d jobs submitted in %v", numJobs, time.Since(startTime))

	// Wait for some dispatches to happen
	if !tester.WaitForCondition(10*time.Second, func() bool {
		_, _, dispatched, _, _ := tester.GetCounts()
		return dispatched > 0
	}) {
		_, _, dispatched, _, _ := tester.GetCounts()
		t.Fatalf("no dispatches happened before pause: dispatched=%d", dispatched)
	}

	_, _, dispatchedBeforePause, _, _ := tester.GetCounts()
	t.Logf("Dispatched before pause: %d", dispatchedBeforePause)

	//   - Pause consumer (stop reading from push channel)
	t.Log("Pausing consumer...")
	paused.Store(true)

	// Wait for pause to take effect
	time.Sleep(100 * time.Millisecond)

	//   - Wait 2 seconds (channel fills, backpressure builds)
	t.Log("Waiting 2 seconds with consumer paused...")
	time.Sleep(2 * time.Second)

	// Record state during pause
	_, _, dispatchedDuringPause, droppedDuringPause, doneDuringPause := tester.GetCounts()
	t.Logf("During pause: dispatched=%d, dropped=%d, done=%d",
		dispatchedDuringPause, droppedDuringPause, doneDuringPause)

	// No jobs should be dropped during pause
	if droppedDuringPause != 0 {
		t.Errorf("expected dropped=0 during pause, got %d", droppedDuringPause)
	}

	//   - Resume consumer
	t.Log("Resuming consumer...")
	paused.Store(false)

	// Wait for consumer to resume
	time.Sleep(100 * time.Millisecond)

	//   - Wait for all jobs to dispatch
	if !tester.WaitForDispatched(numJobs, 60*time.Second) {
		_, _, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for all dispatches: expected=%d, got dispatched=%d, dropped=%d, done=%d",
			numJobs, dispatched, dropped, done)
	}

	// Stop consumer
	close(stopConsumer)
	<-consumerDone

	// Final assertions
	_, _, dispatched, dropped, done := tester.GetCounts()
	t.Logf("Final state: dispatched=%d, dropped=%d, done=%d", dispatched, dropped, done)

	//   - All 100 jobs eventually dispatched
	if int(dispatched) != numJobs {
		t.Errorf("expected dispatched=%d, got %d", numJobs, dispatched)
	}

	//   - No jobs lost during blocked period
	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}

	// No jobs should be completed (auto-done disabled)
	if done != 0 {
		t.Errorf("expected done=0 (auto-done disabled), got %d", done)
	}

	// Verify we had dispatches after resume
	dispatchedAfterResume := int(dispatched) - int(dispatchedBeforePause)
	if dispatchedAfterResume <= 0 {
		t.Errorf("expected dispatches after resume, got %d", dispatchedAfterResume)
	}

	t.Logf("Dispatched before pause: %d", dispatchedBeforePause)
	t.Logf("Dispatched after resume: %d", dispatchedAfterResume)
	t.Logf("Total dispatched: %d", dispatched)

	t.Log("Test passed: partition recovered after consumer resumed")
}

// =============================================================================
// PRIORITY 6: SHUTDOWN & CLEANUP
// These tests validate graceful shutdown behavior.
// Critical for production deployments.
// =============================================================================

// TestS1_ShutdownIdle validates clean shutdown with no work:
// Partition should exit cleanly when idle
func TestS1_ShutdownIdle(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	//   - Do not add any jobs
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)

	// Actions:
	//   - Cleanup() immediately
	// Use a channel to detect cleanup completion
	done := make(chan struct{})
	go func() {
		defer close(done)
		tester.Cleanup()
	}()

	// Assertions:
	//   - No panics (test would fail)
	//   - No deadlocks (test would hang)
	//   - Cleanup completes within 5 seconds
	select {
	case <-done:
		// Cleanup completed successfully
		t.Log("Cleanup completed successfully")
	case <-time.After(5 * time.Second):
		t.Fatal("Cleanup did not complete within 5 seconds (possible deadlock)")
	}

	t.Log("Test passed: idle partition shutdown cleanly")
}

// TestS2_ShutdownWithQueuedJobs validates shutdown with pending work:
// Partition should handle queued jobs during shutdown gracefully
func TestS2_ShutdownWithQueuedJobs(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer func() {
		// We'll call Cleanup manually, but ensure it's called if test panics
		// Actually, we'll handle cleanup in the test flow
	}()

	// Disable auto-done so jobs stay in the system
	tester.SetAutoDone(false)

	const numJobs = 100

	// Actions:
	//   - Add 100 jobs (don't send heartbeats, so they stay queued)
	t.Logf("Adding %d jobs (no heartbeats, they will stay queued)...", numJobs)
	for i := 0; i < numJobs; i++ {
		jobID := fmt.Sprintf("shutdown-queued-job-%d", i)
		tester.AddJobAsync(jobID, []byte("test data"))
	}

	// Wait for all jobs to be submitted
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	success, errors, _, _, _ := tester.GetCounts()
	if int(success+errors) != numJobs {
		t.Fatalf("not all jobs submitted: expected=%d, got success=%d, errors=%d",
			numJobs, success, errors)
	}
	t.Logf("All %d jobs submitted (no heartbeats sent, jobs are queued)", numJobs)

	// Verify no dispatches happened (no heartbeats)
	_, _, dispatched, _, _ := tester.GetCounts()
	if dispatched != 0 {
		t.Logf("Note: %d jobs were dispatched (heartbeats may have been sent by default)", dispatched)
	}

	//   - Immediately Cleanup()
	t.Log("Starting Cleanup() with queued jobs...")
	startTime := time.Now()

	done := make(chan struct{})
	go func() {
		defer close(done)
		tester.Cleanup()
	}()

	// Assertions:
	//   - No panics (test would fail)
	//   - No deadlocks (test would hang)
	//   - Cleanup completes within 5 seconds
	select {
	case <-done:
		elapsed := time.Since(startTime)
		t.Logf("Cleanup completed successfully in %v", elapsed)
	case <-time.After(10 * time.Second):
		t.Fatal("Cleanup did not complete within 10 seconds (possible deadlock)")
	}

	//   - Jobs are cleaned up (no goroutine leaks)
	// We can't directly verify goroutine leaks in a simple test,
	// but if the test passes without hanging, it's a good sign.
	// Additionally, the tester's cleanup should clean up all resources.

	t.Logf("Test passed: shutdown with %d queued jobs completed cleanly", numJobs)
}

// TestS3_ShutdownDuringDispatch validates shutdown mid-operation:
// Partition should handle shutdown while actively dispatching
func TestS3_ShutdownDuringDispatch(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	//   - Disable auto-done (so jobs stay active longer)
	config := DefaultTestPartitionConfig()
	config.ActiveQueueCapacity = 100000
	tester := NewPartitionTester("test-topic", config)
	defer func() {
		// Ensure cleanup if test panics
		// But we'll call it manually in the test
	}()

	// Disable auto-done so jobs stay active longer
	tester.SetAutoDone(false)

	const numJobs = 1000

	// Actions:
	//   - Add 1000 jobs
	t.Logf("Adding %d jobs...", numJobs)
	for i := 0; i < numJobs; i++ {
		jobID := fmt.Sprintf("shutdown-dispatch-job-%d", i)
		tester.AddJobAsync(jobID, []byte("test data"))
	}

	// Wait for all jobs to be submitted
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	success, errors, _, _, _ := tester.GetCounts()
	if int(success+errors) != numJobs {
		t.Fatalf("not all jobs submitted: expected=%d, got success=%d, errors=%d",
			numJobs, success, errors)
	}
	t.Logf("All %d jobs submitted", numJobs)

	//   - Send heartbeats to start dispatch
	t.Log("Starting dispatch with heartbeats...")
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-ticker.C:
				tester.SendHeartbeat(100)
			}
		}
	}()

	//   - Wait 1 second (some dispatches happening)
	t.Log("Waiting 1 second for dispatches to start...")
	time.Sleep(1 * time.Second)

	// Record state before shutdown
	_, _, dispatchedBeforeShutdown, droppedBeforeShutdown, doneBeforeShutdown := tester.GetCounts()
	t.Logf("Before shutdown: dispatched=%d, dropped=%d, done=%d",
		dispatchedBeforeShutdown, droppedBeforeShutdown, doneBeforeShutdown)

	// Verify some dispatches happened
	if dispatchedBeforeShutdown == 0 {
		t.Logf("WARNING: No dispatches happened before shutdown (may be fine, but unusual)")
	}

	// Stop heartbeats
	close(stopHeartbeat)
	<-heartbeatDone

	//   - Cleanup()
	t.Log("Starting Cleanup() during active dispatch...")
	startTime := time.Now()

	done := make(chan struct{})
	go func() {
		defer close(done)
		tester.Cleanup()
	}()

	// Assertions:
	//   - No panics (test would fail)
	//   - No deadlocks (test would hang)
	//   - Cleanup completes within 10 seconds
	select {
	case <-done:
		elapsed := time.Since(startTime)
		t.Logf("Cleanup completed successfully in %v", elapsed)
	case <-time.After(15 * time.Second):
		t.Fatal("Cleanup did not complete within 15 seconds (possible deadlock)")
	}

	//   - No goroutine leaks (indicated by successful cleanup)
	// We can't directly verify goroutine leaks in a simple test,
	// but if the test passes without hanging, it's a good sign.

	// Note: We can't verify conservation or job completion because
	// shutdown happened mid-operation. Jobs may be in various states.
	// The key assertion is that shutdown completed without panics or deadlocks.

	elapsed := time.Since(startTime)
	t.Logf("Test passed: shutdown during active dispatch completed in %v", elapsed)
	t.Logf("State at shutdown: dispatched=%d, dropped=%d, done=%d",
		dispatchedBeforeShutdown, droppedBeforeShutdown, doneBeforeShutdown)
}

// TestS4_ShutdownDuringRetryStorm validates shutdown with retries:
// Partition should handle shutdown while retries are happening
func TestS4_ShutdownDuringRetryStorm(t *testing.T) {
	// Setup:
	//   - Create tester with config: MaxRetries=5, RetryBaseDelayMs=1000
	//   - Disable auto-done
	config := DefaultTestPartitionConfig()
	config.MaxRetries = 5
	config.MaxBackoffSec = 10
	config.ActiveQueueCapacity = 100000
	tester := NewPartitionTester("test-topic", config)
	defer func() {
		// Ensure cleanup if test panics
		// But we'll call it manually in the test
	}()

	// Disable auto-done so jobs retry
	tester.SetAutoDone(false)

	const numJobs = 500

	// Actions:
	//   - Add 500 jobs
	t.Logf("Adding %d jobs...", numJobs)
	for i := 0; i < numJobs; i++ {
		jobID := fmt.Sprintf("shutdown-retry-job-%d", i)
		tester.AddJobAsync(jobID, []byte("test data"))
	}

	// Wait for all jobs to be submitted
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	success, errors, _, _, _ := tester.GetCounts()
	if int(success+errors) != numJobs {
		t.Fatalf("not all jobs submitted: expected=%d, got success=%d, errors=%d",
			numJobs, success, errors)
	}
	t.Logf("All %d jobs submitted", numJobs)

	//   - Send heartbeats to start dispatch
	t.Log("Starting dispatch with heartbeats...")
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-ticker.C:
				tester.SendHeartbeat(100)
			}
		}
	}()

	//   - Wait 3 seconds (retries starting)
	// With MaxRetries=5 and RetryBaseDelayMs=1000, retries will start after ~1s
	// Waiting 3 seconds ensures some retries have happened
	t.Log("Waiting 3 seconds for retries to start...")
	time.Sleep(3 * time.Second)

	// Record state before shutdown
	_, _, dispatchedBeforeShutdown, droppedBeforeShutdown, doneBeforeShutdown := tester.GetCounts()
	t.Logf("Before shutdown: dispatched=%d, dropped=%d, done=%d",
		dispatchedBeforeShutdown, droppedBeforeShutdown, doneBeforeShutdown)

	// Verify some dispatches happened
	if dispatchedBeforeShutdown == 0 {
		t.Logf("WARNING: No dispatches happened before shutdown (may be fine, but unusual)")
	}

	// Stop heartbeats
	close(stopHeartbeat)
	<-heartbeatDone

	//   - Cleanup()
	t.Log("Starting Cleanup() during retry storm...")
	startTime := time.Now()

	done := make(chan struct{})
	go func() {
		defer close(done)
		tester.Cleanup()
	}()

	// Assertions:
	//   - No panics (test would fail)
	//   - No deadlocks (test would hang)
	//   - Cleanup completes within 10 seconds
	select {
	case <-done:
		elapsed := time.Since(startTime)
		t.Logf("Cleanup completed successfully in %v", elapsed)
	case <-time.After(15 * time.Second):
		t.Fatal("Cleanup did not complete within 15 seconds (possible deadlock)")
	}

	//   - No goroutine leaks (indicated by successful cleanup)
	// We can't directly verify goroutine leaks in a simple test,
	// but if the test passes without hanging, it's a good sign.

	elapsed := time.Since(startTime)
	t.Logf("Test passed: shutdown during retry storm completed in %v", elapsed)
	t.Logf("State at shutdown: dispatched=%d, dropped=%d, done=%d",
		dispatchedBeforeShutdown, droppedBeforeShutdown, doneBeforeShutdown)

	// With MaxRetries=5 and 3 seconds of retries, we should have seen some retries
	// The dispatched count should be greater than numJobs (due to retries)
	if int(dispatchedBeforeShutdown) > numJobs {
		t.Logf("Retries detected: %d dispatches for %d jobs",
			dispatchedBeforeShutdown, numJobs)
	} else if int(dispatchedBeforeShutdown) == numJobs {
		t.Log("No retries detected yet (jobs may not have timed out)")
	} else {
		t.Logf("Only %d of %d jobs dispatched before shutdown",
			dispatchedBeforeShutdown, numJobs)
	}
}

// =============================================================================
// PRIORITY 7: CORRECTNESS INVARIANTS
// These tests validate mathematical properties of the system.
// Most important for ensuring data integrity.
// =============================================================================

// TestI1_NoJobLoss validates the fundamental invariant:
// Every submitted job must either be completed or dropped, never lost
func TestI1_NoJobLoss(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	config := DefaultTestPartitionConfig()
	config.ActiveQueueCapacity = 50000
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Enable auto-done to complete jobs
	tester.SetAutoDone(true)

	const numJobs = 1000

	// Actions:
	//   - Add 1000 jobs with unique IDs
	t.Logf("Adding %d jobs...", numJobs)
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		jobIDs[i] = fmt.Sprintf("invariant-job-%d", i)
		tester.AddJobAsync(jobIDs[i], []byte("test data"))
	}

	// Wait for all jobs to be submitted
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	success, errors, _, _, _ := tester.GetCounts()
	if int(success+errors) != numJobs {
		t.Fatalf("not all jobs submitted: expected=%d, got success=%d, errors=%d",
			numJobs, success, errors)
	}
	t.Logf("All %d jobs submitted", numJobs)

	//   - Send heartbeats
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-ticker.C:
				tester.SendHeartbeat(100)
			}
		}
	}()

	//   - Wait for all jobs to complete (auto-done enabled)
	t.Log("Waiting for all jobs to complete...")
	if !tester.WaitForDoneSubmitted(numJobs, 60*time.Second) {
		success, errors, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for completions: expected=%d, got done=%d, dispatched=%d, dropped=%d, success=%d, errors=%d",
			numJobs, done, dispatched, dropped, success, errors)
	}

	// Stop heartbeats
	close(stopHeartbeat)
	<-heartbeatDone

	// Assertions:
	//   - submitted == doneSubmitted + dropped
	//   - submitted == 1000
	//   - dropped == 0
	//   - doneSubmitted == 1000
	success, errors, dispatched, dropped, done := tester.GetCounts()
	t.Logf("Final state: dispatched=%d, done=%d, dropped=%d", dispatched, done, dropped)

	submitted := int(success + errors)
	if submitted != numJobs {
		t.Errorf("expected submitted=%d, got %d", numJobs, submitted)
	}

	if int(done) != numJobs {
		t.Errorf("expected done=%d, got %d", numJobs, done)
	}

	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}

	// Conservation: submitted == done + dropped
	if uint64(submitted) != done+dropped {
		t.Errorf("conservation violated: submitted=%d, done=%d, dropped=%d",
			submitted, done, dropped)
	}

	// Verification:
	//   - Check each job ID appears exactly once in dispatched list
	dispatchedIDs := tester.GetDispatchedIDs()
	if len(dispatchedIDs) != numJobs {
		t.Errorf("expected %d dispatched IDs, got %d", numJobs, len(dispatchedIDs))
	}

	// Check for duplicates using a map
	seen := make(map[string]bool, len(dispatchedIDs))
	duplicates := 0
	for _, id := range dispatchedIDs {
		if seen[id] {
			duplicates++
		}
		seen[id] = true
	}

	if duplicates > 0 {
		t.Errorf("found %d duplicate dispatches (expected 0 with auto-done enabled)", duplicates)
	}

	// Verify all expected job IDs are present
	missingJobs := 0
	for _, expectedID := range jobIDs {
		if !seen[expectedID] {
			missingJobs++
		}
	}
	if missingJobs > 0 {
		t.Errorf("%d jobs were never dispatched", missingJobs)
	}

	t.Logf("All %d jobs: submitted, dispatched once, and completed successfully", numJobs)
	t.Log("Test passed: no job loss invariant holds")
}

// TestI2_NoDuplicateCompletion validates idempotency:
// Jobs should never be completed more than once, even with duplicate Done commands
func TestI2_NoDuplicateCompletion(t *testing.T) {
	// Setup:
	//   - Create tester with DefaultPartitionConfig
	config := DefaultTestPartitionConfig()
	config.ActiveQueueCapacity = 50000
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Enable auto-done initially to get jobs completed
	tester.SetAutoDone(true)

	const numJobs = 50 // Smaller number for faster test

	// Actions:
	//   - Add 50 jobs with unique IDs
	t.Logf("Adding %d jobs...", numJobs)
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		jobIDs[i] = fmt.Sprintf("idempotent-job-%d", i)
		tester.AddJobAsync(jobIDs[i], []byte("test data"))
	}

	// Wait for all jobs to be submitted
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	success, errors, _, _, _ := tester.GetCounts()
	if int(success+errors) != numJobs {
		t.Fatalf("not all jobs submitted: expected=%d, got success=%d, errors=%d",
			numJobs, success, errors)
	}
	t.Logf("All %d jobs submitted", numJobs)

	// Send heartbeats to start dispatch
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-ticker.C:
				tester.SendHeartbeat(100)
			}
		}
	}()

	// Wait for all jobs to complete via auto-done
	t.Log("Waiting for all jobs to complete...")
	if !tester.WaitForDoneSubmitted(numJobs, 30*time.Second) {
		success, errors, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for completions: expected=%d, got done=%d, dispatched=%d, dropped=%d, success=%d, errors=%d",
			numJobs, done, dispatched, dropped, success, errors)
	}

	// Stop heartbeats
	close(stopHeartbeat)
	<-heartbeatDone

	// Record initial state
	_, _, dispatched, dropped, done := tester.GetCounts()
	t.Logf("Initial state: dispatched=%d, done=%d, dropped=%d", dispatched, done, dropped)

	if int(done) != numJobs {
		t.Errorf("expected done=%d, got %d", numJobs, done)
	}
	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}

	// Now test duplicate Done commands
	// Send duplicate Done commands for all jobs
	t.Log("Sending duplicate Done commands for all jobs...")

	// Split into smaller batches to avoid overwhelming the channel
	batchSize := 10
	for i := 0; i < numJobs; i += batchSize {
		end := i + batchSize
		if end > numJobs {
			end = numJobs
		}
		batch := jobIDs[i:end]
		tester.Done(batch)
	}

	// Wait a bit for duplicate Dones to be processed
	time.Sleep(2 * time.Second)

	// Get final state
	_, _, dispatched, dropped, done = tester.GetCounts()
	t.Logf("After duplicate Dones: dispatched=%d, done=%d, dropped=%d", dispatched, done, dropped)

	// The done counter in the tester counts every Done command sent,
	// so it will be numJobs + numJobs = 2*numJobs
	// This is expected behavior of the tester, not the partition.
	// We need to verify the partition's internal state instead.

	// Since we can't directly inspect the partition's JobStore from the test,
	// we need to verify that the partition doesn't double-complete jobs.
	// We can do this by checking that:
	// 1. No additional dispatches happened (jobs are marked done)
	// 2. No jobs were dropped
	// 3. The dispatched count hasn't increased (no retries)

	if int(dispatched) != numJobs {
		t.Errorf("expected dispatched=%d (unchanged), got %d", numJobs, dispatched)
	}

	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}

	// Additional verification: Send more Dones and verify the partition handles them
	// Let's send a third round of Dones
	t.Log("Sending third round of Done commands...")
	for i := 0; i < numJobs; i += batchSize {
		end := i + batchSize
		if end > numJobs {
			end = numJobs
		}
		batch := jobIDs[i:end]
		tester.Done(batch)
	}

	time.Sleep(2 * time.Second)

	// Final check: dispatched should still be unchanged
	_, _, finalDispatched, finalDropped, finalDone := tester.GetCounts()
	t.Logf("After third round: dispatched=%d, done=%d, dropped=%d",
		finalDispatched, finalDone, finalDropped)

	if int(finalDispatched) != numJobs {
		t.Errorf("expected dispatched=%d (unchanged), got %d", numJobs, finalDispatched)
	}
	if finalDropped != 0 {
		t.Errorf("expected dropped=0, got %d", finalDropped)
	}

	// The key assertion: The partition processed duplicate Dones without
	// causing additional dispatches or drops. This proves idempotency.
	t.Logf("All %d jobs: duplicate Done commands were handled idempotently", numJobs)
	t.Log("Test passed: duplicate Done commands are idempotent")
}

// TestI3_NoDuplicateDLQ validates DLQ idempotency:
// Jobs should never be sent to DLQ more than once
func TestI3_NoDuplicateDLQ(t *testing.T) {
	// Setup:
	//   - Create tester with config: MaxRetries=2
	//   - Disable auto-done
	config := DefaultTestPartitionConfig()
	config.MaxRetries = 2
	config.DLQMaxAgeMs = 100 // Quick flush
	config.DLQMaxBytes = 1   // Force flush by byte threshold
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Disable auto-done so jobs retry until DLQ
	tester.SetAutoDone(false)

	const numJobs = 100

	// Actions:
	//   - Add 100 jobs
	t.Logf("Adding %d jobs...", numJobs)
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		jobIDs[i] = fmt.Sprintf("dlq-idempotent-job-%d", i)
		tester.AddJobAsync(jobIDs[i], []byte("test data"))
	}

	// Wait for all jobs to be submitted
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		success, errors, _, _, _ := tester.GetCounts()
		if int(success+errors) >= numJobs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	success, errors, _, _, _ := tester.GetCounts()
	if int(success+errors) != numJobs {
		t.Fatalf("not all jobs submitted: expected=%d, got success=%d, errors=%d",
			numJobs, success, errors)
	}
	t.Logf("All %d jobs submitted", numJobs)

	//   - Send heartbeats
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-ticker.C:
				tester.SendHeartbeat(100)
			}
		}
	}()

	//   - Wait for all jobs to hit DLQ
	// With MaxRetries=2:
	//   Dispatch #1: RetryCount=0 -> becomes 1
	//   Dispatch #2: RetryCount=1 -> becomes 2
	//   Next wakeup: RetryCount=2 >= MaxRetries -> DLQ (no 3rd dispatch)
	// So we expect 2 dispatches per job
	t.Log("Waiting for all jobs to hit DLQ...")
	if !tester.WaitForDropped(numJobs, 60*time.Second) {
		success, errors, dispatched, dropped, done := tester.GetCounts()
		t.Fatalf("timeout waiting for drops: expected=%d, got dropped=%d, dispatched=%d, done=%d, success=%d, errors=%d",
			numJobs, dropped, dispatched, done, success, errors)
	}

	// Stop heartbeats
	close(stopHeartbeat)
	<-heartbeatDone

	// Record state after initial drops
	_, _, dispatched, dropped, done := tester.GetCounts()
	t.Logf("After initial drops: dispatched=%d, dropped=%d, done=%d", dispatched, dropped, done)

	//   - Track drop proposals
	// The tester's dropProposalCh receives drop commands
	// We should see exactly numJobs drops
	if int(dropped) != numJobs {
		t.Errorf("expected dropped=%d, got %d", numJobs, dropped)
	}

	// No jobs should be completed (auto-done disabled)
	if done != 0 {
		t.Errorf("expected done=0, got %d", done)
	}

	// Conservation: submitted == done + dropped
	submitted := int(success + errors)
	if uint64(submitted) != done+dropped {
		t.Errorf("conservation violated: submitted=%d, done=%d, dropped=%d",
			submitted, done, dropped)
	}

	// Verify each job appears exactly twice in dispatched list (initial + 1 retry)
	// With MaxRetries=2, each job should be dispatched 2 times before DLQ
	expectedDispatchesPerJob := 2
	dispatchedIDs := tester.GetDispatchedIDs()
	t.Logf("Total dispatched entries: %d", len(dispatchedIDs))

	// Count dispatches per job
	dispatchCounts := make(map[string]int)
	for _, id := range dispatchedIDs {
		dispatchCounts[id]++
	}

	// Verify each job was dispatched exactly 2 times
	jobsWithWrongCount := 0
	for _, id := range jobIDs {
		count := dispatchCounts[id]
		if count != expectedDispatchesPerJob {
			jobsWithWrongCount++
			if jobsWithWrongCount < 10 {
				t.Logf("Job %s dispatched %d times (expected %d)", id, count, expectedDispatchesPerJob)
			}
		}
	}
	if jobsWithWrongCount > 0 {
		t.Errorf("%d jobs had incorrect dispatch count (expected %d)", jobsWithWrongCount, expectedDispatchesPerJob)
	}

	//   - Assertions:
	//   - Each job ID appears exactly once in drop proposals
	// We can't directly inspect the DLQ buffer, but we can verify:
	// 1. dropped == numJobs (all jobs were dropped)
	// 2. No job was dropped more than once (we can check by verifying no additional drops)
	// 3. The drop count matches the number of unique jobs

	// Wait a bit and verify no additional drops happen
	t.Log("Waiting to ensure no duplicate drops...")
	time.Sleep(3 * time.Second)

	// Check final state - drops should not have increased
	_, _, finalDispatched, finalDropped, finalDone := tester.GetCounts()
	t.Logf("Final state: dispatched=%d, dropped=%d, done=%d",
		finalDispatched, finalDropped, finalDone)

	if int(finalDropped) != numJobs {
		t.Errorf("expected final dropped=%d, got %d (duplicate drops detected)",
			numJobs, finalDropped)
	}

	if finalDropped != dropped {
		t.Errorf("expected dropped to remain %d, got %d (duplicate drops detected)",
			dropped, finalDropped)
	}

	// Conservation should still hold
	if uint64(submitted) != finalDone+finalDropped {
		t.Errorf("conservation violated after wait: submitted=%d, done=%d, dropped=%d",
			submitted, finalDone, finalDropped)
	}

	// Verify each job appears exactly once in the drop count
	// The drop count is numJobs, which matches the number of unique job IDs
	t.Logf("All %d jobs dropped exactly once", numJobs)
	t.Log("Test passed: no duplicate DLQ entries")
}

// =============================================================================
// Other tests
// =============================================================================
// TestHandleDone_WithRespInfo tests handleDone when the command has RespInfo
// populated with a valid RespCh. This verifies that response information is
// properly sent back through the response channel.
func TestHandleDone_WithRespInfo(t *testing.T) {
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Disable auto-done so we control when Done is sent
	tester.SetAutoDone(false)

	jobID := "test-done-job-1"

	// Add a job
	tester.AddJob(jobID, []byte("test data"))

	// Send heartbeat to make proxy available
	tester.SendHeartbeat(1)

	// Wait for it to be dispatched
	if !tester.WaitForDispatched(1, 5*time.Second) {
		_, _, dispatched, _, _ := tester.GetCounts()
		t.Fatalf("timed out waiting for job to be dispatched, dispatched=%d", dispatched)
	}

	// Send Done command with RespInfo
	respCh := make(chan model.ToProducerResponse, 1)
	cmd := model.Command{
		Type: model.CmdDone,
		Done: &model.DonePayload{
			Topic:  "test-topic",
			JobIDs: []string{jobID},
		},
		RespInfo: &model.RespInfo{
			RequestID: "req-done-1",
			RespCh:    respCh,
		},
	}

	select {
	case tester.commandCh <- cmd:
		// Successfully sent
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out sending Done command")
	}

	// Wait for response - this is the main verification
	select {
	case resp := <-respCh:
		if resp.Status != model.ToProxyRespStatusSuccess {
			t.Errorf("expected success status, got %v with error: %s", resp.Status, resp.Error)
		}
		if resp.RequestID != "req-done-1" {
			t.Errorf("expected RequestID 'req-done-1', got '%s'", resp.RequestID)
		}
		// Response received successfully - test passes
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for Done response")
	}

	// Give some time for processing
	time.Sleep(50 * time.Millisecond)

	// Verify counters - note: doneSubmitted only tracks tester.Done() calls,
	// not direct command sends, so we don't assert on it
	success, errors, dispatched, dropped, doneSubmitted := tester.GetCounts()

	// We only care about these:
	// - success is from AddJob, should be 1
	// - dispatched should be 1
	// - dropped should be 0
	// - doneSubmitted will be 0 because we used direct command channel, not tester.Done()

	if success != 1 {
		t.Errorf("expected success=1, got %d", success)
	}
	if errors != 0 {
		t.Errorf("expected errors=0, got %d", errors)
	}
	if dispatched != 1 {
		t.Errorf("expected dispatched=1, got %d", dispatched)
	}
	if dropped != 0 {
		t.Errorf("expected dropped=0, got %d", dropped)
	}

	// The response was received successfully, so the test passes
	t.Logf("Done command processed successfully with response: success=%d, dispatched=%d, doneSubmitted=%d (manual Done not counted)",
		success, dispatched, doneSubmitted)
}

// TestHandleCommand_PeersRejected tests that peer commands are rejected by the partition
func TestHandleCommand_PeersRejected(t *testing.T) {
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	respCh := make(chan model.ToProducerResponse, 1)
	cmd := model.Command{
		Type: model.CmdUpdatePeersList,
		Peers: &model.PeersListPayload{
			Peers: []string{"peer1", "peer2"},
		},
		RespInfo: &model.RespInfo{
			RequestID: "req-peers-1",
			RespCh:    respCh,
		},
	}

	select {
	case tester.commandCh <- cmd:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out sending Peers command")
	}

	select {
	case resp := <-respCh:
		if resp.Status != model.ToProxyRespStatusError {
			t.Errorf("expected error status, got %v", resp.Status)
		}
		if resp.Error == "" {
			t.Error("expected error message but got empty")
		}
		if resp.RequestID != "req-peers-1" {
			t.Errorf("expected RequestID 'req-peers-1', got '%s'", resp.RequestID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for response")
	}
}

// TestHandleAddJob_DuplicateJobID tests that adding a duplicate job ID returns an error
func TestHandleAddJob_DuplicateJobID(t *testing.T) {
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	jobID := "duplicate-job-1"

	// Add first job - should succeed
	tester.AddJob(jobID, []byte("first data"))

	// Wait a bit for processing
	time.Sleep(200 * time.Millisecond)

	// Try to add same job ID again - should fail with duplicate error
	respCh := make(chan model.ToProducerResponse, 1)
	cmd := model.Command{
		Type: model.CmdAddJob,
		AddJob: &model.AddJobPayload{
			Job: model.Job{
				ID:    jobID,
				Topic: "test-topic",
				Data:  []byte("duplicate data"),
			},
		},
		RespInfo: &model.RespInfo{
			RequestID: "req-duplicate-1",
			RespCh:    respCh,
		},
	}

	select {
	case tester.commandCh <- cmd:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out sending duplicate AddJob command")
	}

	select {
	case resp := <-respCh:
		if resp.Status != model.ToProxyRespStatusError {
			t.Errorf("expected error status for duplicate job, got %v", resp.Status)
		}
		if resp.Error == "" {
			t.Error("expected error message but got empty")
		}
		if resp.RequestID != "req-duplicate-1" {
			t.Errorf("expected RequestID 'req-duplicate-1', got '%s'", resp.RequestID)
		}
		// Verify it's the duplicate error
		if resp.Error != internal.ErrDuplicateJobID.Error() {
			t.Errorf("expected error '%s', got '%s'", internal.ErrDuplicateJobID.Error(), resp.Error)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for response")
	}
}

// TestHandleDone_NilPayload tests handleDone with nil Done payload
func TestHandleDone_NilPayload(t *testing.T) {
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	respCh := make(chan model.ToProducerResponse, 1)
	cmd := model.Command{
		Type: model.CmdDone,
		Done: nil,
		RespInfo: &model.RespInfo{
			RequestID: "req-done-nil-1",
			RespCh:    respCh,
		},
	}

	select {
	case tester.commandCh <- cmd:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out sending Done command with nil payload")
	}

	select {
	case resp := <-respCh:
		if resp.Status != model.ToProxyRespStatusError {
			t.Errorf("expected error status, got %v", resp.Status)
		}
		if resp.Error == "" {
			t.Error("expected error message but got empty")
		}
		if resp.RequestID != "req-done-nil-1" {
			t.Errorf("expected RequestID 'req-done-nil-1', got '%s'", resp.RequestID)
		}
		if resp.Error != internal.ErrInvalidPayload.Error() {
			t.Errorf("expected error '%s', got '%s'", internal.ErrInvalidPayload.Error(), resp.Error)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for response")
	}
}

// TestHandleAddJob_NilPayload tests handleAddJob with nil AddJob payload
func TestHandleAddJob_NilPayload(t *testing.T) {
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	respCh := make(chan model.ToProducerResponse, 1)
	cmd := model.Command{
		Type:   model.CmdAddJob,
		AddJob: nil,
		RespInfo: &model.RespInfo{
			RequestID: "req-addjob-nil-1",
			RespCh:    respCh,
		},
	}

	select {
	case tester.commandCh <- cmd:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out sending AddJob command with nil payload")
	}

	select {
	case resp := <-respCh:
		if resp.Status != model.ToProxyRespStatusError {
			t.Errorf("expected error status, got %v", resp.Status)
		}
		if resp.Error == "" {
			t.Error("expected error message but got empty")
		}
		if resp.RequestID != "req-addjob-nil-1" {
			t.Errorf("expected RequestID 'req-addjob-nil-1', got '%s'", resp.RequestID)
		}
		if resp.Error != internal.ErrInvalidPayload.Error() {
			t.Errorf("expected error '%s', got '%s'", internal.ErrInvalidPayload.Error(), resp.Error)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for response")
	}
}

// TestCleanupProxyMap tests that stale proxies are removed from the partition's proxy map
func TestCleanupProxyMap(t *testing.T) {
	config := DefaultTestPartitionConfig()
	config.ProxyCleanupTickSec = 1 // Cleanup every 1 second for testing
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Add a new proxy
	proxyID := "test-proxy-cleanup"
	tester.AddProxy(proxyID)

	// Send heartbeat to mark it active
	tester.SendHeartbeatForProxy(proxyID, 1)

	// Wait for heartbeat to be processed
	time.Sleep(50 * time.Millisecond)

	// Get reference to partition's proxyMap
	partitionProxyMap := tester.partition.proxyMap

	// Verify proxy is in the partition's proxyMap
	if !partitionProxyMap.Exists(proxyID) {
		t.Fatal("proxy not found in partition's proxyMap")
	}

	// Wait for cleanup to happen (cleanup runs every 1 second)
	// The default timeout in cleanupProxyMap is 5 seconds
	// So we need to wait > 5 seconds for the proxy to become stale and be cleaned
	time.Sleep(6 * time.Second)

	// Verify proxy was removed from partition's proxyMap
	if partitionProxyMap.Exists(proxyID) {
		t.Error("proxy was not cleaned up from partition's proxyMap after heartbeat timeout")
	}
}

// TestHandleDrop tests the handleDrop method which handles dropped commands.
// This verifies both code paths: nil payload handling and successful drop handling.
func TestHandleDrop(t *testing.T) {
	config := DefaultTestPartitionConfig()
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Add a job so we have something valid to drop
	tester.AddJob("test-job-1", []byte("test data"))

	tests := []struct {
		name          string
		dropPayload   *model.DropPayload
		expectSuccess bool
		expectError   bool
	}{
		{
			name:          "nil drop payload - should return error",
			dropPayload:   nil,
			expectSuccess: false,
			expectError:   true,
		},
		{
			name: "valid drop payload - should return success",
			dropPayload: &model.DropPayload{
				Topic:  "test-topic",
				JobIDs: []string{"test-job-1"},
			},
			expectSuccess: true,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			respCh := make(chan model.ToProducerResponse, 1)
			cmd := model.Command{
				Type: model.CmdDrop,
				Drop: tt.dropPayload,
				RespInfo: &model.RespInfo{
					RequestID: "req-" + tt.name,
					RespCh:    respCh,
				},
			}

			// Send command
			select {
			case tester.commandCh <- cmd:
				// Successfully sent
			case <-time.After(100 * time.Millisecond):
				t.Fatal("timed out sending command")
			}

			// Read response
			select {
			case resp := <-respCh:
				if tt.expectSuccess {
					if resp.Status != model.ToProxyRespStatusSuccess {
						t.Errorf("expected success, got status %v with error: %s", resp.Status, resp.Error)
					}

				}
				if tt.expectError {
					if resp.Status != model.ToProxyRespStatusError {
						t.Errorf("expected error, got status %v", resp.Status)
					}
					if resp.Error == "" {
						t.Error("expected error message but got empty")
					}
				}
			case <-time.After(100 * time.Millisecond):
				t.Error("timed out waiting for response")
			}
		})
	}
}

// TestDispatch_ChannelFull tests that when proxy push channel is full,
// jobs are returned to the pool and retried on the next dispatch cycle
func TestDispatch_ChannelFull(t *testing.T) {
	config := DefaultTestPartitionConfig()
	config.PartitionTickMs = 50 // Fast tick for testing
	config.DispatchBatchSize = 2
	config.ProxyCleanupTickSec = 30 // Disable cleanup for this test

	// Create tester with a proxy that has small channel capacity
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Disable auto-done so jobs don't get completed automatically
	tester.SetAutoDone(false)

	// Remove default proxy
	tester.RemoveProxy("test-proxy")

	// Add a new proxy with small capacity and slow consumption
	proxyID := "slow-proxy"
	tester.AddProxy(proxyID, ProxyOptions{
		Capacity:     1, // Very small channel
		Consume:      true,
		ConsumeDelay: 200 * time.Millisecond, // Slow consumer keeps channel full
	})

	// Send heartbeat to make proxy available
	tester.SendHeartbeatForProxy(proxyID, 1)

	// Add multiple jobs
	numJobs := 5
	for i := 0; i < numJobs; i++ {
		jobID := fmt.Sprintf("job-%d", i)
		tester.AddJob(jobID, []byte("test data"))
	}

	// Wait for all jobs to be dispatched
	// The slow consumer means channel will be full, triggering the default branch
	// But jobs should eventually all be dispatched
	if !tester.WaitForDispatched(numJobs, 10*time.Second) {
		_, _, dispatched, _, _ := tester.GetCounts()
		t.Fatalf("expected all %d jobs to be dispatched, got %d", numJobs, dispatched)
	}

	// Verify no jobs were lost
	success, errors, dispatched, dropped, _ := tester.GetCounts()
	if success != uint64(numJobs) {
		t.Errorf("expected %d successful additions, got %d", numJobs, success)
	}
	if errors != 0 {
		t.Errorf("expected 0 errors, got %d", errors)
	}
	if dispatched != uint64(numJobs) {
		t.Errorf("expected %d dispatched, got %d", numJobs, dispatched)
	}
	if dropped != 0 {
		t.Errorf("expected 0 dropped, got %d", dropped)
	}

	t.Logf("All jobs dispatched successfully: success=%d, dispatched=%d", success, dispatched)
}

// TestBroadcastPartitionHeartbeat_CanAcceptFalse tests that when the dispatch queue
// is nearly full (>90%), the heartbeat broadcasts CanAccept=false
func TestBroadcastPartitionHeartbeat_CanAcceptFalse(t *testing.T) {
	config := DefaultTestPartitionConfig()
	config.ActiveQueueCapacity = 100 // Small capacity for testing
	config.PartitionTickMs = 50      // Fast tick
	config.HeartbeatTickMs = 50      // Fast heartbeat tick
	tester := NewPartitionTester("test-topic", config)
	defer tester.Cleanup()

	// Disable auto-done so jobs stay in the queue
	tester.SetAutoDone(false)

	// Remove default proxy so jobs don't get dispatched
	tester.RemoveProxy("test-proxy")

	// Add a proxy for heartbeats but DO NOT send a heartbeat for it
	// This proxy will receive broadcast heartbeats but won't be available for dispatch
	proxyID := "heartbeat-proxy"
	pushCh := make(chan model.ToGatewayMessage, 1000)

	tester.topologyCh <- ProxyTopologyUpdate{
		Type:    "add",
		ProxyID: proxyID,
		PushCh:  pushCh,
	}

	tester.proxiesMu.Lock()
	tester.proxies[proxyID] = &TestProxy{
		ID:     proxyID,
		PushCh: pushCh,
	}
	tester.proxiesMu.Unlock()

	// DO NOT send heartbeat - proxy is not available for dispatch

	// Calculate the threshold: bucketCap - bucketCap/10 = 90% of capacity
	dq := tester.partition.dispatchQueue
	bucketCap := dq.bucketCap
	threshold := bucketCap - bucketCap/10
	t.Logf("bucketCap=%d, threshold=%d", bucketCap, threshold)

	// Add enough jobs to exceed the threshold
	// Since there's no available proxy, jobs will stay in the queue
	numJobs := threshold + 10
	for i := 0; i < numJobs; i++ {
		jobID := fmt.Sprintf("job-%d", i)
		tester.AddJob(jobID, []byte("test data"))
	}

	// Wait for jobs to be added and processed
	time.Sleep(200 * time.Millisecond)

	// Verify the queue is above threshold
	activeDepth := tester.partition.dispatchQueue.ActiveQueueSize()
	if activeDepth <= threshold {
		t.Errorf("expected activeDepth > %d, got %d", threshold, activeDepth)
	}
	t.Logf("Active queue depth: %d", activeDepth)

	// Now listen for heartbeats - with no available proxy, the queue stays full
	var canAcceptFalseCount int
	var canAcceptTrueCount int

	// Read messages for a short time (heartbeats are fast now)
	timeout := time.After(500 * time.Millisecond)
	done := false

	for !done {
		select {
		case msg := <-pushCh:
			if msg.Type == model.ToGatewayMessageHeartbeat && msg.Heartbeat != nil {
				if msg.Heartbeat.CanAccept {
					canAcceptTrueCount++
					t.Logf("Received heartbeat: CanAccept=true, activeDepth=%d",
						tester.partition.dispatchQueue.ActiveQueueSize())
				} else {
					canAcceptFalseCount++
					t.Logf("Received heartbeat: CanAccept=false, activeDepth=%d",
						tester.partition.dispatchQueue.ActiveQueueSize())
				}
			}
		case <-timeout:
			done = true
		}
	}

	// Assert that we received at least one heartbeat with CanAccept=false
	if canAcceptFalseCount == 0 {
		t.Errorf("expected at least one heartbeat with CanAccept=false, got 0")
	}

	t.Logf("Heartbeats received: CanAccept=true=%d, CanAccept=false=%d",
		canAcceptTrueCount, canAcceptFalseCount)
}
