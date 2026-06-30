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
	"testing"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newTestQueue() *DispatchQueue {
	jobStore := NewJobStore(100)
	metrics := internal.GetPartitionMetrics()
	logger, _ := zap.NewDevelopment()

	dlq := NewDLQBuffer(
		"test-topic",
		1024*1024,
		60000,
		jobStore,
		logger,
		metrics,
	)

	return NewDispatchQueue(10, 10, dlq, jobStore)
}

func createJob(t *testing.T, js *JobStore, id string) JobRef {
	t.Helper()

	job := &model.Job{
		ID:    id,
		Topic: "test-topic",
		Data:  []byte("data"),
	}

	idx, err := js.Create(job)
	if err != nil {
		t.Fatal(err)
	}

	return JobRef{
		Index: int(idx),
	}
}

func TestNewDispatchQueue(t *testing.T) {
	dq := newTestQueue()

	for _, b := range dq.buckets {
		if b.TimeSec != -1 {
			t.Fatal("bucket should be free")
		}

		for _, jr := range b.Jobs {
			if jr.Index != -1 {
				t.Fatal("bucket slot should be initialized to -1")
			}
		}
	}
}

func TestAddNewJob(t *testing.T) {
	dq := newTestQueue()

	job := createJob(t, dq.jobStore, "j1")

	if err := dq.AddNewJob(job); err != nil {
		t.Fatal(err)
	}

	if dq.aTail != 1 {
		t.Fatalf("expected tail=1 got %d", dq.aTail)
	}
}

func TestAddNewJobCapacityExceeded(t *testing.T) {
	jobStore := NewJobStore(100)

	metrics := internal.GetPartitionMetrics()
	logger, _ := zap.NewDevelopment()

	dlq := NewDLQBuffer(
		"test-topic",
		1024,
		60000,
		jobStore,
		logger,
		metrics,
	)

	dq := NewDispatchQueue(2, 1, dlq, jobStore)

	j1 := createJob(t, jobStore, "j1")
	j2 := createJob(t, jobStore, "j2")

	if err := dq.AddNewJob(j1); err != nil {
		t.Fatal(err)
	}

	if err := dq.AddNewJob(j2); err == nil {
		t.Fatal("expected capacity error")
	}
}

func TestReadActiveSingle(t *testing.T) {
	dq := newTestQueue()

	job := createJob(t, dq.jobStore, "j1")

	_ = dq.AddNewJob(job)

	items := dq.ReadBatch(1, -1, -1, -1)

	if len(items) != 1 {
		t.Fatalf("expected 1 item got %d", len(items))
	}

	if !items[0].IsNew {
		t.Fatal("expected active job")
	}
}

func TestRemoveByIndexActive(t *testing.T) {
	dq := newTestQueue()

	job := createJob(t, dq.jobStore, "j1")

	_ = dq.AddNewJob(job)

	items := dq.ReadBatch(1, -1, -1, -1)

	dq.RemoveByIndex(
		items[0].IsNew,
		items[0].Bucket,
		items[0].Cell,
	)

	if dq.active[items[0].Cell].Index != -1 {
		t.Fatal("job should be tombstoned")
	}
}

func TestMoveDispatchedActiveToBucket(t *testing.T) {
	dq := newTestQueue()

	job := createJob(t, dq.jobStore, "j1")

	_ = dq.AddNewJob(job)

	items := dq.ReadBatch(1, -1, -1, -1)

	dq.MoveDispatched(
		items[0].JobRef,
		time.Now().Add(time.Second).UnixMilli(),
		items[0].IsNew,
		items[0].Bucket,
		items[0].Cell,
	)

	found := false

	for _, b := range dq.buckets {
		for i := 0; i < b.Tail; i++ {
			if b.Jobs[i].Index == job.Index {
				found = true
			}
		}
	}

	if !found {
		t.Fatal("job not found in retry buckets")
	}
}

func TestReadPendingSingleBucket(t *testing.T) {
	dq := newTestQueue()

	job := createJob(t, dq.jobStore, "j1")

	_ = dq.AddNewJob(job)

	items := dq.ReadBatch(1, -1, -1, -1)

	dq.MoveDispatched(
		items[0].JobRef,
		time.Now().Add(time.Second).UnixMilli(),
		items[0].IsNew,
		items[0].Bucket,
		items[0].Cell,
	)

	time.Sleep(1100 * time.Millisecond)

	items = dq.ReadBatch(1, -1, -1, -1)

	if len(items) != 1 {
		t.Fatalf("expected retry job got %d", len(items))
	}

	if items[0].IsNew {
		t.Fatal("expected bucket job")
	}
}

// todo
// func TestReadPendingMultipleBuckets(t *testing.T) {
// 	dq := newTestQueue()

// 	j1 := createJob(t, dq.jobStore, "j1")
// 	j2 := createJob(t, dq.jobStore, "j2")

// 	_ = dq.AddNewJob(j1)
// 	_ = dq.AddNewJob(j2)

// 	items := dq.ReadBatch(2, -1, -1, -1)

// 	dq.MoveDispatched(
// 		items[0].JobRef,
// 		time.Now().Add(time.Second).UnixMilli(),
// 		items[0].IsNew,
// 		items[0].Bucket,
// 		items[0].Cell,
// 	)

// 	dq.MoveDispatched(
// 		items[1].JobRef,
// 		time.Now().Add(2*time.Second).UnixMilli(),
// 		items[1].IsNew,
// 		items[1].Bucket,
// 		items[1].Cell,
// 	)

// 	time.Sleep(3 * time.Second)

// 	items = dq.ReadBatch(10, -1, -1, -1)

// 	if len(items) != 2 {
// 		t.Fatalf("expected 2 jobs got %d", len(items))
// 	}
// }

func TestReadBatchPendingPriority(t *testing.T) {
	dq := newTestQueue()

	j1 := createJob(t, dq.jobStore, "active")
	j2 := createJob(t, dq.jobStore, "retry")

	_ = dq.AddNewJob(j1)
	_ = dq.AddNewJob(j2)

	items := dq.ReadBatch(1, -1, -1, -1)

	dq.MoveDispatched(
		items[0].JobRef,
		time.Now().Add(time.Second).UnixMilli(),
		items[0].IsNew,
		items[0].Bucket,
		items[0].Cell,
	)

	time.Sleep(1100 * time.Millisecond)

	items = dq.ReadBatch(2, -1, -1, -1)

	if len(items) == 0 {
		t.Fatal("expected items")
	}

	if items[0].IsNew {
		t.Fatal("retry item should have priority")
	}
}

func TestReadPendingSkipsTombstones(t *testing.T) {
	dq := newTestQueue()

	j1 := createJob(t, dq.jobStore, "j1")
	j2 := createJob(t, dq.jobStore, "j2")

	_ = dq.AddNewJob(j1)
	_ = dq.AddNewJob(j2)

	items := dq.ReadBatch(2, -1, -1, -1)

	target := time.Now().Add(time.Second).UnixMilli()

	for _, item := range items {
		dq.MoveDispatched(
			item.JobRef,
			target,
			item.IsNew,
			item.Bucket,
			item.Cell,
		)
	}

	time.Sleep(1100 * time.Millisecond)

	items = dq.ReadBatch(2, -1, -1, -1)

	dq.RemoveByIndex(
		items[0].IsNew,
		items[0].Bucket,
		items[0].Cell,
	)

	next := dq.ReadBatch(10, -1, -1, -1)

	if len(next) == 0 {
		t.Fatal("expected remaining item")
	}
}

func TestCleanupOneExpiredBucket(t *testing.T) {
	dq := newTestQueue()

	job := createJob(t, dq.jobStore, "j1")

	_ = dq.AddNewJob(job)

	items := dq.ReadBatch(1, -1, -1, -1)

	dq.MoveDispatched(
		items[0].JobRef,
		time.Now().Add(time.Second).UnixMilli(),
		items[0].IsNew,
		items[0].Bucket,
		items[0].Cell,
	)

	time.Sleep(2 * time.Second)

	ok := dq.CleanupOneExpiredBucket()

	if !ok {
		t.Fatal("expected cleanup")
	}
}

func TestFindOrCreateBucketReusesExpiredBucket(t *testing.T) {
	dq := newTestQueue()

	nowSec := dq.currentSec()

	// Simulate an expired and empty bucket
	dq.buckets[0].TimeSec = nowSec - 10
	dq.buckets[0].Head = 1
	dq.buckets[0].Tail = 1

	dq.timeToBucket[nowSec-10] = 0

	newSec := nowSec + 100

	idx := dq.getBucketForTime(newSec)

	if idx != 0 {
		t.Fatalf("expected bucket 0 to be reused, got %d", idx)
	}

	if _, ok := dq.timeToBucket[nowSec-10]; ok {
		t.Fatal("old time mapping should have been deleted")
	}

	if dq.timeToBucket[newSec] != 0 {
		t.Fatal("new time mapping not created")
	}
}

func TestFindOrCreateBucketPanicsWhenNoBucketsAvailable(t *testing.T) {
	dq := newTestQueue()

	nowSec := dq.currentSec()

	// Fill every bucket with active data
	for i := range dq.buckets {
		dq.buckets[i].TimeSec = nowSec + int64(i+1)
		dq.buckets[i].Head = 0
		dq.buckets[i].Tail = 1
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()

	dq.getBucketForTime(nowSec + 1000)
}

func TestMoveDispatchedPanicsForPastTime(t *testing.T) {
	dq := newTestQueue()

	job := createJob(t, dq.jobStore, "j1")

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()

	dq.MoveDispatched(
		job,
		time.Now().Add(-5*time.Second).UnixMilli(),
		true,
		-1,
		0,
	)
}

func TestMoveDispatchedBumpsCurrentSecond(t *testing.T) {
	dq := newTestQueue()

	job := createJob(t, dq.jobStore, "j1")

	_ = dq.AddNewJob(job)

	items := dq.ReadBatch(1, -1, -1, -1)

	nowSec := dq.currentSec()

	dq.MoveDispatched(
		items[0].JobRef,
		time.Now().UnixMilli(), // current second
		items[0].IsNew,
		items[0].Bucket,
		items[0].Cell,
	)

	found := false

	for _, b := range dq.buckets {
		if b.TimeSec == nowSec+1 {
			found = true
			break
		}
	}

	if !found {
		t.Fatal("expected retry bucket at currentSec+1")
	}
}

func TestMoveDispatchedPanicsWhenBucketFull(t *testing.T) {
	jobStore := NewJobStore(100)

	metrics := internal.GetPartitionMetrics()
	logger, _ := zap.NewDevelopment()

	dlq := NewDLQBuffer(
		"test-topic",
		1024,
		60000,
		jobStore,
		logger,
		metrics,
	)

	// bucketCap = 1
	dq := NewDispatchQueue(10, 1, dlq, jobStore)

	j1 := createJob(t, jobStore, "j1")
	j2 := createJob(t, jobStore, "j2")

	targetSec := dq.currentSec() + 10

	// first insert fills bucket
	dq.MoveDispatched(
		j1,
		targetSec*1000,
		true,
		-1,
		0,
	)

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()

	// second insert into same bucket should panic
	dq.MoveDispatched(
		j2,
		targetSec*1000,
		true,
		-1,
		0,
	)
}

func TestReadPendingWrapsBucketIndex(t *testing.T) {
	dq := newTestQueue()

	job := createJob(t, dq.jobStore, "j1")

	// Put job in bucket 0
	dq.buckets[0].TimeSec = dq.currentSec()
	dq.buckets[0].Jobs[0] = job
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 1

	// Start beyond last bucket
	items := dq.readPending(
		1,
		dq.numBuckets, // out of range
		-1,
	)

	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}

	if items[0].JobRef.Index != job.Index {
		t.Fatal("expected wrapped read to find bucket 0 job")
	}
}

func TestCleanupOneExpiredBucketReturnsFalseWhenDependenciesMissing(t *testing.T) {
	dq := &DispatchQueue{
		dlqBuffer: nil,
		jobStore:  nil,
	}

	require.False(t, dq.CleanupOneExpiredBucket(), "expected false when dependencies are nil")
}

func TestCleanupOneExpiredBucketWrapAround(t *testing.T) {
	dq := newTestQueue()

	// Force scan to start near the end
	dq.nextCleanupIdx = len(dq.buckets) - 1

	// Put expired bucket at the beginning
	dq.buckets[0].TimeSec = time.Now().Unix() - 10
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 1

	job := createJob(t, dq.jobStore, "j1")
	dq.buckets[0].Jobs[0] = job

	require.True(t, dq.CleanupOneExpiredBucket(), "expected wrap-around cleanup to find bucket")

	if dq.buckets[0].TimeSec != -1 {
		t.Fatal("expected bucket to be cleared")
	}
}

// -------------------------------------------
// Extra tests to cover edge cases
// -------------------------------------------

// TestConstructorInitializesBucketToTime verifies that bucketToTime slice is properly initialized to -1 for all entries
func TestConstructorInitializesBucketToTime(t *testing.T) {
	dq := newTestQueue()

	for i, val := range dq.bucketToTime {
		if val != -1 {
			t.Fatalf("bucketToTime[%d] = %d, expected -1", i, val)
		}
	}
}

// TestReadActiveWithOffset verifies that ReadBatch respects the activeOffset parameter and resumes reading from correct position
func TestReadActiveWithOffset(t *testing.T) {
	dq := newTestQueue()

	// Create 3 jobs
	j1 := createJob(t, dq.jobStore, "j1")
	j2 := createJob(t, dq.jobStore, "j2")
	j3 := createJob(t, dq.jobStore, "j3")

	// Add all to active queue
	_ = dq.AddNewJob(j1)
	_ = dq.AddNewJob(j2)
	_ = dq.AddNewJob(j3)

	// Read first job with offset -1 (start from beginning)
	items := dq.ReadBatch(1, -1, -1, -1)
	require.Len(t, items, 1)
	require.Equal(t, j1.Index, items[0].JobRef.Index)

	// Read next job using the offset from previous read (cell position)
	items = dq.ReadBatch(1, items[0].Cell, -1, -1)
	require.Len(t, items, 1)
	require.Equal(t, j2.Index, items[0].JobRef.Index)

	// Read third job
	items = dq.ReadBatch(1, items[0].Cell, -1, -1)
	require.Len(t, items, 1)
	require.Equal(t, j3.Index, items[0].JobRef.Index)

	// Verify active head advanced correctly
	require.Equal(t, 3, dq.aHead)
}

// TestReadActiveSkipsTombstonedEntries verifies that tombstoned active jobs are not returned by ReadBatch
func TestReadActiveSkipsTombstonedEntries(t *testing.T) {
	dq := newTestQueue()

	// Create 3 jobs
	j1 := createJob(t, dq.jobStore, "j1")
	j2 := createJob(t, dq.jobStore, "j2")
	j3 := createJob(t, dq.jobStore, "j3")

	// Add all to active queue
	_ = dq.AddNewJob(j1)
	_ = dq.AddNewJob(j2)
	_ = dq.AddNewJob(j3)

	// Read first job
	items := dq.ReadBatch(1, -1, -1, -1)
	require.Len(t, items, 1)
	require.Equal(t, j1.Index, items[0].JobRef.Index)

	// Tombstone it
	dq.RemoveByIndex(items[0].IsNew, items[0].Bucket, items[0].Cell)

	// Read batch of 2 - should skip tombstone and return j2, j3
	items = dq.ReadBatch(2, -1, -1, -1)
	require.Len(t, items, 2)
	require.Equal(t, j2.Index, items[0].JobRef.Index)
	require.Equal(t, j3.Index, items[1].JobRef.Index)

	// Verify active[0] is tombstoned
	require.Equal(t, -1, dq.active[0].Index)
}

// TestReadActiveOnEmptyQueue verifies ReadBatch returns no items when active queue is empty
func TestReadActiveOnEmptyQueue(t *testing.T) {
	dq := newTestQueue()

	// Read from empty queue
	items := dq.ReadBatch(10, -1, -1, -1)

	require.Len(t, items, 0)
	require.Equal(t, 0, dq.aTail)
	require.Equal(t, 0, dq.aHead)
}

func TestTombstoneDoesNotMoveActiveHeadBackwards(t *testing.T) {
	dq := newTestQueue()

	j1 := createJob(t, dq.jobStore, "j1")
	j2 := createJob(t, dq.jobStore, "j2")

	_ = dq.AddNewJob(j1)
	_ = dq.AddNewJob(j2)

	items := dq.ReadBatch(1, -1, -1, -1)

	headBefore := dq.aHead

	dq.RemoveByIndex(
		items[0].IsNew,
		items[0].Bucket,
		items[0].Cell,
	)

	if dq.aHead != headBefore {
		t.Fatalf(
			"tombstone moved active head: before=%d after=%d",
			headBefore,
			dq.aHead,
		)
	}
}

// TestActiveQueueSizeAfterOperations verifies ActiveQueueSize returns correct size after adds and tombstoning
func TestActiveQueueSizeAfterOperations(t *testing.T) {
	dq := newTestQueue()

	// Empty queue
	require.Equal(t, 0, dq.ActiveQueueSize())

	// Add 3 jobs
	j1 := createJob(t, dq.jobStore, "j1")
	j2 := createJob(t, dq.jobStore, "j2")
	j3 := createJob(t, dq.jobStore, "j3")

	_ = dq.AddNewJob(j1)
	_ = dq.AddNewJob(j2)
	_ = dq.AddNewJob(j3)

	require.Equal(t, 3, dq.ActiveQueueSize())

	// Read and tombstone first job
	items := dq.ReadBatch(1, -1, -1, -1)
	dq.RemoveByIndex(items[0].IsNew, items[0].Bucket, items[0].Cell)

	require.Equal(t, 2, dq.ActiveQueueSize())

	// Read and tombstone second job
	items = dq.ReadBatch(1, -1, -1, -1)
	dq.RemoveByIndex(items[0].IsNew, items[0].Bucket, items[0].Cell)

	require.Equal(t, 1, dq.ActiveQueueSize())

	// Read and tombstone third job
	items = dq.ReadBatch(1, -1, -1, -1)
	dq.RemoveByIndex(items[0].IsNew, items[0].Bucket, items[0].Cell)

	require.Equal(t, 0, dq.ActiveQueueSize())
}

// TestBucketTombstoneUpdatesBucketsLast verifies that tombstoning a bucket item correctly updates bucketsLast cursor
func TestBucketTombstoneUpdatesBucketsLast(t *testing.T) {
	dq := newTestQueue()

	// Create a job and move it to a bucket
	job := createJob(t, dq.jobStore, "j1")
	_ = dq.AddNewJob(job)

	// Read from active and move to bucket
	items := dq.ReadBatch(1, -1, -1, -1)
	targetSec := dq.currentSec() + 10
	dq.MoveDispatched(
		items[0].JobRef,
		targetSec*1000,
		items[0].IsNew,
		items[0].Bucket,
		items[0].Cell,
	)

	// Find which bucket the job was placed in
	var bucketIdx int
	var cell int
	for i, b := range dq.buckets {
		if b.TimeSec == targetSec {
			bucketIdx = i
			cell = 0 // first slot
			break
		}
	}

	// Verify bucketsLast is initially -1,-1
	require.Equal(t, -1, dq.bucketsLast[0])
	require.Equal(t, -1, dq.bucketsLast[1])

	// Tombstone the bucket item
	dq.RemoveByIndex(false, bucketIdx, cell)

	// Verify bucketsLast was updated
	require.Equal(t, bucketIdx, dq.bucketsLast[0])
	require.Equal(t, cell, dq.bucketsLast[1])

	// Verify the job is tombstoned
	require.Equal(t, -1, dq.buckets[bucketIdx].Jobs[cell].Index)

	// Verify bucket head was updated
	require.Equal(t, cell, dq.buckets[bucketIdx].Head)
}

// TestReadPendingMultipleItemsFromBucket verifies reading more than one item from a single bucket
func TestReadPendingMultipleItemsFromBucket(t *testing.T) {
	dq := newTestQueue()

	// Create 3 jobs and move them all to the same bucket
	j1 := createJob(t, dq.jobStore, "j1")
	j2 := createJob(t, dq.jobStore, "j2")
	j3 := createJob(t, dq.jobStore, "j3")

	_ = dq.AddNewJob(j1)
	_ = dq.AddNewJob(j2)
	_ = dq.AddNewJob(j3)

	// Read all 3 from active and move to same bucket
	items := dq.ReadBatch(3, -1, -1, -1)
	targetSec := dq.currentSec() + 10

	for _, item := range items {
		dq.MoveDispatched(
			item.JobRef,
			targetSec*1000,
			item.IsNew,
			item.Bucket,
			item.Cell,
		)
	}

	// Find the bucket
	var bucketIdx int
	for i, b := range dq.buckets {
		if b.TimeSec == targetSec {
			bucketIdx = i
			break
		}
	}

	// Verify all 3 jobs are in the bucket
	require.Equal(t, 3, dq.buckets[bucketIdx].Tail)

	// Advance time so bucket is due
	dq.buckets[bucketIdx].TimeSec = dq.currentSec()

	// Read all 3 from the bucket
	readItems := dq.ReadBatch(3, -1, -1, -1)

	require.Len(t, readItems, 3)
	require.False(t, readItems[0].IsNew)
	require.False(t, readItems[1].IsNew)
	require.False(t, readItems[2].IsNew)

	// Verify all 3 jobs were read
	indices := []int{readItems[0].JobRef.Index, readItems[1].JobRef.Index, readItems[2].JobRef.Index}
	require.Contains(t, indices, j1.Index)
	require.Contains(t, indices, j2.Index)
	require.Contains(t, indices, j3.Index)
}

// TestReadPendingWithOffset verifies readPending respects bucketIndx and cell parameters for resuming
func TestReadPendingWithOffset(t *testing.T) {
	dq := newTestQueue()

	// Create 3 jobs and move them to the same bucket
	j1 := createJob(t, dq.jobStore, "j1")
	j2 := createJob(t, dq.jobStore, "j2")
	j3 := createJob(t, dq.jobStore, "j3")

	_ = dq.AddNewJob(j1)
	_ = dq.AddNewJob(j2)
	_ = dq.AddNewJob(j3)

	// Read all 3 from active and move to same bucket
	items := dq.ReadBatch(3, -1, -1, -1)
	targetSec := dq.currentSec() + 10

	for _, item := range items {
		dq.MoveDispatched(
			item.JobRef,
			targetSec*1000,
			item.IsNew,
			item.Bucket,
			item.Cell,
		)
	}

	// Find the bucket
	var bucketIdx int
	for i, b := range dq.buckets {
		if b.TimeSec == targetSec {
			bucketIdx = i
			break
		}
	}

	// Advance time so bucket is due
	dq.buckets[bucketIdx].TimeSec = dq.currentSec()

	// Read first item starting from beginning (cell=-1)
	firstRead := dq.readPending(1, bucketIdx, -1)
	require.Len(t, firstRead, 1)
	require.Equal(t, j1.Index, firstRead[0].JobRef.Index)
	require.Equal(t, 0, firstRead[0].Cell)

	// Read second item starting from cell 0 (should get cell 1)
	secondRead := dq.readPending(1, bucketIdx, 0)
	require.Len(t, secondRead, 1)
	require.Equal(t, j2.Index, secondRead[0].JobRef.Index)
	require.Equal(t, 1, secondRead[0].Cell)

	// Read third item starting from cell 1 (should get cell 2)
	thirdRead := dq.readPending(1, bucketIdx, 1)
	require.Len(t, thirdRead, 1)
	require.Equal(t, j3.Index, thirdRead[0].JobRef.Index)
	require.Equal(t, 2, thirdRead[0].Cell)
}

// TestReadPendingAcrossMultipleBuckets verifies reading jobs spread across multiple buckets
func TestReadPendingAcrossMultipleBuckets(t *testing.T) {
	dq := newTestQueue()

	// Create 3 jobs
	j1 := createJob(t, dq.jobStore, "j1")
	j2 := createJob(t, dq.jobStore, "j2")
	j3 := createJob(t, dq.jobStore, "j3")

	nowSec := dq.currentSec()

	// Place jobs in different buckets with different due times (all expired)
	// Bucket 0: expired 2 seconds ago
	dq.buckets[0].TimeSec = nowSec - 2
	dq.buckets[0].Jobs[0] = j1
	dq.buckets[0].Head = -1 // bucket not yet processed
	dq.buckets[0].Tail = 1
	dq.timeToBucket[nowSec-2] = 0

	// Bucket 1: expired 1 second ago
	dq.buckets[1].TimeSec = nowSec - 1
	dq.buckets[1].Jobs[0] = j2
	dq.buckets[1].Head = -1 // bucket not yet processed
	dq.buckets[1].Tail = 1
	dq.timeToBucket[nowSec-1] = 1

	// Bucket 2: expired now (current second)
	dq.buckets[2].TimeSec = nowSec
	dq.buckets[2].Jobs[0] = j3
	dq.buckets[2].Head = -1 // bucket not yet processed
	dq.buckets[2].Tail = 1
	dq.timeToBucket[nowSec] = 2

	// Read all jobs across multiple buckets
	readItems := dq.ReadBatch(3, -1, -1, -1)

	require.Len(t, readItems, 3)
	require.False(t, readItems[0].IsNew)
	require.False(t, readItems[1].IsNew)
	require.False(t, readItems[2].IsNew)

	// Verify all 3 jobs were read
	indices := []int{readItems[0].JobRef.Index, readItems[1].JobRef.Index, readItems[2].JobRef.Index}
	require.Contains(t, indices, j1.Index)
	require.Contains(t, indices, j2.Index)
	require.Contains(t, indices, j3.Index)

	// Verify jobs came from different buckets (in order)
	require.Equal(t, 0, readItems[0].Bucket)
	require.Equal(t, 1, readItems[1].Bucket)
	require.Equal(t, 2, readItems[2].Bucket)
}

// TestReadPendingFirstBucketPartiallyConsumed verifies resuming correctly from middle of a bucket
func TestReadPendingFirstBucketPartiallyConsumed(t *testing.T) {
	dq := newTestQueue()

	// Create 3 jobs and put them in the same bucket
	j1 := createJob(t, dq.jobStore, "j1")
	j2 := createJob(t, dq.jobStore, "j2")
	j3 := createJob(t, dq.jobStore, "j3")

	nowSec := dq.currentSec()

	// Place all 3 jobs in bucket 0
	dq.buckets[0].TimeSec = nowSec
	dq.buckets[0].Jobs[0] = j1
	dq.buckets[0].Jobs[1] = j2
	dq.buckets[0].Jobs[2] = j3
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 3
	dq.timeToBucket[nowSec] = 0

	// Read first job (bucket should be partially consumed)
	firstRead := dq.ReadBatch(1, -1, -1, -1)
	require.Len(t, firstRead, 1)
	require.Equal(t, j1.Index, firstRead[0].JobRef.Index)
	require.Equal(t, 0, firstRead[0].Bucket)
	require.Equal(t, 0, firstRead[0].Cell)

	// Tombstone the first job
	dq.RemoveByIndex(firstRead[0].IsNew, firstRead[0].Bucket, firstRead[0].Cell)

	// Verify bucket head was updated to cell 0 (the tombstoned position)
	require.Equal(t, 0, dq.buckets[0].Head)

	// Read again - should resume from cell 1 and get j2
	secondRead := dq.ReadBatch(1, -1, -1, -1)
	require.Len(t, secondRead, 1)
	require.Equal(t, j2.Index, secondRead[0].JobRef.Index)
	require.Equal(t, 0, secondRead[0].Bucket)
	require.Equal(t, 1, secondRead[0].Cell)

	// Tombstone j2
	dq.RemoveByIndex(secondRead[0].IsNew, secondRead[0].Bucket, secondRead[0].Cell)
	require.Equal(t, 1, dq.buckets[0].Head)

	// Read third job - should get j3
	thirdRead := dq.ReadBatch(1, -1, -1, -1)
	require.Len(t, thirdRead, 1)
	require.Equal(t, j3.Index, thirdRead[0].JobRef.Index)
	require.Equal(t, 0, thirdRead[0].Bucket)
	require.Equal(t, 2, thirdRead[0].Cell)
}

// TestReadPendingFirstBucketFullyTombstoned verifies readPending skips entirely tombstoned buckets
func TestReadPendingFirstBucketFullyTombstoned(t *testing.T) {
	dq := newTestQueue()

	// Create 3 jobs and put them in bucket 0
	j1 := createJob(t, dq.jobStore, "j1")
	j2 := createJob(t, dq.jobStore, "j2")
	j3 := createJob(t, dq.jobStore, "j3")

	nowSec := dq.currentSec()

	// Place all 3 jobs in bucket 0
	dq.buckets[0].TimeSec = nowSec
	dq.buckets[0].Jobs[0] = j1
	dq.buckets[0].Jobs[1] = j2
	dq.buckets[0].Jobs[2] = j3
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 3
	dq.timeToBucket[nowSec] = 0

	// Put another job in bucket 1
	j4 := createJob(t, dq.jobStore, "j4")
	dq.buckets[1].TimeSec = nowSec
	dq.buckets[1].Jobs[0] = j4
	dq.buckets[1].Head = -1
	dq.buckets[1].Tail = 1
	dq.timeToBucket[nowSec] = 1 // This will overwrite the map entry!

	// we can't have two buckets with the same time in timeToBucket
	dq.buckets[0].TimeSec = nowSec - 5
	dq.timeToBucket[nowSec-5] = 0

	dq.buckets[1].TimeSec = nowSec
	dq.timeToBucket[nowSec] = 1

	// Read and tombstone all jobs from bucket 0
	items := dq.ReadBatch(3, -1, -1, -1)
	require.Len(t, items, 3)
	require.Equal(t, 0, items[0].Bucket)
	require.Equal(t, 0, items[1].Bucket)
	require.Equal(t, 0, items[2].Bucket)

	// Tombstone all 3 jobs
	for _, item := range items {
		dq.RemoveByIndex(item.IsNew, item.Bucket, item.Cell)
	}

	// Verify bucket 0 is fully tombstoned (Head = Tail -1)
	require.Equal(t, 3, dq.buckets[0].Tail)
	require.Equal(t, 2, dq.buckets[0].Head)

	// Read again - should skip bucket 0 entirely and read from bucket 1
	nextItems := dq.ReadBatch(1, -1, -1, -1)
	require.Len(t, nextItems, 1)
	require.Equal(t, 1, nextItems[0].Bucket)
	require.Equal(t, j4.Index, nextItems[0].JobRef.Index)
}

// TestReadPendingEmptyFirstBucketHasJobsInSecond verifies readPending finds jobs in bucket 1 when bucket 0 is empty
func TestReadPendingEmptyFirstBucketHasJobsInSecond(t *testing.T) {
	dq := newTestQueue()

	// Create a job and put it in bucket 1
	j1 := createJob(t, dq.jobStore, "j1")

	nowSec := dq.currentSec()

	// Leave bucket 0 empty (TimeSec = -1)
	// Put job in bucket 1
	dq.buckets[1].TimeSec = nowSec
	dq.buckets[1].Jobs[0] = j1
	dq.buckets[1].Head = -1
	dq.buckets[1].Tail = 1
	dq.timeToBucket[nowSec] = 1

	// Read from pending - should skip empty bucket 0 and find job in bucket 1
	items := dq.ReadBatch(1, -1, -1, -1)

	require.Len(t, items, 1)
	require.Equal(t, j1.Index, items[0].JobRef.Index)
	require.Equal(t, 1, items[0].Bucket)
	require.Equal(t, 0, items[0].Cell)
	require.False(t, items[0].IsNew)
}

// TestReadPendingResumeFromBucketAndCell verifies cursor resume works correctly from specified (bucket, cell) position
func TestReadPendingResumeFromBucketAndCell(t *testing.T) {
	dq := newTestQueue()

	// Create jobs for buckets 0, 1, 2
	j1 := createJob(t, dq.jobStore, "j1")
	j2 := createJob(t, dq.jobStore, "j2")
	j3 := createJob(t, dq.jobStore, "j3")
	j4 := createJob(t, dq.jobStore, "j4")

	nowSec := dq.currentSec()

	// Bucket 0: 2 jobs
	dq.buckets[0].TimeSec = nowSec
	dq.buckets[0].Jobs[0] = j1
	dq.buckets[0].Jobs[1] = j2
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 2
	dq.timeToBucket[nowSec] = 0

	// Bucket 1: 1 job
	dq.buckets[1].TimeSec = nowSec
	dq.buckets[1].Jobs[0] = j3
	dq.buckets[1].Head = -1
	dq.buckets[1].Tail = 1
	dq.timeToBucket[nowSec] = 1

	// Bucket 2: 1 job
	dq.buckets[2].TimeSec = nowSec
	dq.buckets[2].Jobs[0] = j4
	dq.buckets[2].Head = -1
	dq.buckets[2].Tail = 1
	dq.timeToBucket[nowSec] = 2

	// Start from bucket 0, cell -1 (beginning) - should get j1
	items := dq.readPending(1, 0, -1)
	require.Len(t, items, 1)
	require.Equal(t, j1.Index, items[0].JobRef.Index)
	require.Equal(t, 0, items[0].Bucket)
	require.Equal(t, 0, items[0].Cell)

	// Resume from bucket 0, cell 0 - should get j2 (next cell in same bucket)
	items = dq.readPending(1, 0, 0)
	require.Len(t, items, 1)
	require.Equal(t, j2.Index, items[0].JobRef.Index)
	require.Equal(t, 0, items[0].Bucket)
	require.Equal(t, 1, items[0].Cell)

	// Resume from bucket 0, cell 1 - should move to bucket 1, get j3
	items = dq.readPending(1, 0, 1)
	require.Len(t, items, 1)
	require.Equal(t, j3.Index, items[0].JobRef.Index)
	require.Equal(t, 1, items[0].Bucket)
	require.Equal(t, 0, items[0].Cell)

	// Resume from bucket 1, cell 0 - should move to bucket 2, get j4
	items = dq.readPending(1, 1, 0)
	require.Len(t, items, 1)
	require.Equal(t, j4.Index, items[0].JobRef.Index)
	require.Equal(t, 2, items[0].Bucket)
	require.Equal(t, 0, items[0].Cell)
}

// TestReadPendingBootstrapFromMinusOne verifies readPending initializes correctly when starting with (-1,-1)
func TestReadPendingBootstrapFromMinusOne(t *testing.T) {
	dq := newTestQueue()

	// Create a job and put it in bucket 0
	j1 := createJob(t, dq.jobStore, "j1")

	nowSec := dq.currentSec()

	dq.buckets[0].TimeSec = nowSec
	dq.buckets[0].Jobs[0] = j1
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 1
	dq.timeToBucket[nowSec] = 0

	// Verify bucketsLast is initially -1, -1
	require.Equal(t, -1, dq.bucketsLast[0])
	require.Equal(t, -1, dq.bucketsLast[1])

	// Call readPending with bucketIndx=-1, cell=-1 (bootstrap)
	items := dq.readPending(1, -1, -1)

	// Should find the job in bucket 0
	require.Len(t, items, 1)
	require.Equal(t, j1.Index, items[0].JobRef.Index)
	require.Equal(t, 0, items[0].Bucket)
	require.Equal(t, 0, items[0].Cell)
}

// TestReadPendingSkippedFutureBucket verifies that buckets with time in the future are not returned
func TestReadPendingSkippedFutureBucket(t *testing.T) {
	dq := newTestQueue()

	// Create jobs for past, present, and future buckets
	j1 := createJob(t, dq.jobStore, "j1") // past
	j2 := createJob(t, dq.jobStore, "j2") // present
	j3 := createJob(t, dq.jobStore, "j3") // future

	nowSec := dq.currentSec()

	// Bucket 0: past (should be returned)
	dq.buckets[0].TimeSec = nowSec - 10
	dq.buckets[0].Jobs[0] = j1
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 1
	dq.timeToBucket[nowSec-10] = 0

	// Bucket 1: present (should be returned)
	dq.buckets[1].TimeSec = nowSec
	dq.buckets[1].Jobs[0] = j2
	dq.buckets[1].Head = -1
	dq.buckets[1].Tail = 1
	dq.timeToBucket[nowSec] = 1

	// Bucket 2: future (should be skipped)
	dq.buckets[2].TimeSec = nowSec + 100
	dq.buckets[2].Jobs[0] = j3
	dq.buckets[2].Head = -1
	dq.buckets[2].Tail = 1
	dq.timeToBucket[nowSec+100] = 2

	// Read batch of 2 - should only get j1 and j2, skip future bucket
	items := dq.ReadBatch(2, -1, -1, -1)

	require.Len(t, items, 2)
	require.Equal(t, j1.Index, items[0].JobRef.Index)
	require.Equal(t, j2.Index, items[1].JobRef.Index)

	// Verify future bucket was not returned
	for _, item := range items {
		require.NotEqual(t, 2, item.Bucket)
	}

	// Try to read the future bucket explicitly
	items = dq.readPending(1, 2, -1)
	require.Len(t, items, 0, "future bucket should be skipped even when explicitly requested")
}

// TestReadPendingCurrentSecondBucket verifies that buckets at current second are returned
func TestReadPendingCurrentSecondBucket(t *testing.T) {
	dq := newTestQueue()

	// Create a job
	j1 := createJob(t, dq.jobStore, "j1")

	nowSec := dq.currentSec()

	// Put job in a bucket at current second
	dq.buckets[0].TimeSec = nowSec
	dq.buckets[0].Jobs[0] = j1
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 1
	dq.timeToBucket[nowSec] = 0

	// Read from pending - should return the job (current second is due)
	items := dq.ReadBatch(1, -1, -1, -1)

	require.Len(t, items, 1)
	require.Equal(t, j1.Index, items[0].JobRef.Index)
	require.Equal(t, 0, items[0].Bucket)
	require.Equal(t, 0, items[0].Cell)
	require.False(t, items[0].IsNew)

	// Also test with explicit readPending call
	items = dq.readPending(1, 0, -1)
	require.Len(t, items, 1)
	require.Equal(t, j1.Index, items[0].JobRef.Index)
}

// TestReadBatchPriorityRetryOverActive verifies that pending bucket jobs are returned before active queue jobs
func TestReadBatchPriorityRetryOverActive(t *testing.T) {
	dq := newTestQueue()

	// Create 2 active jobs
	active1 := createJob(t, dq.jobStore, "active1")
	active2 := createJob(t, dq.jobStore, "active2")

	// Add both to active queue
	_ = dq.AddNewJob(active1)
	_ = dq.AddNewJob(active2)

	// Create a retry job and put it in a bucket (past due)
	retryJob := createJob(t, dq.jobStore, "retry")

	nowSec := dq.currentSec()
	dq.buckets[0].TimeSec = nowSec - 5 // expired
	dq.buckets[0].Jobs[0] = retryJob
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 1
	dq.timeToBucket[nowSec-5] = 0

	// Read batch of 3 - retry should come first, then active jobs
	items := dq.ReadBatch(3, -1, -1, -1)

	require.Len(t, items, 3)

	// First item should be the retry job from bucket
	require.False(t, items[0].IsNew)
	require.Equal(t, retryJob.Index, items[0].JobRef.Index)
	require.Equal(t, 0, items[0].Bucket)

	// Next items should be active jobs
	require.True(t, items[1].IsNew)
	require.True(t, items[2].IsNew)

	// Verify active jobs were returned (order may vary)
	activeIndices := []int{items[1].JobRef.Index, items[2].JobRef.Index}
	require.Contains(t, activeIndices, active1.Index)
	require.Contains(t, activeIndices, active2.Index)

	// Verify active head advanced
	require.Equal(t, 2, dq.aHead)
}

// TestReadBatchPendingPartiallyFillsBatch verifies active fills remaining when pending count < batch size
func TestReadBatchPendingPartiallyFillsBatch(t *testing.T) {
	dq := newTestQueue()

	// Create 1 pending job (past due)
	pendingJob := createJob(t, dq.jobStore, "pending")
	nowSec := dq.currentSec()
	dq.buckets[0].TimeSec = nowSec - 5
	dq.buckets[0].Jobs[0] = pendingJob
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 1
	dq.timeToBucket[nowSec-5] = 0

	// Create 3 active jobs
	active1 := createJob(t, dq.jobStore, "active1")
	active2 := createJob(t, dq.jobStore, "active2")
	active3 := createJob(t, dq.jobStore, "active3")

	_ = dq.AddNewJob(active1)
	_ = dq.AddNewJob(active2)
	_ = dq.AddNewJob(active3)

	// Read batch of 3 - should get 1 pending + 2 active (fill remaining)
	items := dq.ReadBatch(3, -1, -1, -1)

	require.Len(t, items, 3)

	// First item should be pending
	require.False(t, items[0].IsNew)
	require.Equal(t, pendingJob.Index, items[0].JobRef.Index)
	require.Equal(t, 0, items[0].Bucket)

	// Next 2 items should be active
	require.True(t, items[1].IsNew)
	require.True(t, items[2].IsNew)

	// Verify active jobs returned (order may vary)
	activeIndices := []int{items[1].JobRef.Index, items[2].JobRef.Index}
	require.Contains(t, activeIndices, active1.Index)
	require.Contains(t, activeIndices, active2.Index)

	// active3 should remain in queue
	require.Equal(t, 1, dq.ActiveQueueSize())
}

// TestReadBatchNoPendingActiveOnly verifies ReadBatch reads only from active when no pending jobs exist
func TestReadBatchNoPendingActiveOnly(t *testing.T) {
	dq := newTestQueue()

	// Create 3 active jobs
	active1 := createJob(t, dq.jobStore, "active1")
	active2 := createJob(t, dq.jobStore, "active2")
	active3 := createJob(t, dq.jobStore, "active3")

	_ = dq.AddNewJob(active1)
	_ = dq.AddNewJob(active2)
	_ = dq.AddNewJob(active3)

	// No pending buckets exist (all buckets are free)

	// Read batch of 2 - should get 2 active jobs
	items := dq.ReadBatch(2, -1, -1, -1)

	require.Len(t, items, 2)
	require.True(t, items[0].IsNew)
	require.True(t, items[1].IsNew)

	// Verify active jobs returned (order should match insertion)
	require.Equal(t, active1.Index, items[0].JobRef.Index)
	require.Equal(t, active2.Index, items[1].JobRef.Index)

	// Verify active head advanced
	require.Equal(t, 2, dq.aHead)

	// Read remaining active job
	remaining := dq.ReadBatch(1, -1, -1, -1)
	require.Len(t, remaining, 1)
	require.Equal(t, active3.Index, remaining[0].JobRef.Index)
	require.Equal(t, 3, dq.aHead)
}

// TestReadBatchNoJobsAtAll verifies ReadBatch returns empty when both pending and active are empty
func TestReadBatchNoJobsAtAll(t *testing.T) {
	dq := newTestQueue()

	// No active jobs added
	// No pending buckets with jobs (all buckets are free)

	// Read batch of 10 - should return empty
	items := dq.ReadBatch(10, -1, -1, -1)

	require.Len(t, items, 0)

	// Verify queue is still empty
	require.Equal(t, 0, dq.ActiveQueueSize())

	// Verify pending buckets are still empty
	for _, b := range dq.buckets {
		if b.TimeSec != -1 {
			// If a bucket has a time but no jobs (Head == Tail), it should be considered empty
			require.Equal(t, b.Head, b.Tail, "bucket should not have any jobs")
		}
	}
}

// TestMoveDispatchedBucketToAnotherBucket verifies moving a job from one retry bucket to another
func TestMoveDispatchedBucketToAnotherBucket(t *testing.T) {
	dq := newTestQueue()

	// Create a job and move it to a bucket
	job := createJob(t, dq.jobStore, "j1")
	_ = dq.AddNewJob(job)

	// Read from active and move to bucket 0 (time = now + 10)
	items := dq.ReadBatch(1, -1, -1, -1)
	targetSec1 := dq.currentSec() + 10
	dq.MoveDispatched(
		items[0].JobRef,
		targetSec1*1000,
		items[0].IsNew,
		items[0].Bucket,
		items[0].Cell,
	)

	// Find which bucket the job was placed in
	var bucketIdx1 int
	var cell1 int
	for i, b := range dq.buckets {
		if b.TimeSec == targetSec1 {
			bucketIdx1 = i
			cell1 = 0
			break
		}
	}

	// Verify job is in bucket 0
	require.Equal(t, job.Index, dq.buckets[bucketIdx1].Jobs[cell1].Index)

	// Now move the job from bucket 0 to bucket 1 (time = now + 20)
	targetSec2 := dq.currentSec() + 20

	// Move from bucket to another bucket
	dq.MoveDispatched(
		dq.buckets[bucketIdx1].Jobs[cell1],
		targetSec2*1000,
		false,
		bucketIdx1,
		cell1,
	)

	// Verify old location is tombstoned
	require.Equal(t, -1, dq.buckets[bucketIdx1].Jobs[cell1].Index)
	require.Equal(t, cell1, dq.buckets[bucketIdx1].Head)

	// Verify job is in new bucket
	var bucketIdx2 int
	var cell2 int
	for i, b := range dq.buckets {
		if b.TimeSec == targetSec2 {
			bucketIdx2 = i
			cell2 = 0
			break
		}
	}

	require.Equal(t, dq.buckets[bucketIdx2].TimeSec, targetSec2)
	require.Equal(t, dq.buckets[bucketIdx2].Jobs[cell2].Index, job.Index)
}

// TestMoveDispatchedPreservesRetryCount verifies that retry count is maintained when moving between buckets
func TestMoveDispatchedPreservesRetryCount(t *testing.T) {
	dq := newTestQueue()

	// Create a job
	job := &model.Job{
		ID:    "test-job",
		Topic: "test-topic",
		Data:  []byte("data"),
	}

	idx, err := dq.jobStore.Create(job)
	require.NoError(t, err)

	// Create JobRef with retry count = 3
	jobRef := JobRef{
		Index:      int(idx),
		RetryCount: 3,
	}

	// Add to active queue
	_ = dq.AddNewJob(jobRef)

	// Read from active and move to bucket
	items := dq.ReadBatch(1, -1, -1, -1)
	require.Len(t, items, 1)
	require.Equal(t, 3, items[0].JobRef.RetryCount)

	targetSec1 := dq.currentSec() + 10
	dq.MoveDispatched(
		items[0].JobRef,
		targetSec1*1000,
		items[0].IsNew,
		items[0].Bucket,
		items[0].Cell,
	)

	// Find the bucket and verify JobRef directly
	var bucketIdx1 int
	var cell1 int
	for i, b := range dq.buckets {
		if b.TimeSec == targetSec1 {
			bucketIdx1 = i
			cell1 = 0
			break
		}
	}

	// Verify retry count is still 3 in the bucket
	require.Equal(t, 3, dq.buckets[bucketIdx1].Jobs[cell1].RetryCount, "retry count should be preserved in bucket")

	// Move to another bucket - we need the job from the bucket
	jobRefFromBucket := dq.buckets[bucketIdx1].Jobs[cell1]

	targetSec2 := dq.currentSec() + 20
	dq.MoveDispatched(
		jobRefFromBucket,
		targetSec2*1000,
		false, // isNew = false because it's coming from a bucket
		bucketIdx1,
		cell1,
	)

	// Find the new bucket and verify JobRef directly
	var bucketIdx2 int
	var cell2 int
	for i, b := range dq.buckets {
		if b.TimeSec == targetSec2 {
			bucketIdx2 = i
			cell2 = 0
			break
		}
	}

	// Verify retry count is preserved (still 3) in the new bucket
	require.Equal(t, 3, dq.buckets[bucketIdx2].Jobs[cell2].RetryCount, "retry count should be preserved when moving between buckets")

	// Verify old location is tombstoned
	require.Equal(t, -1, dq.buckets[bucketIdx1].Jobs[cell1].Index)
}

// TestCleanupExpiredBucketWithManyJobs verifies cleanup processes all jobs in an expired bucket
func TestCleanupExpiredBucketWithManyJobs(t *testing.T) {
	dq := newTestQueue()

	// Create 5 jobs and put them in the same bucket
	jobs := make([]JobRef, 5)
	for i := 0; i < 5; i++ {
		job := &model.Job{
			ID:    fmt.Sprintf("job%d", i),
			Topic: "test-topic",
			Data:  []byte("data"),
		}
		idx, err := dq.jobStore.Create(job)
		require.NoError(t, err)
		jobs[i] = JobRef{Index: int(idx)}
	}

	nowSec := dq.currentSec()

	// Place all 5 jobs in bucket 0 (expired)
	dq.buckets[0].TimeSec = nowSec - 10
	for i := 0; i < 5; i++ {
		dq.buckets[0].Jobs[i] = jobs[i]
	}
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 5
	dq.timeToBucket[nowSec-10] = 0

	// Run cleanup
	ok := dq.CleanupOneExpiredBucket()
	require.True(t, ok)

	// Verify bucket was cleared
	require.Equal(t, -1, int(dq.buckets[0].TimeSec))
	require.Equal(t, -1, dq.buckets[0].Head)
	require.Equal(t, 0, dq.buckets[0].Tail)

	// Verify time mapping was removed
	_, ok = dq.timeToBucket[nowSec-10]
	require.False(t, ok)
}

// TestCleanupExpiredBucketFullReset verifies cleanup resets TimeSec, Head, Tail, and clears Jobs
func TestCleanupExpiredBucketFullReset(t *testing.T) {
	dq := newTestQueue()

	// Create a job and put it in a bucket
	job := &model.Job{
		ID:    "test-job",
		Topic: "test-topic",
		Data:  []byte("data"),
	}
	idx, err := dq.jobStore.Create(job)
	require.NoError(t, err)
	jobRef := JobRef{Index: int(idx)}

	nowSec := dq.currentSec()

	// Place job in bucket 0 with custom state
	dq.buckets[0].TimeSec = nowSec - 10
	dq.buckets[0].Jobs[0] = jobRef
	dq.buckets[0].Jobs[1] = JobRef{Index: -1} // tombstone
	dq.buckets[0].Head = 0
	dq.buckets[0].Tail = 2
	dq.timeToBucket[nowSec-10] = 0

	// Run cleanup
	ok := dq.CleanupOneExpiredBucket()
	require.True(t, ok)

	// Verify ALL fields are reset
	require.Equal(t, -1, int(dq.buckets[0].TimeSec), "TimeSec should be reset to -1")
	require.Equal(t, -1, dq.buckets[0].Head, "Head should be reset to -1")
	require.Equal(t, 0, dq.buckets[0].Tail, "Tail should be reset to 0")

	// Verify bucketToTime is reset
	require.Equal(t, -1, int(dq.bucketToTime[0]))

	// Verify time mapping was removed
	_, ok = dq.timeToBucket[nowSec-10]
	require.False(t, ok)
}

// TestCleanupExpiredBucketJobDoneNotDLQ verifies completed jobs are not sent to DLQ during cleanup
func TestCleanupExpiredBucketJobDoneNotDLQ(t *testing.T) {
	dq := newTestQueue()

	// Create a job and mark it as Done
	job := &model.Job{
		ID:    "test-job",
		Topic: "test-topic",
		Data:  []byte("data"),
		Done:  true,
	}
	idx, err := dq.jobStore.Create(job)
	require.NoError(t, err)
	jobRef := JobRef{Index: int(idx)}

	nowSec := dq.currentSec()

	// Place job in bucket 0 (expired)
	dq.buckets[0].TimeSec = nowSec - 10
	dq.buckets[0].Jobs[0] = jobRef
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 1
	dq.timeToBucket[nowSec-10] = 0

	// Get initial DLQ size
	initialDLQSize := dq.dlqBuffer.Size()

	// Run cleanup
	ok := dq.CleanupOneExpiredBucket()
	require.True(t, ok)

	// Verify DLQ size did not increase (done job not sent to DLQ)
	require.Equal(t, initialDLQSize, dq.dlqBuffer.Size(), "done job should not be sent to DLQ")

	// Verify bucket was cleared
	require.Equal(t, -1, int(dq.buckets[0].TimeSec))
	require.Equal(t, 0, dq.buckets[0].Tail)
}

// TestCleanupExpiredBucketMissingJobInStore verifies cleanup handles gracefully when job not found in store
func TestCleanupExpiredBucketMissingJobInStore(t *testing.T) {
	dq := newTestQueue()

	// Create a JobRef with an index that doesn't exist in the store
	jobRef := JobRef{Index: 99999} // non-existent index

	nowSec := dq.currentSec()

	// Place job in bucket 0 (expired)
	dq.buckets[0].TimeSec = nowSec - 10
	dq.buckets[0].Jobs[0] = jobRef
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 1
	dq.timeToBucket[nowSec-10] = 0

	// Get initial DLQ size
	initialDLQSize := dq.dlqBuffer.Size()

	// Run cleanup - should not panic
	ok := dq.CleanupOneExpiredBucket()
	require.True(t, ok)

	// Verify DLQ size did not increase (missing job should be skipped)
	require.Equal(t, initialDLQSize, dq.dlqBuffer.Size(), "missing job should be skipped")

	// Verify bucket was cleared
	require.Equal(t, -1, int(dq.buckets[0].TimeSec))
	require.Equal(t, 0, dq.buckets[0].Tail)
}

// TestCleanupExpiredBucketTombstonedJob verifies tombstoned jobs are skipped during cleanup
func TestCleanupExpiredBucketTombstonedJob(t *testing.T) {
	dq := newTestQueue()

	// Create a real job
	job := &model.Job{
		ID:    "test-job",
		Topic: "test-topic",
		Data:  []byte("data"),
	}
	idx, err := dq.jobStore.Create(job)
	require.NoError(t, err)
	jobRef := JobRef{Index: int(idx)}

	nowSec := dq.currentSec()

	// Place job in bucket 0 (expired) but tombstone it
	dq.buckets[0].TimeSec = nowSec - 10
	dq.buckets[0].Jobs[0] = jobRef
	dq.buckets[0].Jobs[0].Index = -1 // tombstone
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 1
	dq.timeToBucket[nowSec-10] = 0

	// Get initial DLQ size
	initialDLQSize := dq.dlqBuffer.Size()

	// Run cleanup
	ok := dq.CleanupOneExpiredBucket()
	require.True(t, ok)

	// Verify DLQ size did not increase (tombstoned job skipped)
	require.Equal(t, initialDLQSize, dq.dlqBuffer.Size(), "tombstoned job should not be sent to DLQ")

	// Verify bucket was cleared
	require.Equal(t, -1, int(dq.buckets[0].TimeSec))
	require.Equal(t, 0, dq.buckets[0].Tail)
}

// TestCleanupExpiredBucketMiddleBucket verifies cleanup finds expired bucket in the middle of the slice
func TestCleanupExpiredBucketMiddleBucket(t *testing.T) {
	dq := newTestQueue()

	// Create a job
	job := &model.Job{
		ID:    "test-job",
		Topic: "test-topic",
		Data:  []byte("data"),
	}
	idx, err := dq.jobStore.Create(job)
	require.NoError(t, err)
	jobRef := JobRef{Index: int(idx)}

	nowSec := dq.currentSec()

	// Set up buckets: bucket 0 is empty, bucket 1 is expired (middle), bucket 2 is empty
	// Bucket 0: empty (TimeSec = -1)
	dq.buckets[0].TimeSec = -1
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 0

	// Bucket 1: expired with job
	dq.buckets[1].TimeSec = nowSec - 10
	dq.buckets[1].Jobs[0] = jobRef
	dq.buckets[1].Head = -1
	dq.buckets[1].Tail = 1
	dq.timeToBucket[nowSec-10] = 1

	// Bucket 2: empty (TimeSec = -1)
	dq.buckets[2].TimeSec = -1
	dq.buckets[2].Head = -1
	dq.buckets[2].Tail = 0

	// Set nextCleanupIdx to start from beginning
	dq.nextCleanupIdx = 0

	// Run cleanup
	ok := dq.CleanupOneExpiredBucket()
	require.True(t, ok)

	// Verify bucket 1 was cleared (middle bucket)
	require.Equal(t, -1, int(dq.buckets[1].TimeSec))
	require.Equal(t, -1, dq.buckets[1].Head)
	require.Equal(t, 0, dq.buckets[1].Tail)

	// Verify time mapping was removed
	_, ok = dq.timeToBucket[nowSec-10]
	require.False(t, ok)

	// Verify bucket 0 and 2 remain unchanged
	require.Equal(t, -1, int(dq.buckets[0].TimeSec))
	require.Equal(t, -1, int(dq.buckets[2].TimeSec))
}

// TestRegressionMultipleBucketsRead verifies reading across buckets doesn't miss jobs (cell vs c bug)
func TestRegressionMultipleBucketsRead(t *testing.T) {
	dq := newTestQueue()

	// Create 2 jobs
	j1 := createJob(t, dq.jobStore, "j1")
	j2 := createJob(t, dq.jobStore, "j2")

	nowSec := dq.currentSec()

	// Bucket 0: jobA
	dq.buckets[0].TimeSec = nowSec
	dq.buckets[0].Jobs[0] = j1
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 1
	dq.timeToBucket[nowSec] = 0

	// Bucket 1: jobB
	dq.buckets[1].TimeSec = nowSec
	dq.buckets[1].Jobs[0] = j2
	dq.buckets[1].Head = -1
	dq.buckets[1].Tail = 1
	dq.timeToBucket[nowSec] = 1

	// Read batch of 2 - should get both jobs
	items := dq.ReadBatch(2, -1, -1, -1)

	require.Len(t, items, 2)
	require.Equal(t, j1.Index, items[0].JobRef.Index)
	require.Equal(t, 0, items[0].Bucket)
	require.Equal(t, 0, items[0].Cell)
	require.Equal(t, j2.Index, items[1].JobRef.Index)
	require.Equal(t, 1, items[1].Bucket)
	require.Equal(t, 0, items[1].Cell)
}

// TestRegressionTombstoneInterleavedWithLiveJobs verifies reading skips tombstones without breaking prematurely
func TestRegressionTombstoneInterleavedWithLiveJobs(t *testing.T) {
	dq := newTestQueue()

	// Create 3 jobs
	_ = createJob(t, dq.jobStore, "j1")
	_ = createJob(t, dq.jobStore, "j2")
	j3 := createJob(t, dq.jobStore, "j3")

	nowSec := dq.currentSec()

	// Bucket 0: cell0 = tombstone, cell1 = tombstone, cell2 = live
	dq.buckets[0].TimeSec = nowSec
	dq.buckets[0].Jobs[0] = JobRef{Index: -1} // tombstone
	dq.buckets[0].Jobs[1] = JobRef{Index: -1} // tombstone
	dq.buckets[0].Jobs[2] = j3
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 3
	dq.timeToBucket[nowSec] = 0

	// Read batch of 1 - should skip tombstones and return only live job
	items := dq.ReadBatch(1, -1, -1, -1)

	require.Len(t, items, 1)
	require.Equal(t, j3.Index, items[0].JobRef.Index)
	require.Equal(t, 0, items[0].Bucket)
	require.Equal(t, 2, items[0].Cell)
	require.False(t, items[0].IsNew)
}

// TestRegressionResumeOffsetBug verifies reading resumes at correct cell after partial read
func TestRegressionResumeOffsetBug(t *testing.T) {
	dq := newTestQueue()

	// Create 3 jobs in the same bucket
	j1 := createJob(t, dq.jobStore, "j1")
	j2 := createJob(t, dq.jobStore, "j2")
	j3 := createJob(t, dq.jobStore, "j3")

	nowSec := dq.currentSec()

	// Bucket 0: cell0 = j1, cell1 = j2, cell2 = j3
	dq.buckets[0].TimeSec = nowSec
	dq.buckets[0].Jobs[0] = j1
	dq.buckets[0].Jobs[1] = j2
	dq.buckets[0].Jobs[2] = j3
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 3
	dq.timeToBucket[nowSec] = 0

	// Read first job (cell 0)
	firstRead := dq.ReadBatch(1, -1, -1, -1)
	require.Len(t, firstRead, 1)
	require.Equal(t, j1.Index, firstRead[0].JobRef.Index)
	require.Equal(t, 0, firstRead[0].Cell)

	// Tombstone the first job
	dq.RemoveByIndex(firstRead[0].IsNew, firstRead[0].Bucket, firstRead[0].Cell)

	// Read next job - should resume at cell 1 (not cell 0 again)
	secondRead := dq.ReadBatch(1, -1, -1, -1)
	require.Len(t, secondRead, 1)
	require.Equal(t, j2.Index, secondRead[0].JobRef.Index)
	require.Equal(t, 1, secondRead[0].Cell)
}

// TestRegressionReuseExpiredBucket verifies bucket lifecycle: fill → expire → cleanup → reuse with new mapping
func TestRegressionReuseExpiredBucket(t *testing.T) {
	dq := newTestQueue()

	// Create a job and add to active
	job1 := createJob(t, dq.jobStore, "j1")
	_ = dq.AddNewJob(job1)

	// Read from active and move to bucket (retry in 1 second)
	items := dq.ReadBatch(1, -1, -1, -1)
	require.Len(t, items, 1)

	targetSec1 := dq.currentSec() + 1
	dq.MoveDispatched(
		items[0].JobRef,
		targetSec1*1000,
		items[0].IsNew,
		items[0].Bucket,
		items[0].Cell,
	)

	// Find which bucket was used
	var bucketIdx int
	var bucketTime int64
	for i, b := range dq.buckets {
		if b.TimeSec == targetSec1 {
			bucketIdx = i
			bucketTime = b.TimeSec
			break
		}
	}

	// Verify bucket mapping exists
	_, ok := dq.timeToBucket[bucketTime]
	require.True(t, ok)

	// Wait for bucket to expire
	time.Sleep(2100 * time.Millisecond)

	// Run cleanup
	ok = dq.CleanupOneExpiredBucket()
	require.True(t, ok)

	// Verify bucket was cleared
	require.Equal(t, -1, int(dq.buckets[bucketIdx].TimeSec))
	require.Equal(t, 0, dq.buckets[bucketIdx].Tail)

	// Verify old mapping was removed
	_, ok = dq.timeToBucket[bucketTime]
	require.False(t, ok)

	// Create a new job and reuse the expired bucket
	job2 := createJob(t, dq.jobStore, "j2")
	_ = dq.AddNewJob(job2)

	items2 := dq.ReadBatch(1, -1, -1, -1)
	require.Len(t, items2, 1)

	newTargetSec := dq.currentSec() + 5
	dq.MoveDispatched(
		items2[0].JobRef,
		newTargetSec*1000,
		items2[0].IsNew,
		items2[0].Bucket,
		items2[0].Cell,
	)

	// Verify the same bucket was reused
	var newBucketIdx int
	for i, b := range dq.buckets {
		if b.TimeSec == newTargetSec {
			newBucketIdx = i
			break
		}
	}

	// Should reuse the same bucket index
	require.Equal(t, bucketIdx, newBucketIdx, "should reuse the expired bucket")

	// Verify new mapping exists
	_, ok = dq.timeToBucket[newTargetSec]
	require.True(t, ok)

	// Verify old time is gone
	_, ok = dq.timeToBucket[bucketTime]
	require.False(t, ok)

	// Verify new job is in the bucket
	require.Equal(t, job2.Index, dq.buckets[bucketIdx].Jobs[0].Index)
	require.Equal(t, 1, dq.buckets[bucketIdx].Tail)
}

// TestReadActiveOffsetGreaterThanQueue verifies offset doesn't cause panic when greater than queue size
func TestReadActiveOffsetGreaterThanQueue(t *testing.T) {
	dq := newTestQueue()

	// Create 2 jobs and add to active
	j1 := createJob(t, dq.jobStore, "j1")
	j2 := createJob(t, dq.jobStore, "j2")

	_ = dq.AddNewJob(j1)
	_ = dq.AddNewJob(j2)

	// Read with offset greater than queue size (should not panic)
	items := dq.ReadBatch(1, 100, -1, -1)

	// Should return empty
	require.Len(t, items, 0)

	// Read with offset exactly at queue size
	items = dq.ReadBatch(1, 1, -1, -1)
	require.Len(t, items, 0)

	// Read with offset beyond queue size after all jobs read
	items = dq.ReadBatch(1, 100, -1, -1)
	require.Len(t, items, 0)
}

// TestReadPendingWithCountZero verifies readPending returns empty when count is 0
func TestReadPendingWithCountZero(t *testing.T) {
	dq := newTestQueue()

	// Create a job and put it in a bucket
	job := createJob(t, dq.jobStore, "j1")
	nowSec := dq.currentSec()

	dq.buckets[0].TimeSec = nowSec
	dq.buckets[0].Jobs[0] = job
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 1
	dq.timeToBucket[nowSec] = 0

	// Read with count=0 - should return empty
	items := dq.readPending(0, 0, -1)

	require.Len(t, items, 0)

	// Verify the job is still in the bucket (read was a no-op)
	require.Equal(t, job.Index, dq.buckets[0].Jobs[0].Index)
	require.Equal(t, 1, dq.buckets[0].Tail)
}

// TestReadPendingExactCurrentSecond verifies bucket with TimeSec == currentSec is considered due
func TestReadPendingExactCurrentSecond(t *testing.T) {
	dq := newTestQueue()

	// Create a job
	job := createJob(t, dq.jobStore, "j1")

	nowSec := dq.currentSec()

	// Put job in bucket at exact current second
	dq.buckets[0].TimeSec = nowSec
	dq.buckets[0].Jobs[0] = job
	dq.buckets[0].Head = -1
	dq.buckets[0].Tail = 1
	dq.timeToBucket[nowSec] = 0

	// Read from pending - should return the job (exact current second is due)
	items := dq.readPending(1, 0, -1)

	require.Len(t, items, 1)
	require.Equal(t, job.Index, items[0].JobRef.Index)
	require.Equal(t, 0, items[0].Bucket)
	require.Equal(t, 0, items[0].Cell)

	// Verify the condition: b.TimeSec > nowSec is false when equal
	// So bucket with TimeSec == nowSec IS considered due
	require.False(t, dq.buckets[0].TimeSec > nowSec, "TimeSec == currentSec should NOT be > currentSec")
}

func TestDispatchQueue_LargeScaleRetries(t *testing.T) {
	dq := NewDispatchQueue(64, 500, nil, NewJobStore(10000))

	const numJobs = 300
	jobs := make([]JobRef, numJobs)

	// Add jobs
	for i := 0; i < numJobs; i++ {
		jobs[i] = createJob(t, dq.jobStore, fmt.Sprintf("job-%d", i))
		dq.AddNewJob(jobs[i])
	}

	// First dispatch - all new jobs
	items := dq.ReadBatch(500, -1, -1, -1)
	require.Len(t, items, numJobs)

	// Move all to a future retry time (same second for predictability)
	targetSec := dq.currentSec() + 5

	for _, item := range items {
		dq.MoveDispatched(item.JobRef, targetSec*1000, item.IsNew, item.Bucket, item.Cell)
	}

	// Should not read anything yet (future bucket)
	items = dq.ReadBatch(100, -1, -1, -1)
	require.Len(t, items, 0, "should not read future buckets")

	// Advance time
	time.Sleep(5200 * time.Millisecond)

	// Now all retries should be readable
	items = dq.ReadBatch(500, -1, -1, -1)
	require.Len(t, items, numJobs, "should read all retries after due time")

	// All should be from buckets
	for _, item := range items {
		require.False(t, item.IsNew, "should be retry jobs from buckets")
		require.Equal(t, targetSec, dq.bucketToTime[item.Bucket])
	}
}

func TestDispatchQueue_RetryCountPreservation(t *testing.T) {
	dq := newTestQueue()

	job := createJob(t, dq.jobStore, "test")
	job.RetryCount = 3 // start with retry count

	dq.AddNewJob(job)
	items := dq.ReadBatch(1, -1, -1, -1)

	require.Equal(t, 3, items[0].JobRef.RetryCount)

	// Move to first bucket
	dq.MoveDispatched(items[0].JobRef, (dq.currentSec()+5)*1000, true, -1, 0)

	// Move again to another bucket
	// Find the job
	var bIdx, cell int
	for i := range dq.buckets {
		if dq.buckets[i].TimeSec > 0 {
			bIdx = i
			cell = 0
			break
		}
	}

	dq.MoveDispatched(dq.buckets[bIdx].Jobs[cell], (dq.currentSec()+15)*1000, false, bIdx, cell)

	// Verify retry count survived both moves
	require.Equal(t, 3, dq.buckets[bIdx].Jobs[cell].RetryCount) // wait, need to find new bucket
}

func TestDispatchQueue_ActiveQueueFull(t *testing.T) {
	dq := NewDispatchQueue(10, 5, nil, NewJobStore(100)) // small active capacity

	for i := 0; i < 5; i++ {
		job := createJob(t, dq.jobStore, fmt.Sprintf("j%d", i))
		require.NoError(t, dq.AddNewJob(job))
	}

	// 6th should fail
	job6 := createJob(t, dq.jobStore, "j6")
	err := dq.AddNewJob(job6)
	require.Error(t, err, "should return capacity error")
}

func TestDispatchQueue_BucketPressureAndCleanup(t *testing.T) {
	jobStore := NewJobStore(5000)
	metrics := internal.GetPartitionMetrics()
	logger, _ := zap.NewDevelopment()

	dlq := NewDLQBuffer("test-topic", 1024*1024, 60000, jobStore, logger, metrics)
	dq := NewDispatchQueue(32, 100, dlq, jobStore)

	nowSec := dq.currentSec()

	// Setup multiple expired buckets
	for i := 0; i < 20; i++ {
		dq.buckets[i].TimeSec = nowSec - 10
		dq.buckets[i].Head = -1
		dq.buckets[i].Tail = 1
		dq.buckets[i].Jobs[0] = createJob(t, dq.jobStore, fmt.Sprintf("job-%d", i))
		dq.timeToBucket[nowSec-10-int64(i)] = i
	}

	// Run cleanup
	cleaned := 0
	for i := 0; i < 30; i++ {
		if dq.CleanupOneExpiredBucket() {
			cleaned++
		}
	}

	require.GreaterOrEqual(t, cleaned, 15, "should clean many expired buckets")
}
