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
	"errors"
	"time"
)

type dispatchItem struct {
	JobRef JobRef
	IsNew  bool
	Bucket int // -1 for active queue
	Cell   int // index in the slice
}

// DispatchQueue implements the requirements
type DispatchQueue struct {
	active []JobRef
	aHead  int // next unread active position
	// aTail: append position
	// : readActive advances aHead, tombstone marks slot dead only
	// For the active queue: active -> read -> never returns to active
	// readActive() is effectively consuming the active queue
	aTail int

	buckets      []Bucket
	numBuckets   int
	bucketCap    int
	timeToBucket map[int64]int // second -> bucket index
	bucketToTime []int64

	// for buckets: Head is the last processed bucket position and tombstone advances bucket Head
	bucketsLast [2]int // last processed offset bucket, index

	dlqBuffer *DLQBuffer
	jobStore  *JobStore

	nextCleanupIdx int
}

type Bucket struct {
	Jobs    []JobRef
	TimeSec int64 // -1 if free
	Head    int
	Tail    int
}

// NewDispatchQueue creates a new queue
func NewDispatchQueue(numBuckets, bucketCap int, dlqBuffer *DLQBuffer, jobStore *JobStore) *DispatchQueue {
	dq := &DispatchQueue{
		active:         make([]JobRef, bucketCap),
		buckets:        make([]Bucket, numBuckets),
		numBuckets:     numBuckets,
		bucketCap:      bucketCap,
		timeToBucket:   make(map[int64]int, numBuckets),
		bucketToTime:   make([]int64, numBuckets),
		aHead:          0,
		aTail:          0,
		bucketsLast:    [2]int{-1, -1},
		dlqBuffer:      dlqBuffer,
		jobStore:       jobStore,
		nextCleanupIdx: 0,
	}

	for i := range dq.buckets {
		dq.buckets[i] = Bucket{
			Jobs:    make([]JobRef, bucketCap),
			TimeSec: -1,
		}
		for j := range dq.buckets[i].Jobs {
			dq.buckets[i].Jobs[j].Index = -1
		}
		dq.bucketToTime[i] = -1
	}

	return dq
}

func (dq *DispatchQueue) currentSec() int64 {
	nowMs := time.Now().UnixMilli()
	return nowMs / 1000
}

// ==================== ADD ====================

func (dq *DispatchQueue) AddNewJob(job JobRef) error {
	if dq.aTail >= len(dq.active) {
		return errors.New("Active queue capacity exceeded - increase bucketCap")
	}
	dq.active[dq.aTail] = job
	dq.aTail++
	return nil
}

// ==================== MOVE / REMOVE ====================

// tombstone tells the queue that this slot has been processed (moved or removed)
func (dq *DispatchQueue) tombstone(isNew bool, bucketIdx, cell int) {
	if isNew {
		if cell >= 0 && cell < len(dq.active) {
			dq.active[cell].Index = -1
			// do not advance head here we already consumed the record
			// if we set head == cel here the head reverses back to a wong index
		}
	} else if bucketIdx >= 0 && bucketIdx < len(dq.buckets) {
		b := &dq.buckets[bucketIdx]
		if cell >= 0 && cell < len(b.Jobs) {
			b.Jobs[cell].Index = -1
			b.Head = cell
			dq.bucketsLast[0] = bucketIdx
			dq.bucketsLast[1] = cell
		}
	}
}

// MoveDispatched moves a job from its current location to a future retry bucket
func (dq *DispatchQueue) MoveDispatched(
	job JobRef,
	targetTimeMs int64,
	isNew bool,
	bucket int,
	cell int,
) {
	targetSec := targetTimeMs / 1000
	if targetSec < dq.currentSec() {
		panic("target retry time after dispatch should not be in the past")
	}
	if targetSec == dq.currentSec() {
		targetSec++
	}

	// Invalidate the old location
	dq.tombstone(isNew, bucket, cell)

	// Find or create bucket for the target second
	bIdx := dq.findOrCreateBucket(targetSec)

	// Append to that bucket
	b := &dq.buckets[bIdx]
	if b.Tail >= len(b.Jobs) {
		panic("Bucket capacity exceeded")
	}

	b.Jobs[b.Tail] = job
	b.Tail++
}

// RemoveByIndex is used when we drop a job (max retries reached)
func (dq *DispatchQueue) RemoveByIndex(isNew bool, bucket, cell int) {
	dq.tombstone(isNew, bucket, cell)
}

// ==================== BUCKET MANAGEMENT ====================

func (dq *DispatchQueue) findOrCreateBucket(targetSec int64) int {
	if idx, ok := dq.timeToBucket[targetSec]; ok {
		return idx
	}

	nowSec := dq.currentSec()
	// Reuse a free or expired bucket
	for i := range dq.buckets {
		b := &dq.buckets[i]
		if b.TimeSec == -1 || (b.TimeSec < nowSec && b.Head+1 >= b.Tail) {
			if b.TimeSec != -1 {
				delete(dq.timeToBucket, b.TimeSec)
			}
			b.TimeSec = targetSec
			b.Head = -1
			b.Tail = 0
			dq.timeToBucket[targetSec] = i
			dq.bucketToTime[i] = targetSec
			return i
		}
	}

	panic("No available bucket. Increase numBuckets")
}

// ==================== READ ====================

func (dq *DispatchQueue) ReadBatch(count int, activeOffset, lastBucket, buckOffset int) []dispatchItem {
	// 1. Read from pending buckets (past or current second)
	items := dq.readPending(count, lastBucket, buckOffset) // simplified for now

	// 2. Read from active if needed
	remaining := count - len(items)
	if remaining > 0 {
		items = dq.readActive(remaining, activeOffset, items)
	}

	return items
}

func (dq *DispatchQueue) readPending(count int, bucketIndx, cell int) []dispatchItem {
	nowSec := dq.currentSec()

	// If starting fresh, continue from where we left off
	if bucketIndx == -1 {
		bucketIndx = max(dq.bucketsLast[0], 0)
		cell = dq.bucketsLast[1]
	}

	// Wrap bucket cursor for circular scanning.
	if bucketIndx >= dq.numBuckets {
		bucketIndx = 0
	}

	items := make([]dispatchItem, 0)
	lastBucket := bucketIndx
	lastCell := cell

	for i := bucketIndx; i < dq.numBuckets && len(items) < count; i++ {
		b := &dq.buckets[i]
		startCell := b.Head + 1
		if i == bucketIndx {
			startCell = max(cell+1, b.Head+1)
		}
		if b.TimeSec == -1 || b.TimeSec > nowSec || b.Head+1 >= b.Tail {
			continue
		}

		for c := startCell; c < b.Tail && len(items) < count; c++ {
			jr := b.Jobs[c] // Use cell, not b.Head
			if jr.Index >= 0 {
				items = append(items, dispatchItem{
					JobRef: jr,
					IsNew:  false,
					Bucket: i,
					Cell:   c, // Use cell, not b.Head
				})
				lastCell = c
			}
		}
		lastBucket = i
	}

	// Update last positions
	if len(items) > 0 {
		dq.bucketsLast[0] = lastBucket
		dq.bucketsLast[1] = lastCell
	}

	return items
}

func (dq *DispatchQueue) readActive(max int, offset int, items []dispatchItem) []dispatchItem {
	from := offset + 1
	if from < dq.aHead {
		from = dq.aHead
	}
	startLen := len(items)
	lastRead := from - 1
	for i := from; i < dq.aTail && (len(items)-startLen) < max; i++ {
		jr := dq.active[i]
		if jr.Index >= 0 {
			items = append(items, dispatchItem{
				JobRef: jr,
				IsNew:  true,
				Bucket: -1,
				Cell:   i,
			})
			lastRead = i
		}
	}

	// Update the head pointer to where we've read up to
	// This allows compaction
	if lastRead >= dq.aHead {
		dq.aHead = lastRead + 1
	}

	return items
}

func (dq *DispatchQueue) ActiveQueueSize() int {
	return dq.aTail - dq.aHead
}

// CleanupOneExpiredBucket finds the next expired bucket and sends jobs to DLQ
// Only cleans ONE bucket per call
func (dq *DispatchQueue) CleanupOneExpiredBucket() bool {
	if dq.dlqBuffer == nil || dq.jobStore == nil {
		return false
	}

	nowSec := time.Now().Unix()
	startIdx := dq.nextCleanupIdx

	// Helper to process a single bucket
	processBucket := func(idx int, b *Bucket) bool {
		if b.TimeSec == -1 || b.TimeSec >= nowSec || b.Head+1 >= b.Tail {
			return false
		}

		// Move pointer for next call
		dq.nextCleanupIdx = idx + 1

		// Process all jobs in this bucket
		for j := max(b.Head+1, 0); j < b.Tail; j++ {
			jobRef := b.Jobs[j]
			if jobRef.Index >= 0 {
				// Get job from store to calculate payload size
				job := dq.jobStore.Get(uint32(jobRef.Index))
				if job != nil && !job.Done {
					payloadSize := int64(len(job.Data))
					dq.dlqBuffer.Add(jobRef, payloadSize, job.Done)
				}
			}
		}

		// Clear the bucket
		if b.TimeSec != -1 {
			delete(dq.timeToBucket, b.TimeSec)
		}
		b.TimeSec = -1
		b.Head = -1
		b.Tail = 0
		dq.bucketToTime[idx] = -1

		return true
	}

	// Scan from last position
	for i := startIdx; i < len(dq.buckets); i++ {
		b := &dq.buckets[i]
		if processBucket(i, b) {
			return true
		}
	}

	// Wrap around from start
	for i := 0; i < startIdx && i < len(dq.buckets); i++ {
		b := &dq.buckets[i]
		if processBucket(i, b) {
			return true
		}
	}

	// No expired buckets found, reset pointer
	dq.nextCleanupIdx = 0
	return false
}
