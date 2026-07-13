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
	"testing"
	"time"
)

// TestHelper provides assertion methods for ProcessQueue testing
type TestHelper struct {
	t *testing.T
}

func NewTestHelper(t *testing.T) *TestHelper {
	return &TestHelper{t: t}
}

// AssertActiveSize checks the live count matches expected
func (h *TestHelper) AssertActiveSize(pq *ProcessQueue, expected int) {
	h.t.Helper()
	if pq.ActiveSize() != expected {
		h.t.Errorf("ActiveSize: expected %d, got %d", expected, pq.ActiveSize())
	}
}

// AssertJobAtCell checks a specific cell matches expected JobRef fields
func (h *TestHelper) AssertJobAtCell(pq *ProcessQueue, cell int, expected JobRef) {
	h.t.Helper()
	if cell < 0 || cell >= pq.liveCount {
		h.t.Errorf("AssertJobAtCell: cell %d out of range (liveCount=%d)", cell, pq.liveCount)
		return
	}
	actual := pq.records[cell]
	if actual != expected {
		h.t.Errorf("Job at cell %d:\nexpected: %+v\ngot:      %+v", cell, expected, actual)
	}
}

// AssertJobExists verifies a job with given index exists in the queue
func (h *TestHelper) AssertJobExists(pq *ProcessQueue, index int) {
	h.t.Helper()
	for i := 0; i < pq.liveCount; i++ {
		if pq.records[i].Index == index {
			return
		}
	}
	h.t.Errorf("Job with Index %d not found in queue", index)
}

// AssertJobNotExists verifies a job with given index does NOT exist
func (h *TestHelper) AssertJobNotExists(pq *ProcessQueue, index int) {
	h.t.Helper()
	for i := 0; i < pq.liveCount; i++ {
		if pq.records[i].Index == index {
			h.t.Errorf("Job with Index %d found at cell %d (should not exist)", index, i)
			return
		}
	}
}

// AssertReadBatch verifies ReadBatch returns expected JobRefs (order matters)
func (h *TestHelper) AssertReadBatch(pq *ProcessQueue, count int, nowSec int64, pool *sync.Pool, expected []JobRef) {
	h.t.Helper()
	items := pq.ReadBatch(count, nowSec, pool)
	//nolint:staticcheck
	defer pool.Put(items[:0])

	if len(items) != len(expected) {
		h.t.Errorf("ReadBatch: expected %d items, got %d", len(expected), len(items))
		return
	}

	for i := range items {
		if items[i] != expected[i] {
			h.t.Errorf("ReadBatch item %d:\nexpected: %+v\ngot:      %+v", i, expected[i], items[i])
		}
	}
}

// AssertReadBatchReady verifies ReadBatch returns exactly the ready jobs (ignores not-ready)
func (h *TestHelper) AssertReadBatchReady(pq *ProcessQueue, count int, nowSec int64, pool *sync.Pool, expectedIndices []int) {
	h.t.Helper()
	items := pq.ReadBatch(count, nowSec, pool)
	//nolint:staticcheck
	defer pool.Put(items[:0])

	if len(items) != len(expectedIndices) {
		h.t.Errorf("ReadBatch: expected %d items, got %d", len(expectedIndices), len(items))
		return
	}

	for i, idx := range expectedIndices {
		if items[i].Index != idx {
			h.t.Errorf("ReadBatch item %d: expected Index %d, got %d", i, idx, items[i].Index)
		}
	}
}

// AssertQueueFull verifies IsFull returns expected
func (h *TestHelper) AssertQueueFull(pq *ProcessQueue, expected bool) {
	h.t.Helper()
	if pq.IsFull() != expected {
		h.t.Errorf("IsFull: expected %v, got %v", expected, pq.IsFull())
	}
}

// AssertTombstoneAtCell verifies a cell contains tombstone (only valid beyond liveCount)
func (h *TestHelper) AssertTombstoneAtCell(pq *ProcessQueue, cell int) {
	h.t.Helper()
	if cell < pq.liveCount {
		h.t.Errorf("cell %d is within live region (liveCount=%d), should not be tombstone", cell, pq.liveCount)
		return
	}
	if cell >= len(pq.records) {
		h.t.Errorf("cell %d out of bounds (capacity=%d)", cell, len(pq.records))
		return
	}
	if pq.records[cell] != tombstone {
		h.t.Errorf("cell %d expected tombstone, got %+v", cell, pq.records[cell])
	}
}

// AssertAllTombstones verifies all cells from start to capacity are tombstones
func (h *TestHelper) AssertAllTombstones(pq *ProcessQueue, start int) {
	h.t.Helper()
	for i := start; i < len(pq.records); i++ {
		if pq.records[i] != tombstone {
			h.t.Errorf("cell %d expected tombstone, got %+v", i, pq.records[i])
		}
	}
}

// ---------------------------------
// Tests
// ---------------------------------

func TestProcessQueue_AddNewJob(t *testing.T) {
	h := NewTestHelper(t)

	t.Run("add single job", func(t *testing.T) {
		pq := NewProcessQueue(5)
		job := JobRef{Index: 1, RetryCount: 0, DueTimeSec: 100}

		err := pq.AddNewJob(job)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		h.AssertActiveSize(pq, 1)
		h.AssertJobAtCell(pq, 0, job)
	})

	t.Run("add multiple jobs fills sequentially", func(t *testing.T) {
		pq := NewProcessQueue(5)
		jobs := []JobRef{
			{Index: 1, RetryCount: 0, Cell: 0, DueTimeSec: 100},
			{Index: 2, RetryCount: 0, Cell: 1, DueTimeSec: 200},
			{Index: 3, RetryCount: 0, Cell: 2, DueTimeSec: 300},
		}

		for _, job := range jobs {
			pq.AddNewJob(job)
		}

		h.AssertActiveSize(pq, 3)
		for i, job := range jobs {
			h.AssertJobAtCell(pq, i, job)
		}
	})

	t.Run("returns error when full", func(t *testing.T) {
		pq := NewProcessQueue(2)
		pq.AddNewJob(JobRef{Index: 1})
		pq.AddNewJob(JobRef{Index: 2})

		err := pq.AddNewJob(JobRef{Index: 3})
		if err == nil {
			t.Error("expected error when queue full, got nil")
		}
		h.AssertActiveSize(pq, 2)
	})
}

func TestProcessQueue_RemoveByCell(t *testing.T) {
	h := NewTestHelper(t)
	pq := NewProcessQueue(5)

	// Setup: add 5 jobs
	jobs := []JobRef{
		{Index: 1, RetryCount: 0, Cell: 0, DueTimeSec: 100},
		{Index: 2, RetryCount: 0, Cell: 1, DueTimeSec: 200},
		{Index: 3, RetryCount: 0, Cell: 2, DueTimeSec: 300},
		{Index: 4, RetryCount: 0, Cell: 3, DueTimeSec: 400},
		{Index: 5, RetryCount: 0, Cell: 4, DueTimeSec: 500},
	}
	for _, job := range jobs {
		pq.AddNewJob(job)
	}

	t.Run("remove from middle swaps with last and updates cell", func(t *testing.T) {
		pq := NewProcessQueue(5)
		for _, job := range jobs {
			pq.AddNewJob(job)
		}

		// Remove job at cell 2 (Index 3)
		pq.RemoveByCellIndex(2)

		h.AssertActiveSize(pq, 4)
		// Cell 2 should now have the last job (Index 5) with updated Cell
		expected := JobRef{Index: 5, RetryCount: 0, Cell: 2, DueTimeSec: 500}
		h.AssertJobAtCell(pq, 2, expected)
		// Original positions remain
		h.AssertJobAtCell(pq, 0, jobs[0])
		h.AssertJobAtCell(pq, 1, jobs[1])
		h.AssertJobAtCell(pq, 3, jobs[3])
		// Cell 4 should be tombstone (beyond liveCount)
		h.AssertTombstoneAtCell(pq, 4)
	})

	t.Run("remove last just decrements liveCount", func(t *testing.T) {
		pq := NewProcessQueue(5)
		for _, job := range jobs {
			pq.AddNewJob(job)
		}

		pq.RemoveByCellIndex(4)

		h.AssertActiveSize(pq, 4)
		h.AssertJobAtCell(pq, 0, jobs[0])
		h.AssertJobAtCell(pq, 1, jobs[1])
		h.AssertJobAtCell(pq, 2, jobs[2])
		h.AssertJobAtCell(pq, 3, jobs[3])
		h.AssertTombstoneAtCell(pq, 4)
	})

	t.Run("remove first swaps with last and updates cell", func(t *testing.T) {
		pq := NewProcessQueue(5)
		for _, job := range jobs {
			pq.AddNewJob(job)
		}

		pq.RemoveByCellIndex(0)

		h.AssertActiveSize(pq, 4)
		expected := JobRef{Index: 5, RetryCount: 0, Cell: 0, DueTimeSec: 500}
		h.AssertJobAtCell(pq, 0, expected)
		h.AssertJobAtCell(pq, 1, jobs[1])
		h.AssertJobAtCell(pq, 2, jobs[2])
		h.AssertJobAtCell(pq, 3, jobs[3])
	})

	t.Run("remove invalid cell does nothing", func(t *testing.T) {
		pq := NewProcessQueue(5)
		for _, job := range jobs {
			pq.AddNewJob(job)
		}

		pq.RemoveByCellIndex(-1)
		h.AssertActiveSize(pq, 5)

		pq.RemoveByCellIndex(10)
		h.AssertActiveSize(pq, 5)
	})
}

func TestProcessQueue_UpdateRetry(t *testing.T) {
	h := NewTestHelper(t)
	pq := NewProcessQueue(5)

	// Setup
	job := JobRef{Index: 1, RetryCount: 0, Cell: 0, DueTimeSec: 100}
	pq.AddNewJob(job)

	t.Run("updates retry count and due time", func(t *testing.T) {
		pq := NewProcessQueue(5)
		job := JobRef{Index: 1, RetryCount: 0, DueTimeSec: 100}
		pq.AddNewJob(job)

		now := time.Now().Unix()
		err := pq.UpdateRetry(0, 1, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expected := JobRef{Index: 1, RetryCount: 1, Cell: 0, DueTimeSec: now + 10}
		h.AssertJobAtCell(pq, 0, expected)
	})

	t.Run("returns error for invalid cell", func(t *testing.T) {
		pq := NewProcessQueue(5)
		job := JobRef{Index: 1, RetryCount: 0, DueTimeSec: 100}
		pq.AddNewJob(job)

		err := pq.UpdateRetry(5, 1, 10)
		if err == nil {
			t.Error("expected error for invalid cell, got nil")
		}

		err = pq.UpdateRetry(-1, 1, 10)
		if err == nil {
			t.Error("expected error for invalid cell, got nil")
		}
	})
}

func TestProcessQueue_ReadBatch(t *testing.T) {
	h := NewTestHelper(t)
	pool := &sync.Pool{New: func() interface{} { return make([]JobRef, 0, 100) }}

	t.Run("returns ready jobs oldest first", func(t *testing.T) {
		pq := NewProcessQueue(10)
		now := int64(500)

		jobs := []JobRef{
			{Index: 1, RetryCount: 0, DueTimeSec: 100}, // ready
			{Index: 2, RetryCount: 0, DueTimeSec: 300}, // ready
			{Index: 3, RetryCount: 0, DueTimeSec: 600}, // not ready
			{Index: 4, RetryCount: 0, DueTimeSec: 200}, // ready
			{Index: 5, RetryCount: 0, DueTimeSec: 400}, // ready
		}
		for _, job := range jobs {
			pq.AddNewJob(job)
		}

		// Should return oldest first for two seconds: 100, 200
		expected := []int{1, 4}
		h.AssertReadBatchReady(pq, 10, now, pool, expected)
	})

	t.Run("returns only up to count", func(t *testing.T) {
		pq := NewProcessQueue(10)
		now := int64(500)

		for i := 1; i <= 5; i++ {
			pq.AddNewJob(JobRef{Index: i, RetryCount: 0, DueTimeSec: 100})
		}

		// Request 3 jobs, should get first 3 oldest (all due 100)
		expected := []int{1, 2, 3}
		h.AssertReadBatchReady(pq, 3, now, pool, expected)

		// Request 10 jobs, should get all 5
		expected = []int{1, 2, 3, 4, 5}
		h.AssertReadBatchReady(pq, 10, now, pool, expected)
	})

	t.Run("returns empty when no ready jobs", func(t *testing.T) {
		pq := NewProcessQueue(5)
		now := int64(100)

		pq.AddNewJob(JobRef{Index: 1, DueTimeSec: 200})
		pq.AddNewJob(JobRef{Index: 2, DueTimeSec: 300})

		items := pq.ReadBatch(10, now, pool)
		//nolint:staticcheck
		defer pool.Put(items[:0])

		if len(items) != 0 {
			t.Errorf("expected 0 items, got %d", len(items))
		}
	})

	t.Run("respects queue order after removals", func(t *testing.T) {
		pq := NewProcessQueue(10)
		now := int64(500)

		// Add 5 jobs
		for i := 1; i <= 5; i++ {
			pq.AddNewJob(JobRef{Index: i, RetryCount: 0, DueTimeSec: int64(i * 100)})
		}

		// Remove job at cell 2 (Index 3)
		pq.RemoveByCellIndex(2)

		// Should return: 1 (100), 2 (200)
		// But oldest first: 100, 200
		expected := []int{1, 2}
		h.AssertReadBatchReady(pq, 10, now, pool, expected)
	})

	t.Run("handles mixed ready and not ready with oldest tracking", func(t *testing.T) {
		pq := NewProcessQueue(10)
		now := int64(500)

		jobs := []JobRef{
			{Index: 1, DueTimeSec: 600}, // not ready
			{Index: 2, DueTimeSec: 100}, // ready
			{Index: 3, DueTimeSec: 700}, // not ready
			{Index: 4, DueTimeSec: 200}, // ready
			{Index: 5, DueTimeSec: 800}, // not ready
		}
		for _, job := range jobs {
			pq.AddNewJob(job)
		}

		expected := []int{2, 4} // Only ready jobs, oldest first
		h.AssertReadBatchReady(pq, 10, now, pool, expected)
	})
}

func TestProcessQueue_IsFull(t *testing.T) {
	h := NewTestHelper(t)

	t.Run("returns false when not full", func(t *testing.T) {
		pq := NewProcessQueue(3)
		h.AssertQueueFull(pq, false)

		pq.AddNewJob(JobRef{Index: 1})
		h.AssertQueueFull(pq, false)

		pq.AddNewJob(JobRef{Index: 2})
		h.AssertQueueFull(pq, false)
	})

	t.Run("returns true when full", func(t *testing.T) {
		pq := NewProcessQueue(3)
		pq.AddNewJob(JobRef{Index: 1})
		pq.AddNewJob(JobRef{Index: 2})
		pq.AddNewJob(JobRef{Index: 3})
		h.AssertQueueFull(pq, true)
	})

	t.Run("becomes not full after removal", func(t *testing.T) {
		pq := NewProcessQueue(3)
		pq.AddNewJob(JobRef{Index: 1})
		pq.AddNewJob(JobRef{Index: 2})
		pq.AddNewJob(JobRef{Index: 3})

		pq.RemoveByCellIndex(1)
		h.AssertQueueFull(pq, false)

		pq.AddNewJob(JobRef{Index: 4})
		h.AssertQueueFull(pq, true)
	})
}

func TestProcessQueue_CellIntegrity(t *testing.T) {
	h := NewTestHelper(t)
	pool := &sync.Pool{New: func() interface{} { return make([]JobRef, 0, 10) }}

	t.Run("cell fields stay correct after swap-remove", func(t *testing.T) {
		pq := NewProcessQueue(5)
		now := int64(100)

		// Add 3 jobs
		jobs := []JobRef{
			{Index: 1, RetryCount: 0, DueTimeSec: now},
			{Index: 2, RetryCount: 0, DueTimeSec: now + 10},
			{Index: 3, RetryCount: 0, DueTimeSec: now + 20},
		}
		for _, job := range jobs {
			pq.AddNewJob(job)
		}

		// Remove cell 0 (Index 1) - should swap with cell 2 (Index 3)
		pq.RemoveByCellIndex(0)

		// Cell 0 should now be Index 3 with Cell=0
		h.AssertJobAtCell(pq, 0, JobRef{Index: 3, RetryCount: 0, Cell: 0, DueTimeSec: now + 20})
		// Cell 1 should be Index 2 with Cell=1
		h.AssertJobAtCell(pq, 1, JobRef{Index: 2, RetryCount: 0, Cell: 1, DueTimeSec: now + 10})

		// ReadBatch should work correctly
		items := pq.ReadBatch(10, now+15, pool)
		//nolint:staticcheck
		defer pool.Put(items[:0])

		// Should return Index 2 (due now+10) - Index 3 (due now+20) is not ready yet
		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(items))
		}
		if items[0].Index != 2 {
			t.Errorf("expected first item Index 2, got %d", items[0].Index)
		}
	})

	t.Run("cell fields correct after update and removal sequence", func(t *testing.T) {
		pq := NewProcessQueue(5)
		baseTime := time.Now().Unix()

		// Add 4 jobs with due times: baseTime+10, baseTime+20, baseTime+30, baseTime+40
		for i := 1; i <= 4; i++ {
			pq.AddNewJob(JobRef{Index: i, RetryCount: 0, DueTimeSec: baseTime + int64(i*10)})
		}

		// Update retry on cell 1 (Index 2) - was due baseTime+20, becomes baseTime+30
		// delaySec = 30 means due at currentSec() + 30, not baseTime + 30
		// We pass 30 as delay from current time
		err := pq.UpdateRetry(1, 1, 30)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify cell 1 has updated retry count (due time will be current+30, not base+30)
		updatedJob := pq.records[1]
		if updatedJob.Index != 2 {
			t.Errorf("cell 1 should have Index 2, got %d", updatedJob.Index)
		}
		if updatedJob.RetryCount != 1 {
			t.Errorf("cell 1 RetryCount expected 1, got %d", updatedJob.RetryCount)
		}
		if updatedJob.Cell != 1 {
			t.Errorf("cell 1 Cell expected 1, got %d", updatedJob.Cell)
		}

		// Remove cell 0 (Index 1) - swaps with last (Index 4 at cell 3)
		pq.RemoveByCellIndex(0)

		// After removal, live jobs should be compact: [Index 4, Index 2, Index 3]
		// with liveCount = 3
		h.AssertActiveSize(pq, 3)

		// Cell 0 should now be Index 4 with Cell=0
		h.AssertJobAtCell(pq, 0, JobRef{Index: 4, RetryCount: 0, Cell: 0, DueTimeSec: baseTime + 40})

		// Cell 1 should still be Index 2 with Cell=1 (wasn't moved)
		h.AssertJobAtCell(pq, 1, JobRef{Index: 2, RetryCount: 1, Cell: 1, DueTimeSec: pq.records[1].DueTimeSec})

		// Cell 2 should be Index 3 with Cell=2
		h.AssertJobAtCell(pq, 2, JobRef{Index: 3, RetryCount: 0, Cell: 2, DueTimeSec: baseTime + 30})

		// Cell 3 and beyond should be tombstones
		h.AssertTombstoneAtCell(pq, 3)
		h.AssertTombstoneAtCell(pq, 4)
	})
}

func TestProcessQueue_AfterRemovalReadd(t *testing.T) {
	h := NewTestHelper(t)

	t.Run("can readd after removals fills from end", func(t *testing.T) {
		pq := NewProcessQueue(5)

		// Add 5 jobs
		for i := 1; i <= 5; i++ {
			pq.AddNewJob(JobRef{Index: i, RetryCount: 0, DueTimeSec: int64(i * 100)})
		}

		// Remove cell 1 (Index 2)
		pq.RemoveByCellIndex(1)
		h.AssertActiveSize(pq, 4)

		// Add new job - should go to cell 4 (where last tombstone was)
		newJob := JobRef{Index: 6, RetryCount: 0, DueTimeSec: 600}
		err := pq.AddNewJob(newJob)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		h.AssertActiveSize(pq, 5)
		// Cell 4 should have the new job with Cell=4 (set by AddNewJob)
		h.AssertJobAtCell(pq, 4, JobRef{Index: 6, RetryCount: 0, Cell: 4, DueTimeSec: 600})

		// Cell 1 should have Index 5 (moved from cell 4 when removal happened)
		h.AssertJobAtCell(pq, 1, JobRef{Index: 5, RetryCount: 0, Cell: 1, DueTimeSec: 500})
	})

	t.Run("queue stays compact after multiple add/remove cycles", func(t *testing.T) {
		pq := NewProcessQueue(10)

		// Initial fill
		for i := 1; i <= 5; i++ {
			pq.AddNewJob(JobRef{Index: i, RetryCount: 0, DueTimeSec: int64(i * 100)})
		}

		// Remove all except first
		for i := 4; i >= 1; i-- {
			pq.RemoveByCellIndex(i)
		}

		h.AssertActiveSize(pq, 1)
		h.AssertJobAtCell(pq, 0, JobRef{Index: 1, RetryCount: 0, Cell: 0, DueTimeSec: 100})

		// Add new jobs - should fill from cell 1 onward
		for i := 6; i <= 9; i++ {
			pq.AddNewJob(JobRef{Index: i, RetryCount: 0, DueTimeSec: int64(i * 100)})
		}

		// Index 1 + Index 6,7,8,9 = 5 jobs total
		h.AssertActiveSize(pq, 5)
		h.AssertJobAtCell(pq, 0, JobRef{Index: 1, RetryCount: 0, Cell: 0, DueTimeSec: 100})
		h.AssertJobAtCell(pq, 1, JobRef{Index: 6, RetryCount: 0, Cell: 1, DueTimeSec: 600})
		h.AssertJobAtCell(pq, 2, JobRef{Index: 7, RetryCount: 0, Cell: 2, DueTimeSec: 700})
		h.AssertJobAtCell(pq, 3, JobRef{Index: 8, RetryCount: 0, Cell: 3, DueTimeSec: 800})
		h.AssertJobAtCell(pq, 4, JobRef{Index: 9, RetryCount: 0, Cell: 4, DueTimeSec: 900})
		// Cell 5 and beyond should be tombstones
		h.AssertTombstoneAtCell(pq, 5)
		h.AssertTombstoneAtCell(pq, 6)
	})
}

func TestProcessQueue_ReadBatchTwoOldestTracking(t *testing.T) {
	pool := &sync.Pool{New: func() interface{} { return make([]JobRef, 0, 100) }}

	t.Run("collects from two oldest timestamps when needed", func(t *testing.T) {
		pq := NewProcessQueue(10)
		now := int64(500)

		// Add jobs with three distinct old timestamps
		// 3 jobs at 100, 4 jobs at 200, 2 jobs at 300 (all ready)
		jobs := []JobRef{
			{Index: 1, DueTimeSec: 100},
			{Index: 2, DueTimeSec: 100},
			{Index: 3, DueTimeSec: 100},
			{Index: 4, DueTimeSec: 200},
			{Index: 5, DueTimeSec: 200},
			{Index: 6, DueTimeSec: 200},
			{Index: 7, DueTimeSec: 200},
			{Index: 8, DueTimeSec: 300},
			{Index: 9, DueTimeSec: 300},
		}
		for _, job := range jobs {
			pq.AddNewJob(job)
		}

		// Request 5 jobs - should get all 3 at 100 + first 2 at 200
		expected := []int{1, 2, 3, 4, 5}
		h := NewTestHelper(t)
		h.AssertReadBatchReady(pq, 5, now, pool, expected)

		// Request 7 jobs - should get all 3 at 100 + all 4 at 200 + no record at 300
		expected = []int{1, 2, 3, 4, 5, 6, 7}
		h.AssertReadBatchReady(pq, 7, now, pool, expected)
	})

	t.Run("skips not-ready jobs when finding two oldest", func(t *testing.T) {
		pq := NewProcessQueue(10)
		now := int64(500)

		jobs := []JobRef{
			{Index: 1, DueTimeSec: 100}, // ready
			{Index: 2, DueTimeSec: 600}, // not ready
			{Index: 3, DueTimeSec: 200}, // ready
			{Index: 4, DueTimeSec: 700}, // not ready
			{Index: 5, DueTimeSec: 200}, // ready
			{Index: 6, DueTimeSec: 800}, // not ready
		}
		for _, job := range jobs {
			pq.AddNewJob(job)
		}

		// Request 10 jobs - should get all ready jobs in first two oldest timesec (100, 200)
		expected := []int{1, 3, 5}
		h := NewTestHelper(t)
		h.AssertReadBatchReady(pq, 10, now, pool, expected)
	})

	t.Run("handles batch size smaller than oldest group", func(t *testing.T) {
		pq := NewProcessQueue(10)
		now := int64(500)

		// 10 jobs all at 100
		for i := 1; i <= 10; i++ {
			pq.AddNewJob(JobRef{Index: i, DueTimeSec: 100})
		}

		// Request 3 - should get first 3 (array order since all same time)
		expected := []int{1, 2, 3}
		h := NewTestHelper(t)
		h.AssertReadBatchReady(pq, 3, now, pool, expected)

		// Request 7 - should get first 7
		expected = []int{1, 2, 3, 4, 5, 6, 7}
		h.AssertReadBatchReady(pq, 7, now, pool, expected)
	})

	t.Run("returns from two oldest when oldest group has fewer than count", func(t *testing.T) {
		pq := NewProcessQueue(10)
		now := int64(500)

		// 2 jobs at 100, 5 jobs at 200, 3 jobs at 300
		for i := 1; i <= 2; i++ {
			pq.AddNewJob(JobRef{Index: i, DueTimeSec: 100})
		}
		for i := 3; i <= 7; i++ {
			pq.AddNewJob(JobRef{Index: i, DueTimeSec: 200})
		}
		for i := 8; i <= 10; i++ {
			pq.AddNewJob(JobRef{Index: i, DueTimeSec: 300})
		}

		// Request 4 - should get 2 at 100 + 2 at 200
		expected := []int{1, 2, 3, 4}
		h := NewTestHelper(t)
		h.AssertReadBatchReady(pq, 4, now, pool, expected)

		// Request 7 - should get 2 at 100 + 5 at 200
		expected = []int{1, 2, 3, 4, 5, 6, 7}
		h.AssertReadBatchReady(pq, 7, now, pool, expected)
	})

	t.Run("only returns from oldest timestamp if count satisfied", func(t *testing.T) {
		pq := NewProcessQueue(10)
		now := int64(500)

		// 5 jobs at 100, 5 jobs at 200
		for i := 1; i <= 5; i++ {
			pq.AddNewJob(JobRef{Index: i, DueTimeSec: 100})
		}
		for i := 6; i <= 10; i++ {
			pq.AddNewJob(JobRef{Index: i, DueTimeSec: 200})
		}

		// Request 3 - only from oldest (100)
		expected := []int{1, 2, 3}
		h := NewTestHelper(t)
		h.AssertReadBatchReady(pq, 3, now, pool, expected)

		// Request 5 - only from oldest (100) - count satisfied
		expected = []int{1, 2, 3, 4, 5}
		h.AssertReadBatchReady(pq, 5, now, pool, expected)
	})
}

func TestPartitionDispatch_RetryLifecycle(t *testing.T) {
	t.Run("jobs progress through retries until max retries then DLQ", func(t *testing.T) {
		pq := NewProcessQueue(100)
		pool := &sync.Pool{New: func() interface{} { return make([]JobRef, 0, 100) }}

		maxRetries := 3
		delay := int64(10)
		nowSec := time.Now().Unix()

		// Add 3 jobs with different retry counts
		jobs := []JobRef{
			{Index: 1, RetryCount: 0, DueTimeSec: nowSec},
			{Index: 2, RetryCount: 1, DueTimeSec: nowSec},
			{Index: 3, RetryCount: 2, DueTimeSec: nowSec},
		}
		for _, job := range jobs {
			pq.AddNewJob(job)
		}

		h := NewTestHelper(t)
		h.AssertActiveSize(pq, 3)

		// Simulate dispatch iteration 1
		items := pq.ReadBatch(10, nowSec, pool)
		if len(items) != 3 {
			t.Fatalf("expected 3 items, got %d", len(items))
		}

		validRefs := items[:]
		removeCells := []JobRef{}

		sent := true
		if sent {
			for i := range validRefs {
				ref := &validRefs[i]
				ref.RetryCount++
				pq.UpdateRetry(ref.Cell, ref.RetryCount, delay)
			}
		}

		for _, item := range removeCells {
			pq.RemoveByCellIndex(item.Cell)
		}

		// After iteration 1: all jobs have retry+1 and due = currentSec() + delay
		h.AssertActiveSize(pq, 3)

		// Verify retry counts updated (due times will be current+delay, can't predict exact)
		for i := 0; i < 3; i++ {
			if pq.records[i].RetryCount != i+1 {
				t.Errorf("cell %d: expected RetryCount=%d, got %d", i, i+1, pq.records[i].RetryCount)
			}
			// Due time should be > nowSec (current time + delay)
			if pq.records[i].DueTimeSec <= nowSec {
				t.Errorf("cell %d: DueTimeSec %d should be > nowSec %d", i, pq.records[i].DueTimeSec, nowSec)
			}
		}

		// Wait for jobs to become ready
		time.Sleep(time.Duration(delay) * time.Second)
		nowSec2 := time.Now().Unix()

		// Simulate dispatch iteration 2
		items2 := pq.ReadBatch(10, nowSec2, pool)
		if len(items2) != 3 {
			t.Fatalf("iteration 2: expected 3 items, got %d", len(items2))
		}

		validRefs2 := []JobRef{}
		removeCells2 := []JobRef{}
		for _, item := range items2 {
			if item.RetryCount >= maxRetries {
				removeCells2 = append(removeCells2, item)
			} else {
				validRefs2 = append(validRefs2, item)
			}
		}

		sent2 := true
		if sent2 {
			for i := range validRefs2 {
				ref := &validRefs2[i]
				ref.RetryCount++
				pq.UpdateRetry(ref.Cell, ref.RetryCount, delay)
			}
		}

		for _, item := range removeCells2 {
			pq.RemoveByCellIndex(item.Cell)
		}

		// After iteration 2:
		// Job1: retry=2
		// Job2: retry=3 (will be DLQ next time)
		// Job3: removed (DLQ)
		h.AssertActiveSize(pq, 2)

		// Verify remaining jobs
		expectedRetries := []int{2, 3}
		for i, expected := range expectedRetries {
			if pq.records[i].RetryCount != expected {
				t.Errorf("cell %d: expected RetryCount=%d, got %d", i, expected, pq.records[i].RetryCount)
			}
		}

		// Wait for jobs to become ready
		time.Sleep(time.Duration(delay) * time.Second)
		nowSec3 := time.Now().Unix()

		// Simulate dispatch iteration 3
		items3 := pq.ReadBatch(10, nowSec3, pool)
		if len(items3) != 2 {
			t.Fatalf("iteration 3: expected 2 items, got %d", len(items3))
		}

		validRefs3 := []JobRef{}
		removeCells3 := []JobRef{}
		for _, item := range items3 {
			if item.RetryCount >= maxRetries {
				removeCells3 = append(removeCells3, item)
			} else {
				validRefs3 = append(validRefs3, item)
			}
		}

		if len(removeCells3) != 1 {
			t.Fatalf("expected 1 job to be removed (Job2), got %d", len(removeCells3))
		}
		if removeCells3[0].Index != 2 {
			t.Errorf("expected Job2 (Index 2) to be removed, got Index %d", removeCells3[0].Index)
		}

		if len(validRefs3) != 1 {
			t.Fatalf("expected 1 valid job (Job1), got %d", len(validRefs3))
		}
		if validRefs3[0].Index != 1 {
			t.Errorf("expected Job1 (Index 1) to be valid, got Index %d", validRefs3[0].Index)
		}

		sent3 := true
		if sent3 {
			for i := range validRefs3 {
				ref := &validRefs3[i]
				ref.RetryCount++
				pq.UpdateRetry(ref.Cell, ref.RetryCount, delay)
			}
		}

		for _, item := range removeCells3 {
			pq.RemoveByCellIndex(item.Cell)
		}

		// After iteration 3: Job1: retry=3
		h.AssertActiveSize(pq, 1)
		if pq.records[0].RetryCount != 3 {
			t.Errorf("expected RetryCount=3, got %d", pq.records[0].RetryCount)
		}

		// Wait and do final iteration - Job1 goes to DLQ
		time.Sleep(time.Duration(delay) * time.Second)
		nowSec4 := time.Now().Unix()

		items4 := pq.ReadBatch(10, nowSec4, pool)
		if len(items4) != 1 {
			t.Fatalf("iteration 4: expected 1 item, got %d", len(items4))
		}

		if items4[0].Index != 1 {
			t.Errorf("expected Job1 (Index 1), got Index %d", items4[0].Index)
		}
		if items4[0].RetryCount != maxRetries {
			t.Errorf("expected RetryCount=%d, got %d", maxRetries, items4[0].RetryCount)
		}

		pq.RemoveByCellIndex(items4[0].Cell)
		h.AssertActiveSize(pq, 0)
	})
}
