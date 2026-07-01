package state

import (
	"errors"
	"sync"
	"time"
)

// JobRef represents a job in the processing queue
type JobRef struct {
	Index      int   // Job identifier
	RetryCount int   // Number of retry attempts
	Cell       int   // Current position in the slice
	DueTimeSec int64 // Unix timestamp in seconds (when job is ready)
}

// tombstone represents a dead/removed entry
var tombstone = JobRef{
	Index:      -1,
	RetryCount: -1,
	Cell:       -1,
	DueTimeSec: -1,
}

// ProcessQueue is a single-threaded queue with O(1) insert/remove and O(n) scan
type ProcessQueue struct {
	records   []JobRef // Pre-allocated slice
	liveCount int      // Number of live jobs (logical length)
}

// NewProcessQueue creates a new queue with the given capacity
func NewProcessQueue(capacity int) *ProcessQueue {
	pq := &ProcessQueue{
		records:   make([]JobRef, capacity),
		liveCount: 0,
	}

	// Initialize all slots with tombstone
	for i := 0; i < capacity; i++ {
		pq.records[i] = tombstone
	}

	return pq
}

// currentSec returns the current Unix timestamp in seconds
func (pq *ProcessQueue) currentSec() int64 {
	return time.Now().Unix()
}

// AddNewJob adds a job to the end of the queue
// Returns error if queue is full
func (pq *ProcessQueue) AddNewJob(job JobRef) error {
	if pq.liveCount >= len(pq.records) {
		return errors.New("queue capacity exceeded")
	}

	// Set the cell to the current insert position
	job.Cell = pq.liveCount
	pq.records[pq.liveCount] = job
	pq.liveCount++

	return nil
}

// RemoveByCellIndex removes a job by its cell index using swap-remove
// O(1) operation - moves last job to the removed position
func (pq *ProcessQueue) RemoveByCellIndex(cell int) {
	if cell < 0 || cell >= pq.liveCount {
		return
	}

	lastIdx := pq.liveCount - 1

	// If not removing the last element, swap with the last
	if cell != lastIdx {
		// Move last job to the removed cell
		pq.records[cell] = pq.records[lastIdx]
		pq.records[cell].Cell = cell // CRITICAL: Update the moved job's cell
	}

	// Tombstone the last position
	pq.records[lastIdx] = tombstone
	pq.liveCount--
}

// RemoveCells removes multiple jobs from the queue by their cell indices.
// Processes removals in reverse order to maintain index validity during swap-remove.
// This is critical because RemoveByCell uses swap-remove which changes the array layout,
// and processing in reverse ensures earlier cells (lower indices) are not affected
// when removing higher-indexed cells first.
func (pq *ProcessQueue) RemoveCells(cells []JobRef) {
	// Process in reverse order (highest cell first)
	// This ensures that when we remove a cell, it doesn't affect the indices
	// of cells we haven't processed yet (which are all lower than the current cell).
	for i := len(cells) - 1; i >= 0; i-- {
		pq.RemoveByCellIndex(cells[i].Cell)
	}
}

// UpdateRetry updates a job's retry count and due time in place
// O(1) operation
func (pq *ProcessQueue) UpdateRetry(cell int, retryCount int, delaySec int64) error {
	if cell < 0 || cell >= pq.liveCount {
		return errors.New("invalid cell")
	}

	job := &pq.records[cell]

	// Update
	job.RetryCount = retryCount
	job.DueTimeSec = pq.currentSec() + delaySec

	return nil
}

// ReadBatch reads up to 'count' ready jobs, oldest first.
// Returns a properly sized slice containing only ready jobs (no side effects).
func (pq *ProcessQueue) ReadBatch(count int, nowSec int64, pool *sync.Pool) []JobRef {
	items := pool.Get().([]JobRef)
	items = items[:0]

	if pq.liveCount == 0 {
		return items
	}

	// First pass: find the two oldest due times among ready jobs
	var oldestTime int64 = 1<<63 - 1
	var secondTime int64 = 1<<63 - 1
	oldestCount := 0
	secondCount := 0

	for i := 0; i < pq.liveCount; i++ {
		if pq.records[i].DueTimeSec <= nowSec {
			due := pq.records[i].DueTimeSec
			if due < oldestTime {
				// Shift oldest to second
				secondTime = oldestTime
				secondCount = oldestCount
				oldestTime = due
				oldestCount = 1
			} else if due == oldestTime {
				oldestCount++
			} else if due < secondTime {
				secondTime = due
				secondCount = 1
			} else if due == secondTime {
				secondCount++
			}
		}
	}

	// No ready jobs
	if oldestCount == 0 {
		return items
	}

	// Second pass: collect oldestTime jobs
	collected := 0
	for i := 0; i < pq.liveCount && collected < count; i++ {
		if pq.records[i].DueTimeSec == oldestTime {
			items = append(items, pq.records[i])
			collected++
		}
	}

	// If we need more and have secondTime jobs, collect them
	if collected < count && secondCount > 0 {
		for i := 0; i < pq.liveCount && collected < count; i++ {
			if pq.records[i].DueTimeSec == secondTime {
				items = append(items, pq.records[i])
				collected++
			}
		}
	}

	return items
}

// ActiveSize returns the number of live jobs in the queue
func (pq *ProcessQueue) ActiveSize() int {
	return pq.liveCount
}

// IsFull returns true if the queue has reached capacity
func (pq *ProcessQueue) IsFull() bool {
	return pq.liveCount >= len(pq.records)
}
