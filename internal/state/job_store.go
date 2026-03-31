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
	"github.com/m-javani/cue/internal/model"
)

// JobStore - contiguous array with free list
type JobStore struct {
	jobs     []model.Job
	freeList []uint32
	byID     map[string]uint32
	capacity int
}

func NewJobStore(capacity int) *JobStore {
	// Pre-allocate full capacity to avoid growth
	// Using capacity as the initial size means no append growth
	return &JobStore{
		jobs:     make([]model.Job, 0, capacity), // Keep as slice for safety
		freeList: make([]uint32, 0, capacity),
		byID:     make(map[string]uint32, capacity),
		capacity: capacity,
	}
}

func (js *JobStore) Create(job *model.Job) (uint32, error) {
	if _, exists := js.byID[job.ID]; exists {
		return 0, internal.ErrDuplicateJobID
	}

	var idx uint32

	// First try to use a freed slot
	if len(js.freeList) > 0 {
		idx = js.freeList[len(js.freeList)-1]
		if js.jobs[idx].ID != "" {
			panic("free list corruption")
		}
		js.freeList = js.freeList[:len(js.freeList)-1]
		js.jobs[idx] = *job
	} else {
		// No free slots, need to grow
		idx = uint32(len(js.jobs))

		// Check if we need to grow
		if int(idx) >= cap(js.jobs) {
			// Grow by 50% or at least 1024, whichever is larger
			newCap := cap(js.jobs) * 3 / 2
			if newCap < cap(js.jobs)+1024 {
				newCap = cap(js.jobs) + 1024
			}
			if newCap < 1024 {
				newCap = 1024
			}

			newJobs := make([]model.Job, len(js.jobs), newCap)
			copy(newJobs, js.jobs)
			js.jobs = newJobs
		}

		// Now append
		js.jobs = append(js.jobs, *job)
	}

	js.byID[job.ID] = idx
	return idx, nil
}

func (js *JobStore) Release(idx uint32) {
	if idx >= uint32(len(js.jobs)) {
		return
	}
	if js.jobs[idx].ID == "" {
		panic("double release")
	}
	delete(js.byID, js.jobs[idx].ID)

	// Zero the job to help GC and avoid holding references
	var zero model.Job
	js.jobs[idx] = zero

	js.freeList = append(js.freeList, idx)
}

func (js *JobStore) MarkDone(jobID string) bool {
	idx, exists := js.byID[jobID]
	if !exists {
		return false
	}
	js.jobs[idx].Done = true
	return true
}

func (js *JobStore) ReleaseIfDone(jobID string) bool {
	idx, exists := js.byID[jobID]
	if !exists {
		return false
	}
	if js.jobs[idx].Done {
		js.Release(idx)
		return true
	}
	return false
}

// used in handleDrop
func (js *JobStore) ForceRelease(jobID string) bool {
	idx, exists := js.byID[jobID]
	if !exists {
		return false
	}
	js.Release(idx)
	return true
}

func (js *JobStore) Get(idx uint32) *model.Job {
	if idx >= uint32(len(js.jobs)) {
		return nil
	}
	job := &js.jobs[idx]
	if job.ID == "" {
		return nil // Released
	}
	return job
}

func (js *JobStore) IsDone(jobID string) bool {
	idx, exists := js.byID[jobID]
	if !exists {
		return false
	}
	return js.jobs[idx].Done
}
func (js *JobStore) Len() int {
	return len(js.jobs)
}

// GetByID implements JobStore interface
func (js *JobStore) GetByID(jobID string) (*model.Job, bool) {
	idx, exists := js.byID[jobID]
	if !exists {
		return nil, false
	}
	job := js.Get(idx)
	if job == nil {
		return nil, false
	}
	return job, true
}

// GetIndexByID implements JobStore interface
func (js *JobStore) GetIndexByID(jobID string) (uint32, bool) {
	idx, exists := js.byID[jobID]
	return idx, exists
}

// JobRef - compact reference used in queues
type JobRef struct {
	Index      int
	RetryCount int
}
