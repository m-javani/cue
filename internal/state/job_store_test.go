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

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
)

func TestJobStore_Create(t *testing.T) {
	js := NewJobStore(10)

	job := &model.Job{
		ID:    "job-1",
		Topic: "test",
		Data:  []byte("data"),
	}

	idx, err := js.Create(job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if idx != 0 {
		t.Errorf("expected index 0, got %d", idx)
	}

	// Verify job stored
	stored := js.Get(idx)
	if stored == nil {
		t.Fatal("job not stored")
	}
	if stored.ID != "job-1" {
		t.Errorf("expected job-1, got %s", stored.ID)
	}
}

func TestJobStore_CreateDuplicate(t *testing.T) {
	js := NewJobStore(10)

	job := &model.Job{ID: "job-1"}
	_, err := js.Create(job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = js.Create(job)
	if err != internal.ErrDuplicateJobID {
		t.Errorf("expected ErrDuplicateJobID, got %v", err)
	}
}

func TestJobStore_Release(t *testing.T) {
	js := NewJobStore(10)

	job := &model.Job{ID: "job-1"}
	idx, _ := js.Create(job)

	// Release the job
	js.Release(idx)

	// Should not be findable
	if js.Get(idx) != nil {
		t.Error("job should be nil after release")
	}

	// Should not be in byID
	if _, exists := js.GetByID("job-1"); exists {
		t.Error("job ID should not exist in byID after release")
	}
}

func TestJobStore_MarkDone(t *testing.T) {
	js := NewJobStore(10)

	job := &model.Job{ID: "job-1"}
	idx, _ := js.Create(job)

	// Initially not done
	if js.IsDone("job-1") {
		t.Error("job should not be done initially")
	}

	// Mark done
	ok := js.MarkDone("job-1")
	if !ok {
		t.Error("MarkDone should return true")
	}

	// Verify done
	if !js.IsDone("job-1") {
		t.Error("job should be done after MarkDone")
	}

	// Verify job state
	stored := js.Get(idx)
	if stored == nil || !stored.Done {
		t.Error("stored job Done flag should be true")
	}
}

func TestJobStore_MarkDoneNonExistent(t *testing.T) {
	js := NewJobStore(10)

	ok := js.MarkDone("non-existent")
	if ok {
		t.Error("MarkDone should return false for non-existent job")
	}
}

func TestJobStore_ReleaseIfDone(t *testing.T) {
	js := NewJobStore(10)

	job := &model.Job{ID: "job-1"}
	idx, _ := js.Create(job)

	// Not done - should not release
	released := js.ReleaseIfDone("job-1")
	if released {
		t.Error("ReleaseIfDone should return false for not done job")
	}
	if js.Get(idx) == nil {
		t.Error("job should still exist")
	}

	// Mark done and release
	js.MarkDone("job-1")
	released = js.ReleaseIfDone("job-1")
	if !released {
		t.Error("ReleaseIfDone should return true for done job")
	}
	if js.Get(idx) != nil {
		t.Error("job should be released")
	}
}

func TestJobStore_ForceRelease(t *testing.T) {
	js := NewJobStore(10)

	job := &model.Job{ID: "job-1"}
	idx, _ := js.Create(job)

	// Force release even if not done
	ok := js.ForceRelease("job-1")
	if !ok {
		t.Error("ForceRelease should return true")
	}
	if js.Get(idx) != nil {
		t.Error("job should be released")
	}
}

func TestJobStore_ForceReleaseNonExistent(t *testing.T) {
	js := NewJobStore(10)

	ok := js.ForceRelease("non-existent")
	if ok {
		t.Error("ForceRelease should return false for non-existent job")
	}
}

func TestJobStore_GetNonExistent(t *testing.T) {
	js := NewJobStore(10)

	if js.Get(999) != nil {
		t.Error("Get with out of range index should return nil")
	}
}

func TestJobStore_ReuseFreedSlot(t *testing.T) {
	js := NewJobStore(10)

	// Create and release a job
	job1 := &model.Job{ID: "job-1"}
	idx1, _ := js.Create(job1)
	js.Release(idx1)

	// Create another job - should reuse slot
	job2 := &model.Job{ID: "job-2"}
	idx2, _ := js.Create(job2)

	// Should reuse the same index
	if idx2 != idx1 {
		t.Errorf("expected to reuse index %d, got %d", idx1, idx2)
	}

	// Old job should not exist
	if js.Get(idx1).ID == "job-1" {
		t.Error("old job should not exist")
	}

	// New job should exist
	if js.Get(idx2).ID != "job-2" {
		t.Error("new job not found")
	}
}

func TestJobStore_IsDone(t *testing.T) {
	js := NewJobStore(10)

	job := &model.Job{ID: "job-1"}
	js.Create(job)

	// Not done
	if js.IsDone("job-1") {
		t.Error("should return false for not done job")
	}

	// Mark done
	js.MarkDone("job-1")
	if !js.IsDone("job-1") {
		t.Error("should return true for done job")
	}

	// Non-existent
	if js.IsDone("non-existent") {
		t.Error("should return false for non-existent job")
	}
}

func TestJobStore_CapacityGrowth(t *testing.T) {
	js := NewJobStore(2) // Small capacity

	// Create more jobs than capacity
	for i := 0; i < 10; i++ {
		job := &model.Job{ID: string(rune('a' + i))}
		_, err := js.Create(job)
		if err != nil {
			t.Fatalf("failed to create job %d: %v", i, err)
		}
	}

	// Verify all jobs exist
	for i := 0; i < 10; i++ {
		id := string(rune('a' + i))
		if !js.IsDone(id) {
			// Not checking done, just existence via Get
			// We need to find the index first
			found := false
			for idx := uint32(0); idx < uint32(js.Len()); idx++ {
				if job := js.Get(idx); job != nil && job.ID == id {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("job %s not found after growth", id)
			}
		}
	}
}

func TestJobStore_GetWithZeroValue(t *testing.T) {
	js := NewJobStore(10)

	// Get before any jobs
	if js.Get(0) != nil {
		t.Error("Get should return nil for empty slot")
	}
}

func TestJobStore_ReleaseDouble(t *testing.T) {
	js := NewJobStore(10)

	job := &model.Job{ID: "job-1"}
	idx, _ := js.Create(job)

	// First release should succeed
	js.Release(idx)

	// Second release should panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on double release")
		}
	}()
	js.Release(idx)
}

// -------
func TestJobStore_Len(t *testing.T) {
	js := NewJobStore(10)

	// Empty store
	if js.Len() != 0 {
		t.Errorf("expected Len 0, got %d", js.Len())
	}

	// After creating jobs
	job1 := &model.Job{ID: "job-1"}
	job2 := &model.Job{ID: "job-2"}
	js.Create(job1)
	js.Create(job2)

	if js.Len() != 2 {
		t.Errorf("expected Len 2, got %d", js.Len())
	}

	// After release (Len should still count the slot)
	idx, _ := js.GetIndexByID("job-1")
	js.Release(idx)
	if js.Len() != 2 {
		t.Errorf("expected Len 2 after release, got %d", js.Len())
	}
}

func TestJobStore_GetByID(t *testing.T) {
	js := NewJobStore(10)

	// Non-existent
	job, exists := js.GetByID("non-existent")
	if exists {
		t.Error("GetByID should return false for non-existent job")
	}
	if job != nil {
		t.Error("GetByID should return nil for non-existent job")
	}

	// Existing job
	expected := &model.Job{ID: "job-1", Topic: "test", Data: []byte("data")}
	idx, _ := js.Create(expected)

	job, exists = js.GetByID("job-1")
	if !exists {
		t.Error("GetByID should return true for existing job")
	}
	if job == nil {
		t.Fatal("GetByID returned nil for existing job")
	}
	if job.ID != "job-1" || job.Topic != "test" {
		t.Errorf("GetByID returned wrong job data")
	}

	// After release
	js.Release(idx)
	job, exists = js.GetByID("job-1")
	if exists {
		t.Error("GetByID should return false after release")
	}
	if job != nil {
		t.Error("GetByID should return nil after release")
	}
}

func TestJobStore_GetIndexByID(t *testing.T) {
	js := NewJobStore(10)

	// Non-existent
	idx, exists := js.GetIndexByID("non-existent")
	if exists {
		t.Error("GetIndexByID should return false for non-existent job")
	}
	if idx != 0 {
		t.Errorf("expected idx 0, got %d", idx)
	}

	// Existing job
	job := &model.Job{ID: "job-1"}
	expectedIdx, _ := js.Create(job)

	idx, exists = js.GetIndexByID("job-1")
	if !exists {
		t.Error("GetIndexByID should return true for existing job")
	}
	if idx != expectedIdx {
		t.Errorf("expected idx %d, got %d", expectedIdx, idx)
	}

	// After release
	js.Release(idx)
	_, exists = js.GetIndexByID("job-1")
	if exists {
		t.Error("GetIndexByID should return false after release")
	}
}

func TestJobStore_GetByIDReleasedSlot(t *testing.T) {
	js := NewJobStore(10)

	// Create, release, and reuse slot
	job1 := &model.Job{ID: "job-1"}
	idx, _ := js.Create(job1)
	js.Release(idx)

	// Create job2 reusing slot
	job2 := &model.Job{ID: "job-2"}
	js.Create(job2)

	// job1 should not exist
	job, exists := js.GetByID("job-1")
	if exists {
		t.Error("GetByID should return false for released job")
	}
	if job != nil {
		t.Error("GetByID should return nil for released job")
	}

	// job2 should exist
	job, exists = js.GetByID("job-2")
	if !exists {
		t.Error("GetByID should return true for new job")
	}
	if job == nil {
		t.Error("GetByID should return non-nil for new job")
	}
}

func TestJobStore_GetByIDAfterForceRelease(t *testing.T) {
	js := NewJobStore(10)

	job := &model.Job{ID: "job-1"}
	js.Create(job)

	// Force release
	js.ForceRelease("job-1")

	job, exists := js.GetByID("job-1")
	if exists {
		t.Error("GetByID should return false after ForceRelease")
	}
	if job != nil {
		t.Error("GetByID should return nil after ForceRelease")
	}
}
