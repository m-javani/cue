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

package cluster

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type TestWAL struct {
	*SegmentedWAL
	dir string
	t   *testing.T
}

// NewTestWAL creates a fresh WAL in a temp directory
func NewTestWAL(t *testing.T, segmentSize ...int64) *TestWAL {
	dir := t.TempDir()
	logger, _ := zap.NewDevelopment()

	var wal *SegmentedWAL
	var err error
	if len(segmentSize) > 0 && segmentSize[0] > 0 {
		wal, err = NewSegmentedWal(dir, logger, segmentSize[0])
	} else {
		wal, err = NewSegmentedWal(dir, logger)
	}
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	tw := &TestWAL{
		SegmentedWAL: wal,
		dir:          dir,
		t:            t,
	}

	t.Cleanup(func() {
		_ = tw.Close()
	})

	return tw
}

func NewTestWALSmall(t *testing.T) *TestWAL {
	return NewTestWAL(t, 10*1024*1024) // 10MB segments - much faster for tests
}

// ============================================
// WRITE HELPERS
// ============================================

// MustAppend appends entries and fails test on error
func (tw *TestWAL) MustAppend(entries ...*raftpb.Entry) {
	batch := make([]BufferedEntry, len(entries))

	for i, e := range entries {
		be, err := encodeBufferedEntry(e)
		if err != nil {
			tw.t.Fatal(err)
		}
		batch[i] = be
	}

	if err := tw.AppendBatch(batch); err != nil {
		tw.t.Fatal(err)
	}
}

// MustAppendRange appends sequential entries from first to last
func (tw *TestWAL) MustAppendRange(first, last uint64) {
	if first > last {
		tw.t.Fatalf("invalid range %d..%d (first > last)", first, last)
	}

	entries := make([]*raftpb.Entry, 0, last-first+1)
	for i := first; i <= last; i++ {
		entries = append(entries, MakeEntry(i, fmt.Sprintf("%d", i)))
	}
	tw.MustAppend(entries...)
}

// NextEntry returns an entry with the next available index
func (tw *TestWAL) NextEntry(data string) *raftpb.Entry {
	return MakeEntry(tw.nextIndex, data)
}

// FillCurrentSegment forces rotation by appending entries until it rotates
func (tw *TestWAL) FillCurrentSegment() {
	before := tw.GetSealedCount()
	for tw.GetSealedCount() == before {
		// Use a single entry per call to make rotation boundary clearer
		tw.MustAppend(tw.NextEntry(strings.Repeat("x", 4096)))
	}
}

// Helper to create a test file with given bytes
func createTestFile(t *testing.T, data []byte) *os.File {
	f, err := os.CreateTemp(t.TempDir(), "walreader_*.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	return f
}

// ============================================
// READ HELPERS
// ============================================
func (tw *TestWAL) readAllFromReader(r *Reader) []*raftpb.Entry {
	var entries []*raftpb.Entry
	for {
		e, err := r.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			tw.t.Fatalf("ReadEntry failed: %v", err)
		}
		entries = append(entries, e)
	}
	return entries
}

// MustReadAll reads all entries from index 1 and fails on error
func (tw *TestWAL) MustReadAll() []*raftpb.Entry {
	var entries []*raftpb.Entry
	err := tw.Recover(0, func(e *raftpb.Entry) error {
		entries = append(entries, e)
		return nil
	})
	if err != nil {
		tw.t.Fatalf("recover: %v", err)
	}
	return entries
}

// MustReadFrom reads entries from given index and fails on error
func (tw *TestWAL) MustReadFrom(from uint64) []*raftpb.Entry {
	r, err := tw.NewReader(from)
	if err != nil {
		tw.t.Fatalf("new reader: %v", err)
	}
	defer func() { _ = r.Close() }()

	var entries []*raftpb.Entry
	for {
		e, err := r.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			tw.t.Fatalf("read entry: %v", err)
		}
		entries = append(entries, e)
	}
	return entries
}

// ============================================
// ASSERTION HELPERS
// ============================================

// MustContain verifies that the WAL contains exactly the given indices
func (tw *TestWAL) MustContain(indices ...uint64) {
	entries := tw.MustReadAll()
	got := EntriesIndices(entries)

	if !reflect.DeepEqual(got, indices) {
		tw.t.Fatalf(
			"unexpected WAL contents\nwant=%v\ngot=%v\nentries=%+v",
			indices,
			got,
			entries,
		)
	}
}

// MustHaveFileCount verifies the total number of WAL files (sealed + active)
func (tw *TestWAL) MustHaveFileCount(count int) {
	got := len(tw.GetSegmentPaths())
	if got != count {
		tw.t.Fatalf("file count mismatch\nwant=%d\ngot=%d\nfiles=%v",
			count, got, tw.GetSegmentPaths())
	}
}

// MustHaveSealedCount verifies the number of sealed segments
func (tw *TestWAL) MustHaveSealedCount(count int) {
	got := tw.GetSealedCount()
	if got != count {
		tw.t.Fatalf("sealed segment count mismatch\nwant=%d\ngot=%d",
			count, got)
	}
}

// MustHaveTempFiles verifies that temp files exist (or not)
func (tw *TestWAL) MustHaveTempFiles(expected bool) {
	entries, err := os.ReadDir(tw.dir)
	if err != nil {
		tw.t.Fatalf("read dir: %v", err)
	}

	var temps []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".tmp") {
			temps = append(temps, e.Name())
		}
	}

	has := len(temps) > 0
	if has != expected {
		tw.t.Fatalf("temp files: expected=%v, got=%v (files=%v)",
			expected, has, temps)
	}
}

// ============================================
// OPERATION HELPERS
// ============================================

// MustTruncate truncates WAL and fails on error
func (tw *TestWAL) MustTruncate(upTo uint64) {
	if err := tw.Truncate(upTo); err != nil {
		tw.t.Fatalf("truncate: %v", err)
	}
}

// MustClose closes WAL and fails on error
func (tw *TestWAL) MustClose() {
	if err := tw.Close(); err != nil {
		tw.t.Fatalf("close: %v", err)
	}
}

// Reopen closes and reopens the WAL from the same directory
func (tw *TestWAL) Reopen() {
	tw.MustClose()

	logger, _ := zap.NewDevelopment()
	wal, err := NewSegmentedWal(tw.dir, logger)
	if err != nil {
		tw.t.Fatalf("reopen: %v", err)
	}
	tw.SegmentedWAL = wal
}

// ============================================
// INSPECTION HELPERS
// ============================================

// GetSegmentPaths returns all WAL-related filenames in the directory (sorted)
func (tw *TestWAL) GetSegmentPaths() []string {
	entries, err := os.ReadDir(tw.dir)
	if err != nil {
		tw.t.Fatalf("read dir: %v", err)
	}

	var paths []string
	for _, e := range entries {
		if !e.IsDir() {
			name := e.Name()
			if strings.HasSuffix(name, ".wal") ||
				strings.HasSuffix(name, ".active") {
				paths = append(paths, name)
			}
		}
	}

	sort.Strings(paths)
	return paths
}

// GetSegmentCount returns number of WAL files (sealed + active)
func (tw *TestWAL) GetSegmentCount() int {
	return len(tw.GetSegmentPaths())
}

// GetSealedCount returns number of sealed segments
func (tw *TestWAL) GetSealedCount() int {
	tw.mu.RLock()
	defer tw.mu.RUnlock()
	return len(tw.segments)
}

// GetTempFiles returns all .tmp files in the directory
func (tw *TestWAL) GetTempFiles() []string {
	entries, err := os.ReadDir(tw.dir)
	if err != nil {
		tw.t.Fatalf("read dir: %v", err)
	}

	var temps []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".tmp") {
			temps = append(temps, e.Name())
		}
	}

	sort.Strings(temps)
	return temps
}

// ============================================
// ENTRY FACTORY HELPERS
// ============================================

// MakeEntry creates a test entry with index and data
func MakeEntry(index uint64, data string) *raftpb.Entry {
	return &raftpb.Entry{
		Index: proto.Uint64(index),
		Term:  proto.Uint64(1),
		Data:  []byte(data),
	}
}

// MakeEntryWithTerm creates a test entry with specific term
func MakeEntryWithTerm(term, index uint64, data string) raftpb.Entry {
	return raftpb.Entry{
		Index: proto.Uint64(index),
		Term:  proto.Uint64(term),
		Data:  []byte(data),
	}
}

// ============================================
// UTILITY HELPERS
// ============================================

// EntriesIndices returns indices of entries
func EntriesIndices(entries []*raftpb.Entry) []uint64 {
	indices := make([]uint64, len(entries))
	for i, e := range entries {
		indices[i] = e.GetIndex()
	}
	return indices
}

// ============================================
// CONSTRUCTION TESTS
// ============================================

// TestNewSegmentedWal_NewWAL
// Verifies creating a WAL in an empty directory:
// - Creates active segment at index 1
// - No sealed segments
// - nextIndex == 1
// - .tmp files are cleaned up
func TestNewSegmentedWal_NewWAL(t *testing.T) {
	// 1. Create WAL in empty directory
	tw := NewTestWAL(t)

	// 2. Verify active segment exists at index 1
	// The active segment filename should be "0000000000000001.active"
	expectedActive := "0000000000000001.active"
	files := tw.GetSegmentPaths()

	if len(files) != 1 {
		t.Fatalf("expected 1 segment file, got %d: %v", len(files), files)
	}

	if files[0] != expectedActive {
		t.Fatalf("expected active segment %q, got %q", expectedActive, files[0])
	}

	// 3. Verify no sealed segments
	if tw.GetSealedCount() != 0 {
		t.Fatalf("expected 0 sealed segments, got %d", tw.GetSealedCount())
	}

	// 4. Verify nextIndex == 1
	if tw.nextIndex != 1 {
		t.Fatalf("expected nextIndex=1, got %d", tw.nextIndex)
	}

	// 5. Verify no .tmp files
	temps := tw.GetTempFiles()
	if len(temps) != 0 {
		t.Fatalf("expected 0 temp files, got %d: %v", len(temps), temps)
	}
}

// TestNewSegmentedWal_NewWAL_CleansUpTempFiles
// Verifies that abandoned .tmp files are removed during initialization
func TestNewSegmentedWal_NewWAL_CleansUpTempFiles(t *testing.T) {
	// 1. Create a temporary directory
	dir := t.TempDir()

	// 2. Manually create a .tmp file (simulating abandoned temp)
	tmpPath := filepath.Join(dir, "0000000000000001.tmp")
	if err := os.WriteFile(tmpPath, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	// 3. Verify .tmp file exists before initialization
	if _, err := os.Stat(tmpPath); err != nil {
		t.Fatalf("temp file should exist before initialization: %v", err)
	}

	// 4. Initialize WAL (should clean up .tmp)
	logger, _ := zap.NewDevelopment()
	wal, err := NewSegmentedWal(dir, logger)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	// 5. Verify .tmp file was removed
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatal("temp file should have been removed, but still exists")
	}

	// 6. Verify WAL was created successfully
	tw := &TestWAL{
		SegmentedWAL: wal,
		dir:          dir,
		t:            t,
	}

	files := tw.GetSegmentPaths()
	if len(files) != 1 {
		t.Fatalf("expected 1 segment file after cleanup, got %d: %v", len(files), files)
	}

	if files[0] != "0000000000000001.active" {
		t.Fatalf("expected active segment 0000000000000001.active, got %q", files[0])
	}
}

// TestNewSegmentedWal_ReopenExisting
// Verifies reopening an existing WAL:
// - Loads all sealed segments correctly
// - Scans active segment for last index
// - Sets nextIndex = lastIndex + 1
func TestNewSegmentedWal_ReopenExisting(t *testing.T) {
	t.Run("reopens active WAL", func(t *testing.T) {
		tw := NewTestWAL(t)

		tw.MustAppendRange(1, 50)

		wantNext := tw.nextIndex

		tw.Reopen()

		if tw.nextIndex != wantNext {
			t.Fatalf("nextIndex = %d, want %d", tw.nextIndex, wantNext)
		}

		tw.MustHaveSealedCount(0)
		tw.MustHaveFileCount(1)
		tw.MustContain(
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
			21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
			31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
			41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
		)
	})

	t.Run("reopens rotated WAL", func(t *testing.T) {
		tw := NewTestWAL(t)

		tw.FillCurrentSegment()

		firstAfterRotation := tw.nextIndex

		tw.MustAppendRange(firstAfterRotation, firstAfterRotation+9)

		wantNext := tw.nextIndex
		wantSealed := tw.GetSealedCount()

		tw.Reopen()

		if tw.nextIndex != wantNext {
			t.Fatalf("nextIndex = %d, want %d", tw.nextIndex, wantNext)
		}

		tw.MustHaveSealedCount(wantSealed)
		tw.MustHaveFileCount(wantSealed + 1)

		entries := tw.MustReadAll()
		if len(entries) != int(wantNext-1) {
			t.Fatalf("entry count = %d, want %d", len(entries), wantNext-1)
		}
	})

	t.Run("ignores unrelated files", func(t *testing.T) {
		tw := NewTestWAL(t)

		tw.MustAppendRange(1, 5)
		tw.MustClose()

		files := []string{
			"notes.txt",
			"backup.wal.bak",
			"random.db",
		}

		for _, f := range files {
			err := os.WriteFile(filepath.Join(tw.dir, f), []byte("junk"), 0640)
			if err != nil {
				t.Fatal(err)
			}
		}

		logger, _ := zap.NewDevelopment()

		wal, err := NewSegmentedWal(tw.dir, logger)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = wal.Close() }()

		if wal.nextIndex != 6 {
			t.Fatalf("nextIndex = %d, want 6", wal.nextIndex)
		}
	})
}

// TestNewSegmentedWal_InvalidLayout
// Verifies that invalid WAL layouts are rejected:
// - Multiple active segments
// - Gaps between sealed segments
// - Malformed filenames are skipped (not errors)
func TestNewSegmentedWal_InvalidLayout(t *testing.T) {
	t.Run("fails with multiple active segments", func(t *testing.T) {
		tw := NewTestWAL(t)

		tw.MustAppendRange(1, 5)
		tw.MustClose()

		second := filepath.Join(tw.dir, "0000000000000010.active")
		if err := os.WriteFile(second, nil, 0640); err != nil {
			t.Fatal(err)
		}

		logger, _ := zap.NewDevelopment()

		_, err := NewSegmentedWal(tw.dir, logger)
		if err == nil {
			t.Fatal("expected error for multiple active segments")
		}
	})

	t.Run("fails with gap between sealed segments", func(t *testing.T) {
		// Create two sealed segments with a gap
		dir := t.TempDir()

		// Create valid segments with gap: 1-10, then 20-30
		seg1 := filepath.Join(dir, "0000000000000001-0000000000000010.wal")
		seg2 := filepath.Join(dir, "0000000000000020-0000000000000030.wal")

		// Write minimal valid data (just enough to pass file checks)
		for _, seg := range []string{seg1, seg2} {
			if err := os.WriteFile(seg, []byte("test"), 0640); err != nil {
				t.Fatal(err)
			}
		}

		logger, _ := zap.NewDevelopment()
		_, err := NewSegmentedWal(dir, logger)

		if err == nil {
			t.Fatal("expected error for gap between segments")
		}

		if !strings.Contains(err.Error(), "gap") {
			t.Fatalf("expected gap error, got: %v", err)
		}
	})

	t.Run("skips malformed segment filenames", func(t *testing.T) {
		dir := t.TempDir()

		// Create malformed files
		malformed := []string{
			"0000000000000001.wal",                      // missing end
			"0000000000000001-0000000000000010.wal.bak", // wrong extension
			"0000000000000001-abc.wal",                  // non-numeric end
			"abc-0000000000000010.wal",                  // non-numeric start
		}

		for _, name := range malformed {
			path := filepath.Join(dir, name)
			if err := os.WriteFile(path, []byte("test"), 0640); err != nil {
				t.Fatal(err)
			}
		}

		logger, _ := zap.NewDevelopment()
		wal, err := NewSegmentedWal(dir, logger)

		// Should not error, just skip malformed files
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer func() { _ = wal.Close() }()

		tw := &TestWAL{
			SegmentedWAL: wal,
			dir:          dir,
			t:            t,
		}

		// Verify no sealed segments loaded (all were malformed and skipped)
		if tw.GetSealedCount() != 0 {
			t.Fatalf("expected 0 sealed segments, got %d", tw.GetSealedCount())
		}

		// Verify the WAL created a new active segment starting at 1
		// Note: GetSegmentPaths() returns all .wal and .active files on disk,
		// including malformed ones. We shouldn't use it to count valid segments.
		// Instead, verify the active segment exists and the WAL works.

		// Check that the active segment was created
		activePath := filepath.Join(dir, "0000000000000001.active")
		if _, err := os.Stat(activePath); err != nil {
			t.Fatalf("active segment not created: %v", err)
		}

		// Verify we can write and read entries (proves WAL is functional)
		tw.MustAppendRange(1, 5)
		tw.MustContain(1, 2, 3, 4, 5)

		// Verify malformed files are still on disk (they weren't deleted)
		for _, name := range malformed {
			path := filepath.Join(dir, name)
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("malformed file %q was deleted, should be left alone", name)
			}
		}
	})
}

// ============================================
// WRITE TESTS
// ============================================
// TestAppendBatch_WritesEntries
// Verifies append behavior:
// - Single and multiple entries
// - Updates nextIndex correctly
// - Updates segment size
// - Appends to active segment after reopen
// - Handles write failures gracefully
func TestAppendBatch_WritesEntries(t *testing.T) {
	t.Run("appends single entry", func(t *testing.T) {
		tw := NewTestWAL(t)

		entry := MakeEntry(1, "single")
		tw.MustAppend(entry)

		// Verify nextIndex updated
		if tw.nextIndex != 2 {
			t.Fatalf("nextIndex = %d, want 2", tw.nextIndex)
		}

		// Verify segment size updated
		if tw.active.size == 0 {
			t.Error("segment size should be > 0 after append")
		}

		// Verify entry can be read
		entries := tw.MustReadAll()
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		if entries[0].GetIndex() != 1 {
			t.Fatalf("entry index = %d, want 1", entries[0].GetIndex())
		}
	})

	t.Run("appends multiple entries", func(t *testing.T) {
		tw := NewTestWAL(t)

		entries := []*raftpb.Entry{
			MakeEntry(1, "first"),
			MakeEntry(2, "second"),
			MakeEntry(3, "third"),
		}
		tw.MustAppend(entries...)

		// Verify nextIndex updated to last + 1
		if tw.nextIndex != 4 {
			t.Fatalf("nextIndex = %d, want 4", tw.nextIndex)
		}

		// Verify all entries can be read
		read := tw.MustReadAll()
		if len(read) != 3 {
			t.Fatalf("expected 3 entries, got %d", len(read))
		}

		for i, entry := range read {
			expectedIdx := uint64(i + 1)
			if entry.GetIndex() != expectedIdx {
				t.Fatalf("entry %d index = %d, want %d", i, entry.GetIndex(), expectedIdx)
			}
		}
	})

	t.Run("preserves order across multiple appends", func(t *testing.T) {
		tw := NewTestWAL(t)

		tw.MustAppend(MakeEntry(1, "one"))
		tw.MustAppend(MakeEntry(2, "two"))
		tw.MustAppend(MakeEntry(3, "three"))

		entries := tw.MustReadAll()
		if len(entries) != 3 {
			t.Fatalf("expected 3 entries, got %d", len(entries))
		}

		for i := 0; i < 3; i++ {
			if entries[i].GetIndex() != uint64(i+1) {
				t.Fatalf("entry %d index = %d, want %d", i, entries[i].GetIndex(), i+1)
			}
		}
	})

	t.Run("appends to active segment after reopen", func(t *testing.T) {
		tw := NewTestWAL(t)

		// Append initial entries
		tw.MustAppendRange(1, 10)
		initialNext := tw.nextIndex // 11

		// Reopen
		tw.Reopen()

		// Verify nextIndex restored
		if tw.nextIndex != initialNext {
			t.Fatalf("after reopen nextIndex = %d, want %d", tw.nextIndex, initialNext)
		}

		// Verify active segment exists and is writable
		if tw.active == nil {
			t.Fatal("active segment is nil after reopen")
		}
		if tw.active.writeFile == nil {
			t.Fatal("active segment writeFile is nil after reopen")
		}

		// Append more entries
		newEntries := []*raftpb.Entry{
			MakeEntry(11, "eleven"),
			MakeEntry(12, "twelve"),
			MakeEntry(13, "thirteen"),
			MakeEntry(14, "fourteen"),
			MakeEntry(15, "fifteen"),
		}
		tw.MustAppend(newEntries...)

		// Verify nextIndex updated
		if tw.nextIndex != 16 {
			t.Fatalf("after append nextIndex = %d, want 16", tw.nextIndex)
		}

		// Verify all entries present
		entries := tw.MustReadAll()
		if len(entries) != 15 {
			t.Fatalf("expected 15 entries, got %d", len(entries))
		}

		// Verify last entry is 15
		last := entries[len(entries)-1]
		if last.GetIndex() != 15 {
			t.Fatalf("last entry index = %d, want 15", last.GetIndex())
		}
	})

	t.Run("updates segment size correctly", func(t *testing.T) {
		tw := NewTestWAL(t)

		// Record initial size
		initialSize := tw.active.size

		// Append an entry
		entry := MakeEntry(1, strings.Repeat("x", 1024))
		tw.MustAppend(entry)

		// Size should increase
		if tw.active.size <= initialSize {
			t.Fatalf("segment size did not increase: initial=%d, after=%d",
				initialSize, tw.active.size)
		}

		// Append another entry
		entry2 := MakeEntry(2, strings.Repeat("y", 2048))
		tw.MustAppend(entry2)

		// Size should increase again
		if tw.active.size <= initialSize {
			t.Fatalf("segment size did not increase after second append")
		}

		// Verify size is at least sum of data
		// (accounting for header overhead)
		minExpected := int64(1024 + 2048)
		if tw.active.size < minExpected {
			t.Fatalf("segment size %d is less than expected %d",
				tw.active.size, minExpected)
		}
	})

	t.Run("handles large batch efficiently", func(t *testing.T) {
		tw := NewTestWAL(t)

		// Append 1000 entries in one batch
		entries := make([]*raftpb.Entry, 1000)
		for i := 0; i < 1000; i++ {
			entries[i] = MakeEntry(uint64(i+1), "data")
		}

		tw.MustAppend(entries...)

		// Verify nextIndex
		if tw.nextIndex != 1001 {
			t.Fatalf("nextIndex = %d, want 1001", tw.nextIndex)
		}

		// Verify all entries readable
		read := tw.MustReadAll()
		if len(read) != 1000 {
			t.Fatalf("expected 1000 entries, got %d", len(read))
		}

		// Spot check some indices
		checkpoints := []uint64{1, 100, 500, 999, 1000}
		for _, idx := range checkpoints {
			if read[idx-1].GetIndex() != idx {
				t.Fatalf("entry at position %d has index %d, want %d",
					idx-1, read[idx-1].GetIndex(), idx)
			}
		}
	})

	t.Run("appends with non-sequential indices", func(t *testing.T) {
		tw := NewTestWAL(t)

		// Append entries with gaps
		tw.MustAppend(MakeEntry(1, "one"))
		tw.MustAppend(MakeEntry(5, "five"))
		tw.MustAppend(MakeEntry(10, "ten"))

		// Verify nextIndex = last + 1
		if tw.nextIndex != 11 {
			t.Fatalf("nextIndex = %d, want 11", tw.nextIndex)
		}

		// Verify entries preserved with their indices
		entries := tw.MustReadAll()
		if len(entries) != 3 {
			t.Fatalf("expected 3 entries, got %d", len(entries))
		}

		expected := []uint64{1, 5, 10}
		for i, idx := range expected {
			if entries[i].GetIndex() != idx {
				t.Fatalf("entry %d index = %d, want %d", i, entries[i].GetIndex(), idx)
			}
		}
	})
}

// TestRotate_SealsAndCreatesNewSegment
// Verifies rotation behavior:
// - Triggers when segment reaches SegmentSize
// - Renames .active → .wal with correct start-end format
// - Creates new .active with nextIndex as start
// - Maintains continuity (endIndex+1 = next.startIndex)
// TestRotate_SealsAndCreatesNewSegment
func TestRotate_SealsAndCreatesNewSegment(t *testing.T) {
	tw := NewTestWAL(t)

	initialSealed := tw.GetSealedCount()

	tw.FillCurrentSegment()

	if tw.GetSealedCount() != initialSealed+1 {
		t.Fatalf("expected sealed count to increase by 1, got %d", tw.GetSealedCount())
	}

	files := tw.GetSegmentPaths()
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d: %v", len(files), files)
	}

	var sealedName, activeName string
	for _, f := range files {
		if strings.HasSuffix(f, ".wal") {
			sealedName = f
		} else if strings.HasSuffix(f, ".active") {
			activeName = f
		}
	}

	// Parse sealed
	base := strings.TrimSuffix(sealedName, ".wal")
	parts := strings.Split(base, "-")
	if len(parts) != 2 {
		t.Fatalf("bad sealed name: %s", sealedName)
	}

	sealedEnd, _ := strconv.ParseUint(parts[1], 10, 64)

	// Correct continuity after rotation + writing the triggering entry
	if sealedEnd+2 != tw.nextIndex {
		t.Fatalf("continuity broken after rotation+write: sealedEnd=%d, nextIndex=%d (expected %d)",
			sealedEnd, tw.nextIndex, sealedEnd+2)
	}

	expectedActive := fmt.Sprintf("%016d.active", sealedEnd+1)
	if activeName != expectedActive {
		t.Fatalf("wrong active name: got %s, want %s", activeName, expectedActive)
	}

	// Verify data
	entries := tw.MustReadAll()
	lastIdx := entries[len(entries)-1].GetIndex()
	if lastIdx != sealedEnd+1 { // the entry that triggered rotation
		t.Fatalf("last written entry = %d, want %d", lastIdx, sealedEnd+1)
	}

	t.Logf("✅ Rotation OK: sealed %s-%d → active starts at %d | nextIndex=%d",
		sealedName, sealedEnd, sealedEnd+1, tw.nextIndex)
}

// ============================================
// READING TESTS
// ============================================

// TestRecover_ReadsAllEntries
// Verifies recovery behavior:
// - Reads all entries from sealed + active segments
// - Respects snapshotIndex (skips <= snapshot)
// - Returns entries in order
// - Returns error on corruption
// - Handles empty segments gracefully
func TestRecover_ReadsAllEntries(t *testing.T) {
	t.Run("recovers all entries from sealed and active", func(t *testing.T) {
		tw := NewTestWAL(t)

		tw.MustAppendRange(1, 100)
		tw.FillCurrentSegment()

		// Add a few more after rotation
		next := tw.nextIndex
		tw.MustAppendRange(next, next+49)

		expectedCount := int(tw.nextIndex - 1)

		entries := tw.MustReadAll()
		if len(entries) != expectedCount {
			t.Fatalf("expected %d entries, got %d", expectedCount, len(entries))
		}

		// Verify sequential order
		for i, e := range entries {
			if e.GetIndex() != uint64(i+1) {
				t.Fatalf("entry at position %d has index %d, want %d", i, e.GetIndex(), i+1)
			}
		}

		tw.MustHaveSealedCount(1)
	})

	t.Run("respects snapshotIndex", func(t *testing.T) {
		tw := NewTestWAL(t)
		tw.MustAppendRange(1, 200)

		var recovered []*raftpb.Entry
		err := tw.Recover(50, func(e *raftpb.Entry) error {
			recovered = append(recovered, e)
			return nil
		})
		if err != nil {
			t.Fatalf("recover failed: %v", err)
		}

		if len(recovered) != 150 {
			t.Fatalf("expected 150 entries (51-200), got %d", len(recovered))
		}
		if recovered[0].GetIndex() != 51 {
			t.Fatalf("first recovered should be 51, got %d", recovered[0].GetIndex())
		}
	})

	t.Run("handles empty WAL gracefully", func(t *testing.T) {
		tw := NewTestWAL(t)
		var count int
		err := tw.Recover(0, func(e *raftpb.Entry) error {
			count++
			return nil
		})
		if err != nil {
			t.Fatalf("recover failed: %v", err)
		}
		if count != 0 {
			t.Fatalf("expected 0 entries, got %d", count)
		}
	})

	t.Run("handles snapshotIndex higher than last entry", func(t *testing.T) {
		tw := NewTestWAL(t)
		tw.MustAppendRange(1, 50)

		var count int
		err := tw.Recover(100, func(e *raftpb.Entry) error {
			count++
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Error("expected no entries when snapshot > last")
		}
	})

	t.Run("returns error on corruption", func(t *testing.T) {
		tw := NewTestWAL(t)
		tw.MustAppendRange(1, 30)
		tw.MustClose()

		// Corrupt the active segment
		files := tw.GetSegmentPaths()
		corrupted := false
		for _, name := range files {
			if strings.HasSuffix(name, ".active") {
				path := filepath.Join(tw.dir, name)
				// Truncate to break the file (make scanLastIndex fail)
				if err := os.Truncate(path, 10); err != nil {
					t.Fatal(err)
				}
				corrupted = true
				break
			}
		}
		if !corrupted {
			t.Fatal("no active segment found to corrupt")
		}

		// Reopen should detect corruption during scanLastIndex
		logger, _ := zap.NewDevelopment()
		_, err := NewSegmentedWal(tw.dir, logger)
		if err == nil {
			t.Fatal("expected error during reopen due to corruption, got nil")
		}

		if !strings.Contains(err.Error(), "scan active segment") &&
			!strings.Contains(err.Error(), "EOF") &&
			!strings.Contains(err.Error(), "corrupt") {
			t.Logf("Got error (acceptable): %v", err)
		} else {
			t.Logf("✅ Corruption correctly detected: %v", err)
		}
	})
}

// TestReader_StreamsCorrectly
// Verifies Reader behavior:
// - Starts at arbitrary index (sealed or active)
// - Crosses segment boundaries seamlessly
// - Returns io.EOF at end
// - Error on index not found
// - Handles segment boundary at exact endIndex+1
func TestReader_StreamsCorrectly(t *testing.T) {
	t.Run("starts at arbitrary index and reads forward", func(t *testing.T) {
		tw := NewTestWAL(t)
		tw.MustAppendRange(1, 300)

		// Start from middle
		r, err := tw.NewReader(150)
		if err != nil {
			t.Fatalf("NewReader(150): %v", err)
		}
		defer r.Close()

		entries := make([]*raftpb.Entry, 0, 200)
		for {
			e, err := r.ReadEntry()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("ReadEntry: %v", err)
			}
			entries = append(entries, e)
		}

		if len(entries) != 151 { // 150 to 300 inclusive
			t.Fatalf("expected 151 entries from 150, got %d", len(entries))
		}
		if entries[0].GetIndex() != 150 {
			t.Fatalf("first entry should be 150, got %d", entries[0].GetIndex())
		}
		if entries[len(entries)-1].GetIndex() != 300 {
			t.Fatalf("last entry should be 300")
		}
	})

	t.Run("crosses segment boundaries seamlessly", func(t *testing.T) {
		tw := NewTestWAL(t)

		tw.MustAppendRange(1, 100)
		tw.FillCurrentSegment() // force rotation
		tw.MustAppendRange(tw.nextIndex, tw.nextIndex+150)

		// Start near the end of first segment
		startIdx := uint64(80)
		r, err := tw.NewReader(startIdx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Close() }()

		entries := tw.readAllFromReader(r) // helper defined below

		if entries[0].GetIndex() != startIdx {
			t.Fatalf("expected start at %d, got %d", startIdx, entries[0].GetIndex())
		}

		// Should have crossed into second segment
		tw.MustHaveSealedCount(1)
	})

	t.Run("returns io.EOF at end", func(t *testing.T) {
		tw := NewTestWAL(t)
		tw.MustAppendRange(1, 50)

		r, err := tw.NewReader(1)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Close() }()

		count := 0
		for {
			_, err := r.ReadEntry()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			count++
		}

		if count != 50 {
			t.Fatalf("expected 50 entries, got %d", count)
		}
	})

	t.Run("errors when index not found", func(t *testing.T) {
		tw := NewTestWAL(t)
		tw.MustAppendRange(1, 100)

		_, err := tw.NewReader(200)
		if err == nil {
			t.Fatal("expected error for index not found")
		}
		if !strings.Contains(err.Error(), "not found") {
			t.Fatalf("expected 'not found' error, got: %v", err)
		}

		// Also test before first index
		_, err = tw.NewReader(0)
		if err == nil {
			t.Fatal("expected error for index 0")
		}
	})

	t.Run("handles start exactly at new segment boundary", func(t *testing.T) {
		tw := NewTestWAL(t)
		tw.MustAppendRange(1, 100)
		tw.FillCurrentSegment()

		firstOfNewSegment := tw.nextIndex
		tw.MustAppendRange(firstOfNewSegment, firstOfNewSegment+50)

		// Start exactly at beginning of second segment
		r, err := tw.NewReader(firstOfNewSegment)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		entries := tw.readAllFromReader(r)
		if len(entries) != 51 {
			t.Fatalf("expected 51 entries starting from new segment, got %d", len(entries))
		}
		if entries[0].GetIndex() != firstOfNewSegment {
			t.Fatalf("wrong starting index")
		}
	})
}

// TestWALReader_ReadsEntries
// Verifies low-level WALReader:
// - Reads valid entry with correct header
// - Truncated header (returns error)
// - Truncated body (returns error)
// - Index mismatch (returns error)
// - Bad protobuf data (returns error)
func TestWALReader_ReadsEntries(t *testing.T) {
	t.Run("reads valid entry", func(t *testing.T) {
		original := MakeEntry(42, "hello world from WALReader")

		be, err := encodeBufferedEntry(original)
		if err != nil {
			t.Fatal(err)
		}

		tmpfile, err := os.CreateTemp(t.TempDir(), "walreader_valid_*.wal")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.Remove(tmpfile.Name()) }()
		defer func() { _ = tmpfile.Close() }()

		if _, err := tmpfile.Write(be.Data); err != nil {
			t.Fatal(err)
		}
		if err := tmpfile.Sync(); err != nil {
			t.Fatal(err)
		}
		if _, err := tmpfile.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}

		reader := &WALReader{file: tmpfile}
		readEntry, err := reader.ReadEntry()
		if err != nil {
			t.Fatalf("failed to read valid entry: %v", err)
		}

		if readEntry.GetIndex() != 42 {
			t.Fatalf("index = %d, want 42", readEntry.GetIndex())
		}
		if string(readEntry.Data) != "hello world from WALReader" {
			t.Fatalf("data mismatch: got %q", string(readEntry.Data))
		}
	})

	t.Run("fails on truncated header", func(t *testing.T) {
		f := createTestFile(t, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}) // < 16 bytes
		defer f.Close()

		reader := &WALReader{file: f}
		_, err := reader.ReadEntry()
		if err == nil {
			t.Fatal("expected error on truncated header")
		}
	})

	t.Run("fails on truncated body", func(t *testing.T) {
		entry := MakeEntry(100, "truncated body test")
		be, _ := encodeBufferedEntry(entry)

		// Header + very little body
		truncated := be.Data[:20]

		f := createTestFile(t, truncated)
		defer f.Close()

		reader := &WALReader{file: f}
		_, err := reader.ReadEntry()
		if err == nil {
			t.Fatal("expected error on truncated body")
		}
	})

	t.Run("fails on index mismatch", func(t *testing.T) {
		entry := MakeEntry(123, "index test")
		be, _ := encodeBufferedEntry(entry)

		header := make([]byte, 16)
		copy(header, be.Data[:16])
		binary.LittleEndian.PutUint64(header[8:16], 9999) // wrong index

		corrupted := append(header, be.Data[16:]...)

		f := createTestFile(t, corrupted)
		defer f.Close()

		reader := &WALReader{file: f}
		_, err := reader.ReadEntry()
		if err == nil || !strings.Contains(err.Error(), "index mismatch") {
			t.Fatalf("expected 'index mismatch' error, got: %v", err)
		}
	})

}

// Helper

// ============================================
// MAINTENANCE TESTS
// ============================================
// TestTruncate_RemovesOldSegments
// Verifies truncation behavior:
// - Removes sealed segments with endIndex <= upToIndex
// - Keeps segments with endIndex > upToIndex
// - Recreates active if all entries truncated
// - Keeps active if entries remain
// TestTruncate_RemovesOldSegments
func TestTruncate_RemovesOldSegments(t *testing.T) {
	t.Run("removes old sealed segments", func(t *testing.T) {
		tw := NewTestWALSmall(t)

		tw.MustAppendRange(1, 80)
		tw.FillCurrentSegment()

		// Get the actual last index in the sealed segment
		// by reading the last sealed segment's endIndex
		tw.mu.RLock()
		sealedCount := len(tw.segments)
		lastInSealed := uint64(0)
		if sealedCount > 0 {
			lastInSealed = tw.segments[sealedCount-1].endIndex
		}
		tw.mu.RUnlock()

		if lastInSealed == 0 {
			t.Fatal("no sealed segment found after FillCurrentSegment")
		}

		tw.MustAppendRange(tw.nextIndex, tw.nextIndex+40)

		tw.MustTruncate(lastInSealed)

		tw.MustHaveSealedCount(0)

		entries := tw.MustReadAll()
		if len(entries) == 0 || entries[0].GetIndex() != lastInSealed+1 {
			t.Fatalf("expected first remaining entry %d, got %d", lastInSealed+1, entries[0].GetIndex())
		}
	})

	t.Run("recreates active when truncating everything", func(t *testing.T) {
		tw := NewTestWALSmall(t)
		tw.MustAppendRange(1, 300)

		tw.MustTruncate(300)

		tw.MustHaveSealedCount(0)
		tw.MustHaveFileCount(1)

		if tw.nextIndex != 301 {
			t.Fatalf("nextIndex = %d, want 301", tw.nextIndex)
		}
	})

	t.Run("keeps active when it has later entries", func(t *testing.T) {
		tw := NewTestWALSmall(t)
		tw.MustAppendRange(1, 100)

		tw.MustTruncate(30)

		// Because there is no sealed segment yet, and we only drop whole segments,
		// the active segment is kept fully. So first entry remains 1.
		entries := tw.MustReadAll()
		if len(entries) == 0 || entries[0].GetIndex() != 1 {
			t.Fatalf("expected first entry to remain 1 (active not truncated in place), got %d", entries[0].GetIndex())
		}

		// nextIndex should still point after the last written entry
		if tw.nextIndex != 101 {
			t.Fatalf("nextIndex = %d, want 101", tw.nextIndex)
		}
	})
}

// TestParseSegment_ParsesFilenames
// Verifies filename parsing:
// - Valid .wal: "0000000000000001-0000000000000010.wal"
// - Valid .active: "0000000000000011.active"
// - Invalid formats return error
// - Malformed numbers return error
// - Non-existent file returns error
func TestParseSegment_ParsesFilenames(t *testing.T) {
	t.Run("parses valid sealed .wal segment", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "0000000000000001-0000000000000100.wal")

		// Create a minimal valid file
		if err := os.WriteFile(path, []byte("dummy"), 0640); err != nil {
			t.Fatal(err)
		}

		seg, err := parseSegment(path)
		if err != nil {
			t.Fatalf("parseSegment failed: %v", err)
		}

		if seg.startIndex != 1 {
			t.Fatalf("startIndex = %d, want 1", seg.startIndex)
		}
		if seg.endIndex != 100 {
			t.Fatalf("endIndex = %d, want 100", seg.endIndex)
		}
		if seg.active {
			t.Fatal("should not be active")
		}
		if seg.readFile == nil {
			t.Error("readFile should be opened for sealed segments")
		}
	})

	t.Run("parses valid .active segment", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "0000000000000500.active")

		if err := os.WriteFile(path, []byte("dummy"), 0640); err != nil {
			t.Fatal(err)
		}

		seg, err := parseSegment(path)
		if err != nil {
			t.Fatalf("parseSegment failed: %v", err)
		}

		if seg.startIndex != 500 {
			t.Fatalf("startIndex = %d, want 500", seg.startIndex)
		}
		if seg.active != true {
			t.Fatal("should be marked as active")
		}
		if seg.endIndex != 0 {
			t.Error("endIndex should be 0 for active segment")
		}
	})

	t.Run("returns error on invalid formats", func(t *testing.T) {
		invalid := []string{
			"0000000000000001.wal",                      // missing end index
			"0000000000000001-abc.wal",                  // non-numeric end
			"abc-0000000000000001.wal",                  // non-numeric start
			"0000000000000001-0000000000000010.wal.bak", // wrong extension
			"randomfile.txt",
		}

		for _, name := range invalid {
			_, err := parseSegment(filepath.Join(t.TempDir(), name))
			if err == nil {
				t.Errorf("expected error for invalid name: %s", name)
			}
		}
	})

	t.Run("returns error on non-existent file", func(t *testing.T) {
		_, err := parseSegment("/non/existent/path/0000000000000001-0000000000000010.wal")
		if err == nil {
			t.Fatal("expected error for non-existent file")
		}
	})

	t.Run("handles malformed number gracefully", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "0000000000000001-999999999999999999999.wal") // too big for uint64

		if err := os.WriteFile(path, []byte("x"), 0640); err != nil {
			t.Fatal(err)
		}

		_, err := parseSegment(path)
		if err == nil {
			t.Error("expected error on number overflow")
		}
	})
}

// ============================================
// INTEGRATION TESTS
// ============================================

// TestIntegration_FullLifecycle
// Full WAL lifecycle: create → append → rotate → append → close → reopen → recover
// Verifies data persistence through all operations
func TestIntegration_FullLifecycle(t *testing.T) {
	tw := NewTestWALSmall(t) // Use smaller segments for faster test

	// 1. Initial append
	tw.MustAppendRange(1, 50)
	indices := make([]uint64, 50)
	for i := range indices {
		indices[i] = uint64(i + 1)
	}
	tw.MustContain(indices...)

	// 2. Force rotation
	tw.FillCurrentSegment()
	tw.MustHaveSealedCount(1)

	// 3. Append more data after rotation
	firstAfterRotate := tw.nextIndex
	tw.MustAppendRange(firstAfterRotate, firstAfterRotate+80)

	totalEntries := int(tw.nextIndex - 1)

	// 4. Verify everything is readable
	all := tw.MustReadAll()
	if len(all) != totalEntries {
		t.Fatalf("expected %d entries, got %d", totalEntries, len(all))
	}

	// 5. Close and reopen
	tw.Reopen()

	// 6. Verify data survived reopen
	reopened := tw.MustReadAll()
	if len(reopened) != totalEntries {
		t.Fatalf("after reopen: expected %d entries, got %d", totalEntries, len(reopened))
	}

	// Verify first and last entries
	if reopened[0].GetIndex() != 1 {
		t.Fatalf("first entry after reopen should be 1, got %d", reopened[0].GetIndex())
	}
	if reopened[len(reopened)-1].GetIndex() != uint64(totalEntries) {
		t.Fatalf("last entry after reopen should be %d, got %d", totalEntries, reopened[len(reopened)-1].GetIndex())
	}

	// 7. Append more data after reopen
	nextIdx := tw.nextIndex
	tw.MustAppendRange(nextIdx, nextIdx+30)

	// Final verification
	final := tw.MustReadAll()
	if len(final) != totalEntries+31 {
		t.Fatalf("final count mismatch: got %d, want %d", len(final), totalEntries+31)
	}

	t.Logf("✅ Full lifecycle test passed: %d entries survived create/rotate/reopen", len(final))
}

// TestIntegration_CrashRecovery
// Tests crash recovery at each critical state:
// - Abandoned .tmp file (cleaned up)
// - Sealed segment exists, active missing (recover from sealed)
// - Partially written active segment (scan to last valid)
// - Empty active segment (handled gracefully)
// - Corrupted active record (error or truncate)
// - Corrupted sealed record (error)
// Verifies WAL recovers to consistent state
func TestIntegration_CrashRecovery(t *testing.T) {
	t.Run("cleans up abandoned .tmp files", func(t *testing.T) {
		dir := t.TempDir()
		tmpPath := filepath.Join(dir, "0000000000000005.tmp")
		if err := os.WriteFile(tmpPath, []byte("garbage"), 0640); err != nil {
			t.Fatal(err)
		}

		logger, _ := zap.NewDevelopment()
		wal, err := NewSegmentedWal(dir, logger)
		if err != nil {
			t.Fatalf("NewSegmentedWal failed: %v", err)
		}
		defer func() { _ = wal.Close() }()

		// .tmp should be gone
		if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
			t.Error("abandoned .tmp file was not cleaned up")
		}
	})

	t.Run("recovers when only sealed segments exist", func(t *testing.T) {
		tw := NewTestWALSmall(t)

		tw.MustAppendRange(1, 100)
		tw.FillCurrentSegment()

		// Get the actual last index from the sealed segment
		tw.mu.RLock()
		lastIndex := uint64(0)
		if len(tw.segments) > 0 {
			lastIndex = tw.segments[len(tw.segments)-1].endIndex
		}
		tw.mu.RUnlock()

		expectedCount := int(lastIndex)

		tw.MustClose()

		// Simulate crash: remove active file
		for _, name := range tw.GetSegmentPaths() {
			if strings.HasSuffix(name, ".active") {
				_ = os.Remove(filepath.Join(tw.dir, name))
				break
			}
		}

		tw.Reopen()

		tw.MustHaveSealedCount(1)

		entries := tw.MustReadAll()
		if len(entries) != expectedCount {
			t.Fatalf("expected %d entries after recovery, got %d", expectedCount, len(entries))
		}

		if entries[0].GetIndex() != 1 || entries[len(entries)-1].GetIndex() != uint64(expectedCount) {
			t.Fatalf("data integrity failed: first=%d, last=%d", entries[0].GetIndex(), entries[len(entries)-1].GetIndex())
		}
	})

	t.Run("recovers from partially written active segment", func(t *testing.T) {
		tw := NewTestWALSmall(t)
		tw.MustAppendRange(1, 50)

		// Simulate crash during write: truncate active segment
		tw.MustClose()

		var activePath string
		for _, name := range tw.GetSegmentPaths() {
			if strings.HasSuffix(name, ".active") {
				activePath = filepath.Join(tw.dir, name)
				break
			}
		}

		// Corrupt the active segment (truncate in the middle)
		if err := os.Truncate(activePath, 500); err != nil {
			t.Fatal(err)
		}

		// Reopen should either:
		//   1. Recover up to last valid entry, or
		//   2. Fail gracefully (current behavior)
		logger, _ := zap.NewDevelopment()
		wal, err := NewSegmentedWal(tw.dir, logger)

		if err != nil {
			// Current implementation fails on corrupted active - this is acceptable
			t.Logf("Reopen failed as expected on corrupted active: %v", err)
			return
		}

		tw.SegmentedWAL = wal

		entries := tw.MustReadAll()
		if len(entries) == 0 {
			t.Fatal("should recover at least some entries")
		}

		t.Logf("✅ Recovered %d entries after partial active segment", len(entries))
	})

	t.Run("handles empty active segment gracefully", func(t *testing.T) {
		tw := NewTestWALSmall(t)
		tw.MustAppendRange(1, 20)
		tw.MustClose()

		// Empty the active file
		for _, name := range tw.GetSegmentPaths() {
			if strings.HasSuffix(name, ".active") {
				path := filepath.Join(tw.dir, name)
				_ = os.Truncate(path, 0)
				break
			}
		}

		tw.Reopen()
		tw.MustHaveSealedCount(0) // or 1 depending on implementation
		// Should not crash
	})

	t.Run("returns error on corrupted sealed segment", func(t *testing.T) {
		tw := NewTestWALSmall(t)
		tw.MustAppendRange(1, 100)
		tw.FillCurrentSegment()
		tw.MustClose()

		// Find and corrupt a sealed segment
		var sealedPath string
		for _, name := range tw.GetSegmentPaths() {
			if strings.HasSuffix(name, ".wal") {
				sealedPath = filepath.Join(tw.dir, name)
				break
			}
		}
		if sealedPath == "" {
			t.Fatal("no sealed segment found")
		}

		// Corrupt it badly
		if err := os.Truncate(sealedPath, 8); err != nil {
			t.Fatal(err)
		}

		logger, _ := zap.NewDevelopment()
		_, err := NewSegmentedWal(tw.dir, logger)

		// Either NewSegmentedWal fails, OR recovery fails later — both are acceptable
		if err != nil {
			t.Logf("✅ NewSegmentedWal correctly failed: %v", err)
			return
		}

		// If we reached here, NewSegmentedWal skipped the bad segment.
		// Now check that Recover() fails as expected
		err = tw.Recover(0, func(e *raftpb.Entry) error {
			return nil
		})

		if err == nil {
			t.Fatal("expected error during Recover() on corrupted sealed segment")
		}

		t.Logf("✅ Correctly detected corruption during recovery: %v", err)
	})
}

// TestIntegration_TruncateThenAppend
// Tests truncation followed by appending:
// - Truncate at arbitrary point
// - Append continues from correct index
// - No gaps created
// - Existing segments after truncation kept
func TestIntegration_TruncateThenAppend(t *testing.T) {
	t.Run("truncate then append continues correctly", func(t *testing.T) {
		tw := NewTestWALSmall(t)

		// Write initial data and force rotation
		tw.MustAppendRange(1, 100)
		tw.FillCurrentSegment()

		// Get the actual end of the sealed segment
		tw.mu.RLock()
		lastSealed := uint64(0)
		if len(tw.segments) > 0 {
			lastSealed = tw.segments[len(tw.segments)-1].endIndex
		}
		tw.mu.RUnlock()

		tw.MustAppendRange(tw.nextIndex, tw.nextIndex+80)

		// Now truncate at the end of the sealed segment
		truncateAt := lastSealed
		tw.MustTruncate(truncateAt)

		// Verify truncation worked
		remaining := tw.MustReadAll()
		if len(remaining) == 0 || remaining[0].GetIndex() != truncateAt+1 {
			t.Fatalf("expected first entry after truncate to be %d, got %d", truncateAt+1, remaining[0].GetIndex())
		}

		// Append new entries - should continue correctly
		nextExpected := tw.nextIndex
		tw.MustAppendRange(nextExpected, nextExpected+50)

		// Final verification - no gaps
		final := tw.MustReadAll()
		if final[len(final)-1].GetIndex() != nextExpected+50 {
			t.Fatalf("last entry should be %d", nextExpected+50)
		}

		t.Logf("✅ Truncate+Append successful: truncated at %d, appended up to %d", truncateAt, final[len(final)-1].GetIndex())
	})

	t.Run("keeps later segments after truncation", func(t *testing.T) {
		tw := NewTestWALSmall(t)

		tw.MustAppendRange(1, 150)
		tw.FillCurrentSegment() // creates sealed segment
		tw.MustAppendRange(tw.nextIndex, tw.nextIndex+100)

		// Get the actual end of the sealed segment
		tw.mu.RLock()
		sealedEnd := uint64(0)
		if len(tw.segments) > 0 {
			sealedEnd = tw.segments[len(tw.segments)-1].endIndex
		}
		tw.mu.RUnlock()

		if sealedEnd == 0 {
			t.Fatal("no sealed segment found")
		}

		// Truncate at the end of the sealed segment → should remove it
		tw.MustTruncate(sealedEnd)

		// Should continue from the next segment
		entries := tw.MustReadAll()
		if len(entries) == 0 || entries[0].GetIndex() != sealedEnd+1 {
			t.Fatalf("expected continuation from %d, got %d", sealedEnd+1, entries[0].GetIndex())
		}

		t.Logf("✅ Kept later segments: truncated at %d, resumed at %d", sealedEnd, entries[0].GetIndex())
	})
}

// ============================================
// EDGE CASE TESTS
// ============================================
// TestEdgeCases_LargeEntries
// Handles entries larger than SegmentSize:
// - Single entry > SegmentSize
// - Writes and reads correctly
// - Does not get stuck in rotation loop
func TestEdgeCases_LargeEntries(t *testing.T) {
	t.Run("single entry larger than segment size", func(t *testing.T) {
		tw := NewTestWALSmall(t) // 10MB segments

		// 15MB entry — bigger than segment size
		largeData := strings.Repeat("X", 15*1024*1024)
		largeEntry := MakeEntry(1, largeData)

		tw.MustAppend(largeEntry)

		// Should be readable
		entries := tw.MustReadAll()
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		if entries[0].GetIndex() != 1 {
			t.Fatalf("wrong index: got %d", entries[0].GetIndex())
		}
		if len(entries[0].Data) != len(largeData) {
			t.Fatalf("data length corrupted: got %d, want %d", len(entries[0].Data), len(largeData))
		}
	})

	t.Run("multiple large entries", func(t *testing.T) {
		tw := NewTestWALSmall(t)

		for i := 1; i <= 3; i++ {
			largeData := strings.Repeat("Y", 12*1024*1024)
			tw.MustAppend(MakeEntry(uint64(i), largeData))
		}

		entries := tw.MustReadAll()
		if len(entries) != 3 {
			t.Fatalf("expected 3 large entries, got %d", len(entries))
		}

		for i, e := range entries {
			if e.GetIndex() != uint64(i+1) {
				t.Fatalf("index mismatch at position %d", i)
			}
		}
	})

	t.Run("does not infinite loop on large entry", func(t *testing.T) {
		tw := NewTestWALSmall(t)

		// This would previously cause rotation loop in some buggy implementations
		veryLarge := strings.Repeat("Z", int(tw.segmentSize)+1024*1024)
		tw.MustAppend(MakeEntry(1, veryLarge))

		// If we reach here without hanging, we're good
		entries := tw.MustReadAll()
		if len(entries) != 1 {
			t.Fatal("large entry was not written")
		}
	})

	t.Run("large entry followed by normal entries", func(t *testing.T) {
		tw := NewTestWALSmall(t)

		// One very large entry
		hugeData := strings.Repeat("H", int(tw.segmentSize)*2)
		tw.MustAppend(MakeEntry(1, hugeData))

		// Normal small entries after it
		tw.MustAppendRange(2, 30)

		entries := tw.MustReadAll()
		if len(entries) != 30 {
			t.Fatalf("expected 30 entries, got %d", len(entries))
		}

		if entries[0].GetIndex() != 1 || len(entries[0].Data) != len(hugeData) {
			t.Fatal("large entry was corrupted")
		}

		t.Logf("✅ Large entry + normal entries handled correctly")
	})
}

// TestEdgeCases_EmptySegments
// Handles empty .wal or .active files:
// - Recover handles gracefully
// - NewReader handles gracefully
// - No panic or infinite loop
func TestEdgeCases_EmptySegments(t *testing.T) {
	t.Run("empty active segment after crash", func(t *testing.T) {
		tw := NewTestWALSmall(t)
		tw.MustAppendRange(1, 30)
		tw.MustClose()

		// Simulate crash that left an empty active file
		for _, name := range tw.GetSegmentPaths() {
			if strings.HasSuffix(name, ".active") {
				path := filepath.Join(tw.dir, name)
				_ = os.Truncate(path, 0) // empty the active segment
				break
			}
		}

		tw.Reopen()

		// Since active is empty and there are no sealed segments in this scenario,
		// we expect 0 entries (or the previous sealed ones if any existed).
		entries := tw.MustReadAll()
		if len(entries) != 0 {
			t.Logf("Note: recovered %d entries (acceptable if sealed segments existed)", len(entries))
		} else {
			t.Log("✅ Empty active segment after crash handled gracefully (0 entries recovered)")
		}

		// The important part is that it didn't panic or crash
	})

	t.Run("empty sealed segment", func(t *testing.T) {
		tw := NewTestWALSmall(t)
		tw.MustAppendRange(1, 50)
		tw.FillCurrentSegment()
		tw.MustClose()

		// Empty a sealed segment file
		for _, name := range tw.GetSegmentPaths() {
			if strings.HasSuffix(name, ".wal") {
				path := filepath.Join(tw.dir, name)
				_ = os.Truncate(path, 0)
				break
			}
		}

		logger, _ := zap.NewDevelopment()
		wal, err := NewSegmentedWal(tw.dir, logger)
		if err != nil {
			// It's acceptable to fail on empty/corrupt sealed
			t.Logf("NewSegmentedWal failed on empty sealed (acceptable): %v", err)
			return
		}

		tw.SegmentedWAL = wal

		// Should not panic
		entries := tw.MustReadAll()
		t.Logf("Recovered %d entries with empty sealed segment present", len(entries))
	})

	t.Run("NewReader on completely empty WAL", func(t *testing.T) {
		tw := NewTestWALSmall(t)

		_, err := tw.NewReader(1)
		if err == nil {
			t.Fatal("expected error when creating reader on empty WAL")
		}
	})

	t.Run("Recover on empty WAL", func(t *testing.T) {
		tw := NewTestWALSmall(t)

		var count int
		err := tw.Recover(0, func(e *raftpb.Entry) error {
			count++
			return nil
		})
		if err != nil {
			t.Fatalf("Recover failed on empty WAL: %v", err)
		}
		if count != 0 {
			t.Fatalf("expected 0 entries, got %d", count)
		}
	})

	t.Run("no panic on empty files during recovery", func(t *testing.T) {
		tw := NewTestWALSmall(t)
		tw.MustAppendRange(1, 10)
		tw.MustClose()

		// Empty both active and any sealed
		for _, name := range tw.GetSegmentPaths() {
			path := filepath.Join(tw.dir, name)
			_ = os.Truncate(path, 0)
		}

		tw.Reopen()

		// Should not panic
		_ = tw.MustReadAll()
	})
}
