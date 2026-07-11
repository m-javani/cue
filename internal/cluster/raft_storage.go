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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type JobState struct {
	AddIndex      uint64
	CompleteIndex uint64
	CompleteType  string // "done" or "drop"
}

type BufferedEntry struct {
	Index uint64 // needed for segment boundary decisions
	Data  []byte // pre-serialized raftpb.Entry (header + payload)
}

// ========== Core Storage ==========
type StorageCore struct {
	mu sync.RWMutex // protects: entries, states, indices, jobIndex, writeBuffer, etc.

	// In-memory log
	entries map[uint64]*raftpb.Entry

	// Raft state
	hardState    *raftpb.HardState
	confState    *raftpb.ConfState
	snapshotMeta *raftpb.SnapshotMetadata

	// Index tracking
	firstIndex uint64
	lastIndex  uint64

	// WAL management
	wal            *SegmentedWAL
	writeBuffer    []BufferedEntry
	flushThreshold int
	flushing       atomic.Int32 // 0 = idle, 1 = flushing

	// Paths
	walPath      string
	snapshotPath string

	// Secondary index: jobID -> JobState
	jobIndex map[string]*JobState

	metrics *internal.ClusterMetrics
	logger  *zap.Logger
}

// ========== Update Indices ==========
func (c *StorageCore) updateIndices() {
	if len(c.entries) == 0 {
		// Safe handling when snapshotMeta.Index is nil
		snapIndex := c.snapshotMeta.GetIndex()
		c.firstIndex = snapIndex + 1
		c.lastIndex = snapIndex
		return
	}

	first := uint64(^uint64(0))
	last := uint64(0)

	for idx := range c.entries {
		index := c.entries[idx].GetIndex()
		if index < first {
			first = index
		}
		if index > last {
			last = index
		}
	}

	c.firstIndex = first
	c.lastIndex = last
}

// ========== Load Snapshot Metadata ==========
func (c *StorageCore) loadSnapshotMeta() error {
	if _, err := os.Stat(c.snapshotPath); os.IsNotExist(err) {
		c.snapshotMeta = &raftpb.SnapshotMetadata{
			Index:     proto.Uint64(0),
			Term:      proto.Uint64(0),
			ConfState: &raftpb.ConfState{},
		}
		return nil
	}

	data, err := os.ReadFile(c.snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to read snapshot meta: %w", err)
	}

	if err := proto.Unmarshal(data, c.snapshotMeta); err != nil {
		return fmt.Errorf("failed to decode snapshot meta: %w", err)
	}

	c.logger.Info("loaded snapshot metadata",
		zap.Uint64("index", c.snapshotMeta.GetIndex()),
		zap.Uint64("term", c.snapshotMeta.GetIndex()))
	return nil
}

// ========== Save Snapshot Metadata ==========
func (c *StorageCore) saveSnapshotMeta() error {
	data, err := proto.Marshal(c.snapshotMeta)
	if err != nil {
		return fmt.Errorf("failed to encode snapshot meta: %w", err)
	}

	tmpPath := c.snapshotPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp snapshot meta: %w", err)
	}

	if err := os.Rename(tmpPath, c.snapshotPath); err != nil {
		return fmt.Errorf("failed to rename snapshot meta: %w", err)
	}

	// Sync directory for durability
	if dir, err := os.Open(c.snapshotPath); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	return nil
}

// ========== Install Snapshot ==========
func (c *StorageCore) installSnapshot(meta *raftpb.SnapshotMetadata) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.snapshotMeta = meta

	_ = c.saveSnapshotMeta()
	// do not compact on leader sent snapshots
	return nil
}

// ========== Setters ==========
func (c *StorageCore) setHardState(hs *raftpb.HardState) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hardState = hs
	return nil
}

func (c *StorageCore) setConfState(cs *raftpb.ConfState) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.confState = cs
	c.snapshotMeta.ConfState = cs
	return c.saveSnapshotMeta()
}

// ========== Getters ==========
func (c *StorageCore) getFirstIndex() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.firstIndex
}

func (c *StorageCore) getLastIndex() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastIndex
}

// ========== Recovery ==========
func (c *StorageCore) recover() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.jobIndex = make(map[string]*JobState)

	snapshotIndex := c.snapshotMeta.GetIndex()

	err := c.wal.Recover(snapshotIndex, func(entry *raftpb.Entry) error {
		// Store entry
		c.entries[entry.GetIndex()] = entry

		// Rebuild job secondary index
		var cmd model.Command
		if err := msgpack.Unmarshal(entry.Data, &cmd); err == nil {
			switch cmd.Type {
			case model.CmdAddJob:
				if cmd.AddJob != nil {
					jobID := cmd.AddJob.Job.ID

					if _, exists := c.jobIndex[jobID]; !exists {
						c.jobIndex[jobID] = &JobState{}
					}

					c.jobIndex[jobID].AddIndex = entry.GetIndex()
				}

			case model.CmdDone, model.CmdDrop:
				completeType := "done"

				var jobIDs []string

				if cmd.Done != nil {
					jobIDs = cmd.Done.JobIDs
				} else if cmd.Drop != nil {
					jobIDs = cmd.Drop.JobIDs
					completeType = "drop"
				}

				for _, jobID := range jobIDs {
					if _, exists := c.jobIndex[jobID]; !exists {
						c.jobIndex[jobID] = &JobState{}
					}

					js := c.jobIndex[jobID]
					js.CompleteIndex = entry.GetIndex()
					js.CompleteType = completeType
				}
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	c.updateIndices()

	c.logger.Info(
		"WAL recovery completed",
		zap.Uint64("snapshot_index", snapshotIndex),
		zap.Uint64("first_index", c.firstIndex),
		zap.Uint64("last_index", c.lastIndex),
		zap.Int("entries_recovered", len(c.entries)),
		zap.Int("active_jobs", len(c.jobIndex)),
	)

	return nil
}

// ========== Append Methods ==========
func (c *StorageCore) appendEntries(entries []*raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for idx, entry := range entries {
		// Raft truncation: remove conflicting tail
		if entry.GetIndex() <= c.lastIndex {
			for i := entry.GetIndex(); i <= c.lastIndex; i++ {
				delete(c.entries, i)
			}
		}

		c.entries[entry.GetIndex()] = entries[idx]

		if entry.GetIndex() > c.lastIndex {
			c.lastIndex = entry.GetIndex()
		}
		if c.firstIndex == 0 || entry.GetIndex() < c.firstIndex {
			c.firstIndex = entry.GetIndex()
		}
	}

	return nil
}

// appendCommitted persists a single committed entry to the WAL buffer
// and updates the job secondary index.
func (c *StorageCore) appendCommitted(entry *raftpb.Entry) error {
	c.mu.Lock()

	bufferedEntry, err := encodeBufferedEntry(entry)
	if err != nil {
		return err
	}

	// Append to WAL buffer as structured entry
	c.writeBuffer = append(c.writeBuffer, bufferedEntry)

	// Update job secondary index
	var cmd model.Command
	if err := msgpack.Unmarshal(entry.Data, &cmd); err == nil {
		switch cmd.Type {
		case model.CmdAddJob:
			if cmd.AddJob != nil {
				jobID := cmd.AddJob.Job.ID
				if _, exists := c.jobIndex[jobID]; !exists {
					c.jobIndex[jobID] = &JobState{}
				}
				c.jobIndex[jobID].AddIndex = entry.GetIndex()
				c.jobIndex[jobID].CompleteType = "" // reset if re-added
			}

		case model.CmdDone, model.CmdDrop:
			completeType := "done"
			var jobIDs []string

			if cmd.Done != nil {
				jobIDs = cmd.Done.JobIDs
			} else if cmd.Drop != nil {
				jobIDs = cmd.Drop.JobIDs
				completeType = "drop"
			}

			for _, jobID := range jobIDs {
				if _, exists := c.jobIndex[jobID]; !exists {
					c.jobIndex[jobID] = &JobState{}
				}
				js := c.jobIndex[jobID]
				js.CompleteIndex = entry.GetIndex()
				js.CompleteType = completeType
			}
		}
	}

	c.mu.Unlock()

	// Flush if threshold reached
	if len(c.writeBuffer) >= c.flushThreshold {
		if err := c.flushBuffer(); err != nil {
			return err
		}
	}

	return nil
}

// ========== Flush ==========
func (c *StorageCore) flushBuffer() error {
	if !c.flushing.CompareAndSwap(0, 1) {
		c.logger.Debug("flush already in progress, skipping")
		return nil
	}
	defer c.flushing.Store(0)

	c.mu.Lock()
	if len(c.writeBuffer) == 0 {
		c.mu.Unlock()
		return nil
	}

	// Track how many entries we're about to flush
	flushCount := len(c.writeBuffer)
	batch := make([]BufferedEntry, flushCount)
	copy(batch, c.writeBuffer)
	c.mu.Unlock()

	// Write to WAL
	if err := c.wal.AppendBatch(batch); err != nil {
		c.logger.Error("failed to flush WAL, will retry",
			zap.Error(err),
			zap.Int("entries", len(batch)))
		return err
	}

	// Clear only the flushed entries
	c.mu.Lock()
	if len(c.writeBuffer) >= flushCount {
		// Remove the flushed entries from the front
		c.writeBuffer = c.writeBuffer[flushCount:]
	} else {
		// Shouldn't happen, but handle gracefully
		c.writeBuffer = c.writeBuffer[:0]
	}
	c.mu.Unlock()

	c.metrics.WalFlush()
	c.logger.Debug("flushed wal to disk",
		zap.Int("count", flushCount),
		zap.Int("remaining_in_buffer", len(c.writeBuffer)))

	return nil
}

// ========== Compaction ==========
// compact compacts the log up to the safe boundary.
// It respects pending jobs and never compacts too aggressively.
func (c *StorageCore) compact() error {
	c.mu.Lock()
	upToIndex := c.findFirstIndexToKeep()
	if upToIndex <= c.firstIndex {
		c.mu.Unlock()
		return nil
	}

	// Remove old entries from in-memory map
	for _, e := range c.entries {
		if e.GetIndex() < upToIndex {
			delete(c.entries, e.GetIndex())
		}
	}

	c.updateIndices()

	c.logger.Info("STORAGE INDEXES",
		zap.Uint64("firstIndex", c.firstIndex),
		zap.Uint64("lastIndex", c.lastIndex))

	// Clean completed jobs that are now fully compacted
	for jobID, state := range c.jobIndex {
		if state.CompleteIndex > 0 && state.CompleteIndex < upToIndex {
			delete(c.jobIndex, jobID)
		}
	}

	idx := upToIndex - 1
	snapshotMeta := &raftpb.SnapshotMetadata{
		Index:     &idx,
		Term:      c.hardState.Term,
		ConfState: c.confState,
	}

	c.snapshotMeta = snapshotMeta
	_ = c.saveSnapshotMeta()

	// Truncate WAL file
	// Clean jobIndex: remove entries that were truncated
	for jobID, js := range c.jobIndex {
		if js.AddIndex < upToIndex {
			delete(c.jobIndex, jobID)
		}
	}

	// Truncate WAL - O(1) segment deletion, no expensive file.Truncate()
	if err := c.wal.Truncate(upToIndex); err != nil {
		return fmt.Errorf("truncate WAL: %w", err)
	}

	c.mu.Unlock()

	c.logger.Info("log compaction completed",
		zap.Uint64("up_to_index", upToIndex),
		zap.Uint64("new_first_index", c.firstIndex),
		zap.Int("remaining_jobs", len(c.jobIndex)))

	return nil
}

func (c *StorageCore) findFirstIndexToKeep() uint64 {
	var minPending uint64 = 0

	add := 0
	done := 0
	for _, state := range c.jobIndex {
		if state.CompleteType == "" {
			add += 1
		} else {
			add += 1
			done += 1
		}
	}

	for _, state := range c.jobIndex {
		if state.AddIndex > 0 && state.CompleteType == "" {
			if minPending == 0 || state.AddIndex < minPending {
				minPending = state.AddIndex
			}
		}
	}

	if minPending != 0 {
		return minPending
	}

	// No pending jobs → safe to compact after the last completed job
	var maxComplete uint64 = 0
	for _, state := range c.jobIndex {
		if state.CompleteIndex > maxComplete {
			maxComplete = state.CompleteIndex
		}
	}

	if maxComplete > 0 {
		return maxComplete + 1
	}

	return c.firstIndex // fallback
}

// ========== RaftStorage Wrapper ==========
type RaftStorage struct {
	core   *StorageCore
	logger *zap.Logger
}

func NewRaftStorage(walDir, snapshotDir string, flushThreshold int, logger *zap.Logger, metrics *internal.ClusterMetrics) (*RaftStorage, error) {
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, err
	}

	snapshotPath := filepath.Join(snapshotDir, "snapshot.meta")

	// Open segmented WAL instead of single file
	wal, err := NewSegmentedWal(walDir, logger)
	if err != nil {
		return nil, fmt.Errorf("open segmented WAL: %w", err)
	}

	core := &StorageCore{
		entries:        make(map[uint64]*raftpb.Entry),
		wal:            wal,
		writeBuffer:    make([]BufferedEntry, 0, flushThreshold),
		flushThreshold: flushThreshold,
		walPath:        walDir,
		snapshotPath:   snapshotPath,
		logger:         logger,
		jobIndex:       make(map[string]*JobState),
		metrics:        metrics,
		mu:             sync.RWMutex{},
		hardState:      &raftpb.HardState{},
		confState:      &raftpb.ConfState{},
		snapshotMeta:   &raftpb.SnapshotMetadata{},
		firstIndex:     0,
		lastIndex:      0,
		flushing:       atomic.Int32{},
	}
	core.flushing.Store(0)

	if err := core.loadSnapshotMeta(); err != nil {
		logger.Warn("failed to load snapshot metadata", zap.Error(err))
	}
	if err := core.recover(); err != nil {
		return nil, fmt.Errorf("WAL recovery failed, starting empty %w", err)
	}

	core.updateIndices()

	return &RaftStorage{
		core:   core,
		logger: logger,
	}, nil
}

// raft.Storage interface methods + public methods
func (s *RaftStorage) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	return s.core.hardState, s.core.confState, nil
}

func (s *RaftStorage) Append(entries []*raftpb.Entry) error {
	return s.core.appendEntries(entries)
}

func (s *RaftStorage) Entries(lo, hi, maxSize uint64) ([]*raftpb.Entry, error) {
	if lo < s.core.firstIndex {
		return nil, raft.ErrCompacted
	}
	if hi > s.core.lastIndex+1 {
		return nil, raft.ErrUnavailable
	}

	var entries []*raftpb.Entry
	var size uint64
	for idx := lo; idx < hi; idx++ {
		entry, ok := s.core.entries[idx]
		if !ok {
			return nil, raft.ErrUnavailable
		}
		entrySize := uint64(proto.Size(entry))
		if maxSize > 0 && size+entrySize > maxSize {
			break
		}
		// Return pointer (required by new interface). The struct is copied to heap.
		entries = append(entries, entry)
		size += entrySize
	}
	return entries, nil
}

func (s *RaftStorage) Term(idx uint64) (uint64, error) {
	if idx == s.core.snapshotMeta.GetIndex() {
		return s.core.snapshotMeta.GetTerm(), nil
	}
	if idx == 0 && s.core.snapshotMeta.GetIndex() == 1 {
		return 1, nil
	}
	if idx < s.core.firstIndex {
		return 0, raft.ErrCompacted
	}
	if idx > s.core.lastIndex {
		return 0, raft.ErrUnavailable
	}
	if entry, ok := s.core.entries[idx]; ok {
		return entry.GetTerm(), nil
	}
	if idx <= s.core.lastIndex {
		return 0, raft.ErrUnavailable
	}
	return 0, raft.ErrUnavailable
}

func (s *RaftStorage) FirstIndex() (uint64, error) {
	return s.core.getFirstIndex(), nil
}

func (s *RaftStorage) LastIndex() (uint64, error) {
	return s.core.getLastIndex(), nil
}

// ========== Get Completed Job IDs ==========
// used by cluster agent for avoiding dead jobs
func (s *RaftStorage) GetCompletedJobIDs() map[string]bool {
	s.core.mu.RLock()
	defer s.core.mu.RUnlock()

	completed := make(map[string]bool, len(s.core.jobIndex))

	for jobID, state := range s.core.jobIndex {
		if state.AddIndex > 0 && state.CompleteType != "" &&
			state.CompleteIndex >= state.AddIndex {
			completed[jobID] = true
		}
	}
	return completed
}

func (s *RaftStorage) Snapshot() (*raftpb.Snapshot, error) {
	snapshot := &raftpb.Snapshot{
		Metadata: s.core.snapshotMeta,
		Data:     nil,
	}
	if snapshot.Metadata.GetIndex() == 0 {
		var idx uint64 = 1
		snapshot.Metadata.Index = &idx
	}
	if snapshot.Metadata.GetTerm() == 0 {
		var term uint64 = 1
		snapshot.Metadata.Term = &term
	}
	return snapshot, nil
}

// Public API methods
func (s *RaftStorage) AppendCommitted(entry *raftpb.Entry) error {
	return s.core.appendCommitted(entry)
}

func (s *RaftStorage) Flush() error {
	return s.core.flushBuffer()
}

func (s *RaftStorage) Compact() error {
	return s.core.compact()
}

func (s *RaftStorage) InstallSnapshot(meta *raftpb.SnapshotMetadata) error {
	return s.core.installSnapshot(meta)
}

func (s *RaftStorage) SetHardState(hs *raftpb.HardState) error {
	return s.core.setHardState(hs)
}

func (s *RaftStorage) Close() error {
	if err := s.core.flushBuffer(); err != nil {
		return err
	}
	if s.core.wal != nil {
		return s.core.wal.Close()
	}
	return nil
}
