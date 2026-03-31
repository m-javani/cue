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
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/m-javani/cue/internal/utils"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

const (
	SegmentSize = 100 * 1024 * 1024

	sealedSegmentFmt = "%016d-%016d.wal"
	activeSegmentFmt = "%016d.active"
)

type Segment struct {
	path       string
	startIndex uint64
	endIndex   uint64
	readFile   *os.File // used for sealed segments
	writeFile  *os.File // used only for active segment
	size       int64
	active     bool
}

type SegmentedWAL struct {
	mu          sync.RWMutex
	dir         string
	segments    []*Segment // sealed segments only
	active      *Segment
	nextIndex   uint64
	logger      *zap.Logger
	segmentSize int64
}

// NewSegmentedWal creates or opens a segmented WAL
func NewSegmentedWal(dir string, logger *zap.Logger, segmentSize ...int64) (*SegmentedWAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	sw := &SegmentedWAL{
		dir:    dir,
		logger: logger,
	}

	// Set segment size - use provided value or default
	if len(segmentSize) > 0 && segmentSize[0] > 0 {
		sw.segmentSize = segmentSize[0]
	} else {
		sw.segmentSize = SegmentSize // 100MB default
	}

	// Load existing segments
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read wal dir: %w", err)
	}

	// Clean up abandoned temp files
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			path := filepath.Join(dir, e.Name())
			logger.Warn(
				"removing abandoned WAL temp file",
				zap.String("file", e.Name()),
			)
			if err := os.Remove(path); err != nil {
				return nil, err
			}
		}
	}

	// Parse all segments
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(e.Name(), ".wal") &&
			!strings.HasSuffix(e.Name(), ".active") {
			continue
		}

		seg, err := parseSegment(filepath.Join(dir, e.Name()))
		if err != nil {
			logger.Warn(
				"skipping invalid segment",
				zap.String("file", e.Name()),
				zap.Error(err),
			)
			continue
		}
		sw.segments = append(sw.segments, seg)
	}

	sort.Slice(sw.segments, func(i, j int) bool {
		return sw.segments[i].startIndex < sw.segments[j].startIndex
	})

	if err := sw.validateContinuity(); err != nil {
		return nil, fmt.Errorf("wal continuity validation failed: %w", err)
	}

	// Separate sealed and active segments
	var sealed []*Segment
	for _, seg := range sw.segments {
		if seg.active {
			if sw.active != nil {
				return nil, fmt.Errorf("multiple active segments found")
			}
			sw.active = seg
			continue
		}
		sealed = append(sealed, seg)
	}
	sw.segments = sealed

	// Handle active segment
	if sw.active != nil {
		// Scan to find the real last index
		lastIdx, err := sw.scanLastIndex(sw.active)
		if err != nil {
			return nil, fmt.Errorf("scan active segment: %w", err)
		}
		sw.nextIndex = lastIdx + 1

		// === IMPORTANT FIX: Re-open active segment for writing ===
		writeF, err := os.OpenFile(sw.active.path,
			os.O_RDWR|os.O_APPEND, 0640)
		if err != nil {
			return nil, fmt.Errorf("reopen active segment for writing: %w", err)
		}
		sw.active.writeFile = writeF

		// size was already set from os.Stat in parseSegment()
		// We keep it as-is (good enough for now)

	} else if len(sw.segments) > 0 {
		last := sw.segments[len(sw.segments)-1]
		sw.nextIndex = last.endIndex + 1
		if err := sw.createActiveSegment(); err != nil {
			return nil, err
		}
	} else {
		sw.nextIndex = 1
		if err := sw.createActiveSegment(); err != nil {
			return nil, err
		}
	}

	sw.logger.Info("SegmentedWAL opened",
		zap.Int("sealed_segments", len(sw.segments)),
		zap.Uint64("next_index", sw.nextIndex))

	return sw, nil
}

func (sw *SegmentedWAL) validateContinuity() error {
	for i := 1; i < len(sw.segments); i++ {
		prev := sw.segments[i-1]
		curr := sw.segments[i]

		if prev.endIndex+1 != curr.startIndex {
			return fmt.Errorf(
				"gap between %d-%d and %d-%d",
				prev.startIndex,
				prev.endIndex,
				curr.startIndex,
				curr.endIndex,
			)
		}
	}

	return nil
}

func parseSegment(path string) (*Segment, error) {
	name := filepath.Base(path)

	if strings.HasSuffix(name, ".active") {
		startStr := strings.TrimSuffix(name, ".active")

		start, err := strconv.ParseUint(startStr, 10, 64)
		if err != nil {
			return nil, err
		}

		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}

		return &Segment{
			path:       path,
			startIndex: start,
			endIndex:   0, // temporary startup marker only
			size:       info.Size(),
			active:     true,
		}, nil
	}

	if strings.HasSuffix(name, ".wal") {
		base := strings.TrimSuffix(name, ".wal")

		parts := strings.Split(base, "-")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid segment filename")
		}

		start, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return nil, err
		}

		end, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return nil, err
		}

		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}

		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}

		return &Segment{
			path:       path,
			startIndex: start,
			endIndex:   end,
			readFile:   f,
			size:       info.Size(),
			active:     false,
		}, nil
	}

	return nil, fmt.Errorf("unknown segment type")
}

func (sw *SegmentedWAL) createActiveSegment() error {
	path := filepath.Join(
		sw.dir,
		fmt.Sprintf(activeSegmentFmt, sw.nextIndex),
	)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0640)
	if err != nil {
		return fmt.Errorf("create active segment: %w", err)
	}

	sw.active = &Segment{
		path:       path,
		startIndex: sw.nextIndex,
		endIndex:   0,
		writeFile:  f,
		size:       0,
		active:     true,
	}
	return nil
}

// scanLastIndex uses a fresh read handle
func (sw *SegmentedWAL) scanLastIndex(seg *Segment) (uint64, error) {
	f, err := os.Open(seg.path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	r := &WALReader{file: f}
	var last uint64
	for {
		idx, size, err := r.ReadHeader()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		last = idx
		if _, err := f.Seek(int64(size)-16, io.SeekCurrent); err != nil {
			return 0, err
		}
	}
	return last, nil
}

// ============================================
// WRITE
// ============================================
func (sw *SegmentedWAL) AppendBatch(entries []BufferedEntry) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	for _, be := range entries {
		// Rotate before writing if current segment is full
		if sw.active.size >= sw.segmentSize {
			if err := sw.rotate(); err != nil {
				return fmt.Errorf("rotate: %w", err)
			}
		}

		n, err := sw.active.writeFile.Write(be.Data)
		if err != nil {
			return fmt.Errorf("write entry %d: %w", be.Index, err)
		}
		sw.active.size += int64(n)

		// Only advance nextIndex if this entry is beyond current known nextIndex
		// This prevents double-increment when rotation just happened
		if be.Index+1 > sw.nextIndex {
			sw.nextIndex = be.Index + 1
		}
	}

	return sw.active.writeFile.Sync()
}

func (sw *SegmentedWAL) rotate() error {
	lastIdx := sw.nextIndex - 1
	oldPath := sw.active.path
	oldStart := sw.active.startIndex
	oldSize := sw.active.size
	oldFile := sw.active.writeFile

	//
	// Step 1: Create temp active segment for the *next* batch
	//
	tmpPath := filepath.Join(
		sw.dir,
		fmt.Sprintf("%016d.tmp", sw.nextIndex),
	)
	tmpFile, err := os.OpenFile(
		tmpPath,
		os.O_CREATE|os.O_EXCL|os.O_RDWR,
		0640,
	)
	if err != nil {
		return fmt.Errorf("create temp segment: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return fmt.Errorf("sync temp segment: %w", err)
	}

	//
	// Step 2: Seal old active segment
	//
	if err := oldFile.Sync(); err != nil {
		tmpFile.Close()
		return fmt.Errorf("sync old segment: %w", err)
	}
	if err := oldFile.Close(); err != nil {
		tmpFile.Close()
		return fmt.Errorf("close old segment: %w", err)
	}

	sealedPath := filepath.Join(
		sw.dir,
		fmt.Sprintf(sealedSegmentFmt, oldStart, lastIdx),
	)
	if err := os.Rename(oldPath, sealedPath); err != nil {
		tmpFile.Close()
		return fmt.Errorf("rename sealed segment: %w", err)
	}
	if err := utils.SyncDir(sw.dir); err != nil {
		tmpFile.Close()
		return fmt.Errorf("sync wal dir after sealing: %w", err)
	}

	//
	// Step 3: Reopen sealed segment for reading
	//
	readF, err := os.Open(sealedPath)
	if err != nil {
		tmpFile.Close()
		return fmt.Errorf("open sealed segment: %w", err)
	}
	sw.segments = append(sw.segments, &Segment{
		path:       sealedPath,
		startIndex: oldStart,
		endIndex:   lastIdx,
		readFile:   readF,
		size:       oldSize,
		active:     false,
	})

	//
	// Step 4: Promote temp → new active
	//
	activePath := filepath.Join(
		sw.dir,
		fmt.Sprintf(activeSegmentFmt, sw.nextIndex),
	)
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, activePath); err != nil {
		return fmt.Errorf("promote active segment: %w", err)
	}
	if err := utils.SyncDir(sw.dir); err != nil {
		return fmt.Errorf("sync wal dir after promote: %w", err)
	}

	//
	// Step 5: Open new active segment for writing
	//
	activeFile, err := os.OpenFile(
		activePath,
		os.O_RDWR|os.O_APPEND,
		0640,
	)
	if err != nil {
		return fmt.Errorf("open promoted active segment: %w", err)
	}

	sw.active = &Segment{
		path:       activePath,
		startIndex: sw.nextIndex, // Important: use the current nextIndex
		endIndex:   0,
		writeFile:  activeFile,
		size:       0,
		active:     true,
	}

	sw.logger.Info(
		"WAL segment rotated",
		zap.String("sealed", sealedPath),
		zap.String("active", activePath),
		zap.Uint64("last_index", lastIdx),
	)
	return nil
}

// ============================================
// RECOVERY (callback-based)
// ============================================

func (sw *SegmentedWAL) Recover(snapshotIndex uint64, fn func(raftpb.Entry) error) error {
	sw.mu.RLock()
	defer sw.mu.RUnlock()

	allSegments := append([]*Segment{}, sw.segments...)
	allSegments = append(allSegments, sw.active)

	for _, seg := range allSegments {
		f, err := os.Open(seg.path)
		if err != nil {
			return fmt.Errorf("open %s: %w", seg.path, err)
		}
		reader := &WALReader{file: f}
		for {
			entry, err := reader.ReadEntry()
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return fmt.Errorf("corrupt entry in %s: %w", seg.path, err)
			}
			if entry.Index > snapshotIndex {
				if err := fn(entry); err != nil {
					f.Close()
					return err
				}
			}
		}
		f.Close()
	}
	return nil
}

// ============================================
// READER (hardened with unified list)
// ============================================

type Reader struct {
	segments []*Segment
	segIdx   int

	file   *os.File
	reader *WALReader
}

func (sw *SegmentedWAL) NewReader(fromIndex uint64) (*Reader, error) {
	sw.mu.RLock()
	defer sw.mu.RUnlock()

	all := append([]*Segment{}, sw.segments...)
	all = append(all, sw.active)

	r := &Reader{segments: all}

	found := false

	for i, seg := range all {
		if seg.active {
			if fromIndex >= seg.startIndex {
				r.segIdx = i
				found = true
				break
			}
			continue
		}

		if fromIndex >= seg.startIndex &&
			fromIndex <= seg.endIndex {
			r.segIdx = i
			found = true
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("index %d not found in WAL", fromIndex)
	}

	f, err := os.Open(r.segments[r.segIdx].path)
	if err != nil {
		return nil, err
	}

	r.file = f
	r.reader = &WALReader{file: f}

	// Skip to target index
	for {
		entryStart, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			f.Close()
			return nil, err
		}

		idx, totalSize, err := r.reader.ReadHeader()
		if err == io.EOF {
			f.Close()
			return nil, fmt.Errorf("index %d not found in WAL", fromIndex)
		}
		if err != nil {
			f.Close()
			return nil, err
		}

		if idx >= fromIndex {
			if _, err := f.Seek(entryStart, io.SeekStart); err != nil {
				f.Close()
				return nil, err
			}
			r.reader = &WALReader{file: f}
			break
		}

		if _, err := f.Seek(int64(totalSize)-16, io.SeekCurrent); err != nil {
			f.Close()
			return nil, err
		}
	}

	return r, nil
}

func (r *Reader) ReadEntry() (raftpb.Entry, error) {
	for {
		entry, err := r.reader.ReadEntry()
		if err == nil {
			return entry, nil
		}

		r.segIdx++
		if r.segIdx >= len(r.segments) {
			if r.file != nil {
				_ = r.file.Close()
				r.file = nil
			}
			return raftpb.Entry{}, io.EOF
		}

		// Close previous segment before opening next one
		if r.file != nil {
			if err := r.file.Close(); err != nil {
				return raftpb.Entry{}, err
			}
		}

		f, err := os.Open(r.segments[r.segIdx].path)
		if err != nil {
			return raftpb.Entry{}, err
		}

		r.file = f
		r.reader = &WALReader{file: f}
	}
}

func (r *Reader) Close() error {
	if r.file != nil {
		err := r.file.Close()
		r.file = nil
		r.reader = nil
		return err
	}
	return nil
}

// ============================================
// TRUNCATE & CLOSE
// ============================================

func (sw *SegmentedWAL) Truncate(upToIndex uint64) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	var keep []*Segment
	for _, seg := range sw.segments {
		if seg.endIndex <= upToIndex {
			if err := seg.readFile.Close(); err != nil {
				sw.logger.Warn(
					"failed to close segment",
					zap.String("path", seg.path),
					zap.Error(err),
				)
			}

			if err := os.Remove(seg.path); err != nil {
				sw.logger.Warn(
					"failed to remove segment",
					zap.String("path", seg.path),
					zap.Error(err),
				)
			}
		} else {
			keep = append(keep, seg)
		}
	}
	sw.segments = keep

	if err := utils.SyncDir(sw.dir); err != nil {
		return err
	}

	// Handle active segment
	if sw.active != nil &&
		sw.active.startIndex <= upToIndex {

		last, err := sw.scanLastIndex(sw.active)
		if err != nil {
			return err
		}

		if last <= upToIndex {
			_ = sw.active.writeFile.Close()
			_ = os.Remove(sw.active.path)

			sw.nextIndex = upToIndex + 1

			return sw.createActiveSegment()
		}
	}

	if err := utils.SyncDir(sw.dir); err != nil {
		return err
	}

	return nil
}

func (sw *SegmentedWAL) Close() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	for _, s := range sw.segments {
		if s.readFile != nil {
			s.readFile.Close()
		}
	}
	if sw.active != nil && sw.active.writeFile != nil {
		sw.active.writeFile.Close()
	}
	return nil
}

// WALReader (unchanged)
type WALReader struct {
	file *os.File
}

func (r *WALReader) ReadHeader() (uint64, uint64, error) {
	var header [16]byte
	if _, err := io.ReadFull(r.file, header[:]); err != nil {
		return 0, 0, err
	}
	total := binary.LittleEndian.Uint64(header[0:8])
	index := binary.LittleEndian.Uint64(header[8:16])
	return index, total, nil
}

func (r *WALReader) ReadEntry() (raftpb.Entry, error) {
	idx, totalSize, err := r.ReadHeader()
	if err != nil {
		if err == io.EOF {
			return raftpb.Entry{}, io.EOF
		}
		return raftpb.Entry{}, err
	}

	dataSize := int64(totalSize) - 16
	data := make([]byte, dataSize)
	if _, err := io.ReadFull(r.file, data); err != nil {
		return raftpb.Entry{}, err
	}

	var entry raftpb.Entry
	if err := entry.Unmarshal(data); err != nil {
		return raftpb.Entry{}, err
	}
	if entry.Index != idx {
		return raftpb.Entry{}, fmt.Errorf("index mismatch")
	}
	return entry, nil
}
