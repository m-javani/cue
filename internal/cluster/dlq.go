// Copyright 2026 M. Javani
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"time"
)

const (
	initialCap   = 4096
	dlqCurrent   = "dlq-current.txt"
	dlqTemp      = "dlq-current.tmp"
	dlqSealedFmt = "dlq-%s.txt"
)

type DLQFileManager struct {
	dataDir      string
	maxSizeBytes int64

	mu       sync.Mutex
	wg       sync.WaitGroup
	rotating atomic.Bool
	closed   atomic.Bool
	dropped  atomic.Uint64

	active   int
	buffers  [2][]string
	flushing []string

	busy      bool
	writeFile *os.File
	fileSize  int64
}

func NewDLQFileManager(dataDir string, maxSizeBytes int64) (*DLQFileManager, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create DLQ directory: %w", err)
	}

	m := &DLQFileManager{
		dataDir:      dataDir,
		maxSizeBytes: maxSizeBytes,
		buffers: [2][]string{
			make([]string, 0, initialCap),
			make([]string, 0, initialCap),
		},
	}

	if err := m.openCurrentFile(); err != nil {
		return nil, fmt.Errorf("failed to open current DLQ file: %w", err)
	}

	return m, nil
}

func (m *DLQFileManager) openCurrentFile() error {
	tmpPath := filepath.Join(m.dataDir, dlqTemp)
	_ = os.Remove(tmpPath)

	path := filepath.Join(m.dataDir, dlqCurrent)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}

	m.writeFile = f
	m.fileSize = stat.Size()
	return nil
}

func (m *DLQFileManager) AppendBatch(timestamp int64, topic string, jobIDs []string) {
	if len(jobIDs) == 0 || m.closed.Load() {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	idx := m.active
	for _, jobID := range jobIDs {
		line := fmt.Sprintf("%d|%s|%s\n", timestamp, topic, jobID)
		m.buffers[idx] = append(m.buffers[idx], line)
	}

	if len(m.buffers[idx]) >= cap(m.buffers[idx]) && !m.busy {
		m.triggerFlushLocked()
	}
}

func (m *DLQFileManager) triggerFlushLocked() {
	idx := m.active
	if len(m.buffers[idx]) == 0 {
		return
	}

	newIdx := 1 - idx
	if len(m.buffers[newIdx]) > 0 {
		return // safety
	}

	m.flushing = m.buffers[idx]
	m.buffers[idx] = m.buffers[idx][:0]

	if cap(m.buffers[newIdx]) < len(m.flushing)*2 {
		m.buffers[newIdx] = grow(m.buffers[newIdx])
	}

	m.active = newIdx
	m.busy = true
	m.wg.Add(1)
	go m.write()
}

func grow(s []string) []string {
	newCap := cap(s) * 2
	if newCap == 0 {
		newCap = initialCap
	}
	return make([]string, 0, newCap)
}

func (m *DLQFileManager) write() {
	defer m.wg.Done()

	for {
		m.mu.Lock()
		batch := m.flushing
		if len(batch) == 0 {
			m.busy = false
			m.mu.Unlock()
			return
		}
		m.mu.Unlock()

		if err := m.persistBatch(batch); err != nil {
			if m.closed.Load() {
				m.mu.Lock()
				m.busy = false
				m.mu.Unlock()
				return
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Success path
		m.mu.Lock()
		m.flushing = nil
		m.busy = false // ← This was missing in some paths
		if cap(batch) > initialCap*4 {
			batch = make([]string, 0, initialCap)
		}
		m.mu.Unlock()

		// Check for chained flush
		m.mu.Lock()
		if !m.closed.Load() && len(m.buffers[m.active]) >= cap(m.buffers[m.active]) {
			m.triggerFlushLocked()
		}
		m.mu.Unlock()

		return
	}
}

func (m *DLQFileManager) persistBatch(batch []string) error {
	m.mu.Lock()
	f := m.writeFile
	m.mu.Unlock()
	if f == nil {
		return fmt.Errorf("no active write file")
	}

	var written int64
	for _, rec := range batch {
		n, err := f.WriteString(rec)
		if err != nil {
			return err
		}
		written += int64(n)
	}

	if err := f.Sync(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.fileSize += written

	if m.fileSize > m.maxSizeBytes {
		if m.rotating.CompareAndSwap(false, true) {
			go m.doRotate()
		}
	}
	return nil
}

func (m *DLQFileManager) doRotate() {
	defer m.rotating.Store(false)

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.fileSize <= m.maxSizeBytes || m.writeFile == nil {
		return
	}

	currentPath := filepath.Join(m.dataDir, dlqCurrent)
	tmpPath := filepath.Join(m.dataDir, dlqTemp)
	sealedPath := filepath.Join(m.dataDir, fmt.Sprintf(dlqSealedFmt, time.Now().Format("2006-01-02T15-04-05")))

	// Sync & close
	_ = m.writeFile.Sync()
	_ = m.writeFile.Close()
	m.writeFile = nil

	// Atomic rename
	if err := os.Rename(currentPath, tmpPath); err != nil {
		_ = m.openCurrentFile()
		return
	}
	if err := os.Rename(tmpPath, sealedPath); err != nil {
		_ = os.Rename(tmpPath, currentPath)
		_ = m.openCurrentFile()
		return
	}

	_ = syncDir(m.dataDir)

	// Open fresh current file
	if err := m.openCurrentFile(); err != nil {
		// Best effort recovery
		_ = m.openCurrentFile()
	}
	m.fileSize = 0
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (m *DLQFileManager) Close() error {
	m.closed.Store(true)

	// Wait for any ongoing rotation
	for m.rotating.Load() {
		time.Sleep(10 * time.Millisecond)
	}

	m.mu.Lock()
	if !m.busy && len(m.flushing) == 0 {
		for i := 0; i < 2; i++ {
			idx := (m.active + i) % 2
			if len(m.buffers[idx]) > 0 {
				m.flushing = m.buffers[idx]
				m.buffers[idx] = m.buffers[idx][:0]
				m.busy = true
				m.wg.Add(1)
				go m.write()
				break
			}
		}
	}
	m.mu.Unlock()

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return m.finalSync()
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for DLQ to flush during Close")
	}
}

func (m *DLQFileManager) finalSync() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.writeFile != nil {
		_ = m.writeFile.Sync()
		_ = m.writeFile.Close()
		m.writeFile = nil
	}
	return nil
}

func (m *DLQFileManager) DroppedTotal() uint64 {
	return m.dropped.Load()
}
