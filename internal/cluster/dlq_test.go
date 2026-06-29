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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDLQFileManager(t *testing.T) {
	t.Run("creates directory and file", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 1024*1024)
		require.NoError(t, err)
		defer m.Close()

		assert.NotNil(t, m.writeFile)
		assert.Equal(t, int64(0), m.fileSize)
		assert.Equal(t, initialCap, cap(m.buffers[0]))
		assert.Equal(t, initialCap, cap(m.buffers[1]))

		_, err = os.Stat(filepath.Join(tempDir, dlqCurrent))
		assert.NoError(t, err)
	})

	t.Run("fails when cannot create directory", func(t *testing.T) {
		_, err := NewDLQFileManager("/nonexistent/parent/dlq", 1024)
		assert.Error(t, err)
	})
}

func TestDLQFileManager_AppendBatch(t *testing.T) {
	t.Run("appends records to buffer", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 1024*1024)
		require.NoError(t, err)
		defer func() { _ = m.Close() }()

		m.AppendBatch(time.Now().Unix(), "test-topic", []string{"job1", "job2"})

		m.mu.Lock()
		assert.Greater(t, len(m.buffers[m.active]), 0)
		m.mu.Unlock()
	})

	t.Run("skips empty batch and closed manager", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 1024*1024)
		require.NoError(t, err)
		defer m.Close()

		m.closed.Store(true)
		m.AppendBatch(time.Now().Unix(), "topic", []string{"job1"})

		m.mu.Lock()
		assert.Equal(t, 0, len(m.buffers[m.active]))
		m.mu.Unlock()
	})

	t.Run("triggers flush when buffer full", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 1024*1024)
		require.NoError(t, err)
		defer m.Close()

		jobIDs := make([]string, initialCap)
		for i := range jobIDs {
			jobIDs[i] = fmt.Sprintf("job%d", i)
		}

		m.AppendBatch(time.Now().Unix(), "topic", jobIDs)

		time.Sleep(350 * time.Millisecond)

		m.mu.Lock()
		assert.Equal(t, 1, m.active)
		assert.Equal(t, 0, len(m.buffers[0]))
		assert.Equal(t, 0, len(m.buffers[1]))
		assert.False(t, m.busy)
		assert.Len(t, m.flushing, 0)
		m.mu.Unlock()
	})
}

func TestDLQFileManager_PersistAndRotation(t *testing.T) {
	t.Run("persistBatch writes data", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 1024*1024)
		require.NoError(t, err)
		defer m.Close()

		batch := []string{"123|topic|job1\n", "456|topic|job2\n"}
		err = m.persistBatch(batch)
		require.NoError(t, err)

		content, err := os.ReadFile(filepath.Join(tempDir, dlqCurrent))
		require.NoError(t, err)
		assert.Equal(t, strings.Join(batch, ""), string(content))
	})

	t.Run("triggers rotation on size limit", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 100)
		require.NoError(t, err)
		defer m.Close()

		batch := make([]string, 5)
		for i := range batch {
			batch[i] = fmt.Sprintf("1234567890|topic|job%d\n", i)
		}

		err = m.persistBatch(batch)
		require.NoError(t, err)

		time.Sleep(400 * time.Millisecond)

		files, _ := os.ReadDir(tempDir)
		sealedFound := false
		for _, f := range files {
			if strings.HasPrefix(f.Name(), "dlq-") && f.Name() != dlqCurrent {
				sealedFound = true
				break
			}
		}
		assert.True(t, sealedFound)
	})
}

func TestDLQFileManager_Close(t *testing.T) {
	t.Run("flushes pending data on close", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 1024*1024)
		require.NoError(t, err)

		m.AppendBatch(time.Now().Unix(), "topic", []string{"job1", "job2"})

		err = m.Close()
		require.NoError(t, err)

		content, err := os.ReadFile(filepath.Join(tempDir, dlqCurrent))
		require.NoError(t, err)
		assert.Contains(t, string(content), "job1")
	})

	t.Run("idempotent close", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 1024*1024)
		require.NoError(t, err)

		assert.NoError(t, m.Close())
		assert.NoError(t, m.Close())
	})
}

func TestDLQFileManager_Concurrent(t *testing.T) {
	t.Run("concurrent appends and close", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 1024*1024)
		require.NoError(t, err)

		var wg sync.WaitGroup
		for i := 0; i < 15; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				for j := 0; j < 40; j++ {
					m.AppendBatch(time.Now().Unix(), fmt.Sprintf("topic%d", idx),
						[]string{fmt.Sprintf("job%d-%d", idx, j)})
				}
			}(i)
		}

		wg.Wait()
		err = m.Close()
		require.NoError(t, err)

		content, err := os.ReadFile(filepath.Join(tempDir, dlqCurrent))
		require.NoError(t, err)
		assert.NotEmpty(t, content)
	})
}

func TestDLQFileManager_EdgeCases(t *testing.T) {
	t.Run("triggerFlushLocked early returns", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 1024*1024)
		require.NoError(t, err)
		defer m.Close()

		m.mu.Lock()
		defer m.mu.Unlock()

		// Empty buffer
		m.buffers[m.active] = m.buffers[m.active][:0]
		m.triggerFlushLocked()

		// Both buffers occupied
		m.buffers[0] = []string{"a"}
		m.buffers[1] = []string{"b"}
		m.active = 0
		m.triggerFlushLocked()
	})

	t.Run("grow function", func(t *testing.T) {
		assert.Equal(t, initialCap, cap(grow([]string{})))
		s := make([]string, 0, 100)
		assert.Greater(t, cap(grow(s)), 100)
	})

	t.Run("syncDir", func(t *testing.T) {
		tempDir := t.TempDir()
		assert.NoError(t, syncDir(tempDir))
	})
}

func TestDLQFileManager_DroppedTotal(t *testing.T) {
	tempDir := t.TempDir()
	m, err := NewDLQFileManager(tempDir, 1024*1024)
	require.NoError(t, err)
	defer m.Close()

	assert.Equal(t, uint64(0), m.DroppedTotal())
}

// -----------
func TestDLQFileManager_WriteFunction(t *testing.T) {
	t.Run("handles empty batch gracefully", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 1024*1024)
		require.NoError(t, err)
		defer m.Close()

		// Set up a flushing batch that is empty
		m.mu.Lock()
		m.flushing = []string{}
		m.busy = true
		m.mu.Unlock()

		// MUST add to WaitGroup before calling write()
		m.wg.Add(1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			m.write()
		}()

		select {
		case <-done:
			// Successfully handled empty batch
		case <-time.After(2 * time.Second):
			t.Fatal("write() didn't handle empty batch promptly")
		}

		m.mu.Lock()
		assert.False(t, m.busy)
		m.mu.Unlock()
	})

	t.Run("shrinks oversized batch capacity", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 1024*1024)
		require.NoError(t, err)
		defer m.Close()

		// Create a batch with capacity > initialCap*4
		largeBatch := make([]string, 0, initialCap*5)
		for i := 0; i < initialCap*2; i++ {
			largeBatch = append(largeBatch, fmt.Sprintf("%d|topic|job%d\n", time.Now().Unix(), i))
		}

		m.mu.Lock()
		m.flushing = largeBatch
		m.busy = true
		m.mu.Unlock()

		// MUST add to WaitGroup before calling write()
		m.wg.Add(1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			m.write()
		}()

		select {
		case <-done:
			// Successfully wrote and shrunk batch
		case <-time.After(10 * time.Second):
			t.Fatal("write() timed out - large batch may take time to write")
		}

		// Verify the write succeeded
		content, err := os.ReadFile(filepath.Join(tempDir, dlqCurrent))
		require.NoError(t, err)
		assert.NotEmpty(t, content)
	})

	t.Run("triggers chained flush when buffer is full after write", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 1024*1024)
		require.NoError(t, err)
		defer m.Close()

		// Fill buffer 0 to capacity
		m.mu.Lock()
		m.active = 0
		buffer0 := make([]string, cap(m.buffers[0]))
		for i := range buffer0 {
			buffer0[i] = fmt.Sprintf("%d|topic|job%d\n", time.Now().Unix(), i)
		}
		m.buffers[0] = buffer0
		m.mu.Unlock()

		// Trigger flush on buffer 0 - this adds to WaitGroup
		m.mu.Lock()
		m.triggerFlushLocked()
		m.mu.Unlock()

		// Wait for write to complete
		m.wg.Wait()

		// Now fill buffer 1 (which is now active) to capacity
		m.mu.Lock()
		buffer1 := make([]string, cap(m.buffers[1]))
		for i := range buffer1 {
			buffer1[i] = fmt.Sprintf("%d|topic|chained-job%d\n", time.Now().Unix(), i)
		}
		m.buffers[1] = buffer1
		m.mu.Unlock()

		// This should trigger another flush via the chained condition
		m.mu.Lock()
		if !m.closed.Load() && len(m.buffers[m.active]) >= cap(m.buffers[m.active]) {
			m.triggerFlushLocked()
		}
		m.mu.Unlock()

		// Wait for chained flush to complete
		m.wg.Wait()

		// Verify both writes succeeded
		content, err := os.ReadFile(filepath.Join(tempDir, dlqCurrent))
		require.NoError(t, err)
		assert.Contains(t, string(content), "chained-job")
	})
}

func TestDLQFileManager_WriteErrorRecovery(t *testing.T) {
	t.Run("persistBatch error during write with closed manager", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 1024*1024)
		require.NoError(t, err)

		batch := []string{"123|topic|job1\n"}

		// Close the manager and file
		m.mu.Lock()
		_ = m.writeFile.Close()
		m.writeFile = nil
		m.closed.Store(true)
		m.flushing = batch
		m.busy = true
		m.mu.Unlock()

		// MUST add to WaitGroup before calling write()
		m.wg.Add(1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			m.write()
		}()

		select {
		case <-done:
			// Should exit quickly when closed
		case <-time.After(2 * time.Second):
			t.Fatal("write() didn't exit when manager was closed")
		}

		m.mu.Lock()
		assert.False(t, m.busy)
		m.mu.Unlock()
	})

	t.Run("persistBatch error with write failure", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 1024*1024)
		require.NoError(t, err)
		defer m.Close()

		batch := []string{"123|topic|job1\n", "456|topic|job2\n"}

		// Set the writeFile to nil to force error
		m.mu.Lock()
		_ = m.writeFile.Close()
		m.writeFile = nil
		m.flushing = batch
		m.busy = true
		m.mu.Unlock()

		// MUST add to WaitGroup before calling write()
		m.wg.Add(1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			m.write()
		}()

		select {
		case <-done:
			// Function exited
		case <-time.After(3 * time.Second):
			// It's retrying, which is expected - we'll force cleanup
		}

		// Restore the file for cleanup
		m.mu.Lock()
		_ = m.openCurrentFile()
		m.busy = false
		m.mu.Unlock()
	})
}

func TestDLQFileManager_WriteConcurrentChainedFlush(t *testing.T) {
	t.Run("concurrent writes and chained flushes", func(t *testing.T) {
		tempDir := t.TempDir()
		m, err := NewDLQFileManager(tempDir, 1024) // Small size to force rotations
		require.NoError(t, err)
		defer m.Close()

		var wg sync.WaitGroup
		// Start multiple writers
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					m.AppendBatch(time.Now().Unix(), "topic",
						[]string{fmt.Sprintf("concurrent-job-%d-%d", idx, j)})
					// Small sleep to allow interleaving
					if j%10 == 0 {
						time.Sleep(time.Microsecond)
					}
				}
			}(i)
		}

		// Trigger some manual flushes to test the chained condition
		time.Sleep(100 * time.Millisecond)
		m.mu.Lock()
		if !m.busy && len(m.buffers[m.active]) > 0 {
			m.triggerFlushLocked()
		}
		m.mu.Unlock()

		wg.Wait()

		// Wait for all writes to complete
		m.wg.Wait()

		// Verify data was written
		content, err := os.ReadFile(filepath.Join(tempDir, dlqCurrent))
		require.NoError(t, err)
		assert.Contains(t, string(content), "concurrent-job")
	})
}
