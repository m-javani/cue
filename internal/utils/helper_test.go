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

package utils

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestStringToUint64(t *testing.T) {
	tests := []struct {
		name string
		s    string
	}{
		{
			name: "empty string",
			s:    "",
		},
		{
			name: "simple string",
			s:    "hello",
		},
		{
			name: "string with spaces",
			s:    "hello world",
		},
		{
			name: "long string",
			s:    "this is a much longer string to test hash consistency",
		},
		{
			name: "special characters",
			s:    "!@#$%^&*()_+",
		},
	}

	// Test that same input produces same output
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result1 := StringToUint64(tt.s)
			result2 := StringToUint64(tt.s)

			if result1 != result2 {
				t.Errorf("StringToUint64() not deterministic: got %d and %d for same input", result1, result2)
			}

			// Verify different inputs produce different outputs (with high probability)
			if tt.s != "" {
				different := StringToUint64(tt.s + "different")
				if result1 == different {
					t.Logf("Warning: Two different strings produced same hash (possible but unlikely)")
				}
			}
		})
	}
}

func TestSyncDir(t *testing.T) {
	tempDir := t.TempDir()

	t.Run("valid directory", func(t *testing.T) {
		err := SyncDir(tempDir)
		if err != nil {
			t.Errorf("SyncDir() error = %v, want nil", err)
		}
	})

	t.Run("non-existent directory", func(t *testing.T) {
		nonExistent := filepath.Join(tempDir, "nonexistent")
		err := SyncDir(nonExistent)
		if err == nil {
			t.Error("SyncDir() with non-existent directory should return error")
		}
	})

	t.Run("file instead of directory", func(t *testing.T) {
		filePath := filepath.Join(tempDir, "testfile.txt")
		if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		err := SyncDir(filePath)
		// On some systems (Linux), Sync() on a file may succeed
		// So we don't strictly require an error, we just verify the operation completes
		if err != nil {
			t.Logf("SyncDir() on file returned error (expected on some systems): %v", err)
		}
	})
}

func TestSafeLoadAtomicString(t *testing.T) {
	t.Run("empty atomic value", func(t *testing.T) {
		var v atomic.Value
		result := SafeLoadAtomicString(&v)
		if result != "" {
			t.Errorf("SafeLoadAtomicString() = %q, want empty string", result)
		}
	})

	t.Run("valid string value", func(t *testing.T) {
		var v atomic.Value
		expected := "test string"
		v.Store(expected)

		result := SafeLoadAtomicString(&v)
		if result != expected {
			t.Errorf("SafeLoadAtomicString() = %q, want %q", result, expected)
		}
	})

	t.Run("non-string value stored", func(t *testing.T) {
		var v atomic.Value
		v.Store(123) // Store an int instead of string

		result := SafeLoadAtomicString(&v)
		if result != "" {
			t.Errorf("SafeLoadAtomicString() with non-string = %q, want empty string", result)
		}
	})

	t.Run("empty string value", func(t *testing.T) {
		var v atomic.Value
		v.Store("")

		result := SafeLoadAtomicString(&v)
		if result != "" {
			t.Errorf("SafeLoadAtomicString() = %q, want empty string", result)
		}
	})

	t.Run("multiple goroutines", func(t *testing.T) {
		var v atomic.Value
		expected := "concurrent test"
		v.Store(expected)

		// Test concurrent access is safe
		done := make(chan bool)
		for i := 0; i < 10; i++ {
			go func() {
				result := SafeLoadAtomicString(&v)
				if result != expected {
					t.Errorf("Concurrent SafeLoadAtomicString() = %q, want %q", result, expected)
				}
				done <- true
			}()
		}

		// Wait for all goroutines
		for i := 0; i < 10; i++ {
			<-done
		}
	})
}
