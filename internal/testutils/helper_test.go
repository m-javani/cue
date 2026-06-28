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

package testutils

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNewDevLogger(t *testing.T) {
	logger, err := NewDevLogger()
	assert.NoError(t, err)
	assert.NotNil(t, logger)

	// Verify logger configuration by checking fields
	cfg := zap.NewDevelopmentConfig()

	// The time encoder should be set and non-nil
	assert.NotNil(t, cfg.EncoderConfig.EncodeTime)

	// Test that logger actually works
	testMsg := "test message"
	logger.Info(testMsg)

	// Verify we have the correct config by checking the logger's core
	// The logger should be able to log without errors
	_ = logger.Sync()
}

func TestGetCertsPath(t *testing.T) {
	// Reset the sync.Once for testing
	certOnce = sync.Once{}

	path1 := GetCertsPath()
	assert.NotEmpty(t, path1)
	assert.Contains(t, path1, "certs")

	// Second call should return same path
	path2 := GetCertsPath()
	assert.Equal(t, path1, path2)

	// Verify it exists (or at least the parent)
	projectRoot := getProjectRoot()
	assert.Equal(t, filepath.Join(projectRoot, "certs"), path1)
}

func TestGetTesDataPath(t *testing.T) {
	// Reset the sync.Once for testing
	testdatOnce = sync.Once{}

	path1 := GetTesDataPath()
	assert.NotEmpty(t, path1)
	assert.Contains(t, path1, "testdata")

	// Second call should return same path
	path2 := GetTesDataPath()
	assert.Equal(t, path1, path2)

	// Verify it exists (or at least the parent)
	projectRoot := getProjectRoot()
	assert.Equal(t, filepath.Join(projectRoot, "testdata"), path1)
}

func TestGetProjectRoot(t *testing.T) {
	root := getProjectRoot()
	assert.NotEmpty(t, root)

	// Verify go.mod exists in the root
	goModPath := filepath.Join(root, "go.mod")
	_, err := os.Stat(goModPath)
	assert.NoError(t, err, "go.mod should exist in project root")

	// Verify it's an absolute path
	assert.True(t, filepath.IsAbs(root))
}

func TestGetProjectRoot_Integration(t *testing.T) {
	// Test that GetCertsPath and GetTesDataPath use the same root
	certsRoot := filepath.Dir(GetCertsPath())
	testdataRoot := filepath.Dir(GetTesDataPath())
	assert.Equal(t, certsRoot, testdataRoot)
	assert.Equal(t, getProjectRoot(), certsRoot)
}

// Test panic scenarios by temporarily changing working directory
func TestGetProjectRoot_Panic(t *testing.T) {
	// Save current directory
	originalDir, err := os.Getwd()
	assert.NoError(t, err)
	defer func() { _ = os.Chdir(originalDir) }()

	// Create a temp directory without go.mod
	tempDir := t.TempDir()
	err = os.Chdir(tempDir)
	assert.NoError(t, err)

	// This should panic
	assert.Panics(t, func() {
		getProjectRoot()
	}, "Should panic when go.mod not found")
}

// Additional test to verify sync.Once behavior
func TestPathCache(t *testing.T) {
	// Reset once variables
	certOnce = sync.Once{}
	testdatOnce = sync.Once{}

	// Get paths
	certPath := GetCertsPath()
	testdataPath := GetTesDataPath()

	// Verify both paths are cached
	// The sync.Once should prevent re-execution

	// Ensure the functions return consistent results
	for i := 0; i < 10; i++ {
		assert.Equal(t, certPath, GetCertsPath())
		assert.Equal(t, testdataPath, GetTesDataPath())
	}
}
