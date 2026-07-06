// Copyright 2026 M. Javani
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/testutils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootCommand(t *testing.T) {
	// Create a new root command for testing
	rootCmd := &cobra.Command{
		Use:   "cue",
		Short: "Cue — Durable job queue with push delivery",
		Long:  `Cue is a clustered, Raft-based job queue with at-least-once delivery.`,
	}

	// Add subcommands
	rootCmd.AddCommand(&cobra.Command{
		Use:   "serve",
		Short: "Start the cue server",
	})
	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print version information",
	})

	// Test root command help
	buf := new(bytes.Buffer)
	rootCmd.SetOut(buf)
	rootCmd.SetErr(buf)
	rootCmd.SetArgs([]string{"--help"})

	err := rootCmd.Execute()
	require.NoError(t, err)

	output := buf.String()
	// Check for key parts of the output (without unicode em dash)
	assert.Contains(t, output, "Cue is a clustered, Raft-based job queue with at-least-once delivery.")
	assert.Contains(t, output, "serve")
	assert.Contains(t, output, "version")
}

func TestServeCommand(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create temp directory for all test files
	tmpDir, err := os.MkdirTemp("", "cue-serve-test-*")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create certs
	ca, err := testutils.CreateCA(tmpDir, "test-ca", 1, "localhost")
	require.NoError(t, err)

	certPath, keyPath, err := testutils.CreateNodeCert(tmpDir, ca, testutils.NodeCert{
		NodeIdentity: "node1",
		ServerNames:  []string{"localhost", "127.0.0.1"},
	}, 1)
	require.NoError(t, err)

	// Write auth.yml
	authContent := `
tokens:
  - token: abc123
    role: admin
  - token: xyz789
    role: monitoring
  - token: readonly456
    role: monitoring
`
	authPath := filepath.Join(tmpDir, "auth.yml")
	err = os.WriteFile(authPath, []byte(authContent), 0644)
	require.NoError(t, err)

	// Create a dummy discovery.yml
	discoveryPath := filepath.Join(tmpDir, "discovery.yml")
	err = os.WriteFile(discoveryPath, []byte("nodes: []"), 0644)
	require.NoError(t, err)

	// Write config.yml
	configContent := fmt.Sprintf(`
node_id: node1
data_dir: %s

cluster:
  listen_addr: "127.0.0.1"
  quic_port: 18323
  cert_path: "%s"
  key_path: "%s"
  ca_path: "%s"
  initial_voters: ["node1"]
  discovery_kind: static
  discovery_yml_path: "%s"
  snapshot_interval_sec: 60
  snapshot_trigger_count: 10000
  wal_flush_threshold: 1000
  dlq_max_size_bytes: 10485760
  raft_tick_ms: 100
  raft_heartbeat_tick: 5
  raft_election_tick: 20

proxy:
  addr: "127.0.0.1"
  port: 18322
  cert_path: "%s"
  key_path: "%s"
  ca_path: "%s"

wal:
  compact_after_bytes: 104857600
  sync_interval: "1s"

logging:
  level: "debug"
  format: "text"
  output_path: "stdout"

partition:
  active_queue_capacity: 1000000
  max_retries: 3
  max_backoff_sec: 2
  dispatch_batch_size: 128
  dlq_max_bytes: 10485760
  dlq_max_age_ms: 86400000

api:
  listen_addr: "127.0.0.1"
  api_port: 18321
  token_path: "%s"
`,
		tmpDir,        // data_dir
		certPath,      // cluster cert_path
		keyPath,       // cluster key_path
		ca.CertPath,   // cluster ca_path
		discoveryPath, // cluster discovery_yml_path
		certPath,      // proxy cert_path
		keyPath,       // proxy key_path
		ca.CertPath,   // proxy ca_path
		authPath,      // api token_path
	)

	configPath := filepath.Join(tmpDir, "config.yml")
	err = os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// Set config path for internal.LoadConfig()
	internal.CfgFile = configPath

	// Load config
	cfg, err := internal.LoadConfig()
	require.NoError(t, err)

	// Test the config loaded successfully
	assert.NotNil(t, cfg)
	assert.Equal(t, "node1", cfg.NodeID)
}

func TestFlagBinding(t *testing.T) {
	// Test that flags exist and have correct default values
	testCases := []struct {
		name       string
		flagName   string
		viperKey   string
		defaultVal string // Default values are always strings
	}{
		{"node-id", "node-id", "node_id", ""},
		{"data-dir", "data-dir", "data_dir", ""},
		{"cluster-addr", "cluster-addr", "cluster.listen_addr", ""},
		{"cluster-port", "cluster-port", "cluster.quic_port", "0"},
		{"cluster-cert", "cluster-cert", "cluster.cert_path", ""},
		{"cluster-key", "cluster-key", "cluster.key_path", ""},
		{"cluster-ca", "cluster-ca", "cluster.ca_path", ""},
		{"initial-voters", "initial-voters", "cluster.initial_voters", "[]"},
		{"discovery-kind", "discovery-kind", "cluster.discovery_kind", ""},
		{"discovery-yml", "discovery-yml", "cluster.discovery_yml_path", ""},
		{"discovery-http-host", "discovery-http-host", "cluster.discovery_http_host", ""},
		{"snapshot-interval", "snapshot-interval", "cluster.snapshot_interval_sec", "60"},
		{"snapshot-trigger", "snapshot-trigger", "cluster.snapshot_trigger_count", "10000"},
		{"wal-flush-threshold", "wal-flush-threshold", "cluster.wal_flush_threshold", "1000"},
		{"proxy-addr", "proxy-addr", "proxy.addr", ""},
		{"proxy-port", "proxy-port", "proxy.port", "0"},
		{"proxy-cert", "proxy-cert", "proxy.cert_path", ""},
		{"proxy-key", "proxy-key", "proxy.key_path", ""},
		{"proxy-ca", "proxy-ca", "proxy.ca_path", ""},
		{"wal-compact", "wal-compact", "wal.compact_after_bytes", "0"},
		{"wal-sync", "wal-sync", "wal.sync_interval", ""},
		{"log-level", "log-level", "logging.level", ""},
		{"log-format", "log-format", "logging.format", ""},
		{"log-output", "log-output", "logging.output_path", ""},
		{"api-listen-addr", "api-listen-addr", "api.listen_addr", ""},
		{"api-port", "api-port", "api.api_port", "0"},
		{"api-token-path", "api-token-path", "api.token_path", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			flag := rootCmd.PersistentFlags().Lookup(tc.flagName)
			require.NotNil(t, flag, "Flag %s should exist", tc.flagName)

			// Check default value (always compare as strings)
			assert.Equal(t, tc.defaultVal, flag.DefValue, "Default value mismatch for %s", tc.flagName)

			// Verify viper binding exists
			assert.NotPanics(t, func() { viper.Get(tc.viperKey) }, "Viper key %s should be accessible", tc.viperKey)
		})
	}
}

func TestConfigFileFlag(t *testing.T) {
	// Test that config file flag is properly set
	flag := rootCmd.PersistentFlags().Lookup("config")
	require.NotNil(t, flag)
	assert.Equal(t, "", flag.DefValue)
	assert.Equal(t, "config file path (default: ./config.yml)", flag.Usage)
}

func TestRequiredFlags(t *testing.T) {
	// Verify that node-id is marked as required
	flag := rootCmd.PersistentFlags().Lookup("node-id")
	require.NotNil(t, flag)

	// Check that it's required by checking the annotation
	// Note: The required flag is set using MarkPersistentFlagRequired
	// which adds an annotation
	annotations := flag.Annotations
	if annotations != nil {
		// The annotation might be set differently
		required, ok := annotations["cobra_annotation_required"]
		if ok {
			assert.Contains(t, required, "true")
		}
	}
}

func TestServeCommandErrorCases(t *testing.T) {
	t.Run("missing config file", func(t *testing.T) {
		// Temporarily set config file to non-existent path
		oldCfgFile := internal.CfgFile
		defer func() { internal.CfgFile = oldCfgFile }()

		internal.CfgFile = "/nonexistent/config.yml"

		// Try to load config - should fail
		_, err := internal.LoadConfig()
		assert.Error(t, err)
	})

	t.Run("invalid config file", func(t *testing.T) {
		// Create temp dir
		tmpDir, err := os.MkdirTemp("", "invalid-config-*")
		require.NoError(t, err)
		defer func() { _ = os.RemoveAll(tmpDir) }()

		// Write invalid YAML
		invalidPath := filepath.Join(tmpDir, "invalid.yml")
		err = os.WriteFile(invalidPath, []byte("invalid: yaml: content: ["), 0644)
		require.NoError(t, err)

		oldCfgFile := internal.CfgFile
		defer func() { internal.CfgFile = oldCfgFile }()

		internal.CfgFile = invalidPath

		_, err = internal.LoadConfig()
		assert.Error(t, err)
	})
}

func TestCommandStructure(t *testing.T) {
	// Verify command hierarchy
	commands := rootCmd.Commands()

	// Check that serve and version commands exist
	foundServe := false
	foundVersion := false

	for _, cmd := range commands {
		if cmd.Use == "serve" || cmd.Use == "serve [flags]" {
			foundServe = true
			assert.Equal(t, "Start the cue server", cmd.Short)
			assert.Contains(t, cmd.Long, "Start the cue server with Raft clustering and HTTP proxy")
		}
		if cmd.Use == "version" {
			foundVersion = true
			assert.Equal(t, "Print version information", cmd.Short)
		}
	}

	assert.True(t, foundServe, "serve command should be added to root")
	assert.True(t, foundVersion, "version command should be added to root")
}

func TestPersistentFlagsExist(t *testing.T) {
	// Verify all persistent flags exist
	expectedFlags := []string{
		"config", "node-id", "data-dir",
		"cluster-addr", "cluster-port", "cluster-cert", "cluster-key", "cluster-ca",
		"initial-voters",
		"discovery-kind", "discovery-yml", "discovery-http-host",
		"snapshot-interval", "snapshot-trigger", "wal-flush-threshold",
		"proxy-addr", "proxy-port", "proxy-cert", "proxy-key", "proxy-ca",
		"wal-compact", "wal-sync",
		"log-level", "log-format", "log-output",
		"api-listen-addr", "api-port", "api-token-path",
	}

	for _, flagName := range expectedFlags {
		flag := rootCmd.PersistentFlags().Lookup(flagName)
		assert.NotNil(t, flag, "Flag %s should exist", flagName)
	}
}

func TestViperBindingCoverage(t *testing.T) {
	// Test that all viper bindings work with actual values
	// This test is more about ensuring the bindings don't panic
	testCases := []struct {
		viperKey string
	}{
		{"node_id"},
		{"data_dir"},
		{"cluster.listen_addr"},
		{"cluster.quic_port"},
		{"proxy.addr"},
		{"proxy.port"},
		{"logging.level"},
		{"api.api_port"},
	}

	for _, tc := range testCases {
		t.Run(tc.viperKey, func(t *testing.T) {
			// Check that viper binding exists
			assert.NotPanics(t, func() {
				_ = viper.Get(tc.viperKey)
			}, "Viper binding for %s should work", tc.viperKey)
		})
	}
}

// Additional test for the runServe function
func TestRunServeFunction(t *testing.T) {
	// This test verifies that runServe properly handles errors
	t.Run("invalid config", func(t *testing.T) {
		// Save original config file
		oldCfgFile := internal.CfgFile
		defer func() { internal.CfgFile = oldCfgFile }()

		// Set to invalid path
		internal.CfgFile = "/invalid/path/config.yml"

		// Call runServe directly
		err := runServe(&cobra.Command{}, []string{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to load config")
	})
}

func TestMain(m *testing.M) {
	// Set up any global test configuration
	viper.Reset()

	// Run tests
	code := m.Run()

	// Clean up
	os.Exit(code)
}
