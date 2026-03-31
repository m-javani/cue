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

package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/testutils"
	"github.com/stretchr/testify/require"
)

func TestNodeStartup(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create temp directory for all test files
	tmpDir, err := os.MkdirTemp("", "cue-node-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

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
  peers: ["node1"]
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
  retry_base_delay_ms: 1000
  max_retries: 3
  max_backoff_ms: 2000
  dispatch_batch_size: 128
  dlq_max_bytes: 10485760
  dlq_max_age_ms: 86400000

api:
  listen_addr: "127.0.0.1"
  api_port: 18321
  token_path: "%s"

address_resolver:
  type: static
  config:
    peers:
      node1: "127.0.0.1:18323"

tls_verifier:
  type: cn
  config: {}
`,
		tmpDir,      // data_dir
		certPath,    // cluster cert_path
		keyPath,     // cluster key_path
		ca.CertPath, // cluster ca_path
		certPath,    // proxy cert_path
		keyPath,     // proxy key_path
		ca.CertPath, // proxy ca_path
		authPath,    // api token_path
	)

	configPath := filepath.Join(tmpDir, "config.yml")
	err = os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// Set config path for internal.LoadConfig()
	internal.CfgFile = configPath

	// Load config
	cfg, err := internal.LoadConfig()
	require.NoError(t, err)

	// Create context with cancel
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run node in goroutine
	errCh := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				errCh <- fmt.Errorf("panic: %v", r)
			}
		}()
		err := Run(cfg)
		if err != nil && err != context.Canceled {
			errCh <- err
		}
		close(errCh)
	}()

	// Wait for node to become healthy
	healthURL := "http://127.0.0.1:18321/health"
	client := &http.Client{Timeout: 2 * time.Second}

	t.Log("Waiting for node to become healthy...")
	healthy := false
	for i := 0; i < 30; i++ {
		resp, err := client.Get(healthURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			healthy = true
			t.Log("Node is healthy!")
			break
		}
		if err == nil {
			resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}

	require.True(t, healthy, "Node should become healthy without panic")

	// Cancel context to stop the node
	t.Log("Shutting down node...")
	cancel()

	// Wait for node to stop gracefully
	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("Node stopped with error: %v", err)
		} else {
			t.Log("Node stopped gracefully")
		}
	case <-time.After(10 * time.Second):
		t.Log("Node shutdown timeout")
	}

	// Wait a bit for cleanup
	time.Sleep(500 * time.Millisecond)
	t.Log("Test completed successfully")
}
