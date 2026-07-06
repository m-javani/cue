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

package internal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create a valid config YAML
// Helper to create a valid config YAML
func validConfigYAML() string {
	return `
node_id: node-1
data_dir: /tmp/data
cluster:
  listen_addr: 0.0.0.0
  quic_port: 8323
  cert_path: /certs/cert.pem
  key_path: /certs/key.pem
  ca_path: /certs/ca.pem
  raft_tick_ms: 100
  raft_heartbeat_tick: 5
  raft_election_tick: 20
  discovery_kind: static
  discovery_yml_path: /tmp/discovery.yml
proxy:
  addr: 0.0.0.0
  port: 8322
  cert_path: /certs/proxy-cert.pem
  key_path: /certs/proxy-key.pem
  ca_path: /certs/proxy-ca.pem
api:
  timeout_seconds: 30
partition:
`
}

func TestLoadConfig(t *testing.T) {
	originalCfgFile := CfgFile
	defer func() { CfgFile = originalCfgFile }()

	tests := []struct {
		name         string
		cfgContent   string
		expectError  bool
		errorStrings []string
	}{
		{
			name:        "valid config",
			cfgContent:  validConfigYAML(),
			expectError: false,
		},
		{
			name: "uses defaults for missing fields",
			cfgContent: `
node_id: node-1
cluster:
  cert_path: /certs/cert.pem
  key_path: /certs/key.pem
  ca_path: /certs/ca.pem
  raft_tick_ms: 100
  raft_heartbeat_tick: 5
  raft_election_tick: 20
proxy:
  cert_path: /certs/proxy-cert.pem
  key_path: /certs/proxy-key.pem
  ca_path: /certs/proxy-ca.pem
`,
			expectError: false,
		},
		{
			name: "multiple validation errors",
			cfgContent: `
node_id: node-1
cluster:
  quic_port: 0
  cert_path: /certs/cert.pem
  raft_tick_ms: 100
  raft_heartbeat_tick: 5
  raft_election_tick: 20
proxy:
  port: 99999
`,
			expectError: true,
			errorStrings: []string{
				"cluster.quic_port must be between 1 and 65535",
				"proxy.port must be between 1 and 65535",
				"cluster TLS paths cannot be empty",
				"proxy TLS paths cannot be empty",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			CfgFile = createTempConfig(t, tt.cfgContent)

			cfg, err := LoadConfig()

			if tt.expectError {
				require.Error(t, err)
				for _, expected := range tt.errorStrings {
					assert.Contains(t, err.Error(), expected)
				}
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, cfg)
		})
	}
}

func TestValidateAggregatedErrors(t *testing.T) {
	// Create a base valid config
	validCfg := &Config{
		NodeID:  "node-1",
		DataDir: "/tmp/data",
		Cluster: ClusterConfig{
			QUICPort:          8323,
			RaftTickMs:        100,
			RaftHeartbeatTick: 5,
			RaftElectionTick:  20,
			CertPath:          "/certs/cert.pem",
			KeyPath:           "/certs/key.pem",
			CACertPath:        "/certs/ca.pem",
			DiscoveryKind:     "static",
			DiscoveryYMLPath:  "/tmp/discovery.yml",
		},
		Proxy: ProxyConfig{
			Port:     8322,
			CertPath: "/certs/proxy-cert.pem",
			KeyPath:  "/certs/proxy-key.pem",
			CAPath:   "/certs/proxy-ca.pem",
		},
		ApiConfig: ApiConfig{
			TimeoutSeconds: 30,
		},
		Partition: PartitionConfig{
			PartitionTickMs:     100,
			ProxyCleanupTickSec: 30,
			HeartbeatTickMs:     1000,
		},
	}

	tests := []struct {
		name         string
		mutate       func(*Config)
		expectError  bool
		errorStrings []string
	}{
		{
			name: "valid config",
			mutate: func(c *Config) {
				// No changes
			},
			expectError: false,
		},
		{
			name: "single error - empty data_dir",
			mutate: func(c *Config) {
				c.DataDir = ""
			},
			expectError:  true,
			errorStrings: []string{"data_dir cannot be empty"},
		},
		{
			name: "multiple validation errors",
			mutate: func(c *Config) {
				c.DataDir = ""
				c.Cluster.QUICPort = 0
				c.Proxy.Port = 99999
				c.Cluster.CertPath = ""
				c.Proxy.CertPath = ""
				c.Partition.PartitionTickMs = 0
				c.Cluster.DiscoveryKind = "http"
				c.Cluster.DiscoveryYMLPath = "" // Should be ignored for http
				// Missing DiscoveryHTTPHost will cause error
			},
			expectError: true,
			errorStrings: []string{
				"data_dir cannot be empty",
				"cluster.quic_port must be between 1 and 65535",
				"proxy.port must be between 1 and 65535",
				"cluster TLS paths cannot be empty",
				"proxy TLS paths cannot be empty",
				"internal error: partition.partition_tick_ms not set",
				"discovery_http_host is required when discovery_kind=http",
			},
		},
		{
			name: "all TLS paths missing",
			mutate: func(c *Config) {
				c.Cluster.CertPath = ""
				c.Cluster.KeyPath = ""
				c.Cluster.CACertPath = ""
				c.Proxy.CertPath = ""
				c.Proxy.KeyPath = ""
				c.Proxy.CAPath = ""
			},
			expectError: true,
			errorStrings: []string{
				"cluster TLS paths cannot be empty",
				"proxy TLS paths cannot be empty",
			},
		},
		{
			name: "invalid raft configs",
			mutate: func(c *Config) {
				c.Cluster.RaftTickMs = 5
				c.Cluster.RaftHeartbeatTick = 0
				c.Cluster.RaftElectionTick = 3
			},
			expectError: true,
			errorStrings: []string{
				"raft configs are not set",
				"raft_tick_ms must be between 10 and 1000 (got 5)",
				"raft_heartbeat_tick must be between 1 and 100 (got 0)",
				// Note: election comparison is skipped when heartbeat is 0
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := *validCfg // Copy
			tt.mutate(&cfg)

			err := cfg.Validate()

			if tt.expectError {
				require.Error(t, err)
				for _, expected := range tt.errorStrings {
					assert.Contains(t, err.Error(), expected)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateRaftConfig(t *testing.T) {
	tests := []struct {
		name         string
		cfg          *ClusterConfig
		expectError  bool
		errorStrings []string
	}{
		{
			name: "valid config",
			cfg: &ClusterConfig{
				RaftTickMs:        100,
				RaftHeartbeatTick: 5,
				RaftElectionTick:  20,
			},
			expectError: false,
		},
		{
			name: "multiple errors - zero heartbeat",
			cfg: &ClusterConfig{
				RaftTickMs:        5,
				RaftHeartbeatTick: 0,
				RaftElectionTick:  3,
			},
			expectError: true,
			errorStrings: []string{
				"raft configs are not set",
				"raft_tick_ms must be between 10 and 1000 (got 5)",
				"raft_heartbeat_tick must be between 1 and 100 (got 0)",
				// Election comparison skipped when heartbeat is 0
			},
		},
		{
			name: "election not greater than heartbeat",
			cfg: &ClusterConfig{
				RaftTickMs:        100,
				RaftHeartbeatTick: 10,
				RaftElectionTick:  5,
			},
			expectError: true,
			errorStrings: []string{
				"raft_election_tick (5) must be > heartbeat_tick (10)",
			},
		},
		{
			name: "heartbeat tick at boundary",
			cfg: &ClusterConfig{
				RaftTickMs:        100,
				RaftHeartbeatTick: 100,
				RaftElectionTick:  101,
			},
			expectError: false,
		},
		{
			name: "tick ms at boundary",
			cfg: &ClusterConfig{
				RaftTickMs:        10,
				RaftHeartbeatTick: 5,
				RaftElectionTick:  20,
			},
			expectError: false,
		},
		{
			name: "multiple errors - valid heartbeat with comparison",
			cfg: &ClusterConfig{
				RaftTickMs:        5,
				RaftHeartbeatTick: 10,
				RaftElectionTick:  3,
			},
			expectError: true,
			errorStrings: []string{
				"raft_tick_ms must be between 10 and 1000 (got 5)",
				"raft_election_tick (3) must be > heartbeat_tick (10)",
				// "raft configs are not set" is NOT expected here because none are zero
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs, _ := validateRaftConfig(tt.cfg)

			if tt.expectError {
				require.NotEmpty(t, errs)
				combined := strings.Join(errs, "\n")
				for _, expected := range tt.errorStrings {
					assert.Contains(t, combined, expected)
				}
			} else {
				assert.Empty(t, errs)
			}
		})
	}
}

func TestGetClusterAddr(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *Config
		expected string
	}{
		{"with port", &Config{Cluster: ClusterConfig{ListenAddr: "127.0.0.1:8080", QUICPort: 8323}}, "127.0.0.1:8080"},
		{"without port", &Config{Cluster: ClusterConfig{ListenAddr: "127.0.0.1", QUICPort: 8323}}, "127.0.0.1:8323"},
		{"empty addr", &Config{Cluster: ClusterConfig{ListenAddr: "", QUICPort: 8323}}, ":8323"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.cfg.GetClusterAddr())
		})
	}
}

func TestGetProxyAddr(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *Config
		expected string
	}{
		{"with port", &Config{Proxy: ProxyConfig{Addr: "127.0.0.1:8080", Port: 8322}}, "127.0.0.1:8080"},
		{"without port", &Config{Proxy: ProxyConfig{Addr: "127.0.0.1", Port: 8322}}, "127.0.0.1:8322"},
		{"empty addr", &Config{Proxy: ProxyConfig{Addr: "", Port: 8322}}, ":8322"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.cfg.GetProxyAddr())
		})
	}
}

func TestDefaults(t *testing.T) {
	viper.Reset()
	setDefaults()

	tests := map[string]interface{}{
		"data_dir":                        "./data",
		"cluster.listen_addr":             "0.0.0.0",
		"cluster.quic_port":               8323,
		"cluster.snapshot_interval_sec":   60,
		"cluster.snapshot_trigger_count":  10000,
		"cluster.wal_flush_threshold":     1000,
		"cluster.dlq_max_size_bytes":      DefaultDLQMaxSizeBytes,
		"cluster.discovery_kind":          "static",
		"cluster.discovery_yml_path":      "./discovery.yml",
		"cluster.discovery_http_host":     "",
		"proxy.addr":                      "0.0.0.0",
		"proxy.port":                      8322,
		"api.listen_addr":                 "0.0.0.0",
		"api.api_port":                    8321,
		"api.token_path":                  "./auth.yml",
		"api.timeout_seconds":             45,
		"wal.compact_after_bytes":         104857600,
		"wal.sync_interval":               "1s",
		"logging.level":                   "info",
		"logging.format":                  "json",
		"logging.output_path":             "stdout",
		"partition.active_queue_capacity": 500000,
		"partition.max_retries":           5,
		"partition.max_backoff_sec":       6,
		"partition.dispatch_batch_size":   128,
		"partition.dlq_max_bytes":         DefaultDLQMaxSizeBytes,
		"partition.dlq_max_age_ms":        24 * 60 * 60 * 1000,
	}

	for key, expected := range tests {
		t.Run(key, func(t *testing.T) {
			val := viper.Get(key)
			assert.EqualValues(t, expected, val)
		})
	}
}

func TestValidateDiscoveryConfig(t *testing.T) {
	tests := []struct {
		name         string
		cfg          *ClusterConfig
		expectError  bool
		errorStrings []string
	}{
		{
			name: "valid static discovery",
			cfg: &ClusterConfig{
				DiscoveryKind:    "static",
				DiscoveryYMLPath: "/tmp/discovery.yml",
			},
			expectError: false,
		},
		{
			name: "valid http discovery",
			cfg: &ClusterConfig{
				DiscoveryKind:     "http",
				DiscoveryHTTPHost: "http://discovery-service:8080",
			},
			expectError: false,
		},
		{
			name: "valid static discovery - uppercase",
			cfg: &ClusterConfig{
				DiscoveryKind:    "STATIC",
				DiscoveryYMLPath: "/tmp/discovery.yml",
			},
			expectError: false,
		},
		{
			name: "valid http discovery - mixed case",
			cfg: &ClusterConfig{
				DiscoveryKind:     "Http",
				DiscoveryHTTPHost: "http://discovery-service:8080",
			},
			expectError: false,
		},
		{
			name: "invalid discovery kind",
			cfg: &ClusterConfig{
				DiscoveryKind: "invalid",
			},
			expectError:  true,
			errorStrings: []string{"unknown discovery kind: invalid"},
		},
		{
			name: "static discovery missing yml path",
			cfg: &ClusterConfig{
				DiscoveryKind: "static",
			},
			expectError:  true,
			errorStrings: []string{"discovery_yml_path is required when discovery_kind=static"},
		},
		{
			name: "http discovery missing http host",
			cfg: &ClusterConfig{
				DiscoveryKind: "http",
			},
			expectError:  true,
			errorStrings: []string{"discovery_http_host is required when discovery_kind=http"},
		},
		{
			name: "empty discovery kind defaults to static - missing yml path",
			cfg: &ClusterConfig{
				DiscoveryKind: "",
			},
			expectError:  true,
			errorStrings: []string{"unknown discovery kind: "},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDiscoveryConfig(tt.cfg)

			if tt.expectError {
				require.Error(t, err)
				for _, expected := range tt.errorStrings {
					assert.Contains(t, err.Error(), expected)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDiscoveryKindHelpers(t *testing.T) {
	tests := []struct {
		name             string
		cfg              *Config
		expectedKind     DiscoveryKind
		expectedStatic   bool
		expectedHttp     bool
		expectParseError bool
	}{
		{
			name: "static discovery",
			cfg: &Config{
				Cluster: ClusterConfig{
					DiscoveryKind: "static",
				},
			},
			expectedKind:     DiscoveryKindStatic,
			expectedStatic:   true,
			expectedHttp:     false,
			expectParseError: false,
		},
		{
			name: "http discovery",
			cfg: &Config{
				Cluster: ClusterConfig{
					DiscoveryKind: "http",
				},
			},
			expectedKind:     DiscoveryKindHttp,
			expectedStatic:   false,
			expectedHttp:     true,
			expectParseError: false,
		},
		{
			name: "uppercase static",
			cfg: &Config{
				Cluster: ClusterConfig{
					DiscoveryKind: "STATIC",
				},
			},
			expectedKind:     DiscoveryKindStatic,
			expectedStatic:   true,
			expectedHttp:     false,
			expectParseError: false,
		},
		{
			name: "mixed case http",
			cfg: &Config{
				Cluster: ClusterConfig{
					DiscoveryKind: "Http",
				},
			},
			expectedKind:     DiscoveryKindHttp,
			expectedStatic:   false,
			expectedHttp:     true,
			expectParseError: false,
		},
		{
			name: "invalid discovery kind",
			cfg: &Config{
				Cluster: ClusterConfig{
					DiscoveryKind: "invalid",
				},
			},
			expectedKind:     DiscoveryKindStatic,
			expectedStatic:   false,
			expectedHttp:     false,
			expectParseError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, err := ParseDiscoveryKind(tt.cfg.Cluster.DiscoveryKind)
			if tt.expectParseError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectedKind, kind)
			assert.Equal(t, tt.expectedStatic, kind == DiscoveryKindStatic)
			assert.Equal(t, tt.expectedHttp, kind == DiscoveryKindHttp)
		})
	}
}

func TestNewConfig(t *testing.T) {
	cfg := NewConfig()
	assert.Equal(t, 100, cfg.Partition.PartitionTickMs)
	assert.Equal(t, 30, cfg.Partition.ProxyCleanupTickSec)
	assert.Equal(t, 1000, cfg.Partition.HeartbeatTickMs)
}

// Helper functions
func createTempConfig(t *testing.T, content string) string {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yml")
	err := os.WriteFile(cfgPath, []byte(content), 0644)
	require.NoError(t, err)
	return cfgPath
}
