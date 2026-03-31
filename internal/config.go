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
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

var CfgFile string

const (
	DefaultDLQMaxSizeBytes   = 10 << 20 // 10 MB
	DefaultRaftTickMs        = 100
	DefaultRaftHeartbeatTick = 5
	DefaultRaftElectionTick  = 20
)

// Config is the single source of truth for the whole application
type Config struct {
	NodeID          string          `mapstructure:"node_id"`
	DataDir         string          `mapstructure:"data_dir"`
	Cluster         ClusterConfig   `mapstructure:"cluster"`
	Proxy           ProxyConfig     `mapstructure:"proxy"`
	WAL             WALConfig       `mapstructure:"wal"`
	Partition       PartitionConfig `mapstructure:"partition"`
	Logging         LoggingConfig   `mapstructure:"logging"`
	ApiConfig       ApiConfig       `mapstructure:"api"`
	AddressResolver ResolverConfig  `mapstructure:"address_resolver"`
	TLSVerifier     VerifierConfig  `mapstructure:"tls_verifier"`
}

type ResolverConfig struct {
	Type   string         `mapstructure:"type"` // "dns", "static", "service"
	Config map[string]any `mapstructure:"config"`
}

type VerifierConfig struct {
	Type   string         `mapstructure:"type"` // "dns", "cn", "spiffe"
	Config map[string]any `mapstructure:"config"`
}

// ClusterConfig - matches what NewClusterAgent expects
type ClusterConfig struct {
	InitialVoters        []string `mapstructure:"initial_voters"`
	ListenAddr           string   `mapstructure:"listen_addr"`
	QUICPort             uint16   `mapstructure:"quic_port"`
	Peers                []string `mapstructure:"peers"`
	SnapshotIntervalSec  uint64   `mapstructure:"snapshot_interval_sec"`
	SnapshotTriggerCount uint64   `mapstructure:"snapshot_trigger_count"`
	WALFlushThreshold    int      `mapstructure:"wal_flush_threshold"`
	CertPath             string   `mapstructure:"cert_path"`
	KeyPath              string   `mapstructure:"key_path"`
	CACertPath           string   `mapstructure:"ca_path"`
	DLQMaxSizeBytes      int64    `mapstructure:"dlq_max_size_bytes"`
	RaftTickMs           int      `mapstructure:"raft_tick_ms"`
	RaftHeartbeatTick    int      `mapstructure:"raft_heartbeat_tick"`
	RaftElectionTick     int      `mapstructure:"raft_election_tick"`
	// Logger is set at runtime
	// Logger *zap.Logger `mapstructure:"-"`
}

// ProxyConfig for proxy connections
type ProxyConfig struct {
	Addr     string `mapstructure:"addr"`
	Port     int    `mapstructure:"port"`
	CertPath string `mapstructure:"cert_path"`
	KeyPath  string `mapstructure:"key_path"`
	CAPath   string `mapstructure:"ca_path"`
}

type WALConfig struct {
	CompactAfterBytes int64  `mapstructure:"compact_after_bytes"`
	SyncInterval      string `mapstructure:"sync_interval"`
}

type LoggingConfig struct {
	Level      string `mapstructure:"level"`
	Format     string `mapstructure:"format"`
	OutputPath string `mapstructure:"output_path"`
}

// PartitionConfig
type PartitionConfig struct {
	ActiveQueueCapacity int   `mapstructure:"active_queue_capacity"`
	RetryBaseDelayMs    int64 `mapstructure:"retry_base_delay_ms"`
	MaxRetries          int   `mapstructure:"max_retries"`
	MaxBackoffMs        int64 `mapstructure:"max_backoff_ms"`
	DispatchBatchSize   int   `mapstructure:"dispatch_batch_size"`
	DLQMaxBytes         int64 `mapstructure:"dlq_max_bytes"`
	DLQMaxAgeMs         int64 `mapstructure:"dlq_max_age_ms"`
	PartitionTickMs     int   `mapstructure:"-"`
	ProxyCleanupTickSec int   `mapstructure:"-"`
	HeartbeatTickMs     int   `mapstructure:"-"`
}

type ApiConfig struct {
	ListenAddr     string `mapstructure:"listen_addr"`
	ApiPort        uint16 `mapstructure:"api_port"`
	TokenPath      string `mapstructure:"token_path"`
	TimeoutSeconds int    `mapstructure:"timeout_seconds"`
}

// GetClusterAddr returns the full cluster listen address
func (c *Config) GetClusterAddr() string {
	// If listen_addr already contains a port, use it directly
	if strings.Contains(c.Cluster.ListenAddr, ":") {
		return c.Cluster.ListenAddr
	}
	return fmt.Sprintf("%s:%d", c.Cluster.ListenAddr, c.Cluster.QUICPort)
}

// GetProxyAddr returns the full proxy listen address
func (c *Config) GetProxyAddr() string {
	if strings.Contains(c.Proxy.Addr, ":") {
		return c.Proxy.Addr
	}
	return fmt.Sprintf("%s:%d", c.Proxy.Addr, c.Proxy.Port)
}

func NewConfig() *Config {
	// Internal defaults - these are not exposed and won't be read from config files
	// They're set here so tests can override them
	return &Config{
		Partition: PartitionConfig{
			PartitionTickMs:     100,
			ProxyCleanupTickSec: 30,
			HeartbeatTickMs:     1000,
		},
	}
}

// LoadConfig loads configuration from file, env vars, and flags
func LoadConfig() (*Config, error) {
	if err := setupViper(); err != nil {
		return nil, err
	}

	cfg := NewConfig()
	if err := viper.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func setupViper() error {
	// Set config file if provided via flag
	if CfgFile != "" {
		viper.SetConfigFile(CfgFile)
	} else {
		viper.SetConfigName("config")
		viper.SetConfigType("yml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("./config")
		viper.AddConfigPath("/etc/cue")
	}

	viper.SetEnvPrefix("CUE")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	setDefaults()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return fmt.Errorf("error reading config file: %w", err)
		}
	} else {
		fmt.Printf("Using config file: %s\n", viper.ConfigFileUsed())
	}
	return nil
}

func setDefaults() {
	viper.SetDefault("data_dir", "./data")

	// Cluster defaults
	viper.SetDefault("cluster.listen_addr", "0.0.0.0")
	viper.SetDefault("cluster.quic_port", 8323)
	viper.SetDefault("cluster.snapshot_interval_sec", uint64(60))
	viper.SetDefault("cluster.snapshot_trigger_count", uint64(10000))
	viper.SetDefault("cluster.wal_flush_threshold", 1000)
	viper.SetDefault("cluster.dlq_max_size_bytes", DefaultDLQMaxSizeBytes)
	viper.SetDefault("cluster.raft_tick_ms", DefaultRaftTickMs)
	viper.SetDefault("cluster.raft_heartbeat_tick", DefaultRaftHeartbeatTick)
	viper.SetDefault("cluster.raft_election_tick", DefaultRaftElectionTick)

	// Proxy defaults
	viper.SetDefault("proxy.addr", "0.0.0.0")
	viper.SetDefault("proxy.port", 8322)

	// Api defaults
	viper.SetDefault("api.listen_addr", "0.0.0.0")
	viper.SetDefault("api.api_port", 8321)
	viper.SetDefault("api.token_path", "./auth.yml")
	viper.SetDefault("api.timeout_seconds", 45)

	// Raft, WAL, Logging, Partition defaults...
	viper.SetDefault("wal.compact_after_bytes", 104857600)
	viper.SetDefault("wal.sync_interval", "1s")
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.format", "json")
	viper.SetDefault("logging.output_path", "stdout")

	// =============================================================================
	//  Partition defaults
	// =============================================================================

	viper.SetDefault("partition.active_queue_capacity", 500000) // Hard to fill, protected by min in code
	viper.SetDefault("partition.max_retries", 5)
	viper.SetDefault("partition.retry_base_delay_ms", 1000)
	viper.SetDefault("partition.max_backoff_ms", 60000)
	viper.SetDefault("partition.dispatch_batch_size", 128)
	viper.SetDefault("partition.dlq_max_bytes", DefaultDLQMaxSizeBytes)
	viper.SetDefault("partition.dlq_max_age_ms", 24*60*60*1000) // 24 hours
}

func (c *Config) Validate() error {
	var errs []string
	var warnings []string

	if c.DataDir == "" {
		errs = append(errs, "data_dir cannot be empty")
	}
	if c.Cluster.QUICPort == 0 {
		errs = append(errs, "cluster.quic_port must be between 1 and 65535")
	}
	if c.Proxy.Port <= 0 || c.Proxy.Port > 65535 {
		errs = append(errs, "proxy.port must be between 1 and 65535")
	}
	if c.Cluster.CertPath == "" || c.Cluster.KeyPath == "" || c.Cluster.CACertPath == "" {
		errs = append(errs, "cluster TLS paths cannot be empty")
	}
	if c.Proxy.CertPath == "" || c.Proxy.KeyPath == "" || c.Proxy.CAPath == "" {
		errs = append(errs, "proxy TLS paths cannot be empty")
	}

	// Remove the early return and let validateRaftConfig handle all errors
	raftErrs, raftWarns := validateRaftConfig(&c.Cluster)
	errs = append(errs, raftErrs...)
	warnings = append(warnings, raftWarns...)

	if c.Partition.PartitionTickMs <= 0 {
		errs = append(errs, "internal error: partition.partition_tick_ms not set")
	}
	if c.Partition.ProxyCleanupTickSec <= 0 {
		errs = append(errs, "internal error: partition.proxy_cleanup_tick_sec not set")
	}
	if c.Partition.HeartbeatTickMs <= 0 {
		errs = append(errs, "internal error: partition.heartbeat_tick_ms not set")
	}

	// Validate address resolver
	validResolvers := map[string]bool{
		"service": true,
		"dns":     true,
		"static":  true,
	}
	if !validResolvers[c.AddressResolver.Type] {
		errs = append(errs, fmt.Sprintf("unknown address_resolver type: %q (valid: dns, static, service)", c.AddressResolver.Type))
	}

	// Validate TLS verifier
	validVerifiers := map[string]bool{
		"dns":    true,
		"cn":     true,
		"spiffe": true,
	}
	if !validVerifiers[c.TLSVerifier.Type] {
		errs = append(errs, fmt.Sprintf("unknown tls_verifier type: %q (valid: dns, cn, spiffe)", c.TLSVerifier.Type))
	}

	// Address resolver config validation
	switch c.AddressResolver.Type {
	case "service":
		if _, ok := c.AddressResolver.Config["domain"]; ok {
			errs = append(errs, "address_resolver service should not contain 'domain' in config")
		}
	case "dns":
		if _, ok := c.AddressResolver.Config["domain"]; !ok {
			errs = append(errs, "address_resolver dns requires 'domain' in config")
		}
	case "static":
		if _, ok := c.AddressResolver.Config["peers"]; !ok {
			errs = append(errs, "address_resolver static requires 'peers' map in config")
		}
	}

	// TLS verifier config validation
	switch c.TLSVerifier.Type {
	case "dns":
		if _, ok := c.TLSVerifier.Config["domain"]; !ok {
			errs = append(errs, "tls_verifier dns requires 'domain' in config")
		}
	case "cn":
		if len(c.TLSVerifier.Config) > 0 {
			for key := range c.TLSVerifier.Config {
				errs = append(errs, fmt.Sprintf("tls_verifier cn does not accept config fields, but found key: %q", key))
			}
		}
	case "spiffe":
		if _, ok := c.TLSVerifier.Config["trust_domain"]; !ok {
			errs = append(errs, "tls_verifier spiffe requires 'trust_domain' in config")
		}
	}

	// Log warnings (could be integrated with proper logging)
	for _, w := range warnings {
		fmt.Printf("[WARN] %s\n", w)
	}

	if len(errs) > 0 {
		return fmt.Errorf("validation failed:\n- %s", strings.Join(errs, "\n- "))
	}

	return nil
}

// validateRaftConfig returns errors and warnings as slices
func validateRaftConfig(cfg *ClusterConfig) ([]string, []string) {
	var errs []string
	var warnings []string

	// Check if any raft config is zero first - but still collect other errors
	if cfg.RaftTickMs == 0 || cfg.RaftHeartbeatTick == 0 || cfg.RaftElectionTick == 0 {
		errs = append(errs, "raft configs are not set")
	}

	// 1. Hard limits (must be within these bounds)
	if cfg.RaftTickMs < 10 || cfg.RaftTickMs > 1000 {
		errs = append(errs, fmt.Sprintf("raft_tick_ms must be between 10 and 1000 (got %d)", cfg.RaftTickMs))
	}

	if cfg.RaftHeartbeatTick < 1 || cfg.RaftHeartbeatTick > 100 {
		errs = append(errs, fmt.Sprintf("raft_heartbeat_tick must be between 1 and 100 (got %d)", cfg.RaftHeartbeatTick))
	}

	// Only check election > heartbeat if both are set and heartbeat > 0
	if cfg.RaftHeartbeatTick > 0 && cfg.RaftElectionTick <= cfg.RaftHeartbeatTick {
		errs = append(errs, fmt.Sprintf("raft_election_tick (%d) must be > heartbeat_tick (%d)",
			cfg.RaftElectionTick, cfg.RaftHeartbeatTick))
	}

	// 2. Recommended ranges (warnings, not errors)
	// Only calculate ratio if heartbeat > 0 to avoid division by zero
	if cfg.RaftHeartbeatTick > 0 {
		if cfg.RaftTickMs < 50 || cfg.RaftTickMs > 200 {
			warnings = append(warnings,
				fmt.Sprintf("raft_tick_ms=%d is outside recommended range (50-200ms)", cfg.RaftTickMs))
		}

		if cfg.RaftHeartbeatTick < 3 || cfg.RaftHeartbeatTick > 10 {
			warnings = append(warnings,
				fmt.Sprintf("raft_heartbeat_tick=%d is outside recommended range (3-10)", cfg.RaftHeartbeatTick))
		}

		ratio := cfg.RaftElectionTick / max(cfg.RaftHeartbeatTick, 1)
		if ratio < 4 || ratio > 20 {
			warnings = append(warnings,
				fmt.Sprintf("election_tick (%d) is not 5-20x heartbeat_tick (%d) ratio=%dx",
					cfg.RaftElectionTick, cfg.RaftHeartbeatTick, ratio))
		}

		// 3. Calculate effective times
		heartbeatMs := cfg.RaftTickMs * cfg.RaftHeartbeatTick
		electionMs := cfg.RaftTickMs * cfg.RaftElectionTick

		if heartbeatMs < 200 || heartbeatMs > 2000 {
			warnings = append(warnings,
				fmt.Sprintf("effective heartbeat interval=%dms outside recommended (200-2000ms)", heartbeatMs))
		}

		if electionMs < 1000 || electionMs > 30000 {
			warnings = append(warnings,
				fmt.Sprintf("effective election timeout=%dms outside recommended (1000-30000ms)", electionMs))
		}
	} else {
		// If heartbeat is 0, add warnings about invalid configuration
		warnings = append(warnings, "raft_heartbeat_tick is 0, cannot calculate effective times")
	}

	return errs, warnings
}
