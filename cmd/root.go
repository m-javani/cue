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
	"fmt"

	"github.com/m-javani/cue/internal"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var version = "1.0"
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("cue version %s\n", version)
	},
}

// rootCmd represents the base command
var rootCmd = &cobra.Command{
	Use:   "cue",
	Short: "Cue — Durable job queue with push delivery",
	Long: `Cue is a clustered, Raft-based job queue with at-least-once delivery.
Producers send jobs via HTTP to proxies. Consumers receive jobs via WebSocket or webhook.`,
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the cue server",
	Long:  "Start the cue server with Raft clustering and HTTP proxy",
	RunE:  runServe,
}

func runServe(cmd *cobra.Command, args []string) error {
	// Load configuration
	cfg, err := internal.LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Run the application
	if err := Run(cfg); err != nil {
		return fmt.Errorf("application error: %w", err)
	}
	return nil
}

// Execute adds all child commands to the root command
func Execute() error {
	return rootCmd.Execute()
}

func init() {

	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(versionCmd)

	// Config file flag
	rootCmd.PersistentFlags().StringVar(&internal.CfgFile, "config", "", "config file path (default: ./config.yml)")

	// Node identity
	rootCmd.PersistentFlags().String("node-id", "", "node identifier")
	viper.BindPFlag("node_id", rootCmd.PersistentFlags().Lookup("node-id"))
	rootCmd.MarkPersistentFlagRequired("node-id")

	// Data directory
	rootCmd.PersistentFlags().String("data-dir", "", "data directory (for Raft and WAL)")
	viper.BindPFlag("data_dir", rootCmd.PersistentFlags().Lookup("data-dir"))

	// === Cluster (internal) network ===
	rootCmd.PersistentFlags().String("cluster-addr", "", "cluster listen address")
	rootCmd.PersistentFlags().Int("cluster-port", 0, "cluster QUIC port")
	rootCmd.PersistentFlags().String("cluster-cert", "", "cluster TLS certificate path")
	rootCmd.PersistentFlags().String("cluster-key", "", "cluster TLS key path")
	rootCmd.PersistentFlags().String("cluster-ca", "", "cluster CA certificate path")

	viper.BindPFlag("cluster.listen_addr", rootCmd.PersistentFlags().Lookup("cluster-addr"))
	viper.BindPFlag("cluster.quic_port", rootCmd.PersistentFlags().Lookup("cluster-port"))
	viper.BindPFlag("cluster.cert_path", rootCmd.PersistentFlags().Lookup("cluster-cert"))
	viper.BindPFlag("cluster.key_path", rootCmd.PersistentFlags().Lookup("cluster-key"))
	viper.BindPFlag("cluster.ca_path", rootCmd.PersistentFlags().Lookup("cluster-ca"))

	// Cluster discovery (important for multi-node)
	rootCmd.PersistentFlags().StringSlice("initial-voters", nil, "Initial Raft voters (comma-separated, e.g. node1,node2,node3)")
	rootCmd.PersistentFlags().StringSlice("peers", nil, "Peer nodes for discovery (comma-separated)")

	viper.BindPFlag("cluster.initial_voters", rootCmd.PersistentFlags().Lookup("initial-voters"))
	viper.BindPFlag("cluster.peers", rootCmd.PersistentFlags().Lookup("peers"))

	// Snapshot & WAL tuning
	rootCmd.PersistentFlags().Uint64("snapshot-interval", 60, "Snapshot interval in seconds")
	rootCmd.PersistentFlags().Uint64("snapshot-trigger", 10000, "Snapshot trigger entry count")
	rootCmd.PersistentFlags().Int("wal-flush-threshold", 1000, "WAL flush threshold")

	viper.BindPFlag("cluster.snapshot_interval_sec", rootCmd.PersistentFlags().Lookup("snapshot-interval"))
	viper.BindPFlag("cluster.snapshot_trigger_count", rootCmd.PersistentFlags().Lookup("snapshot-trigger"))
	viper.BindPFlag("cluster.wal_flush_threshold", rootCmd.PersistentFlags().Lookup("wal-flush-threshold"))

	// === Proxy (external) network ===
	rootCmd.PersistentFlags().String("proxy-addr", "", "proxy listen address")
	rootCmd.PersistentFlags().Int("proxy-port", 0, "proxy listen port")
	rootCmd.PersistentFlags().String("proxy-cert", "", "proxy TLS certificate path")
	rootCmd.PersistentFlags().String("proxy-key", "", "proxy TLS key path")
	rootCmd.PersistentFlags().String("proxy-ca", "", "proxy CA certificate path")

	viper.BindPFlag("proxy.addr", rootCmd.PersistentFlags().Lookup("proxy-addr"))
	viper.BindPFlag("proxy.port", rootCmd.PersistentFlags().Lookup("proxy-port"))
	viper.BindPFlag("proxy.cert_path", rootCmd.PersistentFlags().Lookup("proxy-cert"))
	viper.BindPFlag("proxy.key_path", rootCmd.PersistentFlags().Lookup("proxy-key"))
	viper.BindPFlag("proxy.ca_path", rootCmd.PersistentFlags().Lookup("proxy-ca"))

	// WAL tuning
	rootCmd.PersistentFlags().Int64("wal-compact", 0, "WAL compact after bytes")
	rootCmd.PersistentFlags().String("wal-sync", "", "WAL sync interval")
	viper.BindPFlag("wal.compact_after_bytes", rootCmd.PersistentFlags().Lookup("wal-compact"))
	viper.BindPFlag("wal.sync_interval", rootCmd.PersistentFlags().Lookup("wal-sync"))

	// Logging
	rootCmd.PersistentFlags().String("log-level", "", "logging level (debug, info, warn, error)")
	rootCmd.PersistentFlags().String("log-format", "", "logging format (json, text)")
	rootCmd.PersistentFlags().String("log-output", "", "logging output path")
	viper.BindPFlag("logging.level", rootCmd.PersistentFlags().Lookup("log-level"))
	viper.BindPFlag("logging.format", rootCmd.PersistentFlags().Lookup("log-format"))
	viper.BindPFlag("logging.output_path", rootCmd.PersistentFlags().Lookup("log-output"))

	// Api
	rootCmd.PersistentFlags().String("api-listen-addr", "", "API listen address")
	rootCmd.PersistentFlags().Int("api-port", 0, "API port")
	rootCmd.PersistentFlags().String("api-token-path", "", "API token path")

	viper.BindPFlag("api.listen_addr", rootCmd.PersistentFlags().Lookup("api-listen-addr"))
	viper.BindPFlag("api.api_port", rootCmd.PersistentFlags().Lookup("api-port"))
	viper.BindPFlag("api.token_path", rootCmd.PersistentFlags().Lookup("api-token-path"))
}
