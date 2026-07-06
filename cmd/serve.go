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
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/api"
	"github.com/m-javani/cue/internal/cluster"
	"github.com/m-javani/cue/internal/model"
	"github.com/m-javani/cue/internal/proxy"
	"github.com/m-javani/cue/internal/state"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Run starts the Cue node
func Run(cfg *internal.Config) error {
	// Setup logger
	logger, err := setupLogger(cfg.Logging, cfg.NodeID)
	if err != nil {
		return fmt.Errorf("failed to setup logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	logger.Info("Starting Cue node", zap.String("node_id", cfg.NodeID))

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("Received shutdown signal")
		cancel()
	}()

	// Topic command channel
	topicCmdCh := make(chan state.TopicCommand, 1024)

	// Create shared components
	commandRouter := state.NewCommandRouter()
	heartbeatRouter := state.NewHeartbeatRouter()
	handler := state.NewHandler(commandRouter, topicCmdCh, logger)

	// Cluster-to-proxy command channel (used by both Gateway and TopicManager)
	toClusterCh := make(chan model.Command, 1024)

	var status atomic.Uint32
	status.Store(model.NodeStatusUnavailable.ToUin32())
	var currentTerm atomic.Uint64
	currentTerm.Store(0)

	members := model.Members{
		Voters:   []string{},
		Learners: []string{},
	}
	peerStore := model.PeerStore{
		Peers: make(map[string]model.PeerInfo),
	}

	leaderID := &atomic.Value{}
	leaderID.Store("")

	// Create Gateway (Proxy QUIC server)
	gateway, err := proxy.NewGateway(
		cfg.NodeID,
		cfg.Proxy.CertPath,
		cfg.Proxy.KeyPath,
		cfg.Proxy.CAPath,
		cfg.GetProxyAddr(),
		logger,
		topicCmdCh,
		heartbeatRouter,
		toClusterCh,
		&status,
		&currentTerm,
		&members,
		leaderID,
	)
	if err != nil {
		return fmt.Errorf("failed to create gateway: %w", err)
	}

	// build discovery
	var peers []model.PeerInfo
	if cfg.Cluster.DiscoveryKind == internal.DiscoveryKindStatic.String() {
		peers, err = cluster.LoadDiscoveryFile(cfg.Cluster.DiscoveryYMLPath)
		if err != nil {
			return err
		}
		found := false
		for _, peer := range peers {
			if peer.NodeID == cfg.NodeID {
				found = true
				break
			}
		}
		if !found {
			return errors.New("in static discovery mode, self node info should be included")
		}
	}
	discoveryTick := time.Duration(2 * time.Second)
	discoveryKind, err := internal.ParseDiscoveryKind(cfg.Cluster.DiscoveryKind)
	if err != nil {
		return err
	}
	if discoveryKind == internal.DiscoveryKindStatic {
		cfg.Cluster.DiscoveryHTTPHost = ""
	}
	discovery, err := cluster.NewServiceDiscovery(logger, peers, cfg.NodeID, discoveryKind, cfg.Cluster.DiscoveryHTTPHost, discoveryTick)
	if err != nil {
		return err
	}
	if discoveryKind == internal.DiscoveryKindHttp {
		// run discovery
		go discovery.Run(ctx)
	}

	// Create Cluster QUIC server (internal communication)
	quicServer, err := cluster.NewClusterQUIC(
		cfg.NodeID,
		int(cfg.Cluster.QUICPort),
		cfg.Cluster.CertPath,
		cfg.Cluster.KeyPath,
		cfg.Cluster.CACertPath,
		cfg.GetClusterAddr(),
		logger,
		discovery,
	)
	if err != nil {
		return fmt.Errorf("failed to create cluster QUIC server: %w", err)
	}

	if len(cfg.Cluster.InitialVoters) == 0 {
		return fmt.Errorf("initial voters should not be empty")
	}

	// Create ClusterAgent
	agent, err := cluster.NewClusterAgent(
		ctx,
		cancel,
		cfg.NodeID,
		cfg.Cluster,
		cfg.DataDir,
		toClusterCh,
		handler,
		quicServer,
		&status,
		&currentTerm,
		&members,
		&peerStore,
		leaderID,
		discovery,
		logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create cluster agent: %w", err)
	}

	topicManager := state.NewTopicManager(
		topicCmdCh,
		commandRouter,
		heartbeatRouter,
		toClusterCh, // dropProposalCh
		&status,
		logger,
		&cfg.Partition,
		state.DefaultTopicManagerConfig(), // internal default
		ctx,
	)

	// Start everything
	go func() {
		if err := agent.Start(); err != nil {
			logger.Error("Cluster agent stopped unexpectedly", zap.Error(err))
			cancel()
		}
	}()

	go func() {
		if err := gateway.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error("Gateway stopped unexpectedly", zap.Error(err))
			cancel()
		}
	}()

	go topicManager.Run()

	adminAPI := api.NewAdminAPI(
		cfg.ApiConfig.ListenAddr,
		int(cfg.ApiConfig.ApiPort),
		cfg.ApiConfig.TokenPath,
		toClusterCh,
		&members,
		&peerStore,
		leaderID,
		logger,
	)

	go func() {
		if err := adminAPI.Run(ctx); err != nil {
			logger.Error("Admin Api stopped unexpectedly", zap.Error(err))
			cancel()
		}
	}()

	// Wait for shutdown signal
	<-ctx.Done()

	logger.Info("Shutting down Cue node...")

	return nil
}

func setupLogger(cfg internal.LoggingConfig, nodeID string) (*zap.Logger, error) {
	level, err := zap.ParseAtomicLevel(cfg.Level)
	if err != nil {
		level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	}

	config := zap.NewProductionConfig()
	config.Level = level
	config.OutputPaths = []string{cfg.OutputPath}

	if cfg.Format == "text" {
		config.Encoding = "console"
	}

	logger, err := config.Build()
	if err != nil {
		return nil, err
	}

	// Add nodeID as a field to all logs
	logger = logger.With(zap.String("node_id", nodeID))

	return logger, nil
}
