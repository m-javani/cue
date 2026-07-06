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

//go:build integration

package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/m-javani/cue/internal/testutils"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestProbeCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Get integration test directory and prepare certs
	integrationDir, err := os.Getwd()
	require.NoError(t, err)

	caCertDir := filepath.Join(integrationDir, "cert")
	_, err = testutils.CreateCA(caCertDir, "ca", 1, "")
	require.NoError(t, err)

	// Start the test cluster
	cluster, err := NewTestCluster(ctx,
		WithCertsDir(caCertDir),
		// We can override discovery if needed:
		// WithDiscoveryYAML(customDiscoveryData),
	)
	require.NoError(t, err)
	defer cluster.Terminate(ctx)

	// Give the cluster some time to form and elect a leader
	time.Sleep(8 * time.Second)

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	node := cluster.Nodes[0]
	targetAddr := fmt.Sprintf("%s:%s", node.Hostname, node.APIPort)

	sugar.Infof("Probing node %s at %s", node.Name, targetAddr)

	res, err := ProbeNode(ctx, targetAddr)
	require.NoError(t, err)

	require.True(t, res.HealthOK, "Health check should pass")
	require.NotEmpty(t, res.Cluster.LeaderID, "Leader should be elected")
	require.Contains(t, res.Cluster.Members.Voters, node.Name,
		"Node should be listed as voter")

	// Optional: log cluster state
	sugar.Infof("Cluster healthy! Leader: %s | Voters: %v",
		res.Cluster.LeaderID,
		res.Cluster.Members.Voters)

	logger.Info("PASSED: Cluster is healthy and running.")
}
