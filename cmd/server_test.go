package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/testutils"
	"github.com/stretchr/testify/require"
)

func TestSingleNodeCluster(t *testing.T) {
	cfg := &internal.Config{
		NodeID:  "node1",
		DataDir: "",
		Cluster: internal.ClusterConfig{
			InitialVoters:        []string{"node1"},
			ListenAddr:           "127.0.0.0:17777",
			QUICPort:             17777,
			SnapshotIntervalSec:  10,
			SnapshotTriggerCount: 10000,
			WALFlushThreshold:    10000,
			DLQMaxSizeBytes:      1000,
			RaftTickMs:           100,
			RaftHeartbeatTick:    5,
			RaftElectionTick:     5,
			DiscoveryKind:        "static",
			DiscoveryYMLPath:     "../discovery.yml",
			DiscoveryHTTPHost:    "",
		},
		Proxy: internal.ProxyConfig{
			Addr: "127.0.0.1:17778",
			Port: 17778,
		},
		WAL: internal.WALConfig{
			CompactAfterBytes: 100000,
			SyncInterval:      "10s",
		},
		Partition: internal.PartitionConfig{
			ActiveQueueCapacity: 100000,
			MaxRetries:          1,
			MaxBackoffSec:       10,
			DispatchBatchSize:   10,
			DLQMaxBytes:         10000,
			DLQMaxAgeMs:         10,
			PartitionTickMs:     100,
			ProxyCleanupTickSec: 10,
			HeartbeatTickMs:     5000,
		},
		Logging: internal.LoggingConfig{
			Level:      "info",
			Format:     "json",
			OutputPath: "stdout",
		},
		ApiConfig: internal.ApiConfig{
			ListenAddr:     "127.0.0.1:9090",
			ApiPort:        9090,
			TokenPath:      "../auth.yml",
			TimeoutSeconds: 10,
		},
	}
	cfg.NodeID = "node1"
	cfg.Cluster.QUICPort = 17777
	cfg.Cluster.InitialVoters = []string{cfg.NodeID}

	baseDir := testutils.GetTesDataPath()
	testBaseDir := filepath.Join(baseDir, fmt.Sprintf("test-%s", t.Name()))
	cfg.DataDir = testBaseDir

	err := os.MkdirAll(baseDir, 0755)
	require.NoError(t, err)
	err = os.MkdirAll(testBaseDir, 0755)
	require.NoError(t, err)

	certBasePath := testutils.GetCertsPath() + "/" + t.Name()
	ca, err := testutils.CreateCA(certBasePath, "ca", 1, "localhost")
	require.NoError(t, err)
	cfg.Cluster.CACertPath = ca.CertPath
	cfg.Proxy.CAPath = ca.CertPath
	certDir := certBasePath
	nc := testutils.NodeCert{
		NodeIdentity: cfg.NodeID, // user name for cert
		ServerNames:  []string{cfg.NodeID + ".localhost"},
	}
	certPath, keyPath, err := testutils.CreateNodeCert(certDir, ca, nc, 1)
	require.NoError(t, err)
	cfg.Cluster.CertPath = certPath
	cfg.Cluster.KeyPath = keyPath
	cfg.Proxy.CertPath = certPath
	cfg.Proxy.KeyPath = keyPath

	err = Run(cfg)
	require.NoError(t, err)

}
