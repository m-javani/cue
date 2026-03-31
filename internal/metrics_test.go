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
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to reset metrics for testing
func resetMetrics() {
	// Reset all singleton instances
	partitionMetricsInstance = nil
	partitionMetricsOnce = sync.Once{}

	clusterMetricsInstance = nil
	clusterMetricsOnce = sync.Once{}

	gatewayMetricsInstance = nil
	gatewayMetricsOnce = sync.Once{}

	topicMetricsInstance = nil
	topicMetricsOnce = sync.Once{}

	// Reset the default registry
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
}

func TestPartitionMetrics(t *testing.T) {
	resetMetrics()

	metrics := GetPartitionMetrics()
	assert.NotNil(t, metrics)

	topic := "test-topic"
	topic2 := "test-topic-2"

	t.Run("JobAdded", func(t *testing.T) {
		metrics.JobAdded(topic, 5)
		metrics.JobAdded(topic, 3)

		val := getCounterValue(t, metrics.jobsAdded, topic)
		assert.Equal(t, 8.0, val)
	})

	t.Run("JobCompleted", func(t *testing.T) {
		metrics.JobCompleted(topic, 4)
		metrics.JobCompleted(topic, 2)

		val := getCounterValue(t, metrics.jobsCompleted, topic)
		assert.Equal(t, 6.0, val)
	})

	t.Run("JobRetried", func(t *testing.T) {
		metrics.JobRetried(topic, 1)
		metrics.JobRetried(topic, 2)

		val := getCounterValue(t, metrics.jobsRetried, topic)
		assert.Equal(t, 3.0, val)
	})

	t.Run("JobDLQ", func(t *testing.T) {
		metrics.JobDLQ(topic, 1)

		val := getCounterValue(t, metrics.jobsDLQ, topic)
		assert.Equal(t, 1.0, val)
	})

	t.Run("SetActiveDepth", func(t *testing.T) {
		metrics.SetActiveDepth(topic, 100)
		metrics.SetActiveDepth(topic, 75)

		val := getGaugeValue(t, metrics.activeDepth, topic)
		assert.Equal(t, 75.0, val)
	})

	t.Run("MultipleTopics", func(t *testing.T) {
		metrics.JobAdded(topic2, 10)
		metrics.SetActiveDepth(topic2, 50)

		val1 := getCounterValue(t, metrics.jobsAdded, topic)
		val2 := getCounterValue(t, metrics.jobsAdded, topic2)

		assert.Equal(t, 8.0, val1)
		assert.Equal(t, 10.0, val2)

		val3 := getGaugeValue(t, metrics.activeDepth, topic)
		val4 := getGaugeValue(t, metrics.activeDepth, topic2)

		assert.Equal(t, 75.0, val3)
		assert.Equal(t, 50.0, val4)
	})

	t.Run("RemoveTopic", func(t *testing.T) {
		// Add some metrics
		metrics.JobAdded(topic, 1)
		metrics.SetActiveDepth(topic, 10)

		// Remove topic
		metrics.RemoveTopic(topic)

		// All metrics for that topic should be removed
		val := getGaugeValue(t, metrics.activeDepth, topic)
		assert.Equal(t, 0.0, val, "removed gauge should return 0")

		// Verify other topic still exists
		val2 := getGaugeValue(t, metrics.activeDepth, topic2)
		assert.Equal(t, 50.0, val2)
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		// Reset for this test
		resetMetrics()

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				m := GetPartitionMetrics()
				assert.NotNil(t, m)
			}()
		}
		wg.Wait()
	})
}

func TestClusterMetrics(t *testing.T) {
	resetMetrics()

	metrics := GetClusterMetrics()
	assert.NotNil(t, metrics)

	t.Run("ConnectionMetrics", func(t *testing.T) {
		metrics.ConnectionOpened()
		metrics.ConnectionOpened()
		metrics.ConnectionAccepted()
		metrics.ConnectionRejected()
		metrics.ConnectionError()
		metrics.ConnectionDropped()

		assertCounterValue(t, metrics.connectionOpenedTotal, 2.0)
		assertCounterValue(t, metrics.connectionAcceptedTotal, 1.0)
		assertCounterValue(t, metrics.connectionRejectedTotal, 1.0)
		assertCounterValue(t, metrics.connectionErrorTotal, 1.0)
		assertCounterValue(t, metrics.connectionDroppedTotal, 1.0)
	})

	t.Run("RequestResponseMetrics", func(t *testing.T) {
		metrics.MessageSent()
		metrics.MessageSent()
		metrics.MessageReceived()
		metrics.AddBytesSent(1024)
		metrics.AddBytesSent(512)
		metrics.AddBytesReceived(2048)
		metrics.SendError()
		metrics.ReceiveError()

		assertCounterValue(t, metrics.messageSentTotal, 2.0)
		assertCounterValue(t, metrics.messageReceivedTotal, 1.0)
		assertCounterValue(t, metrics.bytesSentTotal, 1536.0)
		assertCounterValue(t, metrics.bytesReceivedTotal, 2048.0)
		assertCounterValue(t, metrics.sendErrorTotal, 1.0)
		assertCounterValue(t, metrics.receiveErrorTotal, 1.0)
	})

	t.Run("ConditionMetrics", func(t *testing.T) {
		metrics.LeaderChanged()
		metrics.LeaderChanged()
		metrics.LeaderChanged()

		assertCounterValue(t, metrics.leaderChangedTotal, 3.0)
	})

	t.Run("WALMetrics", func(t *testing.T) {
		metrics.WalFlush()
		metrics.WalFlush()
		metrics.SetLastAppliedWalIndex(12345)
		metrics.SetLastAppliedWalIndex(12346)

		assertCounterValue(t, metrics.walFlushCountTotal, 2.0)
		assertGaugeValue(t, metrics.lastAppliedWalIndex, 12346.0)
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		resetMetrics()

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				m := GetClusterMetrics()
				assert.NotNil(t, m)
			}()
		}
		wg.Wait()
	})
}

func TestGatewayMetrics(t *testing.T) {
	resetMetrics()

	metrics := GetGatewayMetrics()
	assert.NotNil(t, metrics)

	t.Run("ProxyConnections", func(t *testing.T) {
		metrics.ProxyConnected()
		metrics.ProxyConnected()
		metrics.ProxyDisconnected()

		assertGaugeValue(t, metrics.activeProxies, 1.0)
	})

	t.Run("ConnectionMetrics", func(t *testing.T) {
		metrics.ConnectionOpened()
		metrics.ConnectionOpened()
		metrics.ConnectionOpened()
		metrics.ConnectionClosed()

		assertGaugeValue(t, metrics.activeConnections, 2.0)
	})

	t.Run("MessageMetrics", func(t *testing.T) {
		metrics.MessageReceived()
		metrics.MessageReceived()
		metrics.MessageReceived()
		metrics.MessageSent()
		metrics.MessageSent()

		assertCounterValue(t, metrics.messagesReceived, 3.0)
		assertCounterValue(t, metrics.messagesSent, 2.0)
	})

	t.Run("ErrorMetrics", func(t *testing.T) {
		metrics.ConnectionFailed()
		metrics.ConnectionFailed()
		metrics.NetworkError()

		assertCounterValue(t, metrics.connectionFailures, 2.0)
		assertCounterValue(t, metrics.networkErrors, 1.0)
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		resetMetrics()

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				m := GetGatewayMetrics()
				assert.NotNil(t, m)
			}()
		}
		wg.Wait()
	})
}

func TestTopicManagerMetrics(t *testing.T) {
	resetMetrics()

	metrics := GetTopicManagerMetrics()
	assert.NotNil(t, metrics)

	t.Run("TopicLifecycle", func(t *testing.T) {
		metrics.TopicCreated()
		metrics.TopicCreated()
		metrics.TopicCreated()
		metrics.TopicRemoved()

		assertCounterValue(t, metrics.topicsCreated, 3.0)
		assertCounterValue(t, metrics.topicsRemoved, 1.0)
		assertGaugeValue(t, metrics.activeTopics, 2.0)
	})

	t.Run("SetActiveTopics", func(t *testing.T) {
		metrics.SetActiveTopics(10)
		assertGaugeValue(t, metrics.activeTopics, 10.0)

		metrics.SetActiveTopics(5)
		assertGaugeValue(t, metrics.activeTopics, 5.0)
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		resetMetrics()

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				m := GetTopicManagerMetrics()
				assert.NotNil(t, m)
			}()
		}
		wg.Wait()
	})
}

// Helper functions to get metric values

func getCounterValue(t *testing.T, collector prometheus.Collector, labelValues ...string) float64 {
	t.Helper()

	var metric prometheus.Metric
	var err error

	switch v := collector.(type) {
	case *prometheus.CounterVec:
		metric, err = v.GetMetricWithLabelValues(labelValues...)
		require.NoError(t, err)
	case prometheus.Counter:
		metric = v
	default:
		t.Fatalf("unsupported counter type: %T", collector)
	}

	pb := &dto.Metric{}
	err = metric.Write(pb)
	require.NoError(t, err)

	if pb.Counter == nil {
		return 0.0
	}
	return pb.Counter.GetValue()
}

func getGaugeValue(t *testing.T, collector prometheus.Collector, labelValues ...string) float64 {
	t.Helper()

	var metric prometheus.Metric
	var err error

	switch v := collector.(type) {
	case *prometheus.GaugeVec:
		metric, err = v.GetMetricWithLabelValues(labelValues...)
		require.NoError(t, err)
	case prometheus.Gauge:
		metric = v
	default:
		t.Fatalf("unsupported gauge type: %T", collector)
	}

	pb := &dto.Metric{}
	err = metric.Write(pb)
	require.NoError(t, err)

	if pb.Gauge == nil {
		return 0.0
	}
	return pb.Gauge.GetValue()
}

func assertCounterValue(t *testing.T, collector prometheus.Collector, expected float64) {
	t.Helper()
	val := getCounterValue(t, collector)
	assert.Equal(t, expected, val)
}

func assertGaugeValue(t *testing.T, collector prometheus.Collector, expected float64) {
	t.Helper()
	val := getGaugeValue(t, collector)
	assert.Equal(t, expected, val)
}

// TestMetricsRegistration ensures metrics are properly registered
func TestMetricsRegistration(t *testing.T) {
	resetMetrics()

	// Initialize all metrics
	_ = GetPartitionMetrics()
	_ = GetClusterMetrics()
	_ = GetGatewayMetrics()
	_ = GetTopicManagerMetrics()

	// Verify they're registered by checking if we can gather metrics
	metrics, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	assert.NotEmpty(t, metrics, "metrics should be registered")
}

// TestMetricsNotDuplicated ensures we don't register duplicate metrics
func TestMetricsNotDuplicated(t *testing.T) {
	resetMetrics()

	// First initialization
	_ = GetPartitionMetrics()
	_ = GetClusterMetrics()
	_ = GetGatewayMetrics()
	_ = GetTopicManagerMetrics()

	// Second initialization should not cause duplicate registration
	// because singleton pattern prevents re-initialization
	_ = GetPartitionMetrics()
	_ = GetClusterMetrics()
	_ = GetGatewayMetrics()
	_ = GetTopicManagerMetrics()

	// Should not panic
	assert.True(t, true, "multiple initializations should not cause panic")
}

// Benchmark metric operations
func BenchmarkPartitionMetrics(b *testing.B) {
	resetMetrics()
	metrics := GetPartitionMetrics()
	topic := "bench-topic"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metrics.JobAdded(topic, 1)
		metrics.SetActiveDepth(topic, uint32(i))
	}
}

func BenchmarkClusterMetrics(b *testing.B) {
	resetMetrics()
	metrics := GetClusterMetrics()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metrics.ConnectionOpened()
		metrics.MessageSent()
		metrics.AddBytesSent(1024)
		metrics.LeaderChanged()
	}
}
