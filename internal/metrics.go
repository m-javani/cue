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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Partition Metrics
// =============================================================================
// PartitionMetrics - SINGLETON shared across all partitions
type PartitionMetrics struct {
	// These are metric VECTORS that accept topic label
	activeDepth   *prometheus.GaugeVec
	jobsAdded     *prometheus.CounterVec
	jobsCompleted *prometheus.CounterVec
	jobsRetried   *prometheus.CounterVec
	jobsDLQ       *prometheus.CounterVec
}

var (
	partitionMetricsInstance *PartitionMetrics
	partitionMetricsOnce     sync.Once
)

func GetPartitionMetrics() *PartitionMetrics {
	partitionMetricsOnce.Do(func() {
		partitionMetricsInstance = &PartitionMetrics{
			activeDepth: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "cue_active_queue_depth",
				Help: "Jobs waiting to be dispatched",
			}, []string{"topic"}),
			jobsAdded: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "cue_jobs_added_total",
				Help: "Total jobs added",
			}, []string{"topic"}),
			jobsCompleted: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "cue_jobs_completed_total",
				Help: "Total jobs completed",
			}, []string{"topic"}),
			jobsRetried: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "cue_jobs_retried_total",
				Help: "Total job retries",
			}, []string{"topic"}),
			jobsDLQ: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "cue_jobs_dead_letter_total",
				Help: "Jobs sent to dead letter",
			}, []string{"topic"}),
		}
	})
	return partitionMetricsInstance
}

// Methods now take topic as parameter
func (m *PartitionMetrics) JobAdded(topic string, n uint64) {
	m.jobsAdded.WithLabelValues(topic).Add(float64(n))
}

func (m *PartitionMetrics) JobCompleted(topic string, n uint64) {
	m.jobsCompleted.WithLabelValues(topic).Add(float64(n))
}

func (m *PartitionMetrics) JobRetried(topic string, n uint64) {
	m.jobsRetried.WithLabelValues(topic).Add(float64(n))
}

func (m *PartitionMetrics) JobDLQ(topic string, n uint64) {
	m.jobsDLQ.WithLabelValues(topic).Add(float64(n))
}

func (m *PartitionMetrics) SetActiveDepth(topic string, v uint32) {
	m.activeDepth.WithLabelValues(topic).Set(float64(v))
}

// cleanup when topic is removed
func (m *PartitionMetrics) RemoveTopic(topic string) {
	m.activeDepth.DeleteLabelValues(topic)
	m.jobsAdded.DeleteLabelValues(topic)
	m.jobsCompleted.DeleteLabelValues(topic)
	m.jobsRetried.DeleteLabelValues(topic)
	m.jobsDLQ.DeleteLabelValues(topic)
}

// =============================================================================
// Cluster Metrics
// =============================================================================

// ClusterMetrics caches metric references for cluster-level operations
type ClusterMetrics struct {
	// Connection
	connectionOpenedTotal   prometheus.Counter
	connectionAcceptedTotal prometheus.Counter
	connectionRejectedTotal prometheus.Counter
	connectionErrorTotal    prometheus.Counter
	connectionDroppedTotal  prometheus.Counter

	// Request Response
	messageSentTotal     prometheus.Counter
	messageReceivedTotal prometheus.Counter
	bytesSentTotal       prometheus.Counter
	bytesReceivedTotal   prometheus.Counter
	sendErrorTotal       prometheus.Counter
	receiveErrorTotal    prometheus.Counter

	// Condition
	leaderChangedTotal prometheus.Counter

	// WAL
	walFlushCountTotal  prometheus.Counter
	lastAppliedWalIndex prometheus.Gauge
}

var (
	clusterMetricsInstance *ClusterMetrics
	clusterMetricsOnce     sync.Once
)

// GetClusterMetrics returns the singleton ClusterMetrics instance
func GetClusterMetrics() *ClusterMetrics {
	clusterMetricsOnce.Do(func() {
		clusterMetricsInstance = &ClusterMetrics{
			// Connection
			connectionOpenedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_connection_opened_total",
				Help: "Total number of connections opened",
			}),
			connectionAcceptedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_connection_accepted_total",
				Help: "Total number of connections accepted",
			}),
			connectionRejectedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_connection_rejected_total",
				Help: "Total number of connections rejected",
			}),
			connectionErrorTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_connection_error_total",
				Help: "Total number of connection errors",
			}),
			connectionDroppedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_connection_dropped_total",
				Help: "Total number of connections dropped",
			}),

			// Request Response
			messageSentTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_message_sent_total",
				Help: "Total number of messages sent",
			}),
			messageReceivedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_message_received_total",
				Help: "Total number of messages received",
			}),
			bytesSentTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_bytes_sent_total",
				Help: "Total bytes sent",
			}),
			bytesReceivedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_bytes_received_total",
				Help: "Total bytes received",
			}),
			sendErrorTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_send_error_total",
				Help: "Total number of send errors",
			}),
			receiveErrorTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_receive_error_total",
				Help: "Total number of receive errors",
			}),

			// Condition
			leaderChangedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_leader_changed_total",
				Help: "Total number of leadership changes",
			}),

			// WAL
			walFlushCountTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_wal_flush_count_total",
				Help: "Total number of WAL flushes",
			}),
			lastAppliedWalIndex: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "cluster_last_applied_wal_index",
				Help: "Last applied WAL index",
			}),
		}
	})
	return clusterMetricsInstance
}

// Connection methods
func (m *ClusterMetrics) ConnectionOpened() {
	m.connectionOpenedTotal.Inc()
}

func (m *ClusterMetrics) ConnectionAccepted() {
	m.connectionAcceptedTotal.Inc()
}

func (m *ClusterMetrics) ConnectionRejected() {
	m.connectionRejectedTotal.Inc()
}

func (m *ClusterMetrics) ConnectionError() {
	m.connectionErrorTotal.Inc()
}

func (m *ClusterMetrics) ConnectionDropped() {
	m.connectionDroppedTotal.Inc()
}

// Request/Response methods
func (m *ClusterMetrics) MessageSent() {
	m.messageSentTotal.Inc()
}

func (m *ClusterMetrics) MessageReceived() {
	m.messageReceivedTotal.Inc()
}

func (m *ClusterMetrics) AddBytesSent(bytes uint64) {
	m.bytesSentTotal.Add(float64(bytes))
}

func (m *ClusterMetrics) AddBytesReceived(bytes uint64) {
	m.bytesReceivedTotal.Add(float64(bytes))
}

func (m *ClusterMetrics) SendError() {
	m.sendErrorTotal.Inc()
}

func (m *ClusterMetrics) ReceiveError() {
	m.receiveErrorTotal.Inc()
}

// Condition methods
func (m *ClusterMetrics) LeaderChanged() {
	m.leaderChangedTotal.Inc()
}

// WAL methods
func (m *ClusterMetrics) WalFlush() {
	m.walFlushCountTotal.Inc()
}

func (m *ClusterMetrics) SetLastAppliedWalIndex(index uint64) {
	m.lastAppliedWalIndex.Set(float64(index))
}

// =============================================================================
// Gateway Metrics
// =============================================================================

// GatewayMetrics - Only production-realistic failure modes
type GatewayMetrics struct {
	// What ops actually cares about
	activeProxies     prometheus.Gauge
	activeConnections prometheus.Gauge
	messagesReceived  prometheus.Counter
	messagesSent      prometheus.Counter

	// Real production failures only
	connectionFailures prometheus.Counter // Network issues, cert problems, timeouts
	networkErrors      prometheus.Counter // Timeouts, resets, broken connections
}

var (
	gatewayMetricsInstance *GatewayMetrics
	gatewayMetricsOnce     sync.Once
)

func GetGatewayMetrics() *GatewayMetrics {
	gatewayMetricsOnce.Do(func() {
		gatewayMetricsInstance = &GatewayMetrics{
			activeProxies: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "cue_gateway_active_proxies",
				Help: "Number of connected proxy instances",
			}),
			activeConnections: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "cue_gateway_active_connections",
				Help: "Number of active QUIC connections (2 per proxy)",
			}),
			messagesReceived: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cue_gateway_messages_received_total",
				Help: "Messages received from proxies",
			}),
			messagesSent: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cue_gateway_messages_sent_total",
				Help: "Messages sent to proxies",
			}),
			connectionFailures: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cue_gateway_connection_failures_total",
				Help: "Failed connection attempts (network, TLS, timeout)",
			}),
			networkErrors: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cue_gateway_network_errors_total",
				Help: "Connection drops, timeouts, reset streams",
			}),
		}
	})
	return gatewayMetricsInstance
}

// Methods
func (m *GatewayMetrics) ProxyConnected()    { m.activeProxies.Inc() }
func (m *GatewayMetrics) ProxyDisconnected() { m.activeProxies.Dec() }
func (m *GatewayMetrics) ConnectionOpened()  { m.activeConnections.Inc() }
func (m *GatewayMetrics) ConnectionClosed()  { m.activeConnections.Dec() }
func (m *GatewayMetrics) MessageReceived()   { m.messagesReceived.Inc() }
func (m *GatewayMetrics) MessageSent()       { m.messagesSent.Inc() }
func (m *GatewayMetrics) ConnectionFailed()  { m.connectionFailures.Inc() }
func (m *GatewayMetrics) NetworkError()      { m.networkErrors.Inc() }

// =============================================================================
// TopicManager Metrics
// =============================================================================

// TopicManagerMetrics - tracks topic lifecycle only
type TopicManagerMetrics struct {
	activeTopics  prometheus.Gauge
	topicsCreated prometheus.Counter
	topicsRemoved prometheus.Counter
}

var (
	topicMetricsInstance *TopicManagerMetrics
	topicMetricsOnce     sync.Once
)

func GetTopicManagerMetrics() *TopicManagerMetrics {
	topicMetricsOnce.Do(func() {
		topicMetricsInstance = &TopicManagerMetrics{
			activeTopics: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "cue_topic_manager_active_topics",
				Help: "Number of active topic partitions",
			}),
			topicsCreated: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cue_topic_manager_topics_created_total",
				Help: "Total topics created",
			}),
			topicsRemoved: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cue_topic_manager_topics_removed_total",
				Help: "Total topics removed",
			}),
		}
	})
	return topicMetricsInstance
}

// Methods
func (m *TopicManagerMetrics) TopicCreated() {
	m.topicsCreated.Inc()
	m.activeTopics.Inc()
}

func (m *TopicManagerMetrics) TopicRemoved() {
	m.topicsRemoved.Inc()
	m.activeTopics.Dec()
}

func (m *TopicManagerMetrics) SetActiveTopics(count int) {
	m.activeTopics.Set(float64(count))
}
