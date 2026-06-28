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

package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m-javani/cue/internal/model"
	"github.com/m-javani/cue/internal/state"
	"github.com/m-javani/cue/internal/testutils"
	"github.com/m-javani/cue/pkg/verifier"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
)

// GatewayTester is the main test harness
type GatewayTester struct {
	t         *testing.T
	Gateway   *Gateway
	Proxies   *Proxies
	Internals *InternalsMock
	logger    *zap.Logger
}

func getTestCertPath(certDir, nodeID string) (certPath, keyPath, caPath string) {
	certPath = filepath.Join(certDir, nodeID+".pem")
	keyPath = filepath.Join(certDir, nodeID+"_key.pem")
	caPath = filepath.Join(certDir, "ca_cert.pem")
	return
}

// NewGatewayTester creates a ready-to-use test environment
func NewGatewayTester(t *testing.T) *GatewayTester {
	t.Helper()
	logger, _ := zap.NewDevelopment()

	// Create per-test certificate directory (same pattern as cluster helper)
	certBasePath := testutils.GetCertsPath() + "/" + t.Name()
	require.NoError(t, os.MkdirAll(certBasePath, 0755))

	// Create CA once for this test
	ca, err := testutils.CreateCA(certBasePath, "ca", 1, "localhost")
	require.NoError(t, err)

	// Generate gateway certificate
	nc := testutils.NodeCert{
		NodeIdentity: "node1",
		ServerNames:  []string{"node1.localhost"},
	}
	certPath, keyPath, caPath := getTestCertPath(certBasePath, "node1")
	_, _, err = testutils.CreateNodeCert(certBasePath, ca, nc, 1)
	require.NoError(t, err)

	toClusterCh := make(chan model.Command, 256)
	topicCh := make(chan state.TopicCommand, 64)
	router := state.NewHeartbeatRouter()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Start internal mock first
	mock := NewInternalsMock(topicCh, toClusterCh, router, ctx)
	go mock.run()

	// Create and start Gateway
	status := atomic.Uint32{}
	status.Store(model.NodeStatusLeaderActive.ToUin32())
	currentTerm := atomic.Uint64{}
	currentTerm.Store(0)
	members := &model.Members{
		Voters:   []string{},
		Learners: []string{},
	}
	leaderID := &atomic.Value{}
	leaderID.Store("")

	gw, err := NewGateway(
		"node1",
		certPath,
		keyPath,
		caPath,
		"127.0.0.1:0",
		logger,
		topicCh,
		router,
		toClusterCh,
		&status,
		&currentTerm,
		members,
		leaderID,
		verifier.CNVerifier{},
	)
	require.NoError(t, err)

	// Start gateway acceptor
	go func() {
		if err := gw.Run(ctx); err != nil && ctx.Err() == nil {
			t.Logf("Gateway Run stopped: %v", err)
		}
	}()

	// Wait for gateway to be listening
	time.Sleep(300 * time.Millisecond)

	// Create proxies manager with the certificate directory and CA
	proxies := NewProxies(ctx, gw.Addr().String(), logger, certBasePath, ca)

	tester := &GatewayTester{
		t:         t,
		Gateway:   gw,
		Proxies:   proxies,
		Internals: mock,
		logger:    logger,
	}

	t.Cleanup(func() {
		tester.Close()
		// Clean up certificate directory for this test
		os.RemoveAll(certBasePath)
	})

	// Add default proxy
	_, err = proxies.AddProxy("node2")
	require.NoError(t, err)

	return tester
}

func (gt *GatewayTester) Close() {
	gt.Proxies.Stop()
	if gt.Gateway != nil {
		gt.Gateway.Close()
	}
	gt.Internals.Stop()
}

// ==========================================
// FakeProxy (Client-only)
// ==========================================

type Proxy struct {
	ID           string
	inboundConn  *quic.Conn
	outboundConn *quic.Conn
	gatewayAddr  string
	logger       *zap.Logger
	ctx          context.Context
	cancel       context.CancelFunc
	certDir      string
	ca           *testutils.CAInfo
	wg           sync.WaitGroup

	topics     map[string]int      // topic -> consumer count
	requests   map[string]bool     // reqID -> received response
	dispatches map[string][]string // topic -> job IDs received
	nextReqID  atomic.Uint64
	mu         sync.RWMutex
}

func (p *Proxy) Connect(ctx context.Context, direction ConnectionType) (*quic.Conn, error) {
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tlsConfig, err := p.tlsConfigForProxy()
	if err != nil {
		return nil, fmt.Errorf("failed to create TLS config for proxy %s: %w", p.ID, err)
	}

	conn, err := quic.DialAddr(connectCtx, p.gatewayAddr, tlsConfig, nil)
	if err != nil {
		return nil, fmt.Errorf("quic dial failed to %s: %w", p.gatewayAddr, err)
	}

	// === Handshake ===
	stream, err := conn.OpenStreamSync(connectCtx)
	if err != nil {
		_ = conn.CloseWithError(0, "handshake stream failed")
		return nil, err
	}
	defer stream.Close()

	handshake := Handshake{
		ProxyID:         p.ID,
		ConnectionType:  direction,
		ProtocolVersion: 1,
	}

	if err := msgpack.NewEncoder(stream).Encode(handshake); err != nil {
		return nil, err
	}

	var resp HandshakeResponse
	if err := msgpack.NewDecoder(stream).Decode(&resp); err != nil {
		return nil, err
	}

	if resp.Status != "ok" {
		return nil, fmt.Errorf("handshake rejected: %s", resp.Message)
	}

	p.logger.Info("Proxy connected successfully",
		zap.String("proxy_id", p.ID),
		zap.String("direction", string(direction)))

	return conn, nil
}

// tlsConfigForProxy returns a CLIENT TLS config suitable for tests
func (p *Proxy) tlsConfigForProxy() (*tls.Config, error) {
	certPath, keyPath, caPath := getTestCertPath(p.certDir, p.ID)

	// Load client certificate + key
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load client keypair: %w", err)
	}

	// Load CA to trust gateway
	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true,
		ServerName:         "node1.localhost",
	}, nil
}

func (p *Proxy) Run() {
	p.wg.Add(1)
	defer p.wg.Done()

	for {
		stream, err := p.outboundConn.AcceptStream(p.ctx)
		if err != nil {
			if p.ctx.Err() != nil {
				return
			}
			p.logger.Debug("AcceptStream error", zap.Error(err))
			return
		}

		go p.handleOutboundStream(stream)
	}
}

func (p *Proxy) handleOutboundStream(stream *quic.Stream) {
	defer stream.Close()

	// Read length-prefixed message
	var lenBuf [4]byte
	if _, err := io.ReadFull(stream, lenBuf[:]); err != nil {
		p.logger.Debug("failed to read length prefix", zap.Error(err))
		return
	}

	length := binary.LittleEndian.Uint32(lenBuf[:])
	if length == 0 || length > 10*1024*1024 {
		p.logger.Debug("invalid message size", zap.Uint32("length", length))
		return
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(stream, data); err != nil {
		p.logger.Debug("failed to read payload", zap.Error(err))
		return
	}

	var msg model.ToProxyMessage
	if err := msgpack.Unmarshal(data, &msg); err != nil {
		p.logger.Debug("failed to unmarshal msg", zap.Error(err))
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	switch msg.Type {
	case "response":
		if msg.Response != nil {
			p.requests[msg.Response.RequestID] = true
		}
	case "outbound":
		if msg.Outbound != nil {
			var jobIDs []string
			for _, job := range msg.Outbound.Jobs {
				jobIDs = append(jobIDs, job.ID)
			}
			p.dispatches[msg.Outbound.Topic] = append(p.dispatches[msg.Outbound.Topic], jobIDs...)
		}
	}
}

func (p *Proxy) SendRequest(req model.ProxyRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := p.inboundConn.OpenStreamSync(ctx)
	if err != nil {
		if ctx.Err() == nil && p.ctx.Err() == nil {
			p.logger.Error("Failed to open stream for request",
				zap.String("req_id", req.RequestID),
				zap.Error(err))
		}
		return err
	}
	defer stream.Close()

	// Marshal the request to msgpack
	data, err := msgpack.Marshal(req)
	if err != nil {
		p.logger.Error("Failed to marshal request",
			zap.String("req_id", req.RequestID),
			zap.Error(err))
		return err
	}

	// Write length prefix (4 bytes, little endian)
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := stream.Write(lenBuf); err != nil {
		p.logger.Error("Failed to write length prefix",
			zap.String("req_id", req.RequestID),
			zap.Error(err))
		return err
	}

	// Write the marshaled data
	if _, err := stream.Write(data); err != nil {
		p.logger.Error("Failed to write request data",
			zap.String("req_id", req.RequestID),
			zap.Error(err))
		return err
	}

	// Optional: small delay after sending critical requests
	if req.Type == model.ReqAddTopic || req.Type == model.ReqAddJob {
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func (p *Proxy) SendHeartbeat() {
	// Similar to your previous SendHeartbeats but single call
	var capacities []model.TopicCapacity
	p.mu.RLock()
	for topic, count := range p.topics {
		capacities = append(capacities, model.TopicCapacity{
			Topic:            topic,
			ConsumptionScore: count,
		})
	}
	p.mu.RUnlock()

	hb := &model.HeartbeatReport{
		ProxyID:    p.ID,
		Timestamp:  time.Now().UnixMilli(),
		Capacities: capacities,
	}

	req := model.ProxyRequest{
		RequestID:       fmt.Sprintf("hb-%d", p.nextReqID.Add(1)),
		Type:            model.ReqHeartbeatReport,
		HeartbeatReport: hb,
	}
	_ = p.SendRequest(req) // fire and forget in tests
}

func (p *Proxy) AddConsumer(topic string) {
	p.mu.Lock()
	p.topics[topic]++
	p.mu.Unlock()
}

func (p *Proxy) SendAddTopic(topic string) {
	req := model.ProxyRequest{
		RequestID: fmt.Sprintf("addt-%d", p.nextReqID.Add(1)),
		Type:      model.ReqAddTopic,
		AddTopic:  &model.AddTopicPayload{Topic: topic},
	}
	_ = p.SendRequest(req)
}

func (p *Proxy) SendAddJob(job model.Job) error {
	req := model.ProxyRequest{
		RequestID: fmt.Sprintf("addj-%d", p.nextReqID.Add(1)),
		Type:      model.ReqAddJob,
		AddJob:    &model.AddJobPayload{Job: job},
	}
	return p.SendRequest(req)
}

func (p *Proxy) GetResponseStats() (totalTracked, responded int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	totalTracked = len(p.requests)
	for _, got := range p.requests {
		if got {
			responded++
		}
	}
	return
}

func (p *Proxy) GetDispatchedJobs(topic string) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return append([]string{}, p.dispatches[topic]...)
}

// CloseConnection closes only one direction (useful for reconnection tests)
func (p *Proxy) CloseConnection(direction ConnectionType) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch direction {
	case ConnectionTypeInbound:
		if p.inboundConn != nil {
			_ = p.inboundConn.CloseWithError(0, "test: inbound closed")
			p.inboundConn = nil
		}
	case ConnectionTypeOutbound:
		if p.outboundConn != nil {
			_ = p.outboundConn.CloseWithError(0, "test: outbound closed")
			p.outboundConn = nil
		}
	default:
		return errors.New("invalid direction: must be 'inbound' or 'outbound'")
	}
	return nil
}

// ReConnect reconnects only one direction
func (p *Proxy) ReConnect(direction ConnectionType) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn, err := p.Connect(p.ctx, direction)
	if err != nil {
		return fmt.Errorf("failed to reconnect %s: %w", direction, err)
	}

	switch direction {
	case ConnectionTypeInbound:
		p.inboundConn = conn
	case ConnectionTypeOutbound:
		p.outboundConn = conn
		go p.Run() // restart the listener goroutine for outbound
	default:
		return errors.New("invalid direction")
	}

	return nil
}

func (p *Proxy) Stop() {
	p.cancel()

	// Close connections with error to unblock AcceptStream
	if p.inboundConn != nil {
		_ = p.inboundConn.CloseWithError(0, "proxy stopped")
	}
	if p.outboundConn != nil {
		_ = p.outboundConn.CloseWithError(0, "proxy stopped")
	}

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		p.logger.Warn("Proxy Stop timeout", zap.String("proxy_id", p.ID))
	}
}

// ==========================================
// Proxies manager
// ==========================================

type Proxies struct {
	proxies     map[string]*Proxy
	gatewayAddr string
	ctx         context.Context
	logger      *zap.Logger
	mu          sync.RWMutex
	certDir     string
	ca          *testutils.CAInfo
}

func NewProxies(ctx context.Context,
	gatewayAddr string,
	logger *zap.Logger,
	certDir string,
	ca *testutils.CAInfo) *Proxies {
	return &Proxies{
		proxies:     make(map[string]*Proxy),
		gatewayAddr: gatewayAddr,
		ctx:         ctx,
		logger:      logger,
		certDir:     certDir,
		ca:          ca,
	}
}

func (p *Proxies) AddProxy(nodeID string) (*Proxy, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Generate certificate for this proxy
	nc := testutils.NodeCert{
		NodeIdentity: nodeID,
		ServerNames:  []string{"localhost"},
	}
	_, _, err := testutils.CreateNodeCert(p.certDir, p.ca, nc, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to generate cert for proxy %s: %w", nodeID, err)
	}

	prx := &Proxy{
		ID:          nodeID,
		gatewayAddr: p.gatewayAddr,
		logger:      p.logger,
		certDir:     p.certDir,
		ca:          p.ca,
		topics:      make(map[string]int),
		requests:    make(map[string]bool),
		dispatches:  make(map[string][]string),
	}

	childCtx, cancel := context.WithCancel(p.ctx)
	prx.ctx = childCtx
	prx.cancel = cancel

	prx.inboundConn, err = prx.Connect(childCtx, ConnectionTypeInbound)
	if err != nil {
		return nil, err
	}
	prx.outboundConn, err = prx.Connect(childCtx, ConnectionTypeOutbound)
	if err != nil {
		return nil, err
	}

	go prx.Run()

	p.proxies[nodeID] = prx
	return prx, nil
}

func (p *Proxies) Get(proxyID string) *Proxy {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.proxies[proxyID]
}

func (p *Proxies) Stop() {
	p.mu.Lock()
	for _, prx := range p.proxies {
		prx.Stop()
	}
	p.mu.Unlock()
}

// ==========================================
// InternalsMock (kept close to your original)
// ==========================================

type InternalsMock struct {
	topicMgrCh <-chan state.TopicCommand
	clusterCh  <-chan model.Command
	hbRouter   *state.HeartbeatRouter
	ctx        context.Context
	cancel     context.CancelFunc

	pushChs  map[string]chan<- model.ToGatewayMessage
	commands map[string][]model.Command
	topics   map[string]struct{}
	mu       sync.RWMutex
}

func NewInternalsMock(topicCh <-chan state.TopicCommand, clusterCh <-chan model.Command, router *state.HeartbeatRouter, parentCtx context.Context) *InternalsMock {
	ctx, cancel := context.WithCancel(parentCtx)
	return &InternalsMock{
		topicMgrCh: topicCh,
		clusterCh:  clusterCh,
		hbRouter:   router,
		ctx:        ctx,
		cancel:     cancel,
		pushChs:    make(map[string]chan<- model.ToGatewayMessage),
		commands:   make(map[string][]model.Command),
		topics:     make(map[string]struct{}),
	}
}

func (m *InternalsMock) run() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case cmd, ok := <-m.topicMgrCh:
			if !ok {
				return
			}
			m.handleTopicCommand(cmd)
		case cmd, ok := <-m.clusterCh:
			if !ok {
				return
			}
			m.handleClusterCommand(cmd)
		}
	}
}

func (m *InternalsMock) handleTopicCommand(cmd state.TopicCommand) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch cmd.Type {
	case "spawn":
		m.topics[cmd.Topic] = struct{}{}
		if cmd.RespCh != nil {
			cmd.RespCh <- model.ToProducerResponse{RequestID: cmd.RequestID, Status: "success"}
		}
	case "proxy_topology":
		if cmd.Topology != nil && cmd.Topology.Type == "add" {
			m.pushChs[cmd.Topology.ProxyID] = cmd.Topology.PushCh
			if cmd.RespCh != nil {
				cmd.RespCh <- model.ToProducerResponse{RequestID: cmd.RequestID, Status: "success"}
			}
		}
	}
}

func (m *InternalsMock) handleClusterCommand(cmd model.Command) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cmd.Type == model.CmdAddJob && cmd.AddJob != nil {
		topic := cmd.AddJob.Job.Topic
		m.commands[topic] = append(m.commands[topic], cmd)

		if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
			cmd.RespInfo.RespCh <- model.ToProducerResponse{
				RequestID: cmd.RespInfo.RequestID,
				Status:    "success",
			}
		}
	}
}

func (m *InternalsMock) DispatchOutboundMsgs(topic, proxyID string) error {
	m.mu.RLock()
	pushCh, ok := m.pushChs[proxyID]
	cmds := m.commands[topic]
	m.mu.RUnlock()

	if !ok {
		return errors.New("no push channel for proxy")
	}

	var jobs []*model.Job
	for _, c := range cmds {
		if c.Type == model.CmdAddJob && c.AddJob != nil {
			jobs = append(jobs, &c.AddJob.Job)
		}
	}

	// Build the proxy message
	proxyMsg := model.ToProxyMessage{
		Type: model.ProxyMessageOutbound,
		Outbound: &model.ToConsumerMessage{
			Topic:   topic,
			ProxyID: proxyID,
			Jobs:    jobs,
		},
	}
	// Marshal it
	data, err := msgpack.Marshal(proxyMsg)
	if err != nil {
		// Handle error - panic in test helper
		panic(fmt.Sprintf("failed to marshal ToProxyMessage: %v", err))
	}

	// Now create the gateway message with the marshaled bytes
	msg := model.ToGatewayMessage{
		Type:       model.ToGatewayMessageConsumer,
		ToConsumer: data, // []byte
	}
	select {
	case pushCh <- msg:
		return nil
	default:
		return errors.New("push channel full")
	}
}

func (m *InternalsMock) Stop() {
	m.cancel()
}

func (m *InternalsMock) GetTopics() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var ts []string
	for t := range m.topics {
		ts = append(ts, t)
	}
	return ts
}

// --------------------------------------------
// Happy path
// --------------------------------------------
func TestGateway_HappyPath_SingleProxy(t *testing.T) {
	gt := NewGatewayTester(t)
	defer gt.Close()

	prx := gt.Proxies.Get("node2")
	require.NotNil(t, prx)

	t.Log("Proxy 'node2' connected successfully")

	// === Step 1: Register consumer and send Heartbeat (fire-and-forget) ===
	prx.AddConsumer("topic-1")
	prx.SendHeartbeat()
	t.Log("Sent Heartbeat (fire-and-forget)")

	time.Sleep(200 * time.Millisecond)

	// === Step 2: Add Topic ===
	prx.SendAddTopic("topic-1")
	t.Log("Sent AddTopic")

	// === Step 3: Send Job ===
	job := model.Job{
		ID:    "job-001",
		Topic: "topic-1",
		Data:  []byte(`{"task": "happy-path-test"}`),
	}
	_ = prx.SendAddJob(job)
	t.Log("Sent AddJob")

	// Give time for responses
	time.Sleep(600 * time.Millisecond)

	// === Verify responses (only AddTopic + AddJob should get responses) ===
	total, responded := prx.GetResponseStats()
	t.Logf("Total tracked requests: %d | Responses received: %d", total, responded)

	require.Equal(t, total, responded, "All tracked requests should be acknowledged")
	require.GreaterOrEqual(t, total, 2, "Should have responses for AddTopic + AddJob")

	// === Dispatch the job through the mock and verify delivery ===
	err := gt.Internals.DispatchOutboundMsgs("topic-1", "node2")
	require.NoError(t, err, "Dispatch should succeed")

	time.Sleep(500 * time.Millisecond)

	dispatched := prx.GetDispatchedJobs("topic-1")
	require.Len(t, dispatched, 1, "Should have received 1 job")
	assert.Equal(t, "job-001", dispatched[0])

	// Verify topic was registered
	topics := gt.Internals.GetTopics()
	assert.Contains(t, topics, "topic-1")

	t.Log("Happy path test completed successfully")
}

// --------------------------------------------
// Multi proxy
// --------------------------------------------
func TestGateway_MultiProxy_Topology(t *testing.T) {
	gt := NewGatewayTester(t)
	defer gt.Close()

	// Add second and third prx (node2 is already added by default)
	prx2 := gt.Proxies.Get("node2")
	prx3, err := gt.Proxies.AddProxy("node3")
	require.NoError(t, err)
	prx4, err := gt.Proxies.AddProxy("node4")
	require.NoError(t, err)

	t.Log("Multiple proxies connected: node2, node3, node4")

	// === Setup consumers on different proxies ===
	prx2.AddConsumer("orders")
	prx2.AddConsumer("notifications")

	prx3.AddConsumer("payments")
	prx3.AddConsumer("orders") // overlapping topic

	prx4.AddConsumer("inventory")

	// Send heartbeats to update topology
	prx2.SendHeartbeat()
	prx3.SendHeartbeat()
	prx4.SendHeartbeat()

	time.Sleep(400 * time.Millisecond)

	// === Send AddTopic requests ===
	prx2.SendAddTopic("orders")
	prx3.SendAddTopic("payments")
	prx4.SendAddTopic("inventory")
	time.Sleep(300 * time.Millisecond)

	// === Send jobs to different topics ===
	jobs := []struct {
		topic string
		id    string
	}{
		{"orders", "job-o1"},
		{"payments", "job-p1"},
		{"inventory", "job-i1"},
		{"orders", "job-o2"}, // should go to both prx2 and prx3
	}

	for _, j := range jobs {
		job := model.Job{
			ID:    j.id,
			Topic: j.topic,
			Data:  []byte(`{"test": true}`),
		}
		// Send from any prx (doesn't matter for this test)
		_ = prx2.SendAddJob(job)
	}

	time.Sleep(500 * time.Millisecond)

	// === Verify responses ===
	for _, p := range []*Proxy{prx2, prx3, prx4} {
		total, responded := p.GetResponseStats()
		require.Equal(t, total, responded, "All tracked requests should be acknowledged on "+p.ID)
	}

	// === Dispatch jobs and verify correct routing ===
	testCases := []struct {
		topic    string
		prxID    string
		expected int
	}{
		{"orders", "node2", 2},
		{"orders", "node3", 2},
		{"payments", "node3", 1},
		{"inventory", "node4", 1},
	}

	for _, tc := range testCases {
		err := gt.Internals.DispatchOutboundMsgs(tc.topic, tc.prxID)
		require.NoError(t, err, "dispatch to "+tc.prxID+" should succeed")

		time.Sleep(300 * time.Millisecond)

		_ = len(prx2.GetDispatchedJobs(tc.topic)) // wait, need correct prx
		// Better to get correct prx
		var targetProxy *Proxy
		switch tc.prxID {
		case "node2":
			targetProxy = prx2
		case "node3":
			targetProxy = prx3
		case "node4":
			targetProxy = prx4
		}

		dispatched := targetProxy.GetDispatchedJobs(tc.topic)
		assert.Equal(t, tc.expected, len(dispatched),
			"Proxy %s should receive %d jobs for topic %s", tc.prxID, tc.expected, tc.topic)
	}

	t.Log("Multi-prx topology test completed")
}

// --------------------------------------------
// Proxy reconnect
// --------------------------------------------
func TestGateway_Proxy_Reconnection(t *testing.T) {
	gt := NewGatewayTester(t)
	defer gt.Close()

	prx := gt.Proxies.Get("node2")
	require.NotNil(t, prx)

	// === Initial setup ===
	prx.AddConsumer("orders")
	prx.SendHeartbeat()
	prx.SendAddTopic("orders")
	time.Sleep(400 * time.Millisecond)

	t.Log("Initial setup completed with prx node2")

	// === Send a job before disconnection ===
	job1 := model.Job{
		ID:    "job-before",
		Topic: "orders",
		Data:  []byte(`{"phase": "before"}`),
	}
	_ = prx.SendAddJob(job1)
	time.Sleep(300 * time.Millisecond)

	// === Test 1: Close Inbound connection only ===
	err := prx.CloseConnection("inbound")
	require.NoError(t, err)
	time.Sleep(300 * time.Millisecond)

	// Reconnect inbound
	err = prx.ReConnect("inbound")
	require.NoError(t, err)
	time.Sleep(400 * time.Millisecond)

	t.Log("Inbound reconnection successful")

	// === Test 2: Close Outbound connection only ===
	err = prx.CloseConnection(ConnectionTypeOutbound)
	require.NoError(t, err)
	time.Sleep(300 * time.Millisecond)

	// Reconnect outbound
	err = prx.ReConnect(ConnectionTypeOutbound)
	require.NoError(t, err)
	time.Sleep(400 * time.Millisecond)

	t.Log("Outbound reconnection successful")

	// Re-register after full reconnect
	prx.AddConsumer("orders")
	prx.SendHeartbeat()
	time.Sleep(300 * time.Millisecond)

	// === Send job after reconnection ===
	job2 := model.Job{
		ID:    "job-after",
		Topic: "orders",
		Data:  []byte(`{"phase": "after"}`),
	}
	_ = prx.SendAddJob(job2)
	time.Sleep(400 * time.Millisecond)

	// Verify responses for tracked requests
	total, responded := prx.GetResponseStats()
	require.Equal(t, total, responded, "All tracked requests should be acknowledged after reconnect")

	// === Dispatch both jobs and verify they were received ===
	err = gt.Internals.DispatchOutboundMsgs("orders", "node2")
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)

	dispatched := prx.GetDispatchedJobs("orders")
	require.Contains(t, dispatched, "job-before", "should receive job sent before reconnect")
	require.Contains(t, dispatched, "job-after", "should receive job sent after reconnect")

	t.Log("Proxy reconnection test completed successfully")
}

// --------------------------------------------
// Proxy removal
// --------------------------------------------
func TestGateway_Proxy_GracefulRemoval(t *testing.T) {
	gt := NewGatewayTester(t)
	defer gt.Close()

	prx := gt.Proxies.Get("node2")
	require.NotNil(t, prx)

	// === Initial setup ===
	prx.AddConsumer("orders")
	prx.AddConsumer("payments")
	prx.SendHeartbeat()
	prx.SendAddTopic("orders")
	prx.SendAddTopic("payments")
	time.Sleep(400 * time.Millisecond)

	t.Log("Initial prx setup completed")

	// Send a job before removal
	job1 := model.Job{
		ID:    "job-before-remove",
		Topic: "orders",
		Data:  []byte(`{"phase": "before"}`),
	}
	_ = prx.SendAddJob(job1)
	time.Sleep(300 * time.Millisecond)

	// === Gracefully stop the prx ===
	prx.Stop()
	time.Sleep(600 * time.Millisecond) // give gateway time to detect closure and clean up

	t.Log("Proxy node2 stopped gracefully")

	// === Add a new prx to replace it ===
	newProxy, err := gt.Proxies.AddProxy("node3")
	require.NoError(t, err)

	newProxy.AddConsumer("orders")
	newProxy.SendHeartbeat()
	newProxy.SendAddTopic("orders")
	time.Sleep(400 * time.Millisecond)

	t.Log("New prx node3 registered")

	// Send job after removal
	job2 := model.Job{
		ID:    "job-after-remove",
		Topic: "orders",
		Data:  []byte(`{"phase": "after"}`),
	}
	_ = newProxy.SendAddJob(job2)
	time.Sleep(300 * time.Millisecond)

	// === Dispatch and verify correct routing ===
	err = gt.Internals.DispatchOutboundMsgs("orders", "node3")
	require.NoError(t, err, "should successfully dispatch to active prx")

	time.Sleep(400 * time.Millisecond)

	dispatchedNew := newProxy.GetDispatchedJobs("orders")
	require.Contains(t, dispatchedNew, "job-after-remove",
		"new prx should receive the job")

	// Old prx should not receive new jobs after being stopped
	dispatchedOld := prx.GetDispatchedJobs("orders")
	assert.NotContains(t, dispatchedOld, "job-after-remove",
		"stopped prx should not receive new jobs")

	// Note: Dispatch to old prx may still succeed in mock, but in real system
	// partitions would stop sending due to missing heartbeats.

	t.Log("Proxy graceful removal test completed successfully")
}

// --------------------------------------------
// Multi topic
// --------------------------------------------
func TestGateway_MultipleTopics_PerProxy(t *testing.T) {
	gt := NewGatewayTester(t)
	defer gt.Close()

	prx := gt.Proxies.Get("node2")
	require.NotNil(t, prx)

	// Subscribe to multiple topics
	topics := []string{"orders", "payments", "notifications", "inventory"}
	for _, topic := range topics {
		prx.AddConsumer(topic)
		prx.SendAddTopic(topic)
	}

	// Important: Send heartbeat after topology changes
	prx.SendHeartbeat()
	time.Sleep(500 * time.Millisecond)

	t.Log("Proxy registered for multiple topics")

	// Send jobs across different topics
	jobData := map[string]string{
		"orders":        "job-o1",
		"payments":      "job-p1",
		"notifications": "job-n1",
		"inventory":     "job-i1",
	}

	for topic, jobID := range jobData {
		job := model.Job{
			ID:    jobID,
			Topic: topic,
			Data:  []byte(`{"test": "multi-topic"}`),
		}
		_ = prx.SendAddJob(job)
	}

	time.Sleep(600 * time.Millisecond)

	// Verify all requests were acknowledged
	total, responded := prx.GetResponseStats()
	require.Equal(t, total, responded, "All AddTopic + AddJob requests should be acknowledged")
	require.GreaterOrEqual(t, total, 8) // 4 AddTopic + 4 AddJob

	// Dispatch jobs for each topic and verify delivery through Gateway
	for topic, jobID := range jobData {
		err := gt.Internals.DispatchOutboundMsgs(topic, "node2")
		require.NoError(t, err)

		time.Sleep(300 * time.Millisecond)

		received := prx.GetDispatchedJobs(topic)
		assert.Contains(t, received, jobID,
			"Gateway should have routed job %s for topic %s", jobID, topic)
	}

	// Verify that prx received jobs for all topics
	for topic := range jobData {
		jobs := prx.GetDispatchedJobs(topic)
		assert.NotEmpty(t, jobs, "Should have received at least one job for "+topic)
	}

	t.Log("Multiple topics per prx test completed - Gateway routing verified")
}

// --------------------------------------------
// Gateway shutdown
// --------------------------------------------
func TestGateway_Shutdown_Cleanup(t *testing.T) {
	gt := NewGatewayTester(t)

	prx2 := gt.Proxies.Get("node2")
	_, err := gt.Proxies.AddProxy("node3")
	require.NoError(t, err)
	_, err = gt.Proxies.AddProxy("node4")
	require.NoError(t, err)

	// Minimal setup
	prx2.AddConsumer("orders")
	prx2.SendHeartbeat()
	prx2.SendAddTopic("orders")
	time.Sleep(300 * time.Millisecond)

	t.Log("Setup completed before shutdown")

	// === Measure shutdown time ===
	start := time.Now()
	gt.Close()
	shutdownDuration := time.Since(start)

	t.Logf("Gateway shutdown completed in %v", shutdownDuration)

	// Verify some work was done before shutdown
	_, responded := prx2.GetResponseStats()
	assert.Greater(t, responded, 0, "prx should have received responses before shutdown")

	// Sending after shutdown should fail gracefully
	err = prx2.SendAddJob(model.Job{
		ID:    "job-after-shutdown",
		Topic: "orders",
		Data:  []byte(`{}`),
	})
	assert.Error(t, err, "sending requests after gateway shutdown should fail")

	assert.Less(t, shutdownDuration, 3*time.Second, "shutdown should be reasonably fast")

	t.Log("Gateway shutdown and cleanup test completed successfully")
}

// --------------------------------------------
// Loopback flow - Error responses
// --------------------------------------------
// --------------------------------------------
// Test ToGatewayMessageLoopback path
// --------------------------------------------
func TestGateway_LoopbackErrorResponses(t *testing.T) {
	gt := NewGatewayTester(t)
	defer gt.Close()

	prx := gt.Proxies.Get("node2")
	require.NotNil(t, prx)

	// Setup: register consumer and topic
	prx.AddConsumer("test-topic")
	prx.SendHeartbeat()
	prx.SendAddTopic("test-topic")
	time.Sleep(500 * time.Millisecond)

	t.Run("loopback error when not leader", func(t *testing.T) {
		// Clear any existing responses
		prx.mu.Lock()
		prx.requests = make(map[string]bool)
		prx.mu.Unlock()

		// Change gateway status to not leader (follower)
		gt.Gateway.status.Store(model.NodeStatusFollowerActive.ToUin32())
		time.Sleep(200 * time.Millisecond)

		// Send a job request
		reqID := "loopback-test-001"
		job := model.Job{
			ID:    "job-loopback-test",
			Topic: "test-topic",
			Data:  []byte(`{"test": "loopback"}`),
		}

		req := model.ProxyRequest{
			RequestID: reqID,
			Type:      model.ReqAddJob,
			AddJob:    &model.AddJobPayload{Job: job},
		}

		err := prx.SendRequest(req)
		require.NoError(t, err)

		// Wait for the loopback response
		time.Sleep(600 * time.Millisecond)

		// Check if the error response was received
		prx.mu.RLock()
		_, responded := prx.requests[reqID]
		prx.mu.RUnlock()

		assert.True(t, responded, "error response should be received via loopback")

		// Reset status
		gt.Gateway.status.Store(model.NodeStatusLeaderActive.ToUin32())
		time.Sleep(200 * time.Millisecond)
	})

	t.Run("loopback queue_full error", func(t *testing.T) {
		// Clear previous responses
		prx.mu.Lock()
		prx.requests = make(map[string]bool)
		prx.mu.Unlock()

		// Set capacity to 0 to trigger queue_full
		prx.mu.Lock()
		prx.topics["test-topic"] = 0
		prx.mu.Unlock()

		// Send heartbeat to update gateway's topicStatus
		prx.SendHeartbeat()
		time.Sleep(500 * time.Millisecond)

		// Send a job - should get queue_full
		reqID := "loopback-queue-full-001"
		job := model.Job{
			ID:    "job-queue-full-loopback",
			Topic: "test-topic",
			Data:  []byte(`{"test": "queue full"}`),
		}

		req := model.ProxyRequest{
			RequestID: reqID,
			Type:      model.ReqAddJob,
			AddJob:    &model.AddJobPayload{Job: job},
		}

		err := prx.SendRequest(req)
		require.NoError(t, err)

		// Wait for error response
		time.Sleep(600 * time.Millisecond)

		// Verify error response was received
		prx.mu.RLock()
		_, responded := prx.requests[reqID]
		prx.mu.RUnlock()

		assert.True(t, responded, "queue_full error response should be received via loopback")

		// Clean up: restore capacity
		prx.mu.Lock()
		prx.topics["test-topic"] = 1
		prx.mu.Unlock()
		prx.SendHeartbeat()
		time.Sleep(300 * time.Millisecond)
	})
}
