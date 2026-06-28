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

// internal/gateway/server.go
package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
	"github.com/m-javani/cue/internal/state"
	"github.com/m-javani/cue/internal/utils"
	"github.com/m-javani/cue/pkg/verifier"
	"github.com/quic-go/quic-go"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
)

type ConnectionType string

const (
	ConnectionTypeInbound  ConnectionType = "inbound"
	ConnectionTypeOutbound ConnectionType = "outbound"
)

type HandshakeStatus string

const (
	HandshakeStatusOk  HandshakeStatus = "ok"
	HandshakeStatusErr HandshakeStatus = "error"
)

// Handshake represents the initial handshake message from proxy
type Handshake struct {
	ProxyID         string         `msgpack:"proxy_id"`
	ConnectionType  ConnectionType `msgpack:"connection_type"`
	ProtocolVersion int            `msgpack:"protocol_version"`
}

// HandshakeResponse sent back to proxy
type HandshakeResponse struct {
	Status  HandshakeStatus `msgpack:"status"`
	Message string          `msgpack:"message,omitempty"`
	NodeID  string          `msgpack:"node_id"`
}

type AddTopicRequest struct {
	RequestID string `msgpack:"request_id"`
	Topic     string `msgpack:"topic"`
}

type ProxyTopologyUpdateType string

const (
	ProxyAdd    ProxyTopologyUpdateType = "add"
	ProxyRemove ProxyTopologyUpdateType = "remove"
)

// ProxyTopologyUpdate is sent when proxy connects or disconnects
type ProxyTopologyUpdate struct {
	Type    ProxyTopologyUpdateType      `msgpack:"type"` // "add" or "remove"
	ProxyID string                       `msgpack:"proxy_id"`
	PushCh  chan model.ToConsumerMessage `msgpack:"-"` // not sent over wire
}

// ConnectionPair holds both connections for a single proxy
type ConnectionPair struct {
	ProxyID    string
	responseCh chan model.ToProducerResponse
	pushCh     chan model.ToGatewayMessage // async partition messages

	inboundConn  *quic.Conn
	outboundConn *quic.Conn

	inboundAlive  bool
	outboundAlive bool

	// for reconnect-removal race
	inboundGen  uint64
	outboundGen uint64

	registry *ProxyRegistry

	inboundCancel   context.CancelFunc
	outboundCancel  context.CancelFunc
	topicCmdCh      chan<- state.TopicCommand
	heartbeatRouter *state.HeartbeatRouter

	metrics *internal.GatewayMetrics

	status      *atomic.Uint32
	leaderID    *atomic.Value
	currentTerm *atomic.Uint64
	members     *model.Members

	mu     sync.RWMutex
	logger *zap.Logger

	topicStatus map[string]*model.PartitionHeartbeat
	statusMu    sync.RWMutex
}

func (pc *ConnectionPair) handlePartitionMessage(hearbeat *model.PartitionHeartbeat) {
	pc.statusMu.Lock()
	pc.topicStatus[hearbeat.Topic] = hearbeat
	pc.statusMu.Unlock()
}

func (pc *ConnectionPair) canAcceptJob(topic string) bool {
	pc.statusMu.RLock()
	defer pc.statusMu.RUnlock()

	status := pc.topicStatus[topic]
	if status == nil {
		return true // No status yet, be optimistic
	}
	return status.CanAccept
}

func (pc *ConnectionPair) sendBackErrorResponse(request_id, errStr string) {
	msg := model.ToGatewayMessage{
		Type: model.ToGatewayMessageLoopback,
		LoopbackMessage: &model.ToProducerResponse{
			RequestID: request_id,
			Status:    model.ToProxyRespStatusError,
			Error:     errStr,
		},
	}
	select {
	case pc.pushCh <- msg:
	default:
	}
}

// Close shuts down both connections and cancels handlers
func (p *ConnectionPair) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.inboundCancel != nil {
		p.inboundCancel()
	}
	if p.outboundCancel != nil {
		p.outboundCancel()
	}
	if p.inboundConn != nil {
		_ = p.inboundConn.CloseWithError(0, "connection pair closed")
	}
	if p.outboundConn != nil {
		_ = p.outboundConn.CloseWithError(0, "connection pair closed")
	}
}

// SetInbound registers an inbound connection and starts its handler
func (p *ConnectionPair) SetInbound(ctx context.Context,
	conn *quic.Conn,
	toClusterCh chan<- model.Command,
	topicCmdCh chan<- state.TopicCommand,
	hbRouter *state.HeartbeatRouter) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.topicCmdCh = topicCmdCh
	p.heartbeatRouter = hbRouter

	// Cancel old inbound if exists
	if p.inboundCancel != nil {
		p.inboundCancel()
	}
	if p.inboundConn != nil {
		_ = p.inboundConn.CloseWithError(0, "replaced by new inbound connection")
	}

	handlerCtx, cancel := context.WithCancel(ctx)

	p.inboundGen++
	gen := p.inboundGen

	p.inboundCancel = cancel
	p.inboundConn = conn
	p.inboundAlive = true

	// Start inbound handler with access to response channel
	go p.runInboundHandler(handlerCtx, conn, toClusterCh)

	go func(gen uint64, conn *quic.Conn) {
		<-conn.Context().Done()
		p.inboundClosed(gen)
	}(gen, conn)
}

// SetOutbound registers an outbound connection and starts its handler
func (p *ConnectionPair) SetOutbound(ctx context.Context, conn *quic.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Cancel old outbound if exists
	if p.outboundCancel != nil {
		p.outboundCancel()
	}
	if p.outboundConn != nil {
		_ = p.outboundConn.CloseWithError(0, "replaced by new outbound connection")
	}

	handlerCtx, cancel := context.WithCancel(ctx)

	p.outboundGen++
	gen := p.outboundGen

	p.outboundCancel = cancel
	p.outboundConn = conn
	p.outboundAlive = true

	// Start outbound handler with the pair's response channel
	go p.runOutboundHandler(handlerCtx, conn)

	go func(gen uint64, conn *quic.Conn) {
		<-conn.Context().Done()
		p.outboundClosed(gen)
	}(gen, conn)
}

func (p *ConnectionPair) runOutboundHandler(
	ctx context.Context,
	conn *quic.Conn,
) {
	p.mu.RLock()
	responseCh := p.responseCh
	pushCh := p.pushCh
	p.mu.RUnlock()

	writeMessage := func(data []byte) error {
		stream, err := conn.OpenStreamSync(ctx)
		if err != nil {
			p.logger.Error("OpenStreamSync failed", zap.Error(err), zap.String("proxy_id", p.ProxyID))
			p.metrics.NetworkError()
			return err
		}
		defer stream.Close()

		// Length prefix
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))

		n1, err := stream.Write(lenBuf)
		if err != nil {
			p.logger.Error("failed to write length prefix", zap.Error(err), zap.Int("written", n1))
			p.metrics.NetworkError()
			return err
		}

		n2, err := stream.Write(data)
		if err != nil {
			p.logger.Error("failed to write msgpack data", zap.Error(err), zap.Int("written", n2))
			p.metrics.NetworkError()
			return err
		}

		if err := stream.Close(); err != nil {
			p.logger.Warn("stream.Close() failed", zap.Error(err))
			p.metrics.NetworkError()
			// don't return here - data might still be sent
		}

		p.metrics.MessageSent()
		return nil
	}

	const heartbeatInterval = 1000 * time.Millisecond

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case resp, ok := <-responseCh:
			respCopy := resp
			if !ok {
				return
			}
			msg := model.ToProxyMessage{
				Type:     model.ProxyMessageResponse,
				Response: &respCopy,
			}
			data, err := msgpack.Marshal(msg)
			if err != nil {
				p.logger.Error("msgpack marshal failed", zap.Error(err))
				continue
			}
			if err := writeMessage(data); err != nil {
				p.logger.Warn("failed to write response to proxy stream")
				return
			}

		case toGatewayMsg, ok := <-pushCh:
			if !ok {
				return
			}

			switch toGatewayMsg.Type {
			case model.ToGatewayMessageHeartbeat:
				p.handlePartitionMessage(toGatewayMsg.Heartbeat)
			case model.ToGatewayMessageConsumer:
				if err := writeMessage(toGatewayMsg.ToConsumer); err != nil {
					p.logger.Warn("failed to write outbound message to proxy stream")
					return
				}
			case model.ToGatewayMessageLoopback:
				// comes from inbound connection - direct response
				msg := model.ToProxyMessage{
					Type:     model.ProxyMessageResponse,
					Response: toGatewayMsg.LoopbackMessage,
				}
				data, err := msgpack.Marshal(msg)
				if err != nil {
					p.logger.Error("msgpack marshal failed", zap.Error(err))
					continue
				}
				if err := writeMessage(data); err != nil {
					p.logger.Warn("failed to write outbound message to proxy stream")
					return
				}

			}

			// Send heartbeat
		case <-ticker.C:
			voters, learners := p.members.Get()
			msg := model.ToProxyMessage{
				Type: model.ProxyMessageHeartbeat,
				Heartbeat: &model.ToProxyHeartbeat{
					NodeStatus: model.ClusterNodeStatusFromUint32(p.status.Load()).String(),
					Voters:     voters,
					Learners:   learners,
					Leader:     utils.SafeLoadAtomicString(p.leaderID),
					Term:       p.currentTerm.Load(),
				},
			}
			data, err := msgpack.Marshal(msg)
			if err != nil {
				p.logger.Error("msgpack marshal failed", zap.Error(err))
				continue
			}
			if err := writeMessage(data); err != nil {
				p.logger.Warn("failed to write hearbeat message to proxy stream")
				return
			}
		}
	}
}

// readFramedMsgpack reads a length-prefixed msgpack message from a stream
func (p *ConnectionPair) readFramedMsgpack(stream *quic.Stream, v interface{}) error {
	// Use io.ReadFull for reliability with QUIC streams
	var lenBuf [4]byte
	if _, err := io.ReadFull(stream, lenBuf[:]); err != nil {
		return fmt.Errorf("read length prefix: %w", err)
	}

	length := binary.LittleEndian.Uint32(lenBuf[:])
	if length == 0 || length > 10*1024*1024 { // 10MB safety
		return fmt.Errorf("invalid message size: %d", length)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(stream, data); err != nil {
		return fmt.Errorf("read payload: %w", err)
	}

	if err := msgpack.Unmarshal(data, v); err != nil {
		return fmt.Errorf("msgpack unmarshal: %w", err)
	}

	return nil
}

// runInboundHandler reads requests from the inbound connection and forwards to cluster
func (p *ConnectionPair) runInboundHandler(ctx context.Context,
	conn *quic.Conn,
	toClusterCh chan<- model.Command) {
	for {
		// Accept a new stream
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.metrics.ConnectionFailed()
			return
		}

		// Decode request
		var req model.ProxyRequest
		if err := p.readFramedMsgpack(stream, &req); err != nil {
			p.logger.Warn("failed to read framed request from proxy",
				zap.Error(err),
				zap.String("proxy_id", p.ProxyID))

			stream.CancelRead(0)
			stream.Close()
			p.metrics.NetworkError()
			continue
		}

		p.metrics.MessageReceived()

		// Handle heartbeat messages
		if req.Type == model.ReqHeartbeatReport {
			batch := req.HeartbeatReport
			for _, entry := range batch.Capacities {
				hb := model.ProxyHeartbeat{
					ProxyID:          batch.ProxyID,
					Topic:            entry.Topic,
					ConsumptionScore: entry.ConsumptionScore,
					Timestamp:        time.Now().UnixMilli(),
				}

				if ch, exists := p.heartbeatRouter.GetChannel(entry.Topic); exists {
					select {
					case ch <- hb:
					case <-ctx.Done():
						return
					default:
						// drop
					}
				}
			}
			stream.Close()
			continue
		}

		// reject if  the node is not leader
		if model.ClusterNodeStatus(p.status.Load()) != model.NodeStatusLeaderActive {
			p.sendBackErrorResponse(req.RequestID, "node is not active leader")
			continue
		}

		p.mu.RLock()
		responseCh := p.responseCh
		p.mu.RUnlock()

		// Handle add-topic directly
		if req.Type == model.ReqAddTopic {
			topic := req.AddTopic.Topic
			select {
			case p.topicCmdCh <- state.TopicCommand{
				Type:      state.TopicCommandSpawn,
				Topic:     topic,
				RequestID: req.RequestID,
				RespCh:    responseCh,
			}:
			case <-ctx.Done():
				return
			}
			stream.Close()
			continue
		}

		// Handle add job or done
		var cmd model.Command
		switch req.Type {
		case model.ReqAddJob:
			if !p.canAcceptJob(req.AddJob.Job.Topic) {
				// send a queue is full message to gateway
				p.sendBackErrorResponse(req.RequestID, "queue_full")
				continue
			}

			cmd = model.Command{
				Type:   model.CmdAddJob,
				AddJob: req.AddJob,
				RespInfo: &model.RespInfo{
					RequestID: req.RequestID,
					RespCh:    responseCh,
				},
			}
		case model.ReqDone:
			cmd = model.Command{
				Type: model.CmdDone,
				Done: req.Done,
				RespInfo: &model.RespInfo{
					RequestID: req.RequestID,
					RespCh:    nil, // no response for done signal
				},
			}
		default:
			stream.CancelRead(0)
			stream.Close()
			p.metrics.NetworkError()
			continue
		}

		select {
		case toClusterCh <- cmd:
		case <-ctx.Done():
			return
		}
	}
}

func (p *ConnectionPair) inboundClosed(gen uint64) {
	p.mu.Lock()

	if gen != p.inboundGen {
		p.mu.Unlock()
		return
	}

	p.inboundAlive = false

	remove := !p.inboundAlive && !p.outboundAlive
	registry := p.registry

	p.mu.Unlock()

	if remove {
		registry.Remove(p.ProxyID)
	}
}

func (p *ConnectionPair) outboundClosed(gen uint64) {
	p.mu.Lock()

	if gen != p.outboundGen {
		p.mu.Unlock()
		return
	}

	p.outboundAlive = false

	remove := !p.inboundAlive && !p.outboundAlive
	registry := p.registry

	p.mu.Unlock()

	if remove {
		registry.Remove(p.ProxyID)
	}
}

// ProxyRegistry manages all connected proxies
type ProxyRegistry struct {
	pairs   map[string]*ConnectionPair
	mu      sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
	logger  *zap.Logger
	metrics *internal.GatewayMetrics

	topologyCh chan<- state.TopicCommand
}

// NewProxyRegistry creates a new registry
func NewProxyRegistry(ctx context.Context, logger *zap.Logger, metrics *internal.GatewayMetrics, cmdCh chan<- state.TopicCommand) *ProxyRegistry {
	ctx, cancel := context.WithCancel(ctx)
	return &ProxyRegistry{
		pairs:      make(map[string]*ConnectionPair),
		ctx:        ctx,
		cancel:     cancel,
		logger:     logger,
		topologyCh: cmdCh,
		metrics:    metrics,
	}
}

// GetOrCreate returns existing pair or creates a new one
func (r *ProxyRegistry) GetOrCreate(proxyID string, status *atomic.Uint32, leaderID *atomic.Value, currentTerm *atomic.Uint64, members *model.Members) *ConnectionPair {
	r.mu.Lock()
	defer r.mu.Unlock()

	if pair, exists := r.pairs[proxyID]; exists {
		return pair
	}

	pair := &ConnectionPair{
		ProxyID:     proxyID,
		responseCh:  make(chan model.ToProducerResponse, 10240),
		pushCh:      make(chan model.ToGatewayMessage, 10240),
		metrics:     r.metrics,
		registry:    r,
		status:      status,
		leaderID:    leaderID,
		currentTerm: currentTerm,
		members:     members,
		logger:      r.logger,
		topicStatus: map[string]*model.PartitionHeartbeat{},
		statusMu:    sync.RWMutex{},
	}
	r.pairs[proxyID] = pair

	r.metrics.ProxyConnected()

	select {
	case r.topologyCh <- state.TopicCommand{
		Type: state.TopicCommandTopology,
		Topology: &state.ProxyTopologyUpdate{
			Type:    state.TopologyAddProxy,
			ProxyID: proxyID,
			PushCh:  pair.pushCh,
		},
	}:
	default:
		r.logger.Warn("topology update channel full")
	}

	return pair
}

func (r *ProxyRegistry) Get(proxyID string) (*ConnectionPair, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	pair, exists := r.pairs[proxyID]
	return pair, exists
}

// Remove deletes a proxy pair from registry
func (r *ProxyRegistry) Remove(proxyID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if pair, exists := r.pairs[proxyID]; exists {
		pair.inboundAlive = false
		pair.outboundAlive = false

		pair.Close()
		delete(r.pairs, proxyID)

		r.metrics.ProxyDisconnected()

		// Send remove update
		select {
		case r.topologyCh <- state.TopicCommand{
			Type: state.TopicCommandTopology,
			Topology: &state.ProxyTopologyUpdate{
				Type:    state.TopologyRemoveProxy,
				ProxyID: proxyID,
			},
		}:
		default:
		}

	}
}

// Close shuts down all proxy connections
func (r *ProxyRegistry) Close() {
	r.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()

	for id, pair := range r.pairs {
		pair.Close()
		delete(r.pairs, id)
	}
}

// Gateway manages QUIC connections from proxies
type Gateway struct {
	selfNodeID      string
	certPath        string
	keyPath         string
	caCertPath      string
	metrics         *internal.GatewayMetrics
	transportConfig *quic.Config
	tlsConfig       *tls.Config
	quicListener    *quic.Listener
	addr            *net.UDPAddr
	registry        *ProxyRegistry
	toClusterCh     chan<- model.Command // Channel to cluster agent
	logger          *zap.Logger
	ctx             context.Context
	cancel          context.CancelFunc
	topicMgrCh      chan<- state.TopicCommand
	hbRouter        *state.HeartbeatRouter
	status          *atomic.Uint32
	leaderID        *atomic.Value
	currentTerm     *atomic.Uint64
	members         *model.Members
	mu              sync.RWMutex
	tlsVerifier     verifier.TLSVerifier
}

func (g *Gateway) Addr() net.Addr {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.quicListener != nil {
		return g.quicListener.Addr()
	}
	return nil
}

// CreateTransportConfig creates the QUIC transport configuration
func CreateTransportConfig() *quic.Config {
	// Heartbeat every 5s, idle timeout 30s — generous but not wasteful
	heartbeatInterval := 5 * time.Second
	idleTimeout := 30 * time.Second

	return &quic.Config{
		// Packet size: 1350 is standard and safe across networks
		InitialPacketSize: 1350,

		// ---- Flow Control (THE CRITICAL FIX) ----
		// Per-stream windows: 2MB initial, 8MB max
		// Connection-wide: 8MB initial, 32MB max
		// These numbers prevent stalls under burst load
		InitialStreamReceiveWindow:     2_000_000,  // 2 MB
		MaxStreamReceiveWindow:         8_000_000,  // 8 MB
		InitialConnectionReceiveWindow: 8_000_000,  // 8 MB
		MaxConnectionReceiveWindow:     32_000_000, // 32 MB

		// ---- Connection Limits ----
		// High TPS means many concurrent streams.
		// If each request maps to a stream, set this to your expected concurrency.
		MaxIncomingStreams:    10_000,
		MaxIncomingUniStreams: 0, // Set only if you use unidirectional streams

		// ---- Timeouts ----
		MaxIdleTimeout:       idleTimeout,
		HandshakeIdleTimeout: 10 * time.Second,
		KeepAlivePeriod:      heartbeatInterval,
		// Note: KeepAlivePeriod != 0 here because transport-level keepalive
		// is more reliable than application-level for connection liveness.

		// ---- 0-RTT ----
		// Enable if you have replay-safe idempotent operations
		Allow0RTT: false,

		// ---- Datagrams ----
		// Only enable if you actually use them
		EnableDatagrams: false,
	}
}

// NewGateway creates a new Gateway instance
func NewGateway(
	selfNodeID string,
	certPath string,
	keyPath string,
	caCertPath string,
	listenAddr string,
	logger *zap.Logger,
	topicCmdCh chan state.TopicCommand,
	hbRouter *state.HeartbeatRouter,
	toClusterCh chan<- model.Command,
	status *atomic.Uint32,
	currentTerm *atomic.Uint64,
	members *model.Members,
	leaderID *atomic.Value,
	tlsVerifier verifier.TLSVerifier,
) (*Gateway, error) {
	// Parse listen address
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve listen address: %w", err)
	}

	// Load TLS configuration
	tlsConfig, err := LoadTLSConfig(certPath, keyPath, caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS config: %w", err)
	}

	// Create transport configuration
	transportConfig := CreateTransportConfig()

	// Create UDP connection
	udpConn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on UDP: %w", err)
	}

	// Create QUIC listener
	quicListener, err := quic.Listen(udpConn, tlsConfig, transportConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create QUIC listener: %w", err)
	}

	// Create request channel (buffer sized for concurrent requests)
	metrics := internal.GetGatewayMetrics()

	gateway := &Gateway{
		selfNodeID:      selfNodeID,
		certPath:        certPath,
		keyPath:         keyPath,
		caCertPath:      caCertPath,
		metrics:         metrics,
		transportConfig: transportConfig,
		tlsConfig:       tlsConfig,
		quicListener:    quicListener,
		addr:            addr,
		toClusterCh:     toClusterCh,
		logger:          logger,
		topicMgrCh:      topicCmdCh,
		hbRouter:        hbRouter,
		status:          status,
		leaderID:        leaderID,
		currentTerm:     currentTerm,
		members:         members,
	}

	// Create registry with context
	ctx, cancel := context.WithCancel(context.Background())
	gateway.registry = NewProxyRegistry(ctx, logger, metrics, topicCmdCh)
	gateway.mu.Lock()
	gateway.ctx = ctx
	gateway.cancel = cancel
	gateway.mu.Unlock()

	return gateway, nil
}

// LoadTLSConfig loads and configures TLS with client CA verification
func LoadTLSConfig(certPath, keyPath, caCertPath string) (*tls.Config, error) {
	// Load server certificate
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load keypair: %w", err)
	}

	// Load CA certificate for verifying clients
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		ClientCAs:    caCertPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,

		SessionTicketsDisabled: true,
		InsecureSkipVerify:     false,
	}

	return tlsConfig, nil
}

// Run handles incoming QUIC connections from proxies
func (g *Gateway) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Accept new connection with timeout
		acceptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, err := g.quicListener.Accept(acceptCtx)
		cancel()
		if err != nil {
			// g.logger.Warn("Failed to accept connection", zap.Error(err))
			continue
		}

		// Handle each connection in its own goroutine
		go g.handleConnection(conn)
	}
}

// handleConnection processes a single QUIC connection from a proxy
func (g *Gateway) handleConnection(conn *quic.Conn) {
	// Set handshake timeout
	handshakeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Get peer certificate from TLS connection state
	connState := conn.ConnectionState()
	if len(connState.TLS.PeerCertificates) == 0 {
		_ = conn.CloseWithError(0, "no client certificate")
		return
	}
	rawCerts := make([][]byte, len(connState.TLS.PeerCertificates))
	for i, cert := range connState.TLS.PeerCertificates {
		rawCerts[i] = cert.Raw
	}

	// Accept first stream for handshake
	stream, err := conn.AcceptStream(handshakeCtx)
	if err != nil {
		g.logger.Error("Failed to accept handshake stream", zap.Error(err))
		_ = conn.CloseWithError(0, "handshake stream failed")
		g.metrics.ConnectionFailed()
		return
	}
	defer stream.Close()

	// Decode handshake
	var handshake Handshake
	decoder := msgpack.NewDecoder(stream)
	if err := decoder.Decode(&handshake); err != nil {
		g.logger.Error("Failed to decode handshake", zap.Error(err))
		_ = conn.CloseWithError(0, "invalid handshake")
		g.metrics.ConnectionFailed()
		return
	}

	// Authorize: verify the certificate matches the claimed nodeID
	if g.tlsVerifier != nil {
		if err := g.tlsVerifier.VerifyPeer(rawCerts, handshake.ProxyID); err != nil {
			_ = conn.CloseWithError(0, "tls authorization failed")
			return
		}
	}

	// Validate handshake
	if handshake.ConnectionType != ConnectionTypeInbound && handshake.ConnectionType != ConnectionTypeOutbound {
		g.logger.Error("Invalid connection type", zap.String("type", string(handshake.ConnectionType)))
		_ = conn.CloseWithError(0, "invalid connection type")
		g.metrics.ConnectionFailed()
		return
	}

	// Send response
	response := HandshakeResponse{
		Status: HandshakeStatusOk,
		NodeID: g.selfNodeID,
	}
	encoder := msgpack.NewEncoder(stream)
	if err := encoder.Encode(response); err != nil {
		g.logger.Error("Failed to send handshake response", zap.Error(err))
		_ = conn.CloseWithError(0, "handshake response failed")
		g.metrics.ConnectionFailed()
		return
	}

	// Register connection with appropriate handler
	pair := g.registry.GetOrCreate(handshake.ProxyID, g.status, g.leaderID, g.currentTerm, g.members)

	switch handshake.ConnectionType {
	case ConnectionTypeInbound:
		g.logger.Info("Registered inbound connection",
			zap.String("proxy_id", handshake.ProxyID),
			zap.String("remote_addr", conn.RemoteAddr().String()))
		pair.SetInbound(g.registry.ctx, conn, g.toClusterCh, g.topicMgrCh, g.hbRouter)
		g.metrics.ConnectionOpened()

	case ConnectionTypeOutbound:
		g.logger.Info("Registered outbound connection",
			zap.String("proxy_id", handshake.ProxyID),
			zap.String("remote_addr", conn.RemoteAddr().String()))
		pair.SetOutbound(g.registry.ctx, conn)
		g.metrics.ConnectionOpened()
	}

	<-conn.Context().Done()

	g.logger.Debug("Connection closed",
		zap.String("proxy_id", handshake.ProxyID),
		zap.String("type", string(handshake.ConnectionType)))
}

func (a *Gateway) GetLeaderID() string {
	if val := a.leaderID.Load(); val != nil {
		if s, ok := val.(string); ok {
			return s
		}
	}
	return ""
}

// Close gracefully shuts down the gateway
func (g *Gateway) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.cancel != nil {
		g.cancel()
	}

	if g.registry != nil {
		g.registry.Close()
	}

	if g.quicListener != nil {
		if err := g.quicListener.Close(); err != nil {
			return fmt.Errorf("failed to close listener: %w", err)
		}
	}

	return nil
}
