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

package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/utils"
	"github.com/m-javani/cue/pkg/verifier"
	"github.com/quic-go/quic-go"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
)

// Handshake represents the initial handshake message exchanged between peers
type Handshake struct {
	NodeID           string `msgpack:"node_id"`
	TargetServerName string `msgpack:"target_serverName"`
	// Extensible for future fields
}

// ClusterQUIC manages QUIC connections with peer nodes
type ClusterQUIC struct {
	selfNodeID      string
	certPath        string
	keyPath         string
	caCertPath      string
	metrics         *internal.ClusterMetrics
	transportConfig *quic.Config

	serverConfig atomic.Pointer[tls.Config] // For accepting connections
	clientConfig atomic.Pointer[tls.Config] // For dialing out (optional)

	quicListener *quic.Listener
	addr         *net.UDPAddr

	// Connection management
	outgoingConns         map[string]*quic.Conn // nodeID -> connection
	retiringOutgoingConns []retiringConn
	incomingConns         map[string]*quic.Conn // nodeID -> connection
	retiringIncomingConns []retiringConn
	addressToNode         map[string]string // address string -> nodeID
	nodeToServerName      map[string]string // nodeID -> serverName name
	selfServerName        string

	tlsVerifier verifier.TLSVerifier
	tlsVersion  atomic.Value

	logger *zap.Logger

	mu sync.RWMutex
}

type retiringConn struct {
	timestamp time.Time
	conn      *quic.Conn
}

const maxRetiringConnSize = 100 // Adjust as needed
const rotateTLSWindow = 600 * time.Second

// createTransportConfig creates the QUIC transport configuration
func createTransportConfig() *quic.Config {
	// Heartbeat every 5s, idle timeout 30s — generous but not wasteful
	heartbeatInterval := 5 * time.Second
	idleTimeout := 30 * time.Second

	return &quic.Config{
		// Packet size: 1200 is standard and safe across networks
		InitialPacketSize:       1200,
		DisablePathMTUDiscovery: true,

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

// NewClusterQUIC creates a new QUIC server instance
func NewClusterQUIC(
	selfNodeID string,
	certPath string,
	keyPath string,
	caCertPath string,
	listenAddr string,
	logger *zap.Logger,
	tlsVerifier verifier.TLSVerifier,
) (*ClusterQUIC, error) {
	// Parse listen address
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve listen address: %w", err)
	}

	// Get initial TLS version
	tlsVersionStr, err := utils.GetTLSVersion(certPath, keyPath, caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get TLS version: %w", err)
	}
	tlsVersion := atomic.Value{}
	tlsVersion.Store(tlsVersionStr)

	// Load initial server TLS config
	serverTlsConfig, err := loadServerTLSConfig(certPath, keyPath, caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load server TLS config: %w", err)
	}

	// Load initial client TLS config
	clientTLSConfig, err := loadClientTLSConfig(certPath, keyPath, caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load client TLS config: %w", err)
	}

	// Create transport configuration
	transportConfig := createTransportConfig()

	// Create UDP connection
	udpConn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on UDP: %w", err)
	}

	// Create server struct first (without listener)
	server := &ClusterQUIC{
		selfNodeID:       selfNodeID,
		certPath:         certPath,
		keyPath:          keyPath,
		caCertPath:       caCertPath,
		metrics:          internal.GetClusterMetrics(),
		transportConfig:  transportConfig,
		addr:             addr,
		outgoingConns:    make(map[string]*quic.Conn),
		incomingConns:    make(map[string]*quic.Conn),
		addressToNode:    make(map[string]string),
		nodeToServerName: make(map[string]string),
		tlsVerifier:      tlsVerifier,
		tlsVersion:       tlsVersion,
		logger:           logger,
	}

	// Store initial configs atomically BEFORE creating listener
	server.serverConfig.Store(serverTlsConfig)
	server.clientConfig.Store(clientTLSConfig)

	// Now create listener config with GetConfigForClient that references server
	listenerConfig := &tls.Config{
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			cfg := server.serverConfig.Load()
			if cfg == nil {
				return nil, fmt.Errorf("no server TLS config available")
			}
			return cfg, nil
		},
	}

	// Create QUIC listener - created ONCE, never recreated!
	quicListener, err := quic.Listen(udpConn, listenerConfig, transportConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create QUIC listener: %w", err)
	}

	// Set the listener on the server
	server.quicListener = quicListener

	return server, nil
}

// loadServerTLSConfig loads TLS config for the server side (accepting connections)
func loadServerTLSConfig(certPath, keyPath, caCertPath string) (*tls.Config, error) {
	// Load server certificate
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load keypair: %w", err)
	}

	// Load CA certificate (for verifying peers)
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	tlsConfig := &tls.Config{
		Certificates:           []tls.Certificate{cert},
		MinVersion:             tls.VersionTLS13,
		ClientCAs:              caCertPool,
		ClientAuth:             tls.RequireAndVerifyClientCert,
		SessionTicketsDisabled: true,
		InsecureSkipVerify:     false,
	}

	return tlsConfig, nil
}

// loadClientTLSConfig loads TLS config for the client side (dialing out)
func loadClientTLSConfig(certPath, keyPath, caCertPath string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load keypair: %w", err)
	}

	// Load CA for verifying server certificates
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	tlsConfig := &tls.Config{
		Certificates:           []tls.Certificate{cert},
		RootCAs:                caCertPool,
		MinVersion:             tls.VersionTLS13,
		SessionTicketsDisabled: true,
	}

	return tlsConfig, nil
}

func (s *ClusterQUIC) ReloadTLS() error {
	// Validate files exist
	if err := utils.ValidateFilesExit([]string{s.certPath, s.keyPath, s.caCertPath}); err != nil {
		return fmt.Errorf("cert validation failed: %w", err)
	}

	// Load new server config
	newServerCfg, err := loadServerTLSConfig(s.certPath, s.keyPath, s.caCertPath)
	if err != nil {
		return fmt.Errorf("failed to load server TLS config: %w", err)
	}

	// Load new client config
	newClientCfg, err := loadClientTLSConfig(s.certPath, s.keyPath, s.caCertPath)
	if err != nil {
		return fmt.Errorf("failed to load client TLS config: %w", err)
	}

	// Atomically swap configs
	s.serverConfig.Store(newServerCfg)
	s.clientConfig.Store(newClientCfg)

	// UPDATE THE TLS VERSION
	newVersion, err := utils.GetTLSVersion(s.certPath, s.keyPath, s.caCertPath)
	if err != nil {
		// Log but don't fail - certs loaded successfully, version is secondary
		s.logger.Warn("failed to get TLS version after reload", zap.Error(err))
	} else {
		s.tlsVersion.Store(newVersion)
	}

	s.logger.Info("TLS certificates reloaded successfully")
	return nil
}

// AcceptConnection waits for and accepts a new QUIC connection, performs handshake, returns nodeID and connection
func (s *ClusterQUIC) AcceptConnection(ctx context.Context) (string, *quic.Conn, error) {
	conn, err := s.quicListener.Accept(ctx)
	if err != nil {
		s.metrics.ConnectionError()
		return "", nil, err
	}

	// Get peer certificate from TLS connection state
	connState := conn.ConnectionState()
	if len(connState.TLS.PeerCertificates) == 0 {
		_ = conn.CloseWithError(0, "no client certificate")
		return "", nil, fmt.Errorf("no client certificate")
	}

	nodeID, err := s.performHandshake(conn)
	if err != nil {
		_ = conn.CloseWithError(0, "handshake failed")
		s.metrics.ConnectionRejected()
		return "", nil, err
	}

	// Authorize: verify the certificate matches the claimed nodeID
	if s.tlsVerifier != nil {
		cert := connState.TLS.PeerCertificates[0]
		if err := s.tlsVerifier.VerifyPeer(cert, verifier.Identity{NodeID: nodeID}); err != nil {
			_ = conn.CloseWithError(0, "authorization failed")
			s.metrics.ConnectionRejected()
			return "", nil, fmt.Errorf("peer authorization failed: %w", err)
		}
	}

	// Store incoming connection
	s.mu.Lock()
	oldConn, exists := s.incomingConns[nodeID]
	s.incomingConns[nodeID] = conn
	if exists && oldConn != nil {
		go func() {
			time.Sleep(rotateTLSWindow)
			_ = oldConn.CloseWithError(0, "replaced")
			s.metrics.ConnectionDropped()
		}()
	}
	s.mu.Unlock()

	s.metrics.ConnectionAccepted()

	return nodeID, conn, nil
}

func (s *ClusterQUIC) performHandshake(conn *quic.Conn) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		s.metrics.ReceiveError()
		return "", err
	}
	defer stream.Close()

	var handshake Handshake
	decoder := msgpack.NewDecoder(stream)
	if err := decoder.Decode(&handshake); err != nil {
		s.metrics.ReceiveError()
		return "", err
	}

	s.selfServerName = handshake.TargetServerName

	if handshake.NodeID == s.selfNodeID {
		return "", fmt.Errorf("connect to self not allowed")
	}

	// Send response
	response := Handshake{NodeID: s.selfNodeID, TargetServerName: handshake.TargetServerName}
	encoder := msgpack.NewEncoder(stream)
	if err := encoder.Encode(response); err != nil {
		s.metrics.ReceiveError()
		return "", err
	}

	return handshake.NodeID, nil
}

// SendRequest sends a request on an existing connection and returns the response
func (s *ClusterQUIC) SendRequest(conn *quic.Conn, req *ClusterRequest) (*ClusterResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	// Encode request
	data, err := msgpack.Marshal(req)
	if err != nil {
		return nil, err
	}

	// Write length prefix + data
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := stream.Write(lenBuf); err != nil {
		return nil, err
	}
	if _, err := stream.Write(data); err != nil {
		return nil, err
	}

	s.metrics.MessageSent()
	s.metrics.AddBytesSent(uint64(len(data)))

	// Read response length
	if _, err := io.ReadFull(stream, lenBuf); err != nil {
		return nil, err
	}
	respLen := binary.LittleEndian.Uint32(lenBuf)

	// Read response body
	respData := make([]byte, respLen)
	if _, err := io.ReadFull(stream, respData); err != nil {
		return nil, err
	}

	s.metrics.MessageReceived()
	s.metrics.AddBytesReceived(uint64(respLen))

	var resp ClusterResponse
	if err := msgpack.Unmarshal(respData, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

// ReadRequest reads a framed request from a stream
func (s *ClusterQUIC) ReadRequest(stream *quic.Stream) (*ClusterRequest, error) {
	var lenBuf [4]byte
	// io.ReadFull to guarantee reading all 4 bytes
	if _, err := io.ReadFull(stream, lenBuf[:]); err != nil {
		s.metrics.ReceiveError()
		return nil, err
	}
	length := binary.LittleEndian.Uint32(lenBuf[:])

	data := make([]byte, length)
	// io.ReadFull to guarantee reading all data bytes
	if _, err := io.ReadFull(stream, data); err != nil {
		s.metrics.ReceiveError()
		return nil, err
	}

	s.metrics.MessageReceived()
	s.metrics.AddBytesReceived(uint64(len(data)))

	var req ClusterRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		s.metrics.ReceiveError()
		return nil, err
	}
	return &req, nil
}

// WriteResponse writes a framed response to a stream
func (s *ClusterQUIC) WriteResponse(stream *quic.Stream, resp *ClusterResponse) error {
	data, err := msgpack.Marshal(resp)
	if err != nil {
		return err
	}

	// Write length prefix
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	if err := (*stream).SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	if _, err := (*stream).Write(lenBuf); err != nil {
		return err
	}
	if _, err := (*stream).Write(data); err != nil {
		return err
	}

	s.metrics.MessageSent()
	s.metrics.AddBytesSent(uint64(len(data)))

	return nil
}

// Connect establishes a connection to a peer
func (s *ClusterQUIC) Connect(ctx context.Context, port uint64, remoteAddr string, targetServerName string, targetNodeID string, handshake Handshake) error {
	connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Load current client config
	baseConfig := s.clientConfig.Load()
	if baseConfig == nil {
		return fmt.Errorf("no client TLS config available")
	}
	// Clone and customize for this connection
	tlsConfig := baseConfig.Clone()
	tlsConfig.ServerName = targetServerName

	// Override VerifyPeerCertificate to capture the target node ID
	if s.tlsVerifier != nil {
		// Disable Go's built-in verification so our custom verifier runs
		tlsConfig.InsecureSkipVerify = true

		tlsConfig.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("no peer certificate")
			}

			cert := cs.PeerCertificates[0]

			// verify chain
			if _, err := cert.Verify(x509.VerifyOptions{
				Roots: s.clientConfig.Load().RootCAs,
			}); err != nil {
				return err
			}

			// identity verification
			return s.tlsVerifier.VerifyPeer(cert, verifier.Identity{
				NodeID:     targetNodeID,
				ServerName: targetServerName,
			})
		}
	}

	conn, err := quic.DialAddr(connectCtx, remoteAddr, tlsConfig, s.transportConfig)
	if err != nil {
		s.metrics.ConnectionError()
		return fmt.Errorf("failed to dial QUIC: %w", err)
	}

	stream, err := conn.OpenStreamSync(connectCtx)
	if err != nil {
		s.metrics.ConnectionError()
		_ = conn.CloseWithError(0, "failed to open stream")
		return fmt.Errorf("failed to open stream: %w", err)
	}

	// Send our handshake
	encoder := msgpack.NewEncoder(stream)
	if err := encoder.Encode(handshake); err != nil {
		stream.CancelRead(0)
		stream.CancelWrite(0)
		_ = conn.CloseWithError(0, "handshake encode failed")
		s.metrics.ReceiveError()
		return fmt.Errorf("failed to encode handshake: %w", err)
	}

	// Read the peer's handshake response
	decoder := msgpack.NewDecoder(stream)
	var peerHandshake Handshake
	if err := decoder.Decode(&peerHandshake); err != nil {
		stream.CancelRead(0)
		stream.CancelWrite(0)
		_ = conn.CloseWithError(0, "failed to read peer handshake")
		s.metrics.ConnectionRejected()
		return fmt.Errorf("failed to decode peer handshake: %w", err)
	}

	// Validate peer handshake
	if peerHandshake.NodeID == s.selfNodeID {
		stream.CancelRead(0)
		stream.CancelWrite(0)
		_ = conn.CloseWithError(0, "connect to self rejected")
		s.metrics.ConnectionRejected()
		return fmt.Errorf("connect to self rejected node_id: %s", peerHandshake.NodeID)
	}

	// Store the connection using peer's actual NodeID from their handshake
	s.mu.Lock()
	oldConn, exists := s.outgoingConns[peerHandshake.NodeID]
	s.outgoingConns[peerHandshake.NodeID] = conn
	s.addressToNode[remoteAddr] = peerHandshake.NodeID
	s.nodeToServerName[peerHandshake.NodeID] = peerHandshake.TargetServerName

	if exists && oldConn != nil {
		if len(s.retiringOutgoingConns) < maxRetiringConnSize {
			s.retiringOutgoingConns = append(s.retiringOutgoingConns, retiringConn{
				timestamp: time.Now(),
				conn:      oldConn,
			})
		} else {
			_ = oldConn.CloseWithError(0, "retiring connection limit reached")
			s.metrics.ConnectionDropped()
		}
	}
	s.mu.Unlock()

	s.metrics.ConnectionOpened()

	s.logger.Info("Connected to node",
		zap.String("node", s.selfNodeID),
		zap.String("peer_node_id", peerHandshake.NodeID),
		zap.String("peer_serverName", peerHandshake.TargetServerName),
		zap.String("addr", targetServerName))

	return nil
}

// Helper method for tests to access nodeToServerName
func (s *ClusterQUIC) GetServerNameByNodeID(nodeID string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	serverName, exists := s.nodeToServerName[nodeID]
	return serverName, exists
}

// GetNodeIDByAddress returns node ID for a given address
func (s *ClusterQUIC) GetNodeIDByAddress(addr string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	nodeID, exists := s.addressToNode[addr]
	return nodeID, exists
}

// GetRetiringOutgoingConnections returns all retiring outgoing connections
func (s *ClusterQUIC) GetRetiringOutgoingConnections() []*quic.Conn {
	s.mu.RLock()
	defer s.mu.RUnlock()

	conns := make([]*quic.Conn, 0, len(s.retiringOutgoingConns))
	for _, rc := range s.retiringOutgoingConns {
		conns = append(conns, rc.conn)
	}
	return conns
}

// GetRetiringOutgoingConnections returns all retiring outgoing connections
func (s *ClusterQUIC) GetRetiringIncomingConnections() []*quic.Conn {
	s.mu.RLock()
	defer s.mu.RUnlock()

	conns := make([]*quic.Conn, 0, len(s.retiringIncomingConns))
	for _, rc := range s.retiringIncomingConns {
		conns = append(conns, rc.conn)
	}
	return conns
}

// GetActiveOutgoingNodes returns the node IDs of active outgoing connections
func (s *ClusterQUIC) GetActiveOutgoingNodes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	active := []string{}
	for nodeID, conn := range s.outgoingConns {
		if conn.Context().Err() == nil { // Connection is active
			active = append(active, nodeID)
		}
	}
	return active
}

// GetActiveIncomingNodes returns the node IDs of active incoming connections
func (s *ClusterQUIC) GetActiveIncomingNodes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	active := []string{}
	for nodeID, conn := range s.incomingConns {
		if conn.Context().Err() == nil { // Connection is active
			active = append(active, nodeID)
		}
	}
	return active
}

// GetConnectedNodeIdsAnyDirection returns a unique list of node IDs that have either
// active incoming or active outgoing connections (or both)
func (s *ClusterQUIC) GetConnectedNodeIdsAnyDirection() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Use a map to deduplicate node IDs
	nodeSet := make(map[string]bool)

	// Add nodes with active outgoing connections
	for nodeID, conn := range s.outgoingConns {
		if conn.Context().Err() == nil { // Connection is active
			nodeSet[nodeID] = true
		}
	}

	// Add nodes with active incoming connections
	for nodeID, conn := range s.incomingConns {
		if conn.Context().Err() == nil { // Connection is active
			nodeSet[nodeID] = true
		}
	}

	// Convert map keys to slice
	active := make([]string, 0, len(nodeSet))
	for nodeID := range nodeSet {
		active = append(active, nodeID)
	}

	return active
}

// GetActiveBidirectionalNodes returns the node IDs that have both active incoming and outgoing connections
func (s *ClusterQUIC) GetConnectedBidirectionalNodeIds() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bidirectional := []string{}

	// Check each outgoing connection to see if it also has an active incoming connection
	for nodeID, outConn := range s.outgoingConns {
		// Check if outgoing connection is active
		if outConn.Context().Err() != nil {
			continue
		}

		// Check if there's an incoming connection from the same node that is active
		if inConn, exists := s.incomingConns[nodeID]; exists {
			if inConn.Context().Err() == nil {
				bidirectional = append(bidirectional, nodeID)
			}
		}
	}

	return bidirectional
}

// GetNumOfActiveBidirectionalNodes returns count of nodes with both active incoming and outgoing connections
func (s *ClusterQUIC) GetNumOfActiveBidirectionalNodes() int {
	outgoing := s.GetActiveOutgoingNodes()
	incoming := s.GetActiveIncomingNodes()

	outgoingSet := make(map[string]bool)
	for _, id := range outgoing {
		outgoingSet[id] = true
	}

	count := 0
	for _, id := range incoming {
		if outgoingSet[id] {
			count++
		}
	}
	return count
}

// GetOutgoingConnection returns the outgoing connection for a given node ID
func (s *ClusterQUIC) GetOutgoingConnection(nodeID string) (*quic.Conn, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	conn, exists := s.outgoingConns[nodeID]
	if !exists {
		return nil, fmt.Errorf("no connection for node_id: %s", nodeID)
	}
	return conn, nil
}

// GetIncomingConnection returns the incoming connection for a given node ID
func (s *ClusterQUIC) GetIncomingConnection(nodeID string) (*quic.Conn, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	conn, exists := s.incomingConns[nodeID]
	if !exists {
		return nil, fmt.Errorf("no connection for node_id: %s", nodeID)
	}
	return conn, nil
}

func (s *ClusterQUIC) CleanupRetiringOutgoing() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.retiringOutgoingConns) == 0 {
		return
	}

	now := time.Now()
	newOutgoing := []retiringConn{}

	for _, rc := range s.retiringOutgoingConns {
		if now.Sub(rc.timestamp) >= rotateTLSWindow {
			_ = rc.conn.CloseWithError(0, "rotated TLS")
			s.metrics.ConnectionDropped()
			// false - drop the connection (don't append)
		} else {
			newOutgoing = append(newOutgoing, rc) // keep it
		}
	}

	s.retiringOutgoingConns = newOutgoing
}

func (s *ClusterQUIC) CleanupRetiringIncoming() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.retiringIncomingConns) == 0 {
		return
	}

	newIncoming := []retiringConn{}

	for _, rc := range s.retiringIncomingConns {
		// Check if connection is already closed
		if rc.conn.Context().Err() != nil {
			// false - safe to drop
			continue
		}
		newIncoming = append(newIncoming, rc) // keep it alive
	}

	s.retiringIncomingConns = newIncoming
}

// SyncConnections ensures connections to all provided peers
func (s *ClusterQUIC) SyncConnections(nodeInfos []PeerResolvedInfo) error {
	s.mu.Lock()

	// Validate and filter peers
	nodes := make([]PeerResolvedInfo, 0, len(nodeInfos))
	registryAddrSet := make(map[string]bool)

	for _, pair := range nodeInfos {
		// Skip self
		if s.selfServerName != "" && pair.ServerName == s.selfServerName {
			continue
		}
		// Validate address
		if _, err := net.ResolveUDPAddr("udp", pair.Addr); err == nil {
			nodes = append(nodes, pair)
			registryAddrSet[pair.Addr] = true
		} else {
			s.logger.Sugar().Warnf("%s failed to resolve: %s, %v", s.selfNodeID, pair.Addr, err)
		}
	}

	// Build desired set by nodeID
	desiredNodes := make(map[string]PeerResolvedInfo)
	for _, node := range nodes {
		desiredNodes[node.NodeId] = node
	}

	// Remove undesired or dead outgoing connections
	for nodeID, conn := range s.outgoingConns {
		if _, stillDesired := desiredNodes[nodeID]; !stillDesired {
			delete(s.outgoingConns, nodeID)
			delete(s.nodeToServerName, nodeID)
			delete(s.addressToNode, s.addressToNode[nodeID])
			continue
		}
		if conn.Context().Err() != nil {
			delete(s.outgoingConns, nodeID)
			delete(s.nodeToServerName, nodeID)
			delete(s.addressToNode, s.addressToNode[nodeID])
		}
	}

	// Clean dead incoming connections
	for nodeID, conn := range s.incomingConns {
		if conn.Context().Err() != nil {
			delete(s.incomingConns, nodeID)
		}
	}

	// Only connect to nodes we don't have
	nodesToConnect := []PeerResolvedInfo{}
	for nodeID, info := range desiredNodes {
		if _, hasConn := s.outgoingConns[nodeID]; !hasConn {
			nodesToConnect = append(nodesToConnect, info)
		}
	}

	s.mu.Unlock()

	// Connect in parallel
	var wg sync.WaitGroup
	for _, info := range nodesToConnect {
		if info.NodeId == s.selfNodeID {
			continue
		}
		wg.Add(1)
		go func(pi PeerResolvedInfo) {
			defer wg.Done()

			_, portStr, err := net.SplitHostPort(pi.Addr)
			if err != nil {
				s.logger.Error("invalid address", zap.String("addr", pi.Addr), zap.Error(err))
				return
			}
			port, err := strconv.Atoi(portStr)
			if err != nil {
				s.logger.Error("invalid address", zap.String("addr", pi.Addr), zap.Error(err))
				return
			}

			handshake := Handshake{
				NodeID:           s.selfNodeID,
				TargetServerName: pi.ServerName,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			err = s.Connect(ctx, uint64(port), pi.Addr, pi.ServerName, pi.NodeId, handshake)
			if err != nil {
				s.logger.Warn("couldnt connect",
					zap.String("node", s.selfNodeID),
					zap.String("target", pi.NodeId),
					zap.Error(err))
			} else {
				s.logger.Info("connected",
					zap.String("node", s.selfNodeID),
					zap.String("target", pi.NodeId))
			}
		}(info)
	}
	wg.Wait()

	return nil
}

// ReconnectToPeers attempts to connect to a list of peers and returns successful ones
func (s *ClusterQUIC) ReconnectToPeers(nodeInfos []PeerResolvedInfo) ([]string, error) {
	var remainingPeers []PeerResolvedInfo
	for _, pair := range nodeInfos {
		if pair.NodeId == s.selfNodeID {
			continue
		}
		if _, err := net.ResolveUDPAddr("udp", pair.Addr); err == nil {
			remainingPeers = append(remainingPeers, PeerResolvedInfo{
				NodeId:     pair.NodeId,
				Addr:       pair.Addr,
				ServerName: pair.ServerName,
			})
		}
	}

	var wg sync.WaitGroup
	results := make(chan struct {
		addr string
		err  error
	}, len(remainingPeers))

	for _, peer := range remainingPeers {
		wg.Add(1)
		go func(p PeerResolvedInfo) {
			defer wg.Done()

			handshake := Handshake{
				NodeID:           s.selfNodeID,
				TargetServerName: p.ServerName,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			_, portStr, err := net.SplitHostPort(p.Addr)
			if err != nil {
				s.logger.Error("missing port in connection address", zap.Error(err))
			}
			port, err := strconv.Atoi(portStr)
			if err != nil {
				s.logger.Error("missing port in connection address", zap.Error(err))
			}
			err = s.Connect(ctx, uint64(port), p.Addr, p.ServerName, p.NodeId, handshake)
			results <- struct {
				addr string
				err  error
			}{p.Addr, err}
		}(peer)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var successfulPeers []string
	for res := range results {
		if res.err == nil {
			successfulPeers = append(successfulPeers, res.addr)
		}
	}

	return successfulPeers, nil
}

// Close gracefully shuts down the server and all connections
func (s *ClusterQUIC) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close all outgoing connections
	for nodeID, conn := range s.outgoingConns {
		if err := conn.CloseWithError(0, "server shutting down"); err != nil {
			s.logger.Warn("Failed to close outgoing connection",
				zap.String("node_id", nodeID),
				zap.Error(err))
		} else {
			s.metrics.ConnectionDropped()
		}
	}

	// Close all incoming connections
	for nodeID, conn := range s.incomingConns {
		if err := conn.CloseWithError(0, "server shutting down"); err != nil {
			s.logger.Warn("Failed to close incoming connection",
				zap.String("node_id", nodeID),
				zap.Error(err))
		} else {
			s.metrics.ConnectionDropped()
		}
	}

	// Close listener
	if s.quicListener != nil {
		if err := s.quicListener.Close(); err != nil {
			return fmt.Errorf("failed to close listener: %w", err)
		}
		s.logger.Info("quic listener released", zap.String("node_id", s.selfNodeID))
	}

	return nil
}

// GetNodeID returns the node ID for a given connection (from handshake)
func (s *ClusterQUIC) GetNodeID(conn *quic.Conn) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.addressToNode[conn.RemoteAddr().String()]
}

func (s *ClusterQUIC) GetTLSVersion() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return utils.SafeLoadAtomicString(&s.tlsVersion)
}
