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
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m-javani/cue/internal"
	"github.com/m-javani/cue/internal/model"
	"github.com/m-javani/cue/internal/utils"
	"github.com/quic-go/quic-go"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
)

// Handshake represents the initial handshake message exchanged between peers
type Handshake struct {
	NodeID string `msgpack:"node_id"`
}

// ClusterQUIC manages QUIC connections
type ClusterQUIC struct {
	selfNodeID string
	quicPort   int
	certPath   string
	keyPath    string
	caCertPath string
	metrics    *internal.ClusterMetrics

	transportConfig *quic.Config
	serverConfig    atomic.Pointer[tls.Config]
	clientConfig    atomic.Pointer[tls.Config]
	quicListener    *quic.Listener
	addr            *net.UDPAddr

	// Connection management
	outgoingConns         map[string]*quic.Conn
	retiringOutgoingConns []retiringConn
	incomingConns         map[string]*quic.Conn
	retiringIncomingConns []retiringConn

	tlsVersion atomic.Value
	logger     *zap.Logger
	mu         sync.RWMutex

	discovery *ServiceDiscovery
}

type retiringConn struct {
	timestamp time.Time
	conn      *quic.Conn
}

const rotateTLSWindow = 600 * time.Second

// createTransportConfig creates the QUIC transport configuration
func createTransportConfig() *quic.Config {
	heartbeatInterval := 5 * time.Second
	idleTimeout := 30 * time.Second
	return &quic.Config{
		InitialPacketSize:              1200,
		DisablePathMTUDiscovery:        true,
		InitialStreamReceiveWindow:     2_000_000,
		MaxStreamReceiveWindow:         8_000_000,
		InitialConnectionReceiveWindow: 8_000_000,
		MaxConnectionReceiveWindow:     32_000_000,
		MaxIncomingStreams:             10_000,
		MaxIncomingUniStreams:          0,
		MaxIdleTimeout:                 idleTimeout,
		HandshakeIdleTimeout:           10 * time.Second,
		KeepAlivePeriod:                heartbeatInterval,
		Allow0RTT:                      false,
		EnableDatagrams:                false,
	}
}

// NewClusterQUIC creates a new QUIC server instance
func NewClusterQUIC(
	selfNodeID string,
	quicPort int,
	certPath string,
	keyPath string,
	caCertPath string,
	listenAddr string,
	logger *zap.Logger,
	discovery *ServiceDiscovery,
) (*ClusterQUIC, error) {
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve listen address: %w", err)
	}

	tlsVersionStr, err := utils.GetTLSVersion(certPath, keyPath, caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get TLS version: %w", err)
	}

	serverTlsConfig, err := loadServerTLSConfig(certPath, keyPath, caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load server TLS config: %w", err)
	}

	clientTLSConfig, err := loadClientTLSConfig(certPath, keyPath, caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load client TLS config: %w", err)
	}

	transportConfig := createTransportConfig()

	udpConn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on UDP: %w", err)
	}

	s := &ClusterQUIC{
		selfNodeID:            selfNodeID,
		certPath:              certPath,
		keyPath:               keyPath,
		caCertPath:            caCertPath,
		metrics:               internal.GetClusterMetrics(),
		transportConfig:       transportConfig,
		addr:                  addr,
		outgoingConns:         make(map[string]*quic.Conn),
		incomingConns:         make(map[string]*quic.Conn),
		tlsVersion:            atomic.Value{},
		logger:                logger,
		discovery:             discovery,
		quicPort:              quicPort,
		serverConfig:          atomic.Pointer[tls.Config]{},
		clientConfig:          atomic.Pointer[tls.Config]{},
		quicListener:          &quic.Listener{},
		retiringOutgoingConns: []retiringConn{},
		retiringIncomingConns: []retiringConn{},
		mu:                    sync.RWMutex{},
	}
	s.tlsVersion.Store(tlsVersionStr)
	s.serverConfig.Store(serverTlsConfig)
	s.clientConfig.Store(clientTLSConfig)

	listenerConfig := &tls.Config{
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			cfg := s.serverConfig.Load()
			if cfg == nil {
				return nil, fmt.Errorf("no server TLS config available")
			}
			return cfg, nil
		},
	}

	quicListener, err := quic.Listen(udpConn, listenerConfig, transportConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create QUIC listener: %w", err)
	}
	s.quicListener = quicListener

	return s, nil
}

// loadServerTLSConfig loads TLS config for the server side (accepting connections)
func loadServerTLSConfig(certPath, keyPath, caCertPath string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load keypair: %w", err)
	}
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	return &tls.Config{
		Certificates:           []tls.Certificate{cert},
		MinVersion:             tls.VersionTLS13,
		ClientCAs:              caCertPool,
		ClientAuth:             tls.RequireAndVerifyClientCert,
		SessionTicketsDisabled: true,
	}, nil
}

// loadClientTLSConfig loads TLS config for the client side (dialing out)
func loadClientTLSConfig(certPath, keyPath, caCertPath string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load keypair: %w", err)
	}
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	return &tls.Config{
		Certificates:           []tls.Certificate{cert},
		RootCAs:                caCertPool,
		MinVersion:             tls.VersionTLS13,
		SessionTicketsDisabled: true,
		InsecureSkipVerify:     true,
	}, nil
}

func (s *ClusterQUIC) ReloadTLS() error {
	if err := utils.ValidateFilesExit([]string{s.certPath, s.keyPath, s.caCertPath}); err != nil {
		return fmt.Errorf("cert validation failed: %w", err)
	}

	newServerCfg, err := loadServerTLSConfig(s.certPath, s.keyPath, s.caCertPath)
	if err != nil {
		return fmt.Errorf("failed to load server TLS config: %w", err)
	}
	newClientCfg, err := loadClientTLSConfig(s.certPath, s.keyPath, s.caCertPath)
	if err != nil {
		return fmt.Errorf("failed to load client TLS config: %w", err)
	}

	s.serverConfig.Store(newServerCfg)
	s.clientConfig.Store(newClientCfg)

	if newVersion, err := utils.GetTLSVersion(s.certPath, s.keyPath, s.caCertPath); err == nil {
		s.tlsVersion.Store(newVersion)
	} else {
		s.logger.Warn("failed to get TLS version after reload", zap.Error(err))
	}
	s.logger.Info("TLS certificates reloaded successfully")
	return nil
}

func (s *ClusterQUIC) VerifyTLSIdentity(cert *x509.Certificate, expected model.TLSIdentity) error {
	switch expected.Kind {
	case model.IdentityDNS:
		if slices.Contains(cert.DNSNames, expected.Value) {
			return nil
		}
		return fmt.Errorf("certificate does not contain expected DNS SAN: %s. cert.DNSNames: %+v", expected.Value, cert.DNSNames)

	case model.IdentityIP:
		expectedIP := net.ParseIP(expected.Value)
		if expectedIP == nil {
			return fmt.Errorf("invalid IP in expected identity: %s", expected.Value)
		}
		for _, ip := range cert.IPAddresses {
			if ip.Equal(expectedIP) {
				return nil
			}
		}
		return fmt.Errorf("certificate does not contain expected IP SAN: %s", expected.Value)

	case model.IdentitySPIFFE:
		for _, uri := range cert.URIs {
			if uri.Scheme == "spiffe" && uri.String() == expected.Value {
				return nil
			}
		}
		return fmt.Errorf("certificate does not contain expected SPIFFE URI: %s", expected.Value)

	default:
		return fmt.Errorf("unsupported identity kind: %d", expected.Kind)
	}
}

// AcceptConnection waits for and accepts a new QUIC connection, performs handshake, returns nodeID and connection
func (s *ClusterQUIC) AcceptConnection(ctx context.Context) (string, *quic.Conn, error) {
	conn, err := s.quicListener.Accept(ctx)
	if err != nil {
		s.metrics.ConnectionError()
		return "", nil, err
	}

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

	s.mu.Lock()
	if oldConn, exists := s.incomingConns[nodeID]; exists && oldConn != nil {
		go func() {
			time.Sleep(rotateTLSWindow)
			_ = oldConn.CloseWithError(0, "replaced")
			s.metrics.ConnectionDropped()
		}()
	}
	s.incomingConns[nodeID] = conn
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
	if err := msgpack.NewDecoder(stream).Decode(&handshake); err != nil {
		s.metrics.ReceiveError()
		return "", err
	}

	if handshake.NodeID == s.selfNodeID {
		return "", fmt.Errorf("connect to self not allowed")
	}

	// Send response
	response := Handshake{NodeID: s.selfNodeID}
	if err := msgpack.NewEncoder(stream).Encode(response); err != nil {
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
func (s *ClusterQUIC) Connect(ctx context.Context, nodeID string) error {
	if nodeID == s.selfNodeID {
		return errors.New("self connection not allowed")
	}
	peerInfo, ok := s.discovery.Lookup(nodeID)
	if !ok {
		return fmt.Errorf("peer not found in discovery: %s", nodeID)
	}

	connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	baseConfig := s.clientConfig.Load()
	if baseConfig == nil {
		return fmt.Errorf("no client TLS config available")
	}

	tlsConfig := baseConfig.Clone()

	// Build remote address (adjust quicPort from config if needed)
	// For now, assume fixed port or extend PeerInfo if necessary
	port := s.quicPort
	if peerInfo.Port != 0 {
		port = int(peerInfo.Port)
	}
	host := peerInfo.Host
	if host == "" {
		host = peerInfo.NodeID
	}
	remoteAddr := net.JoinHostPort(host, strconv.Itoa(port))

	tlsConfig.VerifyConnection = func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return fmt.Errorf("no peer certificate")
		}
		leaf := cs.PeerCertificates[0]

		intermediates := x509.NewCertPool()
		for _, cert := range cs.PeerCertificates[1:] {
			intermediates.AddCert(cert)
		}

		// 1. Full chain validation
		if _, err := leaf.Verify(x509.VerifyOptions{
			Roots:         baseConfig.RootCAs,
			Intermediates: intermediates,
		}); err != nil {
			return fmt.Errorf("certificate chain verification failed: %w", err)
		}

		return nil
	}

	conn, err := quic.DialAddr(connectCtx, remoteAddr, tlsConfig, s.transportConfig)
	if err != nil {
		s.metrics.ConnectionError()
		return fmt.Errorf("failed to dial QUIC: %w", err)
	}

	cs := conn.ConnectionState()
	leaf := cs.TLS.PeerCertificates[0]
	if err := s.VerifyTLSIdentity(leaf, peerInfo.Identity); err != nil {
		return err
	}

	// Perform handshake
	stream, err := conn.OpenStreamSync(connectCtx)
	if err != nil {
		_ = conn.CloseWithError(0, "stream failed")
		return err
	}
	defer stream.Close()

	if err := msgpack.NewEncoder(stream).Encode(Handshake{NodeID: s.selfNodeID}); err != nil {
		_ = conn.CloseWithError(0, "handshake failed")
		return err
	}

	var peerResp Handshake
	if err := msgpack.NewDecoder(stream).Decode(&peerResp); err != nil {
		_ = conn.CloseWithError(0, "handshake failed")
		return err
	}

	s.mu.Lock()
	if old, exists := s.outgoingConns[peerResp.NodeID]; exists && old != nil {
		s.retiringOutgoingConns = append(s.retiringOutgoingConns, retiringConn{time.Now(), old})
	}
	s.outgoingConns[peerResp.NodeID] = conn
	s.mu.Unlock()

	s.metrics.ConnectionOpened()
	return nil
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

// SyncConnections ensures connections to all peers in the current discovery view
func (s *ClusterQUIC) SyncConnections() error {
	peers := s.discovery.ListPeers()

	s.mu.Lock()
	// Build desired set
	desired := make(map[string]model.PeerInfo, len(peers))
	for _, p := range peers {
		if p.NodeID != s.selfNodeID {
			desired[p.NodeID] = p
		}
	}

	// Remove connections that are no longer desired or dead
	for nodeID, conn := range s.outgoingConns {
		if _, stillDesired := desired[nodeID]; !stillDesired || conn.Context().Err() != nil {
			delete(s.outgoingConns, nodeID)
		}
	}

	// Clean dead incoming connections
	for nodeID, conn := range s.incomingConns {
		if conn.Context().Err() != nil {
			delete(s.incomingConns, nodeID)
		}
	}

	// Collect nodes that need new outgoing connections
	nodesToConnect := make([]model.PeerInfo, 0, len(desired))
	for nodeID, info := range desired {
		if _, has := s.outgoingConns[nodeID]; !has {
			nodesToConnect = append(nodesToConnect, info)
		}
	}
	s.mu.Unlock()

	// Connect in parallel
	var wg sync.WaitGroup
	for _, info := range nodesToConnect {
		wg.Add(1)
		go func(pi model.PeerInfo) {
			defer wg.Done()
			if err := s.Connect(context.Background(), pi.NodeID); err != nil {
				s.logger.Warn("failed to connect to peer",
					zap.String("self", s.selfNodeID),
					zap.String("target", pi.NodeID),
					zap.Error(err))
			} else {
				s.logger.Info("successfully connected",
					zap.String("self", s.selfNodeID),
					zap.String("target", pi.NodeID))
			}
		}(info)
	}
	wg.Wait()

	return nil
}

// ReconnectToPeers attempts to reconnect to all known peers and returns successfully reconnected addresses
func (s *ClusterQUIC) ReconnectToPeers(peers []model.PeerInfo) ([]string, error) {

	var wg sync.WaitGroup
	results := make(chan struct {
		nodeID string
		err    error
	}, len(peers))

	for _, peer := range peers {
		wg.Add(1)
		go func(p model.PeerInfo) {
			defer wg.Done()
			err := s.Connect(context.Background(), p.NodeID)
			results <- struct {
				nodeID string
				err    error
			}{p.NodeID, err}
		}(peer)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var successful []string
	for res := range results {
		if res.err == nil {
			successful = append(successful, res.nodeID)
		}
		// else {
		// 	s.logger.Debug("reconnect failed", zap.String("target", res.nodeID), zap.Error(res.err))
		// }
	}

	return successful, nil
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

func (s *ClusterQUIC) GetTLSVersion() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return utils.SafeLoadAtomicString(&s.tlsVersion)
}
