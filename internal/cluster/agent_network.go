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
	"io"
	"math/rand"
	"slices"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/m-javani/cue/internal"

	"github.com/m-javani/cue/internal/utils"
	"github.com/quic-go/quic-go"
	"go.uber.org/zap"
)

// sendConnectionHeartbeats runs as a goroutine and sends heartbeats to keep connections alive
func (a *ClusterAgent) sendConnectionHeartbeats() {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return

		case <-ticker.C:
			// Only followers send heartbeats (to keep connections alive through firewalls)
			if a.IsLeader() {
				continue
			}

			request := &ClusterRequest{Type: ReqConnectionHeartbeat, Heartbeat: &HeartbeatPayload{Timestamp: time.Now().UnixMilli()}}
			// Broadcast with short timeout (100ms)
			go a.broadcast(request, 100*time.Millisecond)
		}
	}
}

// sendHeartbeatToRetiringConnections sends heartbeats to retiring connections to keep them alive
// until they are fully rotated out after TLS certificate changes
func (a *ClusterAgent) sendHeartbeatToRetiringConnections() {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return

		case <-ticker.C:
			retiringConns := a.quicServer.GetRetiringOutgoingConnections()
			if len(retiringConns) == 0 {
				continue
			}

			request := &ClusterRequest{Type: ReqConnectionHeartbeat, Heartbeat: &HeartbeatPayload{Timestamp: time.Now().UnixMilli()}}

			for _, conn := range retiringConns {
				conn := conn // capture for goroutine
				go func() {
					// Send heartbeat without waiting for response (fire and forget)
					_, _ = a.quicServer.SendRequest(conn, request)
				}()
			}
		}
	}
}

// sendRequest sends a request to a specific target node with retries and backoff
func (a *ClusterAgent) sendRequest(targetNodeID string, request *ClusterRequest) (*ClusterResponse, error) {
	backoff := 5 * time.Millisecond
	deadline := time.Now().Add(400 * time.Millisecond)
	maxRetries := 3

	for attempt := range maxRetries {
		// Check deadline
		if time.Now().After(deadline) {
			return nil, internal.ErrDeadlineExceeded
		}

		// Get connection to target node
		conn, err := a.quicServer.GetOutgoingConnection(targetNodeID)
		if err != nil {
			if attempt < maxRetries-1 {
				time.Sleep(backoff)
				backoff = min(backoff*2, 100*time.Millisecond)
				continue
			}
			return nil, err
		}

		// Send request on connection
		resp, err := a.quicServer.SendRequest(conn, request)
		if err != nil {
			a.metrics.SendError()
			if attempt < maxRetries-1 {
				time.Sleep(backoff)
				backoff = min(backoff*2, 100*time.Millisecond)
				continue
			}
			return nil, err
		}

		return resp, nil
	}

	return nil, internal.ErrMaxRetriesExceeded
}

// broadcast sends a request to all active nodes concurrently and collects responses
func (a *ClusterAgent) broadcast(request *ClusterRequest, timeout time.Duration) map[string]*ClusterResponse {
	activeNodes := a.quicServer.GetActiveOutgoingNodes()

	type result struct {
		nodeID string
		resp   *ClusterResponse
		err    error
	}

	resultCh := make(chan result, len(activeNodes))
	var wg sync.WaitGroup

	// Send to each node concurrently
	for _, nodeID := range activeNodes {
		if nodeID == a.nodeID {
			continue // Skip self
		}

		wg.Add(1)
		go func(nid string) {
			defer wg.Done()
			resp, err := a.sendRequest(nid, request)
			if err != nil {
				a.logger.Sugar().Debugf("leader failed to send: %+v request:%+v", err, request)
				resultCh <- result{nodeID: nid, err: err}
				return
			}
			resultCh <- result{nodeID: nid, resp: resp}
		}(nodeID)
	}

	// Close result channel when all done
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results with timeout
	results := make(map[string]*ClusterResponse)
	timeoutCh := time.After(timeout)

	for {
		select {
		case res, ok := <-resultCh:
			if !ok {
				return results
			}
			if res.err == nil {
				results[res.nodeID] = res.resp
			}
		case <-timeoutCh:
			return results
		}
	}
}

// handleConnections accepts incoming QUIC connections and spawns handlers
func (a *ClusterAgent) handleConnections() {
	for {
		select {
		case <-a.ctx.Done():
			return

		default:
			nodeID, conn, err := a.quicServer.AcceptConnection(a.ctx)
			if err != nil {
				if a.ctx.Err() != nil {
					return
				}
				// a.logger.Warn("failed to accept connection", zap.Error(err))
				continue
			}
			a.metrics.ConnectionAccepted()

			go a.handlePeerConnection(nodeID, conn)
		}
	}
}

// handlePeerConnection handles a single peer connection
func (a *ClusterAgent) handlePeerConnection(nodeID string, conn *quic.Conn) {
	defer func() {
		_ = conn.CloseWithError(0, "connection closed")
		// a.logger.Debug("peer connection closed", zap.String("peer_node_id", nodeID))
	}()

	for {
		select {
		case <-a.ctx.Done():
			return

		default:
			stream, err := conn.AcceptStream(a.ctx)
			if err != nil {
				a.metrics.ReceiveError()
				if a.ctx.Err() != nil {
					return
				}
				// a.logger.Debug("failed to accept stream",
				// 	zap.String("peer_node_id", nodeID),
				// 	zap.Error(err))
				return
			}

			go a.handleRequestStream(nodeID, stream) // stream is *quic.Stream
		}
	}
}

// handleRequestStream processes a single request stream
func (a *ClusterAgent) handleRequestStream(nodeID string, stream *quic.Stream) {
	defer func() { _ = (*stream).Close() }()

	req, err := a.quicServer.ReadRequest(stream)
	if err != nil {
		a.metrics.ReceiveError()
		if err != io.EOF {
			a.logger.Debug("failed to read request",
				zap.String("peer_node_id", nodeID),
				zap.Error(err))
		}
		return
	}

	resp, err := a.ProcessRequest(req, nodeID)
	if err != nil {
		a.logger.Error("failed to process request",
			zap.String("peer_node_id", nodeID),
			zap.Any("request", req),
			zap.Error(err))
		resp = &ClusterResponse{Type: ResNegative}
	}

	if err := a.quicServer.WriteResponse(stream, resp); err != nil {
		a.metrics.SendError()
		a.logger.Debug("failed to write response",
			zap.String("peer_node_id", nodeID),
			zap.String("request", req.Type.String()),
			zap.Error(err))
	}
}

// syncConnections runs periodically to sync peer connections and handle TLS reloads
func (a *ClusterAgent) syncConnections() {
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return

		case <-ticker.C:
			// Sync QUIC connections
			if err := a.quicServer.SyncConnections(); err != nil {
				a.logger.Sugar().Warnf("failed to sync connections", zap.Error(err))
			}

			// Sync outgoing list (ensure outgoing connections match incoming)
			a.syncOutgoingList()

			// let nodes that has no connection to this node, update their discovery
			a.updateOtherNodes()

			// If leader, sync learner nodes via Raft
			if a.IsLeader() {
				raftIDs := a.discovery.ListPeersRaftIDs()
				select {
				case a.ctrlCh <- ControlCmd{
					Type:    CmdSyncLearnerNodes,
					NodeIDs: raftIDs,
				}:
				default:
					a.logger.Warn("ctrlCh full, skipping sync learner nodes")
				}
			}
		}
	}
}

// syncOutgoingList ensures outgoing connections count matches incoming
func (a *ClusterAgent) syncOutgoingList() {
	incoming := a.quicServer.GetActiveIncomingNodes()
	outgoing := a.quicServer.GetActiveOutgoingNodes()

	if len(outgoing) >= len(incoming) {
		return
	}

	now := time.Now().UnixMilli()
	if now < a.peerSyncOutgoingCoolDown.Load() {
		return
	}

	// === Key Change: Find good candidates first ===
	candidates := a.quicServer.GetConnectedBidirectionalNodeIds()
	if len(candidates) == 0 {
		// No one we can safely ask right now
		a.peerSyncOutgoingCoolDown.Store(now + 800) // shorter cooldown, retry soon
		return
	}

	a.peerSyncOutgoingCoolDown.Store(now + 1500)

	// Pick random from good candidates
	target := candidates[rand.Intn(len(candidates))]

	go func(target string) {
		req := &ClusterRequest{Type: ReqPeersListQuery}
		_, err := a.sendRequest(target, req)
		if err != nil {
			a.logger.Warn("failed to query peers list",
				zap.String("target", target),
				zap.Error(err))
		}
	}(target)
}

// updateOtherNodes shares peer list with nodes that only have outgoing connection to us
func (a *ClusterAgent) updateOtherNodes() {
	incomingNodes := a.quicServer.GetActiveIncomingNodes()
	outgoingNodes := a.quicServer.GetActiveOutgoingNodes()

	if len(incomingNodes) >= len(outgoingNodes) {
		return
	}

	now := time.Now().UnixMilli()
	if now < a.peerUpdateNodesCoolDown.Load() {
		return
	}

	// Build set of nodes that have incoming connection
	incomingSet := make(map[string]bool, len(incomingNodes))
	for _, node := range incomingNodes {
		incomingSet[node] = true
	}

	var nodesMissingIncoming []string
	for _, node := range outgoingNodes {
		if !incomingSet[node] {
			nodesMissingIncoming = append(nodesMissingIncoming, node)
		}
	}

	if len(nodesMissingIncoming) == 0 {
		return
	}

	a.peerUpdateNodesCoolDown.Store(now + 2000) // 2s cooldown

	peers := a.discovery.ListPeers()

	for _, targetID := range nodesMissingIncoming {
		go func(tid string) {
			req := &ClusterRequest{
				Type:       ReqAddMissingPeers,
				AddMissing: &AddMissingPayload{Peers: peers},
			}
			if _, err := a.sendRequest(tid, req); err != nil {
				a.logger.Debug("failed to share peers with node",
					zap.String("target", tid),
					zap.Error(err))
			}
		}(targetID)
	}
}

// startTLSWatcher monitors certificate files for changes and triggers TLS reload
func (a *ClusterAgent) startTLSWatcher() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		a.logger.Error("failed to create file watcher", zap.Error(err))
		return
	}
	defer func() { _ = watcher.Close() }()

	// Add certificate files to watcher
	files := []string{a.certPath, a.keyPath, a.caCertPath}
	for _, file := range files {
		if err := watcher.Add(file); err != nil {
			a.logger.Warn("failed to watch file", zap.String("file", file), zap.Error(err))
		}
	}

	var (
		debounceTimer    *time.Timer
		reloadActive     bool
		debounceMu       sync.Mutex
		debounceDuration = 1 * time.Second

		reloadCh   = make(chan struct{}, 1)
		reloadDone = make(chan struct{}, 1)
	)

	for {
		select {
		case <-a.ctx.Done():
			debounceMu.Lock()
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			debounceMu.Unlock()
			return

		case event, ok := <-watcher.Events:
			if !ok {
				return
			}

			// Only care about write events
			if event.Op&fsnotify.Write == 0 {
				continue
			}

			// Check if any of our cert files changed
			if !slices.Contains(files, event.Name) {
				continue
			}

			// Check if TLS version actually changed before starting debounce
			currentVersion, err := utils.GetTLSVersion(a.certPath, a.keyPath, a.caCertPath)
			if err != nil {
				a.logger.Warn("failed to get TLS version", zap.Error(err))
				continue
			}

			storedVersion := utils.SafeLoadAtomicString(&a.quicServer.tlsVersion)
			if storedVersion == currentVersion {
				a.logger.Debug("TLS file changed but version unchanged, skipping",
					zap.String("file", event.Name))
				continue
			}

			a.logger.Info("TLS file change detected", zap.String("file", event.Name))

			debounceMu.Lock()
			if debounceTimer != nil {
				debounceTimer.Stop()
			}

			debounceTimer = time.AfterFunc(debounceDuration, func() {
				a.logger.Info("TLS file debounce complete")

				select {
				case reloadCh <- struct{}{}:
				default:
				}
			})
			debounceMu.Unlock()

		case <-reloadCh:
			if reloadActive {
				continue
			}

			reloadActive = true

			go func() {
				defer func() {
					reloadDone <- struct{}{}
				}()

				// Double-check version hasn't changed again during debounce
				currentVersion, err := utils.GetTLSVersion(a.certPath, a.keyPath, a.caCertPath)
				if err != nil {
					a.logger.Warn("failed to get TLS version", zap.Error(err))
					return
				}

				storedVersion := utils.SafeLoadAtomicString(&a.quicServer.tlsVersion)
				if storedVersion == currentVersion {
					a.logger.Info("TLS version unchanged after debounce, skipping reload")
					return
				}

				// Perform reload with retries
				var reloadErr error
				for attempt := range 3 {
					if err := a.quicServer.ReloadTLS(); err != nil {
						reloadErr = err
						a.logger.Error("TLS reload failed",
							zap.Int("attempt", attempt+1),
							zap.Error(err))
						time.Sleep(time.Second)
						continue
					}

					reloadErr = nil
					break
				}

				if reloadErr != nil {
					a.logger.Error("TLS reload failed after all attempts", zap.Error(reloadErr))
					return
				}

				// Update stored version after successful reload
				a.quicServer.tlsVersion.Store(currentVersion)
				a.logger.Info("TLS reload successful")

				// Reconnect all peers after TLS rotation
				if err := a.reconnectAfterTLSRotate(); err != nil {
					a.logger.Error("failed to reconnect after TLS rotate", zap.Error(err))
				}
			}()

		case <-reloadDone:
			reloadActive = false

		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			a.logger.Warn("file watcher error", zap.Error(err))
		}
	}
}

// reconnectAfterTLSRotate reconnects to all peers after TLS certificate rotation
func (a *ClusterAgent) reconnectAfterTLSRotate() error {
	maxRetries := 5
	retryInterval := 2 * time.Second
	successfulNodes := make(map[string]bool)

	for attempt := range maxRetries {
		select {
		case <-a.ctx.Done():
			return a.ctx.Err()
		default:
		}

		// Attempt reconnection
		successful, err := a.quicServer.ReconnectToPeers()
		if err != nil {
			a.logger.Warn("reconnect attempt failed",
				zap.Int("attempt", attempt+1),
				zap.Error(err))
		}

		for _, addr := range successful {
			successfulNodes[addr] = true
		}

		// Wait before next retry
		time.Sleep(retryInterval)
	}

	return nil
}
