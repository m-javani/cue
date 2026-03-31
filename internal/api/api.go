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

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m-javani/cue/internal/model"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// =============================================================================
// Configuration
// =============================================================================

type AuthConfig struct {
	Tokens []Token `yaml:"tokens"`
}

type Token struct {
	Token string `yaml:"token"`
	Role  string `yaml:"role"`
}

// =============================================================================
// Commands
// =============================================================================

// =============================================================================
// API Server
// =============================================================================

type AdminAPI struct {
	addr        string
	port        int
	authFile    string
	authConfig  *AuthConfig
	authMu      sync.RWMutex
	authModTime time.Time

	commandCh  chan<- model.Command
	httpServer *http.Server

	members  *model.Members
	leaderID *atomic.Value

	reqIDCounter atomic.Uint64

	logger *zap.Logger
}

// Constructor
func NewAdminAPI(
	Addr string,
	Port int,
	AuthFile string,
	CommandCh chan<- model.Command,
	members *model.Members,
	leaderID *atomic.Value,
	logger *zap.Logger,
) *AdminAPI {
	api := &AdminAPI{
		addr:         Addr,
		port:         Port,
		authFile:     AuthFile,
		commandCh:    CommandCh,
		members:      members,
		leaderID:     leaderID,
		logger:       logger,
		reqIDCounter: atomic.Uint64{},
	}

	// Load initial auth config
	if err := api.loadAuthConfig(); err != nil {
		// Log but don't fail - will retry on each request
		fmt.Printf("Warning: failed to load auth config: %v\n", err)
	}

	return api
}

func (api *AdminAPI) nextRequestID() string {
	return strconv.FormatUint(api.reqIDCounter.Add(1), 36)
}

// Run starts the API server
func (api *AdminAPI) Run(ctx context.Context) error {
	mux := http.NewServeMux()

	// Metrics endpoint - accessible by monitoring and admin roles
	mux.HandleFunc("/metrics", api.authMiddleware(api.handleMetrics, "monitoring", "admin"))

	// Admin-only endpoints
	mux.HandleFunc("/cluster/promote-learner", api.authMiddleware(api.handleAddVoter, "admin"))
	mux.HandleFunc("/cluster/demote-voter", api.authMiddleware(api.handleRemoveNode, "admin"))
	mux.HandleFunc("/cluster/transfer-leader", api.authMiddleware(api.handleTransferLeader, "admin"))

	// Health endpoint (no auth)
	mux.HandleFunc("/health", api.handleHealth)
	mux.HandleFunc("/cluster/info", api.handleGetClusterInfo)

	api.httpServer = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", api.addr, api.port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}

	// Start server in goroutine
	errCh := make(chan error, 1)
	go func() {
		api.logger.Sugar().Infof("Admin API listening on %s:%d", api.addr, api.port)
		if err := api.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("admin API server error: %w", err)
		}
		close(errCh)
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		// Graceful shutdown
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return api.httpServer.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

// =============================================================================
// Authentication
// =============================================================================

func (api *AdminAPI) loadAuthConfig() error {
	// Check if file needs reloading
	info, err := os.Stat(api.authFile)
	if err != nil {
		return fmt.Errorf("failed to stat auth file: %w", err)
	}

	// Only reload if modified
	if !info.ModTime().After(api.authModTime) {
		return nil
	}

	data, err := os.ReadFile(api.authFile)
	if err != nil {
		return fmt.Errorf("failed to read auth file: %w", err)
	}

	var config AuthConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("failed to parse auth YAML: %w", err)
	}

	api.authMu.Lock()
	api.authConfig = &config
	api.authModTime = info.ModTime()
	api.authMu.Unlock()

	fmt.Printf("Loaded %d tokens from %s\n", len(config.Tokens), api.authFile)
	return nil
}

func (api *AdminAPI) authenticate(token string) (string, bool) {
	// Reload config if file changed
	if err := api.loadAuthConfig(); err != nil {
		fmt.Printf("Auth reload error: %v\n", err)
		return "", false
	}

	api.authMu.RLock()
	defer api.authMu.RUnlock()

	if api.authConfig == nil {
		return "", false
	}

	for _, t := range api.authConfig.Tokens {
		if t.Token == token {
			return t.Role, true
		}
	}
	return "", false
}

func (api *AdminAPI) authMiddleware(next http.HandlerFunc, requiredRoles ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract token from Authorization header
		token := r.Header.Get("Authorization")
		if token == "" {
			http.Error(w, `{"error": "missing authorization header"}`, http.StatusUnauthorized)
			return
		}

		// Remove "Bearer " prefix if present
		if len(token) > 7 && token[:7] == "Bearer " {
			token = token[7:]
		}

		// Authenticate
		role, ok := api.authenticate(token)
		if !ok {
			http.Error(w, `{"error": "invalid token"}`, http.StatusUnauthorized)
			return
		}

		// Check role
		authorized := false
		for _, required := range requiredRoles {
			if role == required {
				authorized = true
				break
			}
		}

		if !authorized {
			http.Error(w, `{"error": "insufficient permissions"}`, http.StatusForbidden)
			return
		}

		next(w, r)
	}
}

// =============================================================================
// Handlers
// =============================================================================

func (api *AdminAPI) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (api *AdminAPI) handleMetrics(w http.ResponseWriter, r *http.Request) {
	promhttp.Handler().ServeHTTP(w, r)
}

func (api *AdminAPI) handleAddVoter(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var req model.AddNodePayload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "invalid request: %v"}`, err), http.StatusBadRequest)
		return
	}

	if req.NodeID == "" {
		http.Error(w, `{"error": "node_id is required"}`, http.StatusBadRequest)
		return
	}

	// Send command and wait for response
	respCh := make(chan model.ToProducerResponse, 1)
	select {
	case api.commandCh <- model.Command{
		Type:    model.CmdAddNode,
		AddNode: &model.AddNodePayload{NodeID: req.NodeID},
		RespInfo: &model.RespInfo{
			RequestID: api.nextRequestID(),
			RespCh:    respCh,
		},
	}:
		// Wait for response with timeout
		select {
		case resp := <-respCh:
			if resp.Status == model.ToProxyRespStatusError {
				http.Error(w, fmt.Sprintf(`{"error": "%v"}`, resp.Error), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
		case <-time.After(5 * time.Second):
			http.Error(w, `{"error": "command timeout"}`, http.StatusGatewayTimeout)
		}
	case <-time.After(1 * time.Second):
		http.Error(w, `{"error": "cluster busy, please retry"}`, http.StatusServiceUnavailable)
	}
}

func (api *AdminAPI) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var req model.RemoveNodePayload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "invalid request: %v"}`, err), http.StatusBadRequest)
		return
	}

	if req.NodeID == "" {
		http.Error(w, `{"error": "node_id is required"}`, http.StatusBadRequest)
		return
	}

	respCh := make(chan model.ToProducerResponse, 1)
	select {
	case api.commandCh <- model.Command{
		Type:       model.CmdRemoveNode,
		RemoveNode: &model.RemoveNodePayload{NodeID: req.NodeID},
		RespInfo: &model.RespInfo{
			RequestID: api.nextRequestID(),
			RespCh:    respCh,
		},
	}:
		select {
		case resp := <-respCh:
			if resp.Status == model.ToProxyRespStatusError {
				http.Error(w, fmt.Sprintf(`{"error": "%v"}`, resp.Error), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
		case <-time.After(5 * time.Second):
			http.Error(w, `{"error": "command timeout"}`, http.StatusGatewayTimeout)
		}
	case <-time.After(1 * time.Second):
		http.Error(w, `{"error": "cluster busy, please retry"}`, http.StatusServiceUnavailable)
	}
}

func (api *AdminAPI) handleTransferLeader(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var req model.TransferLeaderPayload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "invalid request: %v"}`, err), http.StatusBadRequest)
		return
	}

	if req.TargetNodeID == "" {
		http.Error(w, `{"error": "node_id is required"}`, http.StatusBadRequest)
		return
	}

	respCh := make(chan model.ToProducerResponse, 1)
	select {
	case api.commandCh <- model.Command{
		Type:     model.CmdTransferLeader,
		Transfer: &model.TransferLeaderPayload{TargetNodeID: req.TargetNodeID},
		RespInfo: &model.RespInfo{
			RequestID: api.nextRequestID(),
			RespCh:    respCh,
		},
	}:
		select {
		case resp := <-respCh:
			if resp.Status == model.ToProxyRespStatusError {
				http.Error(w, fmt.Sprintf(`{"error": "%v"}`, resp.Error), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
		case <-time.After(5 * time.Second):
			http.Error(w, `{"error": "command timeout"}`, http.StatusGatewayTimeout)
		}
	case <-time.After(1 * time.Second):
		http.Error(w, `{"error": "cluster busy, please retry"}`, http.StatusServiceUnavailable)
	}
}

func (api *AdminAPI) handleGetClusterInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Get leader
	leaderID := api.leaderID.Load()
	leaderStr := ""
	if leaderID != nil {
		if leader, ok := leaderID.(string); ok {
			leaderStr = leader
		}
	}

	// Get members
	voters, learners := api.members.Get()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"leader_id": leaderStr,
		"members": map[string]any{
			"voters":   voters,
			"learners": learners,
		},
	})
}
