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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m-javani/cue/internal/model"
	"github.com/m-javani/cue/internal/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Test Helpers
// =============================================================================

func setupTestAPI(t *testing.T) (*AdminAPI, *model.Members, *atomic.Value, chan model.Command, string, func()) {
	logger, err := testutils.NewDevLogger()
	require.NoError(t, err)

	// Create temp directory for auth file
	tempDir := t.TempDir()
	authFile := filepath.Join(tempDir, "auth.yaml")

	// Write initial auth config
	authYAML := `tokens:
  - token: abc123
    role: admin
  - token: xyz789
    role: monitoring
  - token: readonly456
    role: monitoring
`
	err = os.WriteFile(authFile, []byte(authYAML), 0644)
	require.NoError(t, err)

	// Create command channel - buffer large enough for all tests
	commandCh := make(chan model.Command, 100)

	// Create members
	members := &model.Members{}
	members.Update([]string{"node1", "node2"}, []string{"learner1"})

	// Create leaderID
	leaderID := &atomic.Value{}
	leaderID.Store("node1")

	// Create API - this takes send-only channel
	api := NewAdminAPI(
		"127.0.0.1",
		0, // Use random port
		authFile,
		commandCh, // This is chan<- model.Command in the API
		members,
		leaderID,
		logger,
	)

	// Start server in background
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		if err := api.Run(ctx); err != nil && err != context.Canceled {
			errCh <- err
		}
		close(errCh)
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	cleanup := func() {
		cancel()
		// Wait for server to shutdown
		time.Sleep(100 * time.Millisecond)
	}

	// Return the bidirectional channel so we can both send and receive
	return api, members, leaderID, commandCh, authFile, cleanup
}

func makeRequest(t *testing.T, api *AdminAPI, method, path, token string, body interface{}) *httptest.ResponseRecorder {
	var reqBody []byte
	if body != nil {
		var err error
		reqBody, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	rr := httptest.NewRecorder()
	handler := api.httpServer.Handler
	handler.ServeHTTP(rr, req)

	return rr
}

func makeRequestWithoutBearer(t *testing.T, api *AdminAPI, method, path, token string, body interface{}) *httptest.ResponseRecorder {
	var reqBody []byte
	if body != nil {
		var err error
		reqBody, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", token) // No "Bearer " prefix
	}

	rr := httptest.NewRecorder()
	handler := api.httpServer.Handler
	handler.ServeHTTP(rr, req)

	return rr
}

// respondToCommand reads a command from the channel and sends a response
func respondToCommand(t *testing.T, commandCh chan model.Command, success bool, errorMsg string) {
	select {
	case cmd := <-commandCh:
		if cmd.RespInfo != nil && cmd.RespInfo.RespCh != nil {
			resp := model.ToProducerResponse{
				Status: model.ToProxyRespStatusSuccess,
			}
			if !success {
				resp.Status = model.ToProxyRespStatusError
				resp.Error = errorMsg
			}
			select {
			case cmd.RespInfo.RespCh <- resp:
			case <-time.After(1 * time.Second):
				t.Error("timeout sending response")
			}
		}
	case <-time.After(6 * time.Second):
		t.Error("timeout waiting for command")
	}
}

// respondToCommandAsync responds to a command in a separate goroutine
// This is useful when you want to respond after the request is made
func respondToCommandAsync(t *testing.T, commandCh chan model.Command, success bool, errorMsg string) {
	go func() {
		respondToCommand(t, commandCh, success, errorMsg)
	}()
}

// makeRequestAndRespond makes a request and automatically responds to the command
// This is for tests that expect a response from the cluster (success or error)
func makeRequestAndRespond(t *testing.T, api *AdminAPI, commandCh chan model.Command, method, path, token string, body interface{}, success bool, errorMsg string) *httptest.ResponseRecorder {
	// Setup the response handler BEFORE making the request
	go func() {
		// Small delay to ensure the request is being processed
		time.Sleep(50 * time.Millisecond)
		respondToCommand(t, commandCh, success, errorMsg)
	}()

	return makeRequest(t, api, method, path, token, body)
}

// makeRequestAndExpectTimeout makes a request but doesn't respond, expecting a timeout
func makeRequestAndExpectTimeout(t *testing.T, api *AdminAPI, commandCh chan model.Command, method, path, token string, body interface{}) *httptest.ResponseRecorder {
	// Don't respond to the command - it will timeout
	// But we need to consume the command to prevent channel blocking
	go func() {
		time.Sleep(100 * time.Millisecond)
		// Just read and discard the command to keep channel clean
		select {
		case <-commandCh:
			// Command received but we won't respond
		case <-time.After(1 * time.Second):
			// No command received
		}
	}()

	return makeRequest(t, api, method, path, token, body)
}

// makeRequestAndExpectClusterBusy makes a request when the cluster is busy
func makeRequestAndExpectClusterBusy(t *testing.T, api *AdminAPI, commandCh chan model.Command, method, path, token string, body interface{}) *httptest.ResponseRecorder {
	// Fill the command channel to make it busy
	// We need to fill it completely so that sending blocks
	for i := 0; i < cap(commandCh); i++ {
		select {
		case commandCh <- model.Command{Type: model.CmdAddNode}:
			// Successfully sent
		default:
			// Channel is full
			i = 10
		}
	}

	// Wait a moment for the channel to be full
	time.Sleep(10 * time.Millisecond)

	return makeRequest(t, api, method, path, token, body)
}

func TestAdminAPI_AddVoter(t *testing.T) {
	api, _, _, commandCh, _, cleanup := setupTestAPI(t)
	defer cleanup()

	tests := []struct {
		name           string
		token          string
		payload        model.AddNodePayload
		expectedCode   int
		respondSuccess bool
		errorMsg       string
	}{
		{
			name:           "successful add voter with admin",
			token:          "abc123",
			payload:        model.AddNodePayload{NodeID: "new-node"},
			expectedCode:   http.StatusAccepted,
			respondSuccess: true,
			errorMsg:       "",
		},
		{
			name:           "missing node_id",
			token:          "abc123",
			payload:        model.AddNodePayload{NodeID: ""},
			expectedCode:   http.StatusBadRequest,
			respondSuccess: false,
			errorMsg:       "",
		},
		{
			name:           "unauthorized with monitoring role",
			token:          "xyz789",
			payload:        model.AddNodePayload{NodeID: "new-node"},
			expectedCode:   http.StatusForbidden,
			respondSuccess: false,
			errorMsg:       "",
		},
		{
			name:           "invalid token",
			token:          "invalid",
			payload:        model.AddNodePayload{NodeID: "new-node"},
			expectedCode:   http.StatusUnauthorized,
			respondSuccess: false,
			errorMsg:       "",
		},
		{
			name:           "no token",
			token:          "",
			payload:        model.AddNodePayload{NodeID: "new-node"},
			expectedCode:   http.StatusUnauthorized,
			respondSuccess: false,
			errorMsg:       "",
		},
		{
			name:           "error response from cluster",
			token:          "abc123",
			payload:        model.AddNodePayload{NodeID: "new-node"},
			expectedCode:   http.StatusInternalServerError,
			respondSuccess: false,
			errorMsg:       "cluster error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rr *httptest.ResponseRecorder

			// For tests that need a response, handle it
			if tt.expectedCode == http.StatusAccepted || tt.expectedCode == http.StatusInternalServerError {
				rr = makeRequestAndRespond(t, api, commandCh, http.MethodPost, "/cluster/promote-learner",
					tt.token, tt.payload, tt.respondSuccess, tt.errorMsg)
			} else {
				rr = makeRequest(t, api, http.MethodPost, "/cluster/promote-learner", tt.token, tt.payload)
			}

			assert.Equal(t, tt.expectedCode, rr.Code)
		})
	}
}

func TestAdminAPI_RemoveVoter(t *testing.T) {
	api, _, _, commandCh, _, cleanup := setupTestAPI(t)
	defer cleanup()

	tests := []struct {
		name           string
		token          string
		payload        model.RemoveNodePayload
		expectedCode   int
		respondSuccess bool
		errorMsg       string
	}{
		{
			name:           "successful remove voter with admin",
			token:          "abc123",
			payload:        model.RemoveNodePayload{NodeID: "node2"},
			expectedCode:   http.StatusAccepted,
			respondSuccess: true,
			errorMsg:       "",
		},
		{
			name:           "missing node_id",
			token:          "abc123",
			payload:        model.RemoveNodePayload{NodeID: ""},
			expectedCode:   http.StatusBadRequest,
			respondSuccess: false,
			errorMsg:       "",
		},
		{
			name:           "unauthorized with monitoring role",
			token:          "xyz789",
			payload:        model.RemoveNodePayload{NodeID: "node2"},
			expectedCode:   http.StatusForbidden,
			respondSuccess: false,
			errorMsg:       "",
		},
		{
			name:           "invalid token",
			token:          "invalid",
			payload:        model.RemoveNodePayload{NodeID: "node2"},
			expectedCode:   http.StatusUnauthorized,
			respondSuccess: false,
			errorMsg:       "",
		},
		{
			name:           "no token",
			token:          "",
			payload:        model.RemoveNodePayload{NodeID: "node2"},
			expectedCode:   http.StatusUnauthorized,
			respondSuccess: false,
			errorMsg:       "",
		},
		{
			name:           "error response from cluster",
			token:          "abc123",
			payload:        model.RemoveNodePayload{NodeID: "node2"},
			expectedCode:   http.StatusInternalServerError,
			respondSuccess: false,
			errorMsg:       "cluster error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rr *httptest.ResponseRecorder

			if tt.expectedCode == http.StatusAccepted || tt.expectedCode == http.StatusInternalServerError {
				rr = makeRequestAndRespond(t, api, commandCh, http.MethodPost, "/cluster/demote-voter",
					tt.token, tt.payload, tt.respondSuccess, tt.errorMsg)
			} else {
				rr = makeRequest(t, api, http.MethodPost, "/cluster/demote-voter", tt.token, tt.payload)
			}

			assert.Equal(t, tt.expectedCode, rr.Code)
		})
	}
}

func TestAdminAPI_TransferLeader(t *testing.T) {
	api, _, _, commandCh, _, cleanup := setupTestAPI(t)
	defer cleanup()

	tests := []struct {
		name           string
		token          string
		payload        model.TransferLeaderPayload
		expectedCode   int
		respondSuccess bool
		errorMsg       string
	}{
		{
			name:           "successful transfer with admin",
			token:          "abc123",
			payload:        model.TransferLeaderPayload{TargetNodeID: "node2"},
			expectedCode:   http.StatusAccepted,
			respondSuccess: true,
			errorMsg:       "",
		},
		{
			name:           "missing target_node_id",
			token:          "abc123",
			payload:        model.TransferLeaderPayload{TargetNodeID: ""},
			expectedCode:   http.StatusBadRequest,
			respondSuccess: false,
			errorMsg:       "",
		},
		{
			name:           "unauthorized with monitoring role",
			token:          "xyz789",
			payload:        model.TransferLeaderPayload{TargetNodeID: "node2"},
			expectedCode:   http.StatusForbidden,
			respondSuccess: false,
			errorMsg:       "",
		},
		{
			name:           "invalid token",
			token:          "invalid",
			payload:        model.TransferLeaderPayload{TargetNodeID: "node2"},
			expectedCode:   http.StatusUnauthorized,
			respondSuccess: false,
			errorMsg:       "",
		},
		{
			name:           "no token",
			token:          "",
			payload:        model.TransferLeaderPayload{TargetNodeID: "node2"},
			expectedCode:   http.StatusUnauthorized,
			respondSuccess: false,
			errorMsg:       "",
		},
		{
			name:           "error response from cluster",
			token:          "abc123",
			payload:        model.TransferLeaderPayload{TargetNodeID: "node2"},
			expectedCode:   http.StatusInternalServerError,
			respondSuccess: false,
			errorMsg:       "cluster error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rr *httptest.ResponseRecorder

			if tt.expectedCode == http.StatusAccepted || tt.expectedCode == http.StatusInternalServerError {
				rr = makeRequestAndRespond(t, api, commandCh, http.MethodPost, "/cluster/transfer-leader",
					tt.token, tt.payload, tt.respondSuccess, tt.errorMsg)
			} else {
				rr = makeRequest(t, api, http.MethodPost, "/cluster/transfer-leader", tt.token, tt.payload)
			}

			assert.Equal(t, tt.expectedCode, rr.Code)
		})
	}
}
func TestAdminAPI_CommandTimeout(t *testing.T) {
	api, _, _, commandCh, _, cleanup := setupTestAPI(t)
	defer cleanup()

	// Make request but don't respond to command
	rr := makeRequestAndExpectTimeout(t, api, commandCh, http.MethodPost, "/cluster/promote-learner", "abc123",
		model.AddNodePayload{NodeID: "new-node"})

	assert.Equal(t, http.StatusGatewayTimeout, rr.Code)

	var errResp map[string]string
	err := json.Unmarshal(rr.Body.Bytes(), &errResp)
	require.NoError(t, err)
	assert.Equal(t, "command timeout", errResp["error"])
}

func TestAdminAPI_ClusterBusy(t *testing.T) {
	api, _, _, commandCh, _, cleanup := setupTestAPI(t)
	defer cleanup()

	rr := makeRequestAndExpectClusterBusy(t, api, commandCh, http.MethodPost, "/cluster/promote-learner", "abc123",
		model.AddNodePayload{NodeID: "new-node"})

	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)

	var errResp map[string]string
	err := json.Unmarshal(rr.Body.Bytes(), &errResp)
	require.NoError(t, err)
	assert.Equal(t, "cluster busy, please retry", errResp["error"])
}

func TestNextRequestID(t *testing.T) {
	api := &AdminAPI{}

	id1 := api.nextRequestID()
	id2 := api.nextRequestID()

	if id1 == id2 {
		t.Errorf("IDs should be unique, got %s and %s", id1, id2)
	}

	// Verify it's a valid base36 string
	if _, err := strconv.ParseUint(id1, 36, 64); err != nil {
		t.Errorf("Invalid base36 string: %v", err)
	}
}

func TestAdminAPI_Health(t *testing.T) {
	api, _, _, _, _, cleanup := setupTestAPI(t)
	defer cleanup()

	rr := makeRequest(t, api, http.MethodGet, "/health", "", nil)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp map[string]string
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, "ok", resp["status"])
}

func TestAdminAPI_GetClusterInfo(t *testing.T) {
	api, members, leaderID, _, _, cleanup := setupTestAPI(t)
	defer cleanup()

	// Setup known state
	members.Update([]string{"node1", "node2", "node3"}, []string{"learner1", "learner2"})
	leaderID.Store("node2")

	rr := makeRequest(t, api, http.MethodGet, "/cluster/info", "", nil)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp map[string]any
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)

	// Check leader_id
	assert.Equal(t, "node2", resp["leader_id"])

	// Check members
	membersMap, ok := resp["members"].(map[string]any)
	require.True(t, ok, "members should be a map")

	// Check voters
	voters, ok := membersMap["voters"].([]any)
	require.True(t, ok, "voters should be a slice")
	assert.ElementsMatch(t, []any{"node1", "node2", "node3"}, voters)

	// Check learners
	learners, ok := membersMap["learners"].([]any)
	require.True(t, ok, "learners should be a slice")
	assert.ElementsMatch(t, []any{"learner1", "learner2"}, learners)
}
