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

package testutils

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// testAddressResolver implements discovery.AddressResolver for testing
// It resolves node IDs in the format "nodeID-port" to localhost:port
type TestAddressResolver struct{}

func (t TestAddressResolver) Resolve(ctx context.Context, node string) (string, error) {
	// Pattern: "userNodeID-randomPort" e.g. "NC78YT-49217"
	// Extract port after the LAST '-'
	lastDash := strings.LastIndexByte(node, '-')
	if lastDash == -1 {
		return "", fmt.Errorf("invalid node format: %s", node)
	}
	portStr := node[lastDash+1:]
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return "", fmt.Errorf("invalid port in node %s: %w", node, err)
	}
	return fmt.Sprintf("127.0.0.1:%d", port), nil
}
