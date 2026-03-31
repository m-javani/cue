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

package discovery

import (
	"context"
	"fmt"
)

// StaticResolver for testing or static environments
type StaticResolver struct {
	Addresses map[string]string
}

func (r StaticResolver) Resolve(ctx context.Context, nodeID string) (string, error) {
	addr, ok := r.Addresses[nodeID]
	if !ok {
		return "", fmt.Errorf("unknown node: %s", nodeID)
	}
	return addr, nil
}
