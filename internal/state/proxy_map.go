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

package state

// ProxyMap - tracks available consumers
type ProxyState struct {
	Consumers     int
	LastHeartbeat int64
}

type ProxyMap struct {
	proxies        map[string]*ProxyState
	available      []string       // ordered list for round-robin
	availableIndex map[string]int // proxyID → index in available slice
	roundRobin     int
}

func NewProxyMap() *ProxyMap {
	return &ProxyMap{
		proxies:        make(map[string]*ProxyState, 64),
		available:      make([]string, 0, 64),
		availableIndex: make(map[string]int, 64),
	}
}

func (pm *ProxyMap) Update(proxyID string, score int, timestamp int64) {
	state, exists := pm.proxies[proxyID]
	if !exists {
		state = &ProxyState{}
		pm.proxies[proxyID] = state
	}

	wasAvailable := state.Consumers > 0
	state.Consumers = score
	state.LastHeartbeat = timestamp
	isAvailable := score > 0

	if !wasAvailable && isAvailable {
		pm.addToAvailable(proxyID)
	} else if wasAvailable && !isAvailable {
		pm.removeFromAvailable(proxyID)
	}
}

func (pm *ProxyMap) addToAvailable(proxyID string) {
	if _, exists := pm.availableIndex[proxyID]; exists {
		return // already in list
	}
	pm.availableIndex[proxyID] = len(pm.available)
	pm.available = append(pm.available, proxyID)
}

func (pm *ProxyMap) removeFromAvailable(proxyID string) {
	idx, exists := pm.availableIndex[proxyID]
	if !exists {
		return
	}

	// Swap with last element for O(1) removal
	lastIdx := len(pm.available) - 1
	lastID := pm.available[lastIdx]

	pm.available[idx] = lastID
	pm.availableIndex[lastID] = idx

	pm.available = pm.available[:lastIdx]
	delete(pm.availableIndex, proxyID)
}

// GetNextAvailable returns the next available proxy and the number of consumers
func (pm *ProxyMap) GetNextAvailable() (string, int, bool) {
	if len(pm.available) == 0 {
		return "", 0, false
	}

	// Try to find a proxy with capacity, starting from current round-robin position
	startIdx := pm.roundRobin
	for i := 0; i < len(pm.available); i++ {
		idx := (startIdx + i) % len(pm.available)
		proxyID := pm.available[idx]
		state := pm.proxies[proxyID]

		consumers := state.Consumers
		if consumers > 0 {
			// Found a proxy with capacity
			pm.roundRobin = (idx + 1) % len(pm.available) // Move to next for next call
			return proxyID, consumers, true
		}
	}

	// No proxy has capacity
	return "", 0, false
}

func (pm *ProxyMap) CleanupStale(now int64, timeoutSeconds int64) {
	timeout := timeoutSeconds * 1000 // to milliseconds
	cleaned := false

	for id, state := range pm.proxies {
		if now-state.LastHeartbeat > timeout {
			delete(pm.proxies, id)
			pm.removeFromAvailable(id)
			cleaned = true
		}
	}

	if cleaned {
		pm.rebuildAvailableCache()
	}
}

func (pm *ProxyMap) rebuildAvailableCache() {
	pm.available = pm.available[:0]
	clear(pm.availableIndex)

	for id, state := range pm.proxies {
		if state.Consumers > 0 {
			pm.availableIndex[id] = len(pm.available)
			pm.available = append(pm.available, id)
		}
	}
	pm.roundRobin = 0
}

// Exists checks if a proxy with the given ID exists in the proxy map
func (pm *ProxyMap) Exists(proxyID string) bool {
	_, exists := pm.proxies[proxyID]
	return exists
}
