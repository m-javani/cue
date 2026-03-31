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

import (
	"testing"
	"time"
)

func TestProxyMap_Update(t *testing.T) {
	pm := NewProxyMap()
	now := time.Now().UnixMilli()

	// Add a proxy with consumers
	pm.Update("proxy-1", 5, now)

	proxyID, consumers, ok := pm.GetNextAvailable()
	if !ok {
		t.Fatal("expected proxy to be available")
	}
	if proxyID != "proxy-1" {
		t.Errorf("expected proxy-1, got %s", proxyID)
	}
	if consumers != 5 {
		t.Errorf("expected consumers 5, got %d", consumers)
	}

	// Update to 0 consumers (should remove from available)
	pm.Update("proxy-1", 0, now)
	_, _, ok = pm.GetNextAvailable()
	if ok {
		t.Error("expected no available proxies after setting consumers to 0")
	}
}

func TestProxyMap_Exists(t *testing.T) {
	pm := NewProxyMap()
	now := time.Now().UnixMilli()

	// Test: proxy doesn't exist yet
	if pm.Exists("proxy-1") {
		t.Error("expected proxy-1 to not exist initially")
	}

	// Add a proxy
	pm.Update("proxy-1", 5, now)

	// Test: proxy exists after update
	if !pm.Exists("proxy-1") {
		t.Error("expected proxy-1 to exist after update")
	}

	// Test: proxy with different ID doesn't exist
	if pm.Exists("proxy-2") {
		t.Error("expected proxy-2 to not exist")
	}

	// Update proxy to 0 consumers (should still exist in map)
	pm.Update("proxy-1", 0, now)
	if !pm.Exists("proxy-1") {
		t.Error("expected proxy-1 to still exist after setting consumers to 0")
	}

	// Cleanup should remove stale proxies
	timeoutSeconds := int64(10)
	pm.CleanupStale(now+timeoutSeconds*1000+1, timeoutSeconds)

	// Test: proxy removed after cleanup
	if pm.Exists("proxy-1") {
		t.Error("expected proxy-1 to be removed after cleanup")
	}
}

func TestProxyMap_MultipleProxies(t *testing.T) {
	pm := NewProxyMap()
	now := time.Now().UnixMilli()

	// Add multiple proxies
	pm.Update("proxy-A", 3, now)
	pm.Update("proxy-B", 5, now)
	pm.Update("proxy-C", 2, now)

	// Should round-robin through all
	expected := []string{"proxy-A", "proxy-B", "proxy-C"}
	for i := 0; i < 3; i++ {
		id, _, ok := pm.GetNextAvailable()
		if !ok {
			t.Fatalf("expected proxy at iteration %d", i)
		}
		if id != expected[i] {
			t.Errorf("iteration %d: expected %s, got %s", i, expected[i], id)
		}
	}

	// Should wrap around
	id, _, ok := pm.GetNextAvailable()
	if !ok || id != "proxy-A" {
		t.Errorf("expected wrap to proxy-A, got %s", id)
	}
}

func TestProxyMap_RemoveProxy(t *testing.T) {
	pm := NewProxyMap()
	now := time.Now().UnixMilli()

	pm.Update("proxy-A", 3, now)
	pm.Update("proxy-B", 5, now)
	pm.Update("proxy-C", 2, now)

	// Remove proxy-B
	pm.Update("proxy-B", 0, now)

	// Should only get proxy-A and proxy-C
	ids := []string{}
	for i := 0; i < 2; i++ {
		id, _, ok := pm.GetNextAvailable()
		if !ok {
			t.Fatalf("expected proxy at iteration %d", i)
		}
		ids = append(ids, id)
	}

	// Verify proxy-B not in results
	for _, id := range ids {
		if id == "proxy-B" {
			t.Error("proxy-B should not be available")
		}
	}
}

func TestProxyMap_CleanupStale(t *testing.T) {
	pm := NewProxyMap()
	now := time.Now().UnixMilli()

	// Add two proxies
	pm.Update("proxy-1", 3, now)
	pm.Update("proxy-2", 5, now-60000) // 60 seconds old

	// Cleanup with 30 second timeout
	pm.CleanupStale(now, 30)

	// Only proxy-1 should remain
	_, _, ok := pm.GetNextAvailable()
	if !ok {
		t.Fatal("expected proxy-1 to be available")
	}

	id, _, _ := pm.GetNextAvailable()
	if id != "proxy-1" {
		t.Errorf("expected proxy-1, got %s", id)
	}

	// Should not have proxy-2
	if _, exists := pm.proxies["proxy-2"]; exists {
		t.Error("proxy-2 should have been cleaned up")
	}
}

func TestProxyMap_ZeroConsumersNotAvailable(t *testing.T) {
	pm := NewProxyMap()
	now := time.Now().UnixMilli()

	// Add proxy with 0 consumers - should not be available
	pm.Update("proxy-1", 0, now)

	_, _, ok := pm.GetNextAvailable()
	if ok {
		t.Error("proxy with 0 consumers should not be available")
	}

	// Update to positive consumers - should become available
	pm.Update("proxy-1", 3, now)

	_, _, ok = pm.GetNextAvailable()
	if !ok {
		t.Error("proxy should be available after updating to positive consumers")
	}
}

func TestProxyMap_RoundRobinPersistence(t *testing.T) {
	pm := NewProxyMap()
	now := time.Now().UnixMilli()

	pm.Update("proxy-A", 3, now)
	pm.Update("proxy-B", 5, now)

	// First call gets proxy-A
	id1, _, _ := pm.GetNextAvailable()
	if id1 != "proxy-A" {
		t.Errorf("expected proxy-A, got %s", id1)
	}

	// Second call gets proxy-B
	id2, _, _ := pm.GetNextAvailable()
	if id2 != "proxy-B" {
		t.Errorf("expected proxy-B, got %s", id2)
	}

	// Third call wraps to proxy-A
	id3, _, _ := pm.GetNextAvailable()
	if id3 != "proxy-A" {
		t.Errorf("expected wrap to proxy-A, got %s", id3)
	}
}

func TestProxyMap_EmptyProxyMap(t *testing.T) {
	pm := NewProxyMap()

	_, _, ok := pm.GetNextAvailable()
	if ok {
		t.Error("GetNextAvailable on empty map should return false")
	}
}

func TestProxyMap_UpdateExistingProxy(t *testing.T) {
	pm := NewProxyMap()
	now := time.Now().UnixMilli()

	pm.Update("proxy-1", 3, now)

	// Update consumers
	pm.Update("proxy-1", 7, now+1000)

	id, consumers, ok := pm.GetNextAvailable()
	if !ok {
		t.Fatal("expected proxy to be available")
	}
	if id != "proxy-1" {
		t.Errorf("expected proxy-1, got %s", id)
	}
	if consumers != 7 {
		t.Errorf("expected consumers 7, got %d", consumers)
	}
}
