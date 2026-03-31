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
	"testing"
)

// TestSuite defines common tests that all resolvers should pass
type TestSuite struct {
	Name       string
	Resolver   AddressResolver
	NodeID     string
	Expected   string
	ShouldFail bool
}

// RunResolverTests runs a suite of tests against any AddressResolver
func RunResolverTests(t *testing.T, suite TestSuite) {
	t.Helper()
	t.Run(suite.Name, func(t *testing.T) {
		ctx := context.Background()

		// Test successful resolution
		addr, err := suite.Resolver.Resolve(ctx, suite.NodeID)
		if suite.ShouldFail {
			if err == nil {
				t.Errorf("expected error but got none")
			}
			return
		}

		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if addr != suite.Expected {
			t.Errorf("got %v, want %v", addr, suite.Expected)
		}
	})

	// Test context cancellation - skip for static resolvers
	t.Run(suite.Name+"_canceled_context", func(t *testing.T) {
		// Skip if this is a StaticResolver (it doesn't use context)
		if _, ok := suite.Resolver.(StaticResolver); ok {
			t.Skip("StaticResolver doesn't use context")
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := suite.Resolver.Resolve(ctx, suite.NodeID)
		// Some resolvers might not support context cancellation
		// We just check that it doesn't panic
		if err == nil {
			t.Log("Resolver completed despite context cancellation (may be expected)")
		}
	})
}
