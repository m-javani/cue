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
	"net"
	"strconv"
)

// ServiceResolver resolves using DNS without domain
type ServiceResolver struct {
	Port     int
	resolver dnsLookuper // unexported, for testing
}

// NewServiceResolver creates a ServiceResolver with default DNS
func NewServiceResolver(port int) ServiceResolver {
	return ServiceResolver{
		Port:     port,
		resolver: net.DefaultResolver,
	}
}

// newServiceResolverWithResolver is for testing only
func newServiceResolverWithResolver(port int, resolver dnsLookuper) ServiceResolver {
	return ServiceResolver{
		Port:     port,
		resolver: resolver,
	}
}

func (r ServiceResolver) Resolve(ctx context.Context, nodeID string) (string, error) {
	if r.resolver == nil {
		// Log the issue (if you have a logger, use it)
		r.resolver = net.DefaultResolver
	}

	addrs, err := r.resolver.LookupHost(ctx, nodeID)
	if err != nil {
		return "", err
	}

	if len(addrs) == 0 {
		return "", fmt.Errorf("no IPs found for %s", nodeID)
	}

	hp := net.JoinHostPort(addrs[0], strconv.Itoa(r.Port))
	return hp, nil
}
