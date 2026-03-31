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

// dnsLookuper interface for testing
type dnsLookuper interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// DNSResolver uses DNS for service discovery
type DNSResolver struct {
	Port     int
	Domain   string
	resolver dnsLookuper
}

// NewDNSResolver creates a DNS resolver with default settings
func NewDNSResolver(port int, domain string) DNSResolver {
	return DNSResolver{
		Port:     port,
		Domain:   domain,
		resolver: net.DefaultResolver,
	}
}

// newDNSResolverWithResolver is for testing only
func newDNSResolverWithResolver(port int, domain string, resolver dnsLookuper) DNSResolver {
	return DNSResolver{
		Port:     port,
		Domain:   domain,
		resolver: resolver,
	}
}

func (r DNSResolver) Resolve(ctx context.Context, nodeID string) (string, error) {
	host := nodeID
	if r.Domain != "" {
		host = nodeID + "." + r.Domain
	}

	addrs, err := r.resolver.LookupHost(ctx, host)
	if err != nil {
		return "", err
	}

	if len(addrs) == 0 {
		return "", fmt.Errorf("no IPs found for %s", host)
	}

	return net.JoinHostPort(addrs[0], strconv.Itoa(r.Port)), nil
}
