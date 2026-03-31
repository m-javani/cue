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

package verifier

import (
	"crypto/x509"
	"fmt"
	"strings"
)

// SPIFFEVerifier validates certificates using SPIFFE (Secure Production Identity Framework for Everyone)
// SPIFFE IDs have the format: spiffe://<trust_domain>/<namespace>/<workload>
type SPIFFEVerifier struct {
	TrustDomain string // e.g., "example.org"
	Namespace   string // e.g., "production"
}

// VerifyPeer validates that the certificate's SPIFFE ID matches the expected peer ID
func (s SPIFFEVerifier) VerifyPeer(rawCerts [][]byte, peerID string) error {
	if len(rawCerts) == 0 {
		return fmt.Errorf("no certificates provided")
	}

	cert, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return fmt.Errorf("failed to parse certificate: %w", err)
	}

	// Expected SPIFFE ID: spiffe://<trust_domain>/<namespace>/<peerID>
	expectedSpiffeID := fmt.Sprintf("spiffe://%s/%s/%s", s.TrustDomain, s.Namespace, peerID)

	// Check all URIs in the certificate for the expected SPIFFE ID
	for _, uri := range cert.URIs {
		if uri.String() == expectedSpiffeID {
			return nil
		}
	}

	// DNS SAN fallback - but ONLY if the DNS name includes the namespace
	// Format: <peerID>.<namespace>.<trust_domain>
	expectedDNSName := fmt.Sprintf("%s.%s.%s", peerID, s.Namespace, s.TrustDomain)
	for _, dnsName := range cert.DNSNames {
		if dnsName == expectedDNSName {
			return nil
		}
	}

	return fmt.Errorf("SPIFFE ID not found in certificate, expected: %s", expectedSpiffeID)
}

// ValidateSPIFFEID validates a SPIFFE ID format
func ValidateSPIFFEID(id string) bool {
	// spiffe://<trust_domain>/<namespace>/<workload>
	if !strings.HasPrefix(id, "spiffe://") {
		return false
	}

	// Remove the spiffe:// prefix
	rest := strings.TrimPrefix(id, "spiffe://")
	parts := strings.Split(rest, "/")

	// Need at least 3 parts: trust_domain, namespace, workload
	if len(parts) < 3 {
		return false
	}

	// Trust domain and namespace cannot be empty
	if parts[0] == "" || parts[1] == "" {
		return false
	}

	return true
}
