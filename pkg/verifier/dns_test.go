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
	"testing"
)

func TestDNSVerifier_VerifyPeer(t *testing.T) {
	// Generate a test certificate with DNS SAN (DER format - matches cert.Raw)
	certDER, err := generateTestCertDER(certOptions{
		commonName: "node1",
		dnsNames:   []string{"node1.cluster.local"},
	})
	if err != nil {
		t.Fatalf("failed to generate test cert: %v", err)
	}

	tests := []struct {
		name      string
		verifier  DNSVerifier
		rawCerts  [][]byte
		peerID    string
		expectErr bool
	}{
		{
			name: "valid certificate",
			verifier: DNSVerifier{
				Domain: "cluster.local",
			},
			rawCerts:  [][]byte{certDER},
			peerID:    "node1",
			expectErr: false,
		},
		{
			name: "invalid certificate - wrong domain",
			verifier: DNSVerifier{
				Domain: "other.local",
			},
			rawCerts:  [][]byte{certDER},
			peerID:    "node1",
			expectErr: true,
		},
		{
			name: "invalid certificate - wrong node ID",
			verifier: DNSVerifier{
				Domain: "cluster.local",
			},
			rawCerts:  [][]byte{certDER},
			peerID:    "node2",
			expectErr: true,
		},
		{
			name: "no certificates",
			verifier: DNSVerifier{
				Domain: "cluster.local",
			},
			rawCerts:  [][]byte{},
			peerID:    "node1",
			expectErr: true,
		},
		{
			name: "invalid certificate data",
			verifier: DNSVerifier{
				Domain: "cluster.local",
			},
			rawCerts:  [][]byte{[]byte("invalid cert data")},
			peerID:    "node1",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.verifier.VerifyPeer(tt.rawCerts, tt.peerID)
			if (err != nil) != tt.expectErr {
				t.Errorf("error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}
