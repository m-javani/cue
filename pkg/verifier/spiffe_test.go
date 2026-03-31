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

func TestSPIFFEVerifier_VerifyPeer(t *testing.T) {
	tests := []struct {
		name      string
		verifier  SPIFFEVerifier
		peerID    string
		certOpts  certOptions
		expectErr bool
	}{
		{
			name: "valid SPIFFE ID via URI SAN",
			verifier: SPIFFEVerifier{
				TrustDomain: "example.org",
				Namespace:   "production",
			},
			peerID: "node1",
			certOpts: certOptions{
				commonName: "node1",
				dnsNames:   []string{"node1.production.example.org"},
				uris:       []string{"spiffe://example.org/production/node1"},
			},
			expectErr: false,
		},
		{
			name: "valid DNS SAN fallback",
			verifier: SPIFFEVerifier{
				TrustDomain: "example.org",
				Namespace:   "production",
			},
			peerID: "node1",
			certOpts: certOptions{
				commonName: "node1",
				dnsNames:   []string{"node1.production.example.org"}, // Format: <peerID>.<namespace>.<trust_domain>
				uris:       []string{},                               // No SPIFFE URI, should fallback to DNS
			},
			expectErr: false,
		},
		{
			name: "wrong trust domain",
			verifier: SPIFFEVerifier{
				TrustDomain: "wrong.org",
				Namespace:   "production",
			},
			peerID: "node1",
			certOpts: certOptions{
				commonName: "node1",
				dnsNames:   []string{"node1.production.example.org"},
				uris:       []string{"spiffe://example.org/production/node1"},
			},
			expectErr: true,
		},
		{
			name: "wrong namespace - should fail",
			verifier: SPIFFEVerifier{
				TrustDomain: "example.org",
				Namespace:   "staging",
			},
			peerID: "node1",
			certOpts: certOptions{
				commonName: "node1",
				dnsNames:   []string{"node1.production.example.org"},
				uris:       []string{"spiffe://example.org/production/node1"},
			},
			expectErr: true,
		},
		{
			name: "wrong peer ID",
			verifier: SPIFFEVerifier{
				TrustDomain: "example.org",
				Namespace:   "production",
			},
			peerID: "node2",
			certOpts: certOptions{
				commonName: "node1",
				dnsNames:   []string{"node1.production.example.org"},
				uris:       []string{"spiffe://example.org/production/node1"},
			},
			expectErr: true,
		},
		{
			name: "multiple URIs - correct one found",
			verifier: SPIFFEVerifier{
				TrustDomain: "example.org",
				Namespace:   "production",
			},
			peerID: "node1",
			certOpts: certOptions{
				commonName: "node1",
				dnsNames:   []string{"node1.production.example.org"},
				uris: []string{
					"spiffe://other.org/staging/node1",
					"spiffe://example.org/production/node1",
					"spiffe://example.org/staging/node2",
				},
			},
			expectErr: false,
		},
		{
			name: "no certificates",
			verifier: SPIFFEVerifier{
				TrustDomain: "example.org",
				Namespace:   "production",
			},
			peerID:    "node1",
			certOpts:  certOptions{},
			expectErr: true,
		},
		{
			name: "invalid certificate data",
			verifier: SPIFFEVerifier{
				TrustDomain: "example.org",
				Namespace:   "production",
			},
			peerID:    "node1",
			certOpts:  certOptions{},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rawCerts [][]byte

			switch tt.name {
			case "no certificates":
				rawCerts = [][]byte{}
			case "invalid certificate data":
				rawCerts = [][]byte{[]byte("invalid cert data")}
			default:
				// Generate certificate with the given options
				certDER, err := generateTestCertDER(tt.certOpts)
				if err != nil {
					t.Fatalf("failed to generate test cert: %v", err)
				}
				rawCerts = [][]byte{certDER}
			}

			err := tt.verifier.VerifyPeer(rawCerts, tt.peerID)
			if (err != nil) != tt.expectErr {
				t.Errorf("error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}

func TestValidateSPIFFEID(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want bool
	}{
		{
			name: "valid SPIFFE ID",
			id:   "spiffe://example.org/production/node1",
			want: true,
		},
		{
			name: "valid SPIFFE ID with path",
			id:   "spiffe://example.org/production/workload/service",
			want: true,
		},
		{
			name: "missing spiffe prefix",
			id:   "https://example.org/production/node1",
			want: false,
		},
		{
			name: "too few parts",
			id:   "spiffe://example.org/node1",
			want: false,
		},
		{
			name: "empty trust domain",
			id:   "spiffe:///production/node1",
			want: false,
		},
		{
			name: "empty namespace",
			id:   "spiffe://example.org//node1",
			want: false,
		},
		{
			name: "empty string",
			id:   "",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidateSPIFFEID(tt.id); got != tt.want {
				t.Errorf("ValidateSPIFFEID(%q) = %v, want %v", tt.id, got, tt.want)
			}
		})
	}
}
