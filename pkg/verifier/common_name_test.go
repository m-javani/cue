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
	"testing"
)

func TestCNVerifier_VerifyPeer(t *testing.T) {
	// Generate a test certificate with CN (DER format - matches cert.Raw)
	cert, err := generateX509Certificate(certOptions{
		commonName: "node1",
	})
	if err != nil {
		t.Fatalf("failed to generate test cert: %v", err)
	}

	tests := []struct {
		name         string
		verifier     CNVerifier
		cert         *x509.Certificate
		expectedNode string
		expectErr    bool
	}{
		{
			name:         "valid certificate",
			verifier:     CNVerifier{},
			cert:         cert,
			expectedNode: "node1",
			expectErr:    false,
		},
		{
			name:         "invalid - wrong CN",
			verifier:     CNVerifier{},
			cert:         cert,
			expectedNode: "node2",
			expectErr:    true,
		},
		{
			name:         "no certificates",
			verifier:     CNVerifier{},
			cert:         cert,
			expectedNode: "node1",
			expectErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.verifier.VerifyPeer(tt.cert, Identity{NodeID: tt.expectedNode})
			if (err != nil) != tt.expectErr {
				t.Errorf("error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}
