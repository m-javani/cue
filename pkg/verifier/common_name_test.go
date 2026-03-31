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

func TestCNVerifier_VerifyPeer(t *testing.T) {
	// Generate a test certificate with CN (DER format - matches cert.Raw)
	certDER, err := generateTestCertDER(certOptions{
		commonName: "node1",
	})
	if err != nil {
		t.Fatalf("failed to generate test cert: %v", err)
	}

	tests := []struct {
		name         string
		verifier     CNVerifier
		rawCerts     [][]byte
		expectedNode string
		expectErr    bool
	}{
		{
			name:         "valid certificate",
			verifier:     CNVerifier{},
			rawCerts:     [][]byte{certDER},
			expectedNode: "node1",
			expectErr:    false,
		},
		{
			name:         "invalid - wrong CN",
			verifier:     CNVerifier{},
			rawCerts:     [][]byte{certDER},
			expectedNode: "node2",
			expectErr:    true,
		},
		{
			name:         "no certificates",
			verifier:     CNVerifier{},
			rawCerts:     [][]byte{},
			expectedNode: "node1",
			expectErr:    true,
		},
		{
			name:         "invalid certificate data",
			verifier:     CNVerifier{},
			rawCerts:     [][]byte{[]byte("invalid cert data")},
			expectedNode: "node1",
			expectErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.verifier.VerifyPeer(tt.rawCerts, tt.expectedNode)
			if (err != nil) != tt.expectErr {
				t.Errorf("error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}
