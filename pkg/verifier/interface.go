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

import "crypto/x509"

type Identity struct {
	NodeID     string
	ServerName string
}

// PeerVerifier: Validates peer certificates during TLS handshake
type TLSVerifier interface {
	// VerifyPeer validates the peer's certificate for a given node ID
	VerifyPeer(cert *x509.Certificate, expected Identity) error
}
