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

package utils

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"os"
)

func ValidateFilesExit(filesPaths []string) error {
	for _, path := range filesPaths {
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("file %s: %w", path, err)
		}
	}
	return nil
}

func LoadCACerts(caPath string) (*x509.CertPool, error) {
	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}
	return caCertPool, nil
}

// GetTLSVersion generates a stable "TLS version" string from cert/key/CA files.
// Returns the current version hash as a string.
func GetTLSVersion(certPath, keyPath, caPath string) (string, error) {
	hasher := sha256.New()

	// Hash all files in consistent order
	for _, path := range []string{certPath, keyPath, caPath} {
		file, err := os.Open(path)
		if err != nil {
			return "", fmt.Errorf("failed to open %s: %w", path, err)
		}
		defer file.Close()

		if _, err := io.Copy(hasher, file); err != nil {
			return "", fmt.Errorf("failed to hash %s: %w", path, err)
		}
	}

	// Base64-URL (shorter, URL-safe)
	currentVersion := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(hasher.Sum(nil))
	return currentVersion, nil
}

// IsTLSVersionUpdated checks if TLS version has changed compared to previous version.
// Returns (current_version, changed) where changed indicates if files were modified.
func IsTLSVersionUpdated(certPath, keyPath, caPath string, previousVersion string) (string, bool, error) {
	currentVersion, err := GetTLSVersion(certPath, keyPath, caPath)
	if err != nil {
		return "", false, err
	}

	changed := previousVersion == "" || previousVersion != currentVersion
	return currentVersion, changed, nil
}
