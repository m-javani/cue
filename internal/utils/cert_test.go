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
	"os"
	"path/filepath"
	"testing"
)

func giveExampleCertStr() string {
	return `-----BEGIN CERTIFICATE-----
MIIE+TCCAuGgAwIBAgIQCcA7ejfi5pK62N30w7kh+jANBgkqhkiG9w0BAQsFADAN
MQswCQYDVQQDEwJjYTAeFw0yNjA2MjQwNzE0MTVaFw0yNzA2MjQwODE0MTVaMA0x
CzAJBgNVBAMTAmNhMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAyi1P
2GOfyMTkm8o+eEGggOge5onKM0iah55pohg3El2L6CA35bAX+28GaEkYwCryFwmF
JSaqqET9LGXAbv1rW6k6byi6Xcjjt4wzyK2dIXYOLF8aCYuiDHlwLIazqmP+Gw5I
IaYozr0Wm6At+EF7eQjx2P3+8B8U1b98lYE9kH5NfKH6p2H/EK3jTHqAK25Qdfg0
iRgK657elof9dyDXlVMrpZ0PkQEBNI7I4NIxS6ZALKc5pYr2LcAYmRNWfTNtT4Qh
P6U3ccDO0z0fUoQXfkjSUnUD2VdQPNF1ltoou2KWkW+mjTJ/u+0kZ7U/mhqObk3g
mVTPEP0JpzVbWgjY8cdD+lWXp3K+xUvR78Y+zzPdj0Z6znKDUWekfTNt7l7+JHsW
hKopl0B1Wsc9ssF+73v5fZc2VZdabL6Db0Rw5HCQno9EA9WHVfPqUDHdz/OyK7qU
xKu8gqyj2lCeRxe3z95kqaoHMDTVJM5+EWidnoMzerOL037M8DiZmwwDqvp4fGmj
SFyXkmt8oi4WMzjw1t3yvZtkLkBmWCYJkf/U1OQfebeXTNRvd+d3+UkmwPS8m4yh
aUTuYz/ZdR3qFln5EG0zdCyS6a1loZs3LQzQPM+fsYt6Qjr4yeZINsNkonK/XV6k
NaCIC/3ad0zdLJgrGMUO8vRQJ+LblY9GxSwGlV0CAwEAAaNVMFMwDgYDVR0PAQH/
BAQDAgEGMBIGA1UdEwEB/wQIMAYBAf8CAQAwHQYDVR0OBBYEFKmq2PyEMxyfBiG2
tnGPU1wQJQmjMA4GA1UdEQQHMAWCA2NhLjANBgkqhkiG9w0BAQsFAAOCAgEAK5bs
GuizdPGWeMCxFFLS9pCaOac4aCKn70FsoU+05me6gGi9/In14SUQvuL7LaBbtwqt
IZzTkHc+CEqM9kAznVeoVjqZblPuFYxikq1SXkmt7zc510xQP9BHvhS8J6KWbcPM
iYZflF2t2zD9Vc792Vpe4STebFoOI5qHcwtRH0Aoz8wV1qSStFv/rriD72Ujdbmy
Run4CJ7UG2rhFi/YnxWFSnxB22JyUWfY7Qs3KkUN6Cpnium+pruMHSE+F8KeuoQ5
oDJGkiYRNsdys7sQ2n/efUMDt8ceK7ovZlg6fe4hZnjuJEAOmiWoRIiGspX38CkO
gnR1WA6OWD6w4rRspNATDz/27crWIrMgo+opt11pNrX3I490zSafJbM1SbigsLSq
e9RRFykiggCiqXqMGSVRruaKW/AvgTNY2lZ0YNLK6AsDfBu+11/LafQ4wAQFfrZ0
LERTCegZmVa7JikuOPrKdsG9NXmpU09IG6tN3qSv2DRLu9mlEHcD5F2ksG+UP+F8
U3NaOgMm6uwvKLq9xNfG53IhF6NZbaXErl45NuQi2uUOUOvXzkseiZab07TsKVG+
AMCXHIxgHSd33Piv0iZSkpTtFgJZnj7Po+th/MpyjH2lWQfcAyinu+w+XA/5sMFj
v0bYK2oJ44KdV/3sClaHcyuhVRfcNrxP5qMpS4c=
-----END CERTIFICATE-----
`
}

func TestValidateFilesExit(t *testing.T) {
	tempDir := t.TempDir()

	// Create test files
	certPath := filepath.Join(tempDir, "cert.pem")
	keyPath := filepath.Join(tempDir, "key.pem")
	caPath := filepath.Join(tempDir, "ca.pem")

	for _, path := range []string{certPath, keyPath, caPath} {
		if err := os.WriteFile(path, []byte("test"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
	}

	tests := []struct {
		name    string
		paths   []string
		wantErr bool
	}{
		{
			name:    "all files exist",
			paths:   []string{certPath, keyPath, caPath},
			wantErr: false,
		},
		{
			name:    "missing file",
			paths:   []string{certPath, filepath.Join(tempDir, "missing.pem"), caPath},
			wantErr: true,
		},
		{
			name:    "empty paths",
			paths:   []string{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateFilesExit(tt.paths)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateFilesExit() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLoadCACerts(t *testing.T) {
	tempDir := t.TempDir()

	t.Run("valid CA certificate", func(t *testing.T) {
		caPath := filepath.Join(tempDir, "ca.pem")
		certPEM := giveExampleCertStr() // Your helper function

		if err := os.WriteFile(caPath, []byte(certPEM), 0644); err != nil {
			t.Fatalf("Failed to write CA cert: %v", err)
		}

		pool, err := LoadCACerts(caPath)
		if err != nil {
			t.Errorf("LoadCACerts() error = %v, want nil", err)
		}
		if pool == nil {
			t.Error("LoadCACerts() returned nil pool")
		}
	})

	t.Run("invalid CA certificate", func(t *testing.T) {
		caPath := filepath.Join(tempDir, "invalid.pem")
		if err := os.WriteFile(caPath, []byte("invalid cert data"), 0644); err != nil {
			t.Fatalf("Failed to write invalid cert: %v", err)
		}

		_, err := LoadCACerts(caPath)
		if err == nil {
			t.Error("LoadCACerts() with invalid cert should return error")
		}
	})

	t.Run("non-existent CA file", func(t *testing.T) {
		_, err := LoadCACerts(filepath.Join(tempDir, "nonexistent.pem"))
		if err == nil {
			t.Error("LoadCACerts() with non-existent file should return error")
		}
	})
}

// ---------
func TestGetTLSVersion(t *testing.T) {
	tempDir := t.TempDir()

	// Create test files
	certPath := filepath.Join(tempDir, "cert.pem")
	keyPath := filepath.Join(tempDir, "key.pem")
	caPath := filepath.Join(tempDir, "ca.pem")

	certPEM := giveExampleCertStr()
	keyPEM := `-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC+ne6EfNvjMAsF
9FQKHwz6tVHh4O8aI+LwQcP8JzWmMNPjOQaJmW5uR7hUJfSw+8vF9vJk1xKL/
5eDnI8P9oR5yH7qXhJvQyRqKbNlZMjfP0s3cVoTmScW2gK8f5eX3oHjV3JmP2jQ
-----END PRIVATE KEY-----
`
	caPEM := giveExampleCertStr()

	// Write initial files
	if err := os.WriteFile(certPath, []byte(certPEM), 0644); err != nil {
		t.Fatalf("Failed to write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, []byte(keyPEM), 0644); err != nil {
		t.Fatalf("Failed to write key: %v", err)
	}
	if err := os.WriteFile(caPath, []byte(caPEM), 0644); err != nil {
		t.Fatalf("Failed to write ca: %v", err)
	}

	t.Run("successful version generation", func(t *testing.T) {
		version, err := GetTLSVersion(certPath, keyPath, caPath)
		if err != nil {
			t.Errorf("GetTLSVersion() error = %v, want nil", err)
		}
		if version == "" {
			t.Error("GetTLSVersion() returned empty string")
		}
		// Check that it's a valid base64 URL-encoded string
		if len(version) != 43 { // SHA256 is 32 bytes, base64 URL encoding without padding
			t.Errorf("GetTLSVersion() returned length %d, expected 43", len(version))
		}
	})

	t.Run("different files produce different versions", func(t *testing.T) {
		// Create a slightly different certificate
		diffCertPath := filepath.Join(tempDir, "diff_cert.pem")
		diffCert := certPEM + "\n"
		if err := os.WriteFile(diffCertPath, []byte(diffCert), 0644); err != nil {
			t.Fatalf("Failed to write diff cert: %v", err)
		}

		version1, err := GetTLSVersion(certPath, keyPath, caPath)
		if err != nil {
			t.Fatalf("GetTLSVersion() error = %v", err)
		}
		version2, err := GetTLSVersion(diffCertPath, keyPath, caPath)
		if err != nil {
			t.Fatalf("GetTLSVersion() error = %v", err)
		}
		if version1 == version2 {
			t.Error("Different files should produce different versions")
		}
	})

	t.Run("same files produce same version", func(t *testing.T) {
		version1, err := GetTLSVersion(certPath, keyPath, caPath)
		if err != nil {
			t.Fatalf("GetTLSVersion() error = %v", err)
		}
		version2, err := GetTLSVersion(certPath, keyPath, caPath)
		if err != nil {
			t.Fatalf("GetTLSVersion() error = %v", err)
		}
		if version1 != version2 {
			t.Error("Same files should produce same version")
		}
	})

	t.Run("missing file returns error", func(t *testing.T) {
		_, err := GetTLSVersion(
			filepath.Join(tempDir, "missing.pem"),
			keyPath,
			caPath,
		)
		if err == nil {
			t.Error("GetTLSVersion() with missing file should return error")
		}
	})
}

func TestIsTLSVersionUpdated(t *testing.T) {
	tempDir := t.TempDir()

	// Create test files
	certPath := filepath.Join(tempDir, "cert.pem")
	keyPath := filepath.Join(tempDir, "key.pem")
	caPath := filepath.Join(tempDir, "ca.pem")

	certPEM := giveExampleCertStr()
	keyPEM := `-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC+ne6EfNvjMAsF
9FQKHwz6tVHh4O8aI+LwQcP8JzWmMNPjOQaJmW5uR7hUJfSw+8vF9vJk1xKL/
5eDnI8P9oR5yH7qXhJvQyRqKbNlZMjfP0s3cVoTmScW2gK8f5eX3oHjV3JmP2jQ
-----END PRIVATE KEY-----
`
	caPEM := giveExampleCertStr()

	if err := os.WriteFile(certPath, []byte(certPEM), 0644); err != nil {
		t.Fatalf("Failed to write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, []byte(keyPEM), 0644); err != nil {
		t.Fatalf("Failed to write key: %v", err)
	}
	if err := os.WriteFile(caPath, []byte(caPEM), 0644); err != nil {
		t.Fatalf("Failed to write ca: %v", err)
	}

	t.Run("first time check (no previous version)", func(t *testing.T) {
		currentVersion, changed, err := IsTLSVersionUpdated(certPath, keyPath, caPath, "")
		if err != nil {
			t.Errorf("IsTLSVersionUpdated() error = %v, want nil", err)
		}
		if !changed {
			t.Error("First check with empty previous version should report changed")
		}
		if currentVersion == "" {
			t.Error("Current version should not be empty")
		}
	})

	t.Run("same version - no change", func(t *testing.T) {
		// Get current version first
		version, _, err := IsTLSVersionUpdated(certPath, keyPath, caPath, "")
		if err != nil {
			t.Fatalf("IsTLSVersionUpdated() error = %v", err)
		}

		currentVersion, changed, err := IsTLSVersionUpdated(certPath, keyPath, caPath, version)
		if err != nil {
			t.Errorf("IsTLSVersionUpdated() error = %v, want nil", err)
		}
		if changed {
			t.Error("Same version should not report changed")
		}
		if currentVersion != version {
			t.Errorf("Current version = %v, want %v", currentVersion, version)
		}
	})

	t.Run("different version - changed", func(t *testing.T) {
		// Get initial version
		version, _, err := IsTLSVersionUpdated(certPath, keyPath, caPath, "")
		if err != nil {
			t.Fatalf("IsTLSVersionUpdated() error = %v", err)
		}

		// Modify a file
		modifiedCert := certPEM + "\n"
		if err := os.WriteFile(certPath, []byte(modifiedCert), 0644); err != nil {
			t.Fatalf("Failed to modify cert: %v", err)
		}

		currentVersion, changed, err := IsTLSVersionUpdated(certPath, keyPath, caPath, version)
		if err != nil {
			t.Errorf("IsTLSVersionUpdated() error = %v, want nil", err)
		}
		if !changed {
			t.Error("Modified files should report changed")
		}
		if currentVersion == version {
			t.Error("Current version should differ from previous version")
		}
	})

	t.Run("missing file returns error", func(t *testing.T) {
		_, _, err := IsTLSVersionUpdated(
			filepath.Join(tempDir, "missing.pem"),
			keyPath,
			caPath,
			"some-version",
		)
		if err == nil {
			t.Error("IsTLSVersionUpdated() with missing file should return error")
		}
	})
}
