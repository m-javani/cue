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

package testutils

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCreateCA(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	caName := "test-ca"
	domain := "example.com"
	yearsValid := 10

	// Test successful CA creation
	caInfo, err := CreateCA(tempDir, caName, yearsValid, domain)
	if err != nil {
		t.Fatalf("CreateCA failed: %v", err)
	}

	// Validate CAInfo
	if caInfo.Name != caName {
		t.Errorf("Expected CA name %s, got %s", caName, caInfo.Name)
	}

	// Check files exist
	expectedCertPath := filepath.Join(tempDir, caName+"_cert.pem")
	expectedKeyPath := filepath.Join(tempDir, caName+"_key.pem")

	if caInfo.CertPath != expectedCertPath {
		t.Errorf("Expected cert path %s, got %s", expectedCertPath, caInfo.CertPath)
	}
	if caInfo.KeyPath != expectedKeyPath {
		t.Errorf("Expected key path %s, got %s", expectedKeyPath, caInfo.KeyPath)
	}

	// Check files actually exist
	if _, err := os.Stat(expectedCertPath); os.IsNotExist(err) {
		t.Errorf("Certificate file %s does not exist", expectedCertPath)
	}
	if _, err := os.Stat(expectedKeyPath); os.IsNotExist(err) {
		t.Errorf("Key file %s does not exist", expectedKeyPath)
	}

	// Validate certificate content
	certData, err := os.ReadFile(expectedCertPath)
	if err != nil {
		t.Fatalf("Failed to read cert file: %v", err)
	}
	certBlock, _ := pem.Decode(certData)
	if certBlock == nil {
		t.Fatal("Failed to decode cert PEM")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		t.Fatalf("Failed to parse cert: %v", err)
	}

	// Check certificate fields
	if cert.Subject.CommonName != caName {
		t.Errorf("Expected CommonName %s, got %s", caName, cert.Subject.CommonName)
	}
	if !cert.IsCA {
		t.Error("Expected certificate to be a CA")
	}
	if len(cert.DNSNames) != 1 || cert.DNSNames[0] != caName+"."+domain {
		t.Errorf("Expected DNSNames [%s.%s], got %v", caName, domain, cert.DNSNames)
	}

	// Check key usage
	if cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		t.Error("Expected KeyUsageCertSign to be set")
	}
	if cert.KeyUsage&x509.KeyUsageCRLSign == 0 {
		t.Error("Expected KeyUsageCRLSign to be set")
	}

	// Validate private key
	if caInfo.PrivateKey == nil {
		t.Error("Expected private key to be set")
	}
	if caInfo.Certificate == nil {
		t.Error("Expected certificate to be set")
	}
}

func TestCreateCA_WithDomain(t *testing.T) {
	tempDir := t.TempDir()
	caName := "domain-ca"
	domain := "test.local"
	yearsValid := 5

	caInfo, err := CreateCA(tempDir, caName, yearsValid, domain)
	if err != nil {
		t.Fatalf("CreateCA failed: %v", err)
	}

	if caInfo.Certificate == nil {
		t.Fatal("Certificate should not be nil")
	}

	// Verify DNS name includes domain
	if len(caInfo.Certificate.DNSNames) != 1 {
		t.Fatalf("Expected 1 DNS name, got %d", len(caInfo.Certificate.DNSNames))
	}
	if !strings.Contains(caInfo.Certificate.DNSNames[0], domain) {
		t.Errorf("DNS name %s should contain domain %s", caInfo.Certificate.DNSNames[0], domain)
	}
}

func TestCreateCA_DirectoryCreation(t *testing.T) {
	tempDir := t.TempDir()
	deepPath := filepath.Join(tempDir, "nested", "deep", "path")
	caName := "deep-ca"

	_, err := CreateCA(deepPath, caName, 1, "example.com")
	if err != nil {
		t.Fatalf("CreateCA in nested directory failed: %v", err)
	}

	// Verify directory was created
	if _, err := os.Stat(deepPath); os.IsNotExist(err) {
		t.Errorf("Directory %s was not created", deepPath)
	}
}

func TestLoadCA(t *testing.T) {
	tempDir := t.TempDir()
	caName := "load-test-ca"
	yearsValid := 10
	domain := "example.com"

	// Create a CA first
	originalCA, err := CreateCA(tempDir, caName, yearsValid, domain)
	if err != nil {
		t.Fatalf("CreateCA failed: %v", err)
	}

	// Load the CA
	loadedCA, err := LoadCA(tempDir, caName)
	if err != nil {
		t.Fatalf("LoadCA failed: %v", err)
	}

	// Compare loaded CA with original
	if loadedCA.Name != originalCA.Name {
		t.Errorf("Expected name %s, got %s", originalCA.Name, loadedCA.Name)
	}
	if loadedCA.CertPath != originalCA.CertPath {
		t.Errorf("Expected cert path %s, got %s", originalCA.CertPath, loadedCA.CertPath)
	}
	if loadedCA.KeyPath != originalCA.KeyPath {
		t.Errorf("Expected key path %s, got %s", originalCA.KeyPath, loadedCA.KeyPath)
	}

	// Validate certificate
	if loadedCA.Certificate == nil {
		t.Error("Loaded CA certificate should not be nil")
	}
	if loadedCA.PrivateKey == nil {
		t.Error("Loaded CA private key should not be nil")
	}

	// Verify certificate content
	if loadedCA.Certificate.Subject.CommonName != caName {
		t.Errorf("Expected CommonName %s, got %s", caName, loadedCA.Certificate.Subject.CommonName)
	}
	if !loadedCA.Certificate.IsCA {
		t.Error("Loaded certificate should be a CA")
	}
}

func TestLoadCA_NotFound(t *testing.T) {
	tempDir := t.TempDir()
	nonExistentCA := "non-existent"

	_, err := LoadCA(tempDir, nonExistentCA)
	if err == nil {
		t.Error("LoadCA should fail for non-existent CA")
	}
}

func TestLoadCA_InvalidPEM(t *testing.T) {
	tempDir := t.TempDir()
	caName := "invalid-ca"

	// Create invalid PEM files
	certPath := filepath.Join(tempDir, caName+"_cert.pem")
	keyPath := filepath.Join(tempDir, caName+"_key.pem")

	if err := os.WriteFile(certPath, []byte("invalid certificate data"), 0644); err != nil {
		t.Fatalf("Failed to write invalid cert: %v", err)
	}
	if err := os.WriteFile(keyPath, []byte("invalid key data"), 0644); err != nil {
		t.Fatalf("Failed to write invalid key: %v", err)
	}

	_, err := LoadCA(tempDir, caName)
	if err == nil {
		t.Error("LoadCA should fail for invalid PEM data")
	}
}

func TestLoadCA_InvalidKey(t *testing.T) {
	tempDir := t.TempDir()
	caName := "invalid-key-ca"

	// Create valid cert PEM but invalid key
	certPath := filepath.Join(tempDir, caName+"_cert.pem")
	keyPath := filepath.Join(tempDir, caName+"_key.pem")

	// Write an invalid key PEM
	invalidKeyPEM := `-----BEGIN RSA PRIVATE KEY-----
invalid key data
-----END RSA PRIVATE KEY-----`
	if err := os.WriteFile(certPath, []byte("-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----"), 0644); err != nil {
		t.Fatalf("Failed to write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, []byte(invalidKeyPEM), 0644); err != nil {
		t.Fatalf("Failed to write key: %v", err)
	}

	_, err := LoadCA(tempDir, caName)
	if err == nil {
		t.Error("LoadCA should fail for invalid key")
	}
}

func TestCreateNodeCert(t *testing.T) {
	tempDir := t.TempDir()
	caName := "node-ca"
	domain := "example.com"

	// Create CA first
	ca, err := CreateCA(tempDir, caName, 10, domain)
	if err != nil {
		t.Fatalf("CreateCA failed: %v", err)
	}

	// Test node certificate creation
	node := NodeCert{
		NodeIdentity: "node1",
		ServerNames:  []string{"node1.localhost", "localhost", "127.0.0.1"},
	}
	yearsValid := 5

	certPath, keyPath, err := CreateNodeCert(tempDir, ca, node, yearsValid)
	if err != nil {
		t.Fatalf("CreateNodeCert failed: %v", err)
	}

	// Check file paths
	expectedCertPath := filepath.Join(tempDir, "node1.pem")
	expectedKeyPath := filepath.Join(tempDir, "node1_key.pem")

	if certPath != expectedCertPath {
		t.Errorf("Expected cert path %s, got %s", expectedCertPath, certPath)
	}
	if keyPath != expectedKeyPath {
		t.Errorf("Expected key path %s, got %s", expectedKeyPath, keyPath)
	}

	// Check files exist
	if _, err := os.Stat(certPath); os.IsNotExist(err) {
		t.Errorf("Certificate file %s does not exist", certPath)
	}
	if _, err := os.Stat(keyPath); os.IsNotExist(err) {
		t.Errorf("Key file %s does not exist", keyPath)
	}

	// Validate certificate content (should be fullchain)
	certData, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatalf("Failed to read cert file: %v", err)
	}

	// Should contain both leaf and CA certificates
	pemBlocks := strings.Count(string(certData), "-----BEGIN CERTIFICATE-----")
	if pemBlocks != 2 {
		t.Errorf("Expected 2 PEM blocks (leaf + CA), got %d", pemBlocks)
	}

	// Decode first block (leaf certificate)
	certBlock, _ := pem.Decode(certData)
	if certBlock == nil {
		t.Fatal("Failed to decode cert PEM")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		t.Fatalf("Failed to parse cert: %v", err)
	}

	// Check certificate fields
	if cert.Subject.CommonName != node.NodeIdentity {
		t.Errorf("Expected CommonName %s, got %s", node.NodeIdentity, cert.Subject.CommonName)
	}
	if cert.IsCA {
		t.Error("Node certificate should not be a CA")
	}

	// Check DNS names
	if len(cert.DNSNames) != len(node.ServerNames) {
		t.Errorf("Expected %d DNS names, got %d", len(node.ServerNames), len(cert.DNSNames))
	}
	for i, name := range node.ServerNames {
		if i < len(cert.DNSNames) && cert.DNSNames[i] != name {
			t.Errorf("Expected DNS name %s, got %s", name, cert.DNSNames[i])
		}
	}

	// Check key usage
	if cert.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		t.Error("Expected KeyUsageDigitalSignature to be set")
	}
	if cert.KeyUsage&x509.KeyUsageKeyEncipherment == 0 {
		t.Error("Expected KeyUsageKeyEncipherment to be set")
	}

	// Check extended key usage
	if len(cert.ExtKeyUsage) != 2 {
		t.Errorf("Expected 2 extended key usages, got %d", len(cert.ExtKeyUsage))
	}
	foundServerAuth := false
	foundClientAuth := false
	for _, usage := range cert.ExtKeyUsage {
		if usage == x509.ExtKeyUsageServerAuth {
			foundServerAuth = true
		}
		if usage == x509.ExtKeyUsageClientAuth {
			foundClientAuth = true
		}
	}
	if !foundServerAuth {
		t.Error("Expected ExtKeyUsageServerAuth to be set")
	}
	if !foundClientAuth {
		t.Error("Expected ExtKeyUsageClientAuth to be set")
	}
}

func TestCreateNodeCert_WithMultipleServerNames(t *testing.T) {
	tempDir := t.TempDir()
	ca, err := CreateCA(tempDir, "multi-ca", 10, "example.com")
	if err != nil {
		t.Fatalf("CreateCA failed: %v", err)
	}

	node := NodeCert{
		NodeIdentity: "multi-node",
		ServerNames:  []string{"node1.local", "node1.example.com", "localhost"},
	}

	certPath, _, err := CreateNodeCert(tempDir, ca, node, 1)
	if err != nil {
		t.Fatalf("CreateNodeCert failed: %v", err)
	}

	certData, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatalf("Failed to read cert: %v", err)
	}

	certBlock, _ := pem.Decode(certData)
	if certBlock == nil {
		t.Fatal("Failed to decode cert PEM")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		t.Fatalf("Failed to parse cert: %v", err)
	}

	if len(cert.DNSNames) != len(node.ServerNames) {
		t.Errorf("Expected %d DNS names, got %d", len(node.ServerNames), len(cert.DNSNames))
	}
	for i, name := range node.ServerNames {
		if i < len(cert.DNSNames) && cert.DNSNames[i] != name {
			t.Errorf("Expected DNS name %s, got %s", name, cert.DNSNames[i])
		}
	}
}

func TestCreateNodeCert_InvalidCA(t *testing.T) {
	tempDir := t.TempDir()

	// Create a valid CA first
	_, err := CreateCA(tempDir, "valid-ca", 10, "example.com")
	if err != nil {
		t.Fatalf("CreateCA failed: %v", err)
	}

	// Create an invalid CA with nil certificate and private key
	invalidCA := &CAInfo{
		Name:        "invalid",
		CertPath:    filepath.Join(tempDir, "invalid_cert.pem"),
		KeyPath:     filepath.Join(tempDir, "invalid_key.pem"),
		Certificate: nil,
		PrivateKey:  nil,
	}

	node := NodeCert{
		NodeIdentity: "invalid-node",
		ServerNames:  []string{"localhost"},
	}

	_, _, err = CreateNodeCert(tempDir, invalidCA, node, 1)
	if err == nil {
		t.Error("CreateNodeCert should fail with invalid CA")
	}
}

func TestCreateNodeCert_CAKeyNotFound(t *testing.T) {
	tempDir := t.TempDir()

	// Create a CA
	ca, err := CreateCA(tempDir, "missing-ca", 10, "example.com")
	if err != nil {
		t.Fatalf("CreateCA failed: %v", err)
	}

	// Create a new CAInfo with the same certificate but nil private key
	// to simulate missing key without deleting the file
	invalidCA := &CAInfo{
		Name:        ca.Name,
		CertPath:    ca.CertPath,
		KeyPath:     ca.KeyPath,
		Certificate: ca.Certificate,
		PrivateKey:  nil, // nil private key will cause failure
	}

	node := NodeCert{
		NodeIdentity: "missing-key-node",
		ServerNames:  []string{"localhost"},
	}

	_, _, err = CreateNodeCert(tempDir, invalidCA, node, 1)
	if err == nil {
		t.Error("CreateNodeCert should fail when CA private key is nil")
	}
}

func TestCreateNodeCert_MultipleNodes(t *testing.T) {
	tempDir := t.TempDir()
	ca, err := CreateCA(tempDir, "multi-node-ca", 10, "example.com")
	if err != nil {
		t.Fatalf("CreateCA failed: %v", err)
	}

	nodes := []NodeCert{
		{NodeIdentity: "node1", ServerNames: []string{"node1.local"}},
		{NodeIdentity: "node2", ServerNames: []string{"node2.local"}},
		{NodeIdentity: "node3", ServerNames: []string{"node3.local"}},
	}

	for _, node := range nodes {
		certPath, keyPath, err := CreateNodeCert(tempDir, ca, node, 1)
		if err != nil {
			t.Fatalf("CreateNodeCert for %s failed: %v", node.NodeIdentity, err)
		}

		// Verify files exist
		if _, err := os.Stat(certPath); os.IsNotExist(err) {
			t.Errorf("Certificate file %s for %s does not exist", certPath, node.NodeIdentity)
		}
		if _, err := os.Stat(keyPath); os.IsNotExist(err) {
			t.Errorf("Key file %s for %s does not exist", keyPath, node.NodeIdentity)
		}
	}
}

func TestCreateNodeCert_Expiration(t *testing.T) {
	tempDir := t.TempDir()
	ca, err := CreateCA(tempDir, "expire-ca", 10, "example.com")
	if err != nil {
		t.Fatalf("CreateCA failed: %v", err)
	}

	yearsValid := 2
	node := NodeCert{
		NodeIdentity: "expire-node",
		ServerNames:  []string{"localhost"},
	}

	certPath, _, err := CreateNodeCert(tempDir, ca, node, yearsValid)
	if err != nil {
		t.Fatalf("CreateNodeCert failed: %v", err)
	}

	certData, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatalf("Failed to read cert: %v", err)
	}

	certBlock, _ := pem.Decode(certData)
	if certBlock == nil {
		t.Fatal("Failed to decode cert PEM")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		t.Fatalf("Failed to parse cert: %v", err)
	}

	// Check expiration (allow some flexibility for time)
	expectedDuration := time.Duration(yearsValid) * 365 * 24 * time.Hour
	actualDuration := cert.NotAfter.Sub(cert.NotBefore)
	if actualDuration < expectedDuration-time.Hour || actualDuration > expectedDuration+time.Hour {
		t.Errorf("Expected duration ~%v, got %v", expectedDuration, actualDuration)
	}
}

func TestWritePEM_Error(t *testing.T) {
	// Test with invalid path (e.g., a path to a directory that doesn't exist)
	invalidPath := filepath.Join(t.TempDir(), "non-existent-dir", "cert.pem")
	err := writePEM(invalidPath, "CERTIFICATE", []byte("test"))
	if err == nil {
		t.Error("writePEM should fail with non-existent directory")
	}
}

func TestWriteKeyPEM_Error(t *testing.T) {
	// Test with invalid path
	invalidPath := filepath.Join(t.TempDir(), "non-existent-dir", "key.pem")
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	err = writeKeyPEM(invalidPath, key)
	if err == nil {
		t.Error("writeKeyPEM should fail with non-existent directory")
	}
}

func TestLoadCA_WithDifferentDomain(t *testing.T) {
	tempDir := t.TempDir()
	caName := "domain-test"
	domain := "custom.domain.com"
	yearsValid := 5

	_, err := CreateCA(tempDir, caName, yearsValid, domain)
	if err != nil {
		t.Fatalf("CreateCA failed: %v", err)
	}

	loadedCA, err := LoadCA(tempDir, caName)
	if err != nil {
		t.Fatalf("LoadCA failed: %v", err)
	}

	if len(loadedCA.Certificate.DNSNames) != 1 {
		t.Fatalf("Expected 1 DNS name, got %d", len(loadedCA.Certificate.DNSNames))
	}
	if loadedCA.Certificate.DNSNames[0] != caName+"."+domain {
		t.Errorf("Expected DNS name %s.%s, got %s", caName, domain, loadedCA.Certificate.DNSNames[0])
	}
}

func TestLoadCA_KeyPathTraversal(t *testing.T) {
	tempDir := t.TempDir()
	caName := "traversal-ca"

	_, err := CreateCA(tempDir, caName, 1, "example.com")
	if err != nil {
		t.Fatalf("CreateCA failed: %v", err)
	}

	loadedCA, err := LoadCA(tempDir, caName)
	if err != nil {
		t.Fatalf("LoadCA failed: %v", err)
	}

	// Verify key path is correct
	expectedKeyPath := filepath.Join(tempDir, caName+"_key.pem")
	if loadedCA.KeyPath != expectedKeyPath {
		t.Errorf("Expected key path %s, got %s", expectedKeyPath, loadedCA.KeyPath)
	}
}

func TestCreateNodeCert_WithDifferentKeySizes(t *testing.T) {
	// CA uses 4096-bit key by default
	tempDir := t.TempDir()
	ca, err := CreateCA(tempDir, "key-size-ca", 10, "example.com")
	if err != nil {
		t.Fatalf("CreateCA failed: %v", err)
	}

	node := NodeCert{
		NodeIdentity: "key-size-node",
		ServerNames:  []string{"localhost"},
	}

	// Node uses 2048-bit key
	certPath, keyPath, err := CreateNodeCert(tempDir, ca, node, 1)
	if err != nil {
		t.Fatalf("CreateNodeCert failed: %v", err)
	}

	// Verify key file exists and is valid
	keyData, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("Failed to read key: %v", err)
	}
	keyBlock, _ := pem.Decode(keyData)
	if keyBlock == nil {
		t.Fatal("Failed to decode key PEM")
	}
	_, err = x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
	if err != nil {
		t.Fatalf("Failed to parse key: %v", err)
	}

	// Verify certificate exists and is valid
	certData, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatalf("Failed to read cert: %v", err)
	}
	certBlock, _ := pem.Decode(certData)
	if certBlock == nil {
		t.Fatal("Failed to decode cert PEM")
	}
	_, err = x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		t.Fatalf("Failed to parse cert: %v", err)
	}
}
