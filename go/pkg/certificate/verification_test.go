package certificate

import (
	"bytes"
	"crypto/sha1"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	rootCaFilePath                  = "testdata/certs/rootCA.cert.pem"
	subCaFilePath                   = "testdata/certs/subCA.cert.pem"
	clientSignedBySubChainFilePath  = "testdata/certs/client_sub_chain.cert.pem"
	clientSignedByRootChainFilePath = "testdata/certs/client_root_chain.cert.pem"
)

func TestVerifyServerCertificate_ReturnsError_WhenUnexpectedServerCertIsProvided(t *testing.T) {
	// arrange
	expectedThumbprint := "1234567890"
	serverCert := &x509.Certificate{}

	serverThumbprint := sha1.Sum(serverCert.Raw)
	serverThumbprintString := strings.ToUpper(hex.EncodeToString(serverThumbprint[:]))

	err := VerifyServerCertificate(serverCert, expectedThumbprint)
	expectedError := ErrUnexpectedServerCertificate(expectedThumbprint, serverThumbprintString)
	if err == nil || err.Error() != expectedError.Error() {
		t.Errorf(
			"Expected error to be '%v', but got '%v'",
			expectedError,
			err,
		)
	}
}

func TestVerifyServerCertificate_ReturnsNil_WhenExpectedServerCertIsProvided(t *testing.T) {
	// arrange
	serverCert := &x509.Certificate{
		SerialNumber: big.NewInt(12345678901234),
		Subject: pkix.Name{
			CommonName: "Test Certificate",
		},
		Issuer: pkix.Name{
			CommonName: "Test Certificate",
		},
		NotBefore:             time.Now().AddDate(0, 0, -1),
		NotAfter:              time.Now().AddDate(100, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  false,
		BasicConstraintsValid: true,
	}

	thumbprint := sha1.Sum(serverCert.Raw)
	expectedThumbprintStr := strings.ToUpper(hex.EncodeToString(thumbprint[:]))

	err := VerifyServerCertificate(serverCert, expectedThumbprintStr)
	if err != nil {
		t.Errorf("Expected error to be nil, but got '%v'", err)
	}
}

func TestParseCertificates_ReturnsCorrectData_ForChainedCertificates(t *testing.T) {
	// arrange
	subCaCerts, err := getCertificates(subCaFilePath)
	if err != nil {
		t.Fatalf("Expected no error getting sub CA certificates, got %v", err)
	}
	if len(subCaCerts) != 1 {
		t.Fatalf("Expected 1 certificate, got %d", len(subCaCerts))
	}
	subCaCert := subCaCerts[0]
	data, err := getRawCertificates(clientSignedBySubChainFilePath)
	if err != nil {
		t.Fatalf("Expected no error getting raw certificates, got %v", err)
	}

	// act
	certChain, _ := ParseCertificates(data)

	// assert
	if len(certChain) != 3 {
		t.Fatalf("Expected 3 certificates, got %d", len(certChain))
	}
	if bytes.Compare(certChain[1].RawSubject, subCaCert.RawSubject) != 0 {
		t.Errorf("Expected second certificate to be %s, got %s",
			subCaCert.Subject.CommonName,
			certChain[1].Subject.CommonName)
	}
}

func TestVerifyServerCertificate_CorrectlyVerifies_WhenUsingSubCaSignedCert_UsingCustomRootCa(t *testing.T) {
	// arrange
	clientCerts, err := getCertificates(clientSignedBySubChainFilePath)
	if err != nil {
		t.Fatalf("Expected no error getting client certificates, got %v", err)
	}
	if len(clientCerts) != 3 {
		t.Fatalf("Expected 3 certificates, got %d", len(clientCerts))
	}
	rootBundle := systemPoolWith(t, rootCaFilePath)

	// act/assert
	err = VerifyServerCertificateWithRoot(clientCerts, rootBundle)
	if err != nil {
		t.Fatalf("Expected no error verifying server certificate, got %v", err)
	}
}

func TestVerifyServerCertificate_DoesNotVerify_WhenUsingSubCaSignedCert_NotUsingCustomRootCa(t *testing.T) {
	// arrange
	clientCerts, err := getCertificates(clientSignedBySubChainFilePath)
	if err != nil {
		t.Fatalf("Expected no error getting client certificates, got %v", err)
	}
	if len(clientCerts) != 3 {
		t.Fatalf("Expected 3 certificates, got %d", len(clientCerts))
	}
	rootBundle := systemPoolWith(t)

	// act/assert
	err = VerifyServerCertificateWithRoot(clientCerts, rootBundle)
	if err == nil {
		t.Fatal("Expected error verifying server certificate, got none")
	}
}

func TestVerifyServerCertificate_CorrectlyVerifies_WhenUsingRootCaSignedCert_UsingCustomRootCa(t *testing.T) {
	// arrange
	clientCerts, err := getCertificates(clientSignedByRootChainFilePath)
	if err != nil {
		t.Fatalf("Expected no error getting client certificates, got %v", err)
	}
	if len(clientCerts) != 2 {
		t.Fatalf("Expected 2 certificates, got %d", len(clientCerts))
	}
	rootBundle := systemPoolWith(t, rootCaFilePath)

	// act/assert
	err = VerifyServerCertificateWithRoot(clientCerts, rootBundle)
	if err != nil {
		t.Fatalf("Expected no error verifying server certificate, got %v", err)
	}
}

// systemPoolWith reproduces what the gateway's InitializeCertificatePool gave these
// tests: the system roots, plus any bundles named. Passing none is how the gateway
// test pointed the pool at a directory holding no .pem files.
func systemPoolWith(t *testing.T, bundlePaths ...string) *x509.CertPool {
	t.Helper()

	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}

	for _, path := range bundlePaths {
		bundle, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("Expected no error reading %s, got %v", path, readErr)
		}
		if !pool.AppendCertsFromPEM(bundle) {
			t.Fatalf("Expected to parse PEM certificate(s) in file: %s", path)
		}
	}

	return pool
}

func getCertificates(filePath string) ([]*x509.Certificate, error) {
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read certificate file: %w", err)
	}
	var certs []*x509.Certificate
	for block, rest := pem.Decode(fileData); block != nil; block, rest = pem.Decode(rest) {
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse certificate: %w", err)
		}
		certs = append(certs, cert)
	}
	return certs, nil
}

func getRawCertificates(filePath string) ([][]byte, error) {
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read certificate file: %w", err)
	}
	var data [][]byte
	for block, rest := pem.Decode(fileData); block != nil; block, rest = pem.Decode(rest) {
		if block.Type != "CERTIFICATE" {
			continue
		}
		data = append(data, block.Bytes)
	}
	return data, nil
}
