package certificate

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha1"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	subCaFilePath                  = "testdata/certs/subCA.cert.pem"
	clientSignedBySubChainFilePath = "testdata/certs/client_sub_chain.cert.pem"
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

const testServerName = "octopus.example.com"

func TestVerifyServerCertificateWithRoot_Verifies_WhenUsingSubCaSignedCert_UsingCustomRootCa(t *testing.T) {
	root, chain := newTestChain(t, true, testServerName)

	err := VerifyServerCertificateWithRoot(chain, systemPoolWith(root), testServerName)
	if err != nil {
		t.Fatalf("Expected no error verifying server certificate, got %v", err)
	}
}

func TestVerifyServerCertificateWithRoot_DoesNotVerify_WhenUsingSubCaSignedCert_NotUsingCustomRootCa(t *testing.T) {
	_, chain := newTestChain(t, true, testServerName)

	err := VerifyServerCertificateWithRoot(chain, systemPoolWith(), testServerName)
	if err == nil {
		t.Fatal("Expected error verifying server certificate, got none")
	}
}

func TestVerifyServerCertificateWithRoot_Verifies_WhenUsingRootCaSignedCert_UsingCustomRootCa(t *testing.T) {
	root, chain := newTestChain(t, false, testServerName)

	err := VerifyServerCertificateWithRoot(chain, systemPoolWith(root), testServerName)
	if err != nil {
		t.Fatalf("Expected no error verifying server certificate, got %v", err)
	}
}

// A certificate that chains to a trusted root is only valid for the hosts it names.
func TestVerifyServerCertificateWithRoot_DoesNotVerify_WhenServerNameDoesNotMatch(t *testing.T) {
	root, chain := newTestChain(t, true, "other-host.example.com")

	err := VerifyServerCertificateWithRoot(chain, systemPoolWith(root), testServerName)
	var hostnameErr x509.HostnameError
	if !errors.As(err, &hostnameErr) {
		t.Fatalf("Expected a hostname error, got %v", err)
	}
}

func TestVerifyServerCertificateWithRoot_DoesNotVerify_WhenServerNameIsEmpty(t *testing.T) {
	root, chain := newTestChain(t, true, testServerName)

	err := VerifyServerCertificateWithRoot(chain, systemPoolWith(root), "")
	if err == nil {
		t.Fatal("Expected an empty server name to be rejected, it verified")
	}
}

// systemPoolWith reproduces what the gateway's InitializeCertificatePool gives
// callers: the system roots, plus any custom roots.
func systemPoolWith(roots ...*x509.Certificate) *x509.CertPool {
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}

	for _, root := range roots {
		pool.AddCert(root)
	}

	return pool
}

// newTestChain mints a root CA and a leaf valid for dnsName, optionally issued
// through an intermediate. It returns the root and the chain as a server would
// present it: leaf first, then any intermediate.
func newTestChain(t *testing.T, withIntermediate bool, dnsName string) (*x509.Certificate, []*x509.Certificate) {
	t.Helper()

	root, rootKey := newTestCert(t, &x509.Certificate{
		Subject:               pkix.Name{CommonName: "Test Root CA"},
		KeyUsage:              x509.KeyUsageCertSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}, nil, nil)

	issuer, issuerKey := root, rootKey
	var intermediates []*x509.Certificate
	if withIntermediate {
		issuer, issuerKey = newTestCert(t, &x509.Certificate{
			Subject:               pkix.Name{CommonName: "Test Sub CA"},
			KeyUsage:              x509.KeyUsageCertSign,
			IsCA:                  true,
			BasicConstraintsValid: true,
		}, root, rootKey)
		intermediates = append(intermediates, issuer)
	}

	leaf, _ := newTestCert(t, &x509.Certificate{
		Subject:     pkix.Name{CommonName: dnsName},
		DNSNames:    []string{dnsName},
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, issuer, issuerKey)

	return root, append([]*x509.Certificate{leaf}, intermediates...)
}

// newTestCert signs template with parentKey, or self-signs it when parent is nil.
func newTestCert(
	t *testing.T,
	template *x509.Certificate,
	parent *x509.Certificate,
	parentKey *ecdsa.PrivateKey,
) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("Expected to generate a key, got %v", err)
	}

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("Expected to generate a serial number, got %v", err)
	}
	template.SerialNumber = serial
	template.NotBefore = time.Now().Add(-time.Hour)
	template.NotAfter = time.Now().Add(time.Hour)

	if parent == nil {
		parent, parentKey = template, key
	}

	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatalf("Expected to create a certificate, got %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("Expected to parse the certificate, got %v", err)
	}

	return cert, key
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
