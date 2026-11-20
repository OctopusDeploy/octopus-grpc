package connection

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"
	"time"
)

const verifyTestServerName = "octopus.example.com"

// These tests drive the TLS callback directly with the state a handshake would
// hand it, so they need no listener and cover exactly the decision under test.
func TestServerTLSConfig_RejectsATrustedCertificateForAnotherHost(t *testing.T) {
	ca := newVerifyTestCA(t)
	server := ca.issue(t, verifyTestServerName)
	otherHost := ca.issue(t, "other-host.example.com")

	err := runVerification(t, TLSConfig{
		Thumbprint: thumbprintOf(server),
		RootCAs:    ca.pool(),
	}, verifyTestServerName, otherHost)

	if err == nil {
		t.Fatal("Expected a trusted certificate for another host to be rejected, it was accepted")
	}
}

func TestServerTLSConfig_AcceptsATrustedCertificateForTheServer(t *testing.T) {
	ca := newVerifyTestCA(t)
	genuine := ca.issue(t, verifyTestServerName)

	err := runVerification(t, TLSConfig{
		Thumbprint: strings.Repeat("A", 40),
		RootCAs:    ca.pool(),
	}, verifyTestServerName, genuine)

	if err != nil {
		t.Fatalf("Expected a trusted certificate for the server to be accepted, got %v", err)
	}
}

func TestServerTLSConfig_AcceptsAnUntrustedCertificateWhoseThumbprintMatches(t *testing.T) {
	ca := newVerifyTestCA(t)
	genuine := ca.issue(t, verifyTestServerName)

	err := runVerification(t, TLSConfig{
		Thumbprint: thumbprintOf(genuine),
		RootCAs:    x509.NewCertPool(),
	}, verifyTestServerName, genuine)

	if err != nil {
		t.Fatalf("Expected the thumbprint to carry the connection, got %v", err)
	}
}

func TestServerTLSConfig_RejectsAnUntrustedCertificateWhoseThumbprintDoesNotMatch(t *testing.T) {
	ca := newVerifyTestCA(t)
	cert := ca.issue(t, verifyTestServerName)

	err := runVerification(t, TLSConfig{
		Thumbprint: strings.Repeat("A", 40),
		RootCAs:    x509.NewCertPool(),
	}, verifyTestServerName, cert)

	if err == nil || !strings.Contains(err.Error(), "does not match the expected thumbprint") {
		t.Fatalf("Expected a thumbprint mismatch, got %v", err)
	}
}

// runVerification builds the TLS config Dial would use and runs whichever
// verification callback it installs against the presented chain.
func runVerification(t *testing.T, cfg TLSConfig, serverName string, chain ...*x509.Certificate) error {
	t.Helper()

	tlsConfig := serverTLSConfig(cfg, discardLogger())

	switch {
	case tlsConfig.VerifyConnection != nil:
		return tlsConfig.VerifyConnection(tls.ConnectionState{
			ServerName:       serverName,
			PeerCertificates: chain,
		})
	case tlsConfig.VerifyPeerCertificate != nil:
		raw := make([][]byte, len(chain))
		for i, cert := range chain {
			raw[i] = cert.Raw
		}
		return tlsConfig.VerifyPeerCertificate(raw, nil)
	default:
		t.Fatal("Expected a verification callback to be installed when a thumbprint is set")
		return nil
	}
}

func thumbprintOf(cert *x509.Certificate) string {
	sum := sha1.Sum(cert.Raw)
	return strings.ToUpper(hex.EncodeToString(sum[:]))
}

type verifyTestCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newVerifyTestCA(t *testing.T) verifyTestCA {
	t.Helper()

	key := newVerifyTestKey(t)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test Root CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	return verifyTestCA{cert: createVerifyTestCert(t, template, template, key, key), key: key}
}

func (ca verifyTestCA) issue(t *testing.T, dnsName string) *x509.Certificate {
	t.Helper()

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("Expected to generate a serial number, got %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: dnsName},
		DNSNames:     []string{dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	return createVerifyTestCert(t, template, ca.cert, newVerifyTestKey(t), ca.key)
}

func (ca verifyTestCA) pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return pool
}

func newVerifyTestKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("Expected to generate a key, got %v", err)
	}
	return key
}

func createVerifyTestCert(
	t *testing.T,
	template, parent *x509.Certificate,
	key, parentKey *ecdsa.PrivateKey,
) *x509.Certificate {
	t.Helper()

	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatalf("Expected to create a certificate, got %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("Expected to parse the certificate, got %v", err)
	}
	return cert
}
