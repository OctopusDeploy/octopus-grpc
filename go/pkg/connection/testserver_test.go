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
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// testTLS is a freshly minted self-signed certificate and the two ends of a TLS
// connection that trusts it. Generated per test rather than checked in, so the
// thumbprint the tests assert against is the real one for the cert in play.
type testTLS struct {
	serverCreds credentials.TransportCredentials
	roots       *x509.CertPool
	thumbprint  string
}

type tlsHealthServer struct {
	addr       string
	thumbprint string
	roots      *x509.CertPool
}

func startHealthServer(t *testing.T) (string, *health.Server) {
	t.Helper()

	healthServer := health.NewServer()
	return serve(t, healthServer), healthServer
}

func startTLSHealthServer(t *testing.T) *tlsHealthServer {
	t.Helper()

	certs := selfSignedTLS(t)
	addr := serve(t, newServingHealthServer(), grpc.Creds(certs.serverCreds))

	return &tlsHealthServer{addr: addr, thumbprint: certs.thumbprint, roots: certs.roots}
}

func newServingHealthServer() *health.Server {
	server := health.NewServer()
	server.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	return server
}

func serve(t *testing.T, healthServer *health.Server, options ...grpc.ServerOption) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Expected to listen, got %v", err)
	}

	server := grpc.NewServer(options...)
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	return listener.Addr().String()
}

func selfSignedTLS(t *testing.T) testTLS {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("Expected to generate a key, got %v", err)
	}

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("Expected to create a certificate, got %v", err)
	}

	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("Expected to parse the certificate, got %v", err)
	}

	roots := x509.NewCertPool()
	roots.AddCert(leaf)

	sum := sha1.Sum(der)

	return testTLS{
		serverCreds: credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}},
		}),
		roots:      roots,
		thumbprint: strings.ToUpper(hex.EncodeToString(sum[:])),
	}
}
