package connection

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/OctopusDeploy/octopus-grpc/go/pkg/certificate"
)

// Dial builds a client for an Octopus Server. It does not connect: gRPC resolves
// and dials lazily, so a client comes back even when the server is unreachable.
func Dial(cfg Config, logger *slog.Logger) (*grpc.ClientConn, error) {
	options := []grpc.DialOption{
		transportCredentials(cfg.TLS, logger),
		grpc.WithDefaultServiceConfig(ServiceConfig()),
		grpc.WithInitialWindowSize(MaxWindowSize()),
		grpc.WithInitialConnWindowSize(MaxWindowSize()),
		grpc.WithKeepaliveParams(KeepAliveParams()),
	}

	if cfg.Credentials != nil {
		options = append(options, grpc.WithPerRPCCredentials(cfg.Credentials))
	}

	if len(cfg.CallOptions) > 0 {
		options = append(options, grpc.WithDefaultCallOptions(cfg.CallOptions...))
	}

	options = append(options, cfg.DialOptions...)

	client, err := grpc.NewClient(cfg.ServerURL, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}

	return client, nil
}

func transportCredentials(cfg TLSConfig, logger *slog.Logger) grpc.DialOption {
	if cfg.Plaintext {
		return grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	return grpc.WithTransportCredentials(credentials.NewTLS(serverTLSConfig(cfg, logger)))
}

func serverTLSConfig(cfg TLSConfig, logger *slog.Logger) *tls.Config {
	tlsConfig := &tls.Config{RootCAs: cfg.RootCAs}

	if cfg.Thumbprint == "" {
		return tlsConfig
	}

	// Standard verification has to be turned off for VerifyPeerCertificate to get
	// a say at all. The callback replaces that verification rather than dropping
	// it: every path through it either verifies or returns an error.
	tlsConfig.InsecureSkipVerify = true
	tlsConfig.VerifyPeerCertificate = verifyServer(cfg, logger)

	return tlsConfig
}

func verifyServer(cfg TLSConfig, logger *slog.Logger) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		certs, err := certificate.ParseCertificates(rawCerts)
		if err != nil {
			return fmt.Errorf("failed to parse server certificate: %w", err)
		}

		if cfg.PinCertificate {
			logger.Debug("gRPC certificate pinning enabled, skipping system CA validation")
		} else if rootErr := certificate.VerifyServerCertificateWithRoot(certs, cfg.RootCAs); rootErr == nil {
			return nil
		} else {
			logger.Debug(
				"System CA validation failed, falling back to thumbprint verification",
				slog.Any("error", rootErr),
			)
		}

		return certificate.VerifyServerCertificate(certs[0], cfg.Thumbprint)
	}
}
