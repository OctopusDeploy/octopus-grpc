package connection

import (
	"crypto/x509"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Config describes how to reach an Octopus Server over gRPC.
type Config struct {
	// ServerURL is passed to gRPC as-is. Anything a target needs -- a scheme
	// stripped, a default port added -- is the caller's to do first, because what
	// counts as a valid target differs between the products dialling.
	ServerURL string

	// Credentials is usually BearerCredentials, or PlaintextBearerCredentials
	// against a server with TLS disabled. A caller still on an older Octopus auth
	// scheme can supply its own implementation.
	Credentials credentials.PerRPCCredentials

	TLS TLSConfig

	// CallOptions and DialOptions carry whatever the caller needs beyond the
	// shared contract: message size limits, compression, an OpenTelemetry stats
	// handler. They live on the Config rather than being passed per call so that
	// a client rebuilt later is built the same way as the first one.
	CallOptions []grpc.CallOption
	DialOptions []grpc.DialOption
}

type TLSConfig struct {
	// Plaintext dials without TLS at all. Credentials that require transport
	// security will refuse to build a client, so this has to agree with them.
	Plaintext bool

	// Thumbprint, when set, turns on Octopus's own server verification: the
	// certificate is accepted if it chains to RootCAs, or failing that if its
	// SHA-1 thumbprint matches. Left empty, ordinary TLS verification applies.
	Thumbprint string

	// PinCertificate accepts the thumbprint alone and never consults RootCAs.
	// Only consulted when Thumbprint is set.
	PinCertificate bool

	// RootCAs is the pool to verify against. Building it -- reading bundles off
	// disk, merging system roots -- belongs to the caller, whose config says
	// where those bundles live.
	RootCAs *x509.CertPool
}
