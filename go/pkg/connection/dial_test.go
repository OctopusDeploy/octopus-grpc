package connection

import (
	"context"
	"crypto/x509"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestDial_VerifiesAServerThatChainsToTheRootPool(t *testing.T) {
	server := startTLSHealthServer(t)

	err := check(t, Config{
		ServerURL: server.addr,
		TLS:       TLSConfig{Thumbprint: server.thumbprint, RootCAs: server.roots},
	})
	if err != nil {
		t.Fatalf("Expected the call to succeed, got %v", err)
	}
}

// With the server absent from the root pool, chain verification fails and the
// thumbprint is the fallback that has to carry the connection.
func TestDial_FallsBackToTheThumbprintWhenTheChainDoesNotVerify(t *testing.T) {
	server := startTLSHealthServer(t)

	err := check(t, Config{
		ServerURL: server.addr,
		TLS:       TLSConfig{Thumbprint: server.thumbprint, RootCAs: x509.NewCertPool()},
	})
	if err != nil {
		t.Fatalf("Expected the thumbprint to carry the connection, got %v", err)
	}
}

func TestDial_RejectsAServerWhoseThumbprintDoesNotMatch(t *testing.T) {
	server := startTLSHealthServer(t)

	err := check(t, Config{
		ServerURL: server.addr,
		TLS:       TLSConfig{Thumbprint: strings.Repeat("A", 40), RootCAs: x509.NewCertPool()},
	})
	if err == nil {
		t.Fatal("Expected the call to be rejected, it succeeded")
	}
	if !strings.Contains(err.Error(), "does not match the expected thumbprint") {
		t.Errorf("Expected a thumbprint mismatch, got %v", err)
	}
}

// Pinning is the reason PinCertificate exists: an operator who has pinned a
// thumbprint should not need the certificate to chain to anything.
func TestDial_PinnedThumbprintIgnoresTheRootPoolEntirely(t *testing.T) {
	server := startTLSHealthServer(t)

	err := check(t, Config{
		ServerURL: server.addr,
		TLS: TLSConfig{
			Thumbprint:     server.thumbprint,
			PinCertificate: true,
			RootCAs:        x509.NewCertPool(),
		},
	})
	if err != nil {
		t.Fatalf("Expected a pinned thumbprint to be enough, got %v", err)
	}
}

func TestDial_PinnedThumbprintStillRejectsAMismatch(t *testing.T) {
	server := startTLSHealthServer(t)

	err := check(t, Config{
		ServerURL: server.addr,
		TLS: TLSConfig{
			Thumbprint:     strings.Repeat("A", 40),
			PinCertificate: true,
			RootCAs:        server.roots,
		},
	})
	if err == nil {
		t.Fatal("Expected the call to be rejected, it succeeded")
	}
}

func TestDial_PlaintextNeedsNoCertificateAtAll(t *testing.T) {
	addr, _ := startHealthServer(t)

	err := check(t, Config{ServerURL: addr, TLS: TLSConfig{Plaintext: true}})
	if err != nil {
		t.Fatalf("Expected the plaintext call to succeed, got %v", err)
	}
}

func check(t *testing.T, cfg Config) error {
	t.Helper()

	client, err := Dial(cfg, discardLogger())
	if err != nil {
		return err
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// WaitForReady(false) overrides the service config, which turns it on for every
	// method. Left on, a rejected handshake does not surface as an error at all --
	// gRPC keeps retrying and the call blocks until the context deadline. These
	// tests are about whether verification accepts or rejects, so they ask to be
	// told rather than waited on. See TestDial_WaitForReadyHidesARejectedHandshake.
	_, err = grpc_health_v1.NewHealthClient(client).Check(
		ctx,
		&grpc_health_v1.HealthCheckRequest{},
		grpc.WaitForReady(false),
	)

	return err
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// waitForReady turns a rejected server into a call that blocks until its own
// deadline rather than an error, so a probe pays its whole timeout every attempt.
func TestDial_WaitForReadyTurnsARejectedHandshakeIntoADeadline(t *testing.T) {
	server := startTLSHealthServer(t)

	client, err := Dial(Config{
		ServerURL: server.addr,
		TLS:       TLSConfig{Thumbprint: strings.Repeat("A", 40), RootCAs: x509.NewCertPool()},
	}, discardLogger())
	if err != nil {
		t.Fatalf("Expected to build a client, got %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	started := time.Now()
	err = invokeAnyMethod(ctx, client)

	if err == nil {
		t.Fatal("Expected the call to fail, it succeeded")
	}
	if status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("Expected the rejection to arrive as a deadline, got %v", status.Code(err))
	}
	if elapsed := time.Since(started); elapsed < 400*time.Millisecond {
		t.Errorf("Expected the call to block until its deadline, it returned after %s", elapsed)
	}
}

// healthCheckConfig is left out on purpose: it would take the only subchannel out of READY
// on NOT_SERVING, so waitForReady would queue the probes that would have seen the server
// recover.
func TestDial_ProbesAnUnhealthyServerRatherThanQueueingBehindIt(t *testing.T) {
	addr, healthServer := startHealthServer(t)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	client, err := Dial(Config{ServerURL: addr, TLS: TLSConfig{Plaintext: true}}, discardLogger())
	if err != nil {
		t.Fatalf("Expected to build a client, got %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := grpc_health_v1.NewHealthClient(client).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Expected the probe to get an answer, got %v", err)
	}
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Errorf("Expected the probe to report NOT_SERVING, got %s", resp.GetStatus())
	}
}

// Positive control for the test above, which would pass just as well if this binary never
// linked google.golang.org/grpc/health. Asking for health checking explicitly proves the
// feature is live here, so the absent config is what keeps probes answerable.
func TestDial_HealthCheckingQueuesProbesWhenTheServiceConfigAsksForIt(t *testing.T) {
	addr, healthServer := startHealthServer(t)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	client, err := Dial(Config{
		ServerURL:   addr,
		TLS:         TLSConfig{Plaintext: true},
		DialOptions: []grpc.DialOption{grpc.WithDefaultServiceConfig(healthCheckingServiceConfig)},
	}, discardLogger())
	if err != nil {
		t.Fatalf("Expected to build a client, got %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = grpc_health_v1.NewHealthClient(client).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Expected the probe to be queued until its deadline, got %v", err)
	}
	// The code alone would also match a slow dial; the balancer error is what says the probe
	// was held back by health checking rather than anything else.
	if !strings.Contains(status.Convert(err).Message(), "health check failed") {
		t.Errorf("Expected the deadline to blame health checking, got %q", status.Convert(err).Message())
	}
}

// healthCheckingServiceConfig is the shared config with healthCheckConfig put back, which
// is all it takes to switch the feature on.
const healthCheckingServiceConfig = `{
  "loadBalancingConfig": [{ "round_robin": {} }],
  "healthCheckConfig": { "serviceName": "" },
  "methodConfig": [{ "name": [{}], "waitForReady": true }]
}`

// waitForReady covers every method, not just Health. The handshake is rejected
// before dispatch, so the method need not exist and the payload types never matter.
func invokeAnyMethod(ctx context.Context, client *grpc.ClientConn) error {
	return client.Invoke(
		ctx,
		"/octopus.Anything/AnyMethod",
		&grpc_health_v1.HealthCheckRequest{},
		&grpc_health_v1.HealthCheckResponse{},
	)
}
