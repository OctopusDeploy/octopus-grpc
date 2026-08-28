package connection

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

func TestBearerCredentials_SendMetadataTheServerCanRead(t *testing.T) {
	received := callWith(t, nil, PlaintextBearerCredentials{ClientID: "gateway-1", Token: "tok"})

	assertMetadata(t, received, "client-id", "gateway-1")
	assertMetadata(t, received, "authorization", "Bearer tok")
}

// PlaintextBearerCredentials is the only reason these credentials travel over a
// connection without TLS. gRPC refuses to build the client at all rather than risk
// sending the token in the clear, so the choice of type is load-bearing: a plaintext
// deployment does not get a degraded client, it gets no client.
func TestBearerCredentials_AreRefusedOnAnInsecureTransport(t *testing.T) {
	addr, _ := startHealthServer(t)

	_, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(BearerCredentials{ClientID: "gateway-1", Token: "tok"}),
	)
	if err == nil {
		t.Fatal("Expected gRPC to refuse credentials requiring transport security on an insecure transport")
	}
}

// The insecure transport the other tests dial over is the one gRPC lets any
// credentials onto. This dials the transport BearerCredentials actually demands.
func TestBearerCredentials_SendMetadataOverTLS(t *testing.T) {
	certs := selfSignedTLS(t)

	received := callWith(
		t,
		&transport{server: certs.serverCreds, client: certs.clientCreds},
		BearerCredentials{ClientID: "gateway-1", Token: "tok"},
	)

	assertMetadata(t, received, "client-id", "gateway-1")
	assertMetadata(t, received, "authorization", "Bearer tok")
}

type transport struct {
	server credentials.TransportCredentials
	client credentials.TransportCredentials
}

// callWith dials a real server through creds, makes one RPC, and returns the
// metadata the server saw.
func callWith(t *testing.T, tr *transport, creds credentials.PerRPCCredentials) metadata.MD {
	t.Helper()

	seen := make(chan metadata.MD, 1)
	capture := func(
		ctx context.Context,
		req any,
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		seen <- md
		return handler(ctx, req)
	}

	serverOpts := []grpc.ServerOption{grpc.UnaryInterceptor(capture)}
	clientTransport := grpc.WithTransportCredentials(insecure.NewCredentials())
	if tr != nil {
		serverOpts = append(serverOpts, grpc.Creds(tr.server))
		clientTransport = grpc.WithTransportCredentials(tr.client)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Expected to listen, got %v", err)
	}

	server := grpc.NewServer(serverOpts...)
	grpc_health_v1.RegisterHealthServer(server, newServingHealthServer())
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	client, err := grpc.NewClient(listener.Addr().String(), clientTransport, grpc.WithPerRPCCredentials(creds))
	if err != nil {
		t.Fatalf("Expected to build a client, got %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err = grpc_health_v1.NewHealthClient(client).Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatalf("Expected the call to reach the server, got %v", err)
	}

	select {
	case md := <-seen:
		return md
	case <-ctx.Done():
		t.Fatal("Expected the server to have seen the call")
		return nil
	}
}

func assertMetadata(t *testing.T, md metadata.MD, key, want string) {
	t.Helper()

	got := md.Get(key)
	if len(got) != 1 || got[0] != want {
		t.Errorf("Expected metadata %q to be %q, got %v", key, want, got)
	}
}
