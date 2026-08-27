package connection

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestServiceConfig_IsAcceptedByGrpc(t *testing.T) {
	client, err := grpc.NewClient("passthrough:///unused",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(ServiceConfig()),
	)
	if err != nil {
		t.Fatalf("Expected gRPC to accept the embedded service config, got %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
}

// The service config asks for client-side health checking, which gRPC performs by
// streaming Health/Watch. Octopus Server serves Watch via Grpc.AspNetCore.HealthChecks,
// so this is live rather than inert -- a server reporting NOT_SERVING must take the
// client out of READY even though the connection stays up. Deleting healthCheckConfig
// from the JSON leaves the client READY and fails this test.
func TestServiceConfig_HealthCheckingTakesAnUnhealthyServerOutOfReady(t *testing.T) {
	addr, healthServer := startHealthServer(t)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	client, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(ServiceConfig()),
		grpc.WithInitialWindowSize(MaxWindowSize()),
		grpc.WithInitialConnWindowSize(MaxWindowSize()),
		grpc.WithKeepaliveParams(KeepAliveParams()),
	)
	if err != nil {
		t.Fatalf("Expected to build a client, got %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	client.Connect()
	awaitState(t, client, connectivity.Ready)

	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	awaitStateChangeFrom(t, client, connectivity.Ready)
}

func startHealthServer(t *testing.T) (string, *health.Server) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Expected to listen, got %v", err)
	}

	healthServer := health.NewServer()
	server := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	return listener.Addr().String(), healthServer
}

func awaitState(t *testing.T, client *grpc.ClientConn, want connectivity.State) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for client.GetState() != want {
		if !client.WaitForStateChange(ctx, client.GetState()) {
			t.Fatalf("Expected the client to reach %s, it was %s", want, client.GetState())
		}
	}
}

func awaitStateChangeFrom(t *testing.T, client *grpc.ClientConn, from connectivity.State) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if !client.WaitForStateChange(ctx, from) {
		t.Fatalf("Expected the client to leave %s, it stayed", from)
	}
}
