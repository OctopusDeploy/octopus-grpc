package connection

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// testConnection is a Connection backed by a real in-process health server, so the
// probes under test are real RPCs over a real client. Only Rebuild is faked, so a
// test can count what the health check asked for without redialling anything.
type testConnection struct {
	mu         sync.Mutex
	client     *grpc.ClientConn
	health     *health.Server
	addr       string
	checks     atomic.Int32
	rebuilds   atomic.Int32
	rebuildErr error
}

func newTestConnection(t *testing.T) *testConnection {
	t.Helper()

	c := &testConnection{health: health.NewServer()}
	c.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	count := func(
		ctx context.Context,
		req any,
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		c.checks.Add(1)

		return handler(ctx, req)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Expected to listen, got %v", err)
	}

	server := grpc.NewServer(grpc.UnaryInterceptor(count))
	grpc_health_v1.RegisterHealthServer(server, c.health)

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	c.addr = listener.Addr().String()
	c.client = c.dial(t, c.addr)

	return c
}

func (c *testConnection) Get() *grpc.ClientConn {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.client
}

func (c *testConnection) Rebuild() error {
	c.rebuilds.Add(1)

	return c.rebuildErr
}

// serving flips what the server answers. A NOT_SERVING response is a failed probe
// as far as the health check is concerned, which is how these tests drive failure
// without tearing the connection down.
func (c *testConnection) serving(serving bool) {
	status := grpc_health_v1.HealthCheckResponse_NOT_SERVING
	if serving {
		status = grpc_health_v1.HealthCheckResponse_SERVING
	}

	c.health.SetServingStatus("", status)
}

// unreachable points the client at a port nothing listens on, so probes fail at
// the transport rather than with a response.
func (c *testConnection) unreachable(t *testing.T) {
	t.Helper()
	c.swap(c.dial(t, "127.0.0.1:1"))
}

func (c *testConnection) swap(client *grpc.ClientConn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.client = client
}

func (c *testConnection) dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()

	client, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Expected to build a client, got %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	client.Connect()

	return client
}

var errUnreachable = errors.New("dial failed")

// awaitReady blocks until the client has actually connected. Probes fail fast, so
// a test that starts probing an IDLE client sees a failure that says nothing about
// the server.
func (c *testConnection) awaitReady(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := c.Get()
	for client.GetState() != connectivity.Ready {
		if !client.WaitForStateChange(ctx, client.GetState()) {
			t.Fatalf("Expected the client to become ready, it was %s", client.GetState())
		}
	}
}

func (c *testConnection) Close() error {
	return c.Get().Close()
}
