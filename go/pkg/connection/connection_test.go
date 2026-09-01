package connection

import (
	"errors"
	"log/slog"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConnection_GetReturnsTheReplacementAfterARebuild(t *testing.T) {
	conn, _ := newDialedConnection(t)

	before := conn.Get()
	if err := conn.Rebuild(); err != nil {
		t.Fatalf("Expected the rebuild to succeed, got %v", err)
	}

	if conn.Get() == before {
		t.Error("Expected Get to return the replacement, it returned the client that was replaced")
	}
}

// The old client is closed so its subchannels and goroutines go away; a rebuild
// on every failed probe would otherwise leak one per attempt.
func TestConnection_RebuildClosesTheClientItReplaced(t *testing.T) {
	conn, _ := newDialedConnection(t)

	before := conn.Get()
	if err := conn.Rebuild(); err != nil {
		t.Fatalf("Expected the rebuild to succeed, got %v", err)
	}

	if err := before.Close(); err == nil {
		t.Error("Expected the replaced client to have been closed already")
	}
}

// A connection that might work beats none, so a failed rebuild leaves the current
// client in place rather than tearing it down.
func TestConnection_FailedRebuildKeepsTheExistingClient(t *testing.T) {
	conn, dialer := newDialedConnection(t)

	before := conn.Get()
	dialer.fail(errors.New("dial failed"))

	if err := conn.Rebuild(); err == nil {
		t.Fatal("Expected the rebuild to report the failure")
	}
	if conn.Get() != before {
		t.Error("Expected the existing client to be left in place after a failed rebuild")
	}
}

func TestConnection_ConcurrentReadsAndRebuildsAreSafe(t *testing.T) {
	conn, _ := newDialedConnection(t)

	var waiting sync.WaitGroup
	for range 8 {
		waiting.Add(1)
		go func() {
			defer waiting.Done()
			for range 50 {
				if conn.Get() == nil {
					t.Error("Expected Get never to return nil while the connection is open")

					return
				}
			}
		}()
	}

	waiting.Add(1)
	go func() {
		defer waiting.Done()
		for range 20 {
			_ = conn.Rebuild()
		}
	}()

	waiting.Wait()
}

func TestNew_ReportsADialFailureRatherThanReturningAConnection(t *testing.T) {
	// An empty target is rejected by grpc.NewClient itself.
	if _, err := New(Config{ServerURL: "\x00"}, discardLogger()); err == nil {
		t.Error("Expected New to report the dial failure")
	}
}

func newDialedConnection(t *testing.T) (Connection, *stubDialer) {
	t.Helper()

	dialer := &stubDialer{}
	conn, err := newConnection(Config{ServerURL: "passthrough:///unused"}, discardLogger(), dialer.dial)
	if err != nil {
		t.Fatalf("Expected to build a connection, got %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return conn, dialer
}

// stubDialer hands out a fresh client per call without touching the network, so a
// test can tell one client from another by pointer.
type stubDialer struct {
	mu  sync.Mutex
	err error
}

func (s *stubDialer) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *stubDialer) dial(cfg Config, _ *slog.Logger) (*grpc.ClientConn, error) {
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return grpc.NewClient(cfg.ServerURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
