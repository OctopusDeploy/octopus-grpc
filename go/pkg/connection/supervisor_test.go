package connection

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The reason the Supervisor exists: the streams the subscribers hold do not
// survive an outage, and the connection underneath them is replaced, so nothing
// they do on their own brings them back.
func TestSupervisor_Run_RestartsTheSubscribersAfterAnOutage(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	connection := newTestConnection(t)
	connection.awaitReady(t)
	subscribers := &testSubscribers{}
	supervisor := NewSupervisor(
		NewHealthCheck(ctx, connection, testConfig(20*time.Millisecond, time.Minute), discardLogger()),
		subscribers,
		discardLogger(),
	)
	if err := supervisor.Start(ctx); err != nil {
		t.Fatalf("Expected the subscribers to start, got %v", err)
	}

	go func() { _ = supervisor.Run(ctx) }()

	if !eventually(2*time.Second, func() bool { return subscribers.running.Load() == 1 }) {
		t.Fatal("Expected the subscribers to be started")
	}

	connection.serving(false)
	if !eventually(2*time.Second, func() bool { return subscribers.running.Load() == 0 }) {
		t.Fatal("Expected the outage to stop the subscribers")
	}

	connection.serving(true)
	if !eventually(2*time.Second, func() bool { return subscribers.running.Load() == 1 }) {
		t.Fatal("Expected recovery to restart the subscribers")
	}

	if got := subscribers.starts.Load(); got != 2 {
		t.Errorf("Expected exactly one restart, the subscribers were started %d times", got)
	}
}

// Restarting subscribers drops every stream, so a Supervisor that restarted on
// anything short of an outage would cost work for nothing.
func TestSupervisor_Run_DoesNotRestartWhileTheServerAnswers(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	connection := newTestConnection(t)
	connection.awaitReady(t)
	subscribers := &testSubscribers{}
	supervisor := NewSupervisor(
		NewHealthCheck(ctx, connection, testConfig(20*time.Millisecond, time.Minute), discardLogger()),
		subscribers,
		discardLogger(),
	)
	if err := supervisor.Start(ctx); err != nil {
		t.Fatalf("Expected the subscribers to start, got %v", err)
	}

	go func() { _ = supervisor.Run(ctx) }()

	if !eventually(2*time.Second, func() bool { return subscribers.starts.Load() == 1 }) {
		t.Fatal("Expected the subscribers to be started")
	}

	time.Sleep(300 * time.Millisecond)

	if got := subscribers.starts.Load(); got != 1 {
		t.Errorf("Expected no restart while every probe succeeds, the subscribers were started %d times", got)
	}
}

// Run carries the health check's own error out, rather than reporting that it gave
// up and leaving the caller to say why. The last error from Octopus Server is the
// one thing worth having in the logs of a pod that is about to exit.
func TestSupervisor_Run_ReturnsTheErrorTheHealthCheckGaveUpWith(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	connection := newTestConnection(t)
	connection.unreachable(t)
	subscribers := &testSubscribers{}
	supervisor := NewSupervisor(
		NewHealthCheck(
			ctx, connection, testConfig(20*time.Millisecond, 300*time.Millisecond), discardLogger(),
		),
		subscribers,
		discardLogger(),
	)

	if err := supervisor.Start(ctx); err != nil {
		t.Fatalf("Expected the subscribers to start, got %v", err)
	}

	errs := make(chan error, 1)
	go func() { errs <- supervisor.Run(ctx) }()

	select {
	case err := <-errs:
		if err == nil {
			t.Fatal("Expected Run to return the fatal error")
		}
		if !strings.Contains(err.Error(), "no answer from Octopus Server") {
			t.Errorf("Expected the error to name the outage, got %v", err)
		}
		if got := subscribers.running.Load(); got != 0 {
			t.Errorf("Expected the subscribers to be stopped before Run returned, %d were running", got)
		}
	case <-ctx.Done():
		t.Fatal("Expected Run to return a fatal error, it did not")
	}
}

// testSubscribers counts what the Supervisor asked for, and keeps a goroutine per
// start so Wait has something to wait for.
type testSubscribers struct {
	starts  atomic.Int32
	running atomic.Int32
	wg      sync.WaitGroup
}

func (s *testSubscribers) Start(ctx context.Context) error {
	s.starts.Add(1)
	s.running.Add(1)
	s.wg.Add(1)

	go func() {
		defer s.wg.Done()
		<-ctx.Done()
		s.running.Add(-1)
	}()

	return nil
}

func (s *testSubscribers) Wait() {
	s.wg.Wait()
}
