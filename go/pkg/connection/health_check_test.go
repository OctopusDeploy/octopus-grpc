package connection

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestHealthCheck_Start_ProbesWhileTheServerIsHealthy(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	connection := newTestConnection(t)
	go NewHealthCheck(ctx, connection, 50*time.Millisecond, 100, discardLogger()).Start()
	time.Sleep(300 * time.Millisecond)

	if connection.checks.Load() == 0 {
		t.Error("Expected at least one health check to reach the server")
	}
}

func TestHealthCheck_Start_SendsDownOnTheFirstFailedProbe(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	connection := newTestConnection(t)
	healthCheck := NewHealthCheck(ctx, connection, 20*time.Millisecond, 100, discardLogger())
	connection.serving(false)
	go healthCheck.Start()

	awaitTransition(t, ctx, healthCheck, Down)
}

// A server answering NOT_SERVING is reachable but unhealthy. That is a failed
// probe as much as a refused connection is.
func TestHealthCheck_Start_TreatsAnUnreachableServerAsAFailureToo(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	connection := newTestConnection(t)
	healthCheck := NewHealthCheck(ctx, connection, 20*time.Millisecond, 100, discardLogger())
	connection.unreachable(t)
	go healthCheck.Start()

	awaitTransition(t, ctx, healthCheck, Down)
}

// Down is an edge. A caller cancels its subscribers once on Down, so repeating it
// on every subsequent failed probe would have it cancelling work it had already
// stopped.
func TestHealthCheck_Start_DoesNotRepeatDownWhileFailuresContinue(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	connection := newTestConnection(t)
	healthCheck := NewHealthCheck(ctx, connection, 20*time.Millisecond, 100, discardLogger())
	connection.serving(false)
	go healthCheck.Start()

	awaitTransition(t, ctx, healthCheck, Down)

	select {
	case transition := <-healthCheck.Events():
		t.Errorf("Expected no further transition while failures continue, got %s", transition)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestHealthCheck_Start_SendsUpWhenTheServerRecovers(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	connection := newTestConnection(t)
	healthCheck := NewHealthCheck(ctx, connection, 20*time.Millisecond, 100, discardLogger())
	connection.serving(false)
	go healthCheck.Start()

	awaitTransition(t, ctx, healthCheck, Down)
	connection.serving(true)
	awaitTransition(t, ctx, healthCheck, Up)
}

// Up is an edge too: with no preceding Down there is nothing to recover from, and
// a caller acting on every event would restart its subscribers on every probe.
func TestHealthCheck_Start_SendsNoUpWithoutAPrecedingDown(t *testing.T) {
	// Cancelled after the assertion window, not as part of it. Probe contexts derive
	// from this one, so a deadline expiring mid-probe fails that probe and shows up
	// as a transition the server never caused.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	connection := newTestConnection(t)
	healthCheck := NewHealthCheck(ctx, connection, 50*time.Millisecond, 100, discardLogger())
	go healthCheck.Start()

	select {
	case transition := <-healthCheck.Events():
		t.Fatalf("Expected no transition while every probe succeeds, got %s", transition)
	case <-time.After(400 * time.Millisecond):
	}
}

// A recovery has to zero the failure count, not just report Up. Counting probes
// rather than timing them keeps this deterministic: after recovering, giving up
// must take a whole limit's worth of failures again, so at least that many probes
// have to reach the server.
func TestHealthCheck_Start_ResetsTheFailureCountOnRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	const maxFailures = 8

	connection := newTestConnection(t)
	connection.awaitReady(t)
	healthCheck := NewHealthCheck(ctx, connection, 20*time.Millisecond, maxFailures, discardLogger())
	go healthCheck.Start()

	// Accumulate some failures, but stop short of the limit.
	connection.serving(false)
	awaitTransition(t, ctx, healthCheck, Down)

	before := connection.checks.Load()
	if !eventually(2*time.Second, func() bool { return connection.checks.Load() >= before+5 }) {
		t.Fatal("Expected the probe loop to keep failing")
	}

	connection.serving(true)
	awaitTransition(t, ctx, healthCheck, Up)
	atRecovery := connection.checks.Load()

	connection.serving(false)

	select {
	case <-healthCheck.Errors():
		if since := connection.checks.Load() - atRecovery; since < maxFailures {
			t.Errorf(
				"Expected %d failures after recovery before giving up, it gave up after %d probes",
				maxFailures, since,
			)
		}
	case <-ctx.Done():
		t.Fatal("Expected it to give up eventually, it did not")
	}
}

// maxConsecutiveFailures is the failure that ends the process, not the last one it
// survives. An off-by-one costs an extra interval of downtime before Kubernetes
// gets the chance to restart the pod.
func TestHealthCheck_Start_GivesUpOnTheMaximumFailureNotTheOneAfter(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	const maxFailures = 3

	connection := newTestConnection(t)
	connection.unreachable(t)
	healthCheck := NewHealthCheck(ctx, connection, 20*time.Millisecond, maxFailures, discardLogger())
	go healthCheck.Start()

	select {
	case err := <-healthCheck.Errors():
		if err == nil {
			t.Fatal("Expected a non-nil fatal error")
		}
		if !strings.Contains(err.Error(), "3 consecutive failures") {
			t.Errorf("Expected the error to name the failure count, got %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Expected a fatal error, got none")
	}
}

func TestHealthCheck_Start_StopsWhenItsContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	connection := newTestConnection(t)
	healthCheck := NewHealthCheck(ctx, connection, 20*time.Millisecond, 100, discardLogger())

	stopped := make(chan struct{})
	go func() {
		healthCheck.Start()
		close(stopped)
	}()

	cancel()

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected Start to return once its context was cancelled")
	}
}

// The reason Connection exists: reconnecting cannot fix an instance that has
// moved, because a client only re-resolves its target when it is rebuilt. So a
// failed probe has to replace the client, not just count.
func TestHealthCheck_Start_ReplacesTheClientAfterAFailedProbe(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	connection := newTestConnection(t)
	connection.serving(false)
	go NewHealthCheck(ctx, connection, 20*time.Millisecond, 100, discardLogger()).Start()

	if !eventually(2*time.Second, func() bool { return connection.rebuilds.Load() >= 3 }) {
		t.Errorf("Expected repeated failures to keep replacing the client, saw %d replacements",
			connection.rebuilds.Load())
	}
}

func TestHealthCheck_Start_DoesNotReplaceTheClientWhileProbesSucceed(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	connection := newTestConnection(t)
	go NewHealthCheck(ctx, connection, 50*time.Millisecond, 100, discardLogger()).Start()
	time.Sleep(300 * time.Millisecond)

	if got := connection.rebuilds.Load(); got != 0 {
		t.Errorf("Expected no replacements while the server answers, got %d", got)
	}
}

// A failed replacement leaves the existing client in place, so the loop must carry
// on and eventually give up properly rather than stopping where it is.
func TestHealthCheck_Start_KeepsGoingWhenAReplacementFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	connection := newTestConnection(t)
	connection.rebuildErr = errUnreachable
	connection.unreachable(t)
	healthCheck := NewHealthCheck(ctx, connection, 20*time.Millisecond, 3, discardLogger())
	go healthCheck.Start()

	select {
	case err := <-healthCheck.Errors():
		if err == nil {
			t.Fatal("Expected a non-nil fatal error")
		}
	case <-ctx.Done():
		t.Fatal("Expected the loop to reach its failure limit despite the replacements failing")
	}
}

func TestNewHealthCheck_FallsBackToDefaultsForZeroValues(t *testing.T) {
	healthCheck := NewHealthCheck(t.Context(), newTestConnection(t), 0, 0, discardLogger())

	if healthCheck.interval != DefaultHealthCheckInterval {
		t.Errorf("Expected the default interval, got %s", healthCheck.interval)
	}
	if healthCheck.maxConsecutiveFailures != DefaultHealthCheckMaxFailures {
		t.Errorf("Expected the default failure limit, got %d", healthCheck.maxConsecutiveFailures)
	}
}

func awaitTransition(t *testing.T, ctx context.Context, healthCheck *HealthCheck, want Transition) {
	t.Helper()

	select {
	case got := <-healthCheck.Events():
		if got != want {
			t.Fatalf("Expected transition %s, got %s", want, got)
		}
	case <-ctx.Done():
		t.Fatalf("Expected transition %s, got none", want)
	}
}

func eventually(within time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}

	return condition()
}
