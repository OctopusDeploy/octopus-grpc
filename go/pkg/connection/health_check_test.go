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
	go NewHealthCheck(ctx, connection, testConfig(50*time.Millisecond, time.Minute), discardLogger()).Start()
	time.Sleep(300 * time.Millisecond)

	if connection.checks.Load() == 0 {
		t.Error("Expected at least one health check to reach the server")
	}
}

func TestHealthCheck_Start_SendsDownOnTheFirstFailedProbe(t *testing.T) {
	// A server answering NOT_SERVING is reachable but unhealthy. That is a failed
	// probe as much as a refused connection is.
	tests := []struct {
		name string
		fail func(*testing.T, *testConnection)
	}{
		{
			name: "the server answers that it is not serving",
			fail: func(_ *testing.T, c *testConnection) { c.serving(false) },
		},
		{
			name: "the server cannot be reached at all",
			fail: func(t *testing.T, c *testConnection) { c.unreachable(t) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()

			connection := newTestConnection(t)
			healthCheck := NewHealthCheck(
				ctx, connection, testConfig(20*time.Millisecond, time.Minute), discardLogger(),
			)
			test.fail(t, connection)
			go healthCheck.Start()

			awaitTransition(t, ctx, healthCheck, Down)
		})
	}
}

// Down is an edge. A caller cancels its subscribers once on Down, so repeating it
// on every subsequent failed probe would have it cancelling work it had already
// stopped.
func TestHealthCheck_Start_DoesNotRepeatDownWhileFailuresContinue(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	connection := newTestConnection(t)
	healthCheck := NewHealthCheck(ctx, connection, testConfig(20*time.Millisecond, time.Minute), discardLogger())
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
	healthCheck := NewHealthCheck(ctx, connection, testConfig(20*time.Millisecond, time.Minute), discardLogger())
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
	healthCheck := NewHealthCheck(ctx, connection, testConfig(50*time.Millisecond, time.Minute), discardLogger())
	go healthCheck.Start()

	select {
	case transition := <-healthCheck.Events():
		t.Fatalf("Expected no transition while every probe succeeds, got %s", transition)
	case <-time.After(400 * time.Millisecond):
	}
}

// Otherwise a server that flapped hours ago would count towards the next blip.
func TestHealthCheck_Start_RestartsTheOutageClockOnRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	const giveUpAfter = 500 * time.Millisecond

	connection := newTestConnection(t)
	connection.awaitReady(t)
	healthCheck := NewHealthCheck(ctx, connection, testConfig(20*time.Millisecond, giveUpAfter), discardLogger())
	go healthCheck.Start()

	// Fail for a while, but recover before the budget runs out.
	connection.serving(false)
	awaitTransition(t, ctx, healthCheck, Down)
	time.Sleep(giveUpAfter / 2)

	connection.serving(true)
	awaitTransition(t, ctx, healthCheck, Up)

	secondOutage := time.Now()
	connection.serving(false)

	select {
	case <-healthCheck.Errors():
		if lasted := time.Since(secondOutage); lasted < giveUpAfter {
			t.Errorf("Expected a whole %s budget after recovery, it gave up after %s", giveUpAfter, lasted)
		}
	case <-ctx.Done():
		t.Fatal("Expected it to give up eventually, it did not")
	}
}

func TestHealthCheck_Start_GivesUpOnceTheOutageOutlastsTheBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	const giveUpAfter = 300 * time.Millisecond

	connection := newTestConnection(t)
	connection.unreachable(t)
	healthCheck := NewHealthCheck(ctx, connection, testConfig(20*time.Millisecond, giveUpAfter), discardLogger())

	started := time.Now()
	go healthCheck.Start()

	select {
	case err := <-healthCheck.Errors():
		if err == nil {
			t.Fatal("Expected a non-nil fatal error")
		}
		if !strings.Contains(err.Error(), "no answer from Octopus Server") {
			t.Errorf("Expected the error to name the outage, got %v", err)
		}
		if lasted := time.Since(started); lasted < giveUpAfter {
			t.Errorf("Expected it to tolerate the outage for %s, it gave up after %s", giveUpAfter, lasted)
		}
	case <-ctx.Done():
		t.Fatal("Expected a fatal error, got none")
	}
}

func TestHealthCheck_Start_StopsWhenItsContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	connection := newTestConnection(t)
	healthCheck := NewHealthCheck(ctx, connection, testConfig(20*time.Millisecond, time.Minute), discardLogger())

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
	go NewHealthCheck(ctx, connection, testConfig(20*time.Millisecond, time.Minute), discardLogger()).Start()

	if !eventually(2*time.Second, func() bool { return connection.rebuilds.Load() >= 3 }) {
		t.Errorf("Expected repeated failures to keep replacing the client, saw %d replacements",
			connection.rebuilds.Load())
	}
}

func TestHealthCheck_Start_DoesNotReplaceTheClientWhileProbesSucceed(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	connection := newTestConnection(t)
	go NewHealthCheck(ctx, connection, testConfig(50*time.Millisecond, time.Minute), discardLogger()).Start()
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
	healthCheck := NewHealthCheck(
		ctx, connection, testConfig(20*time.Millisecond, 300*time.Millisecond), discardLogger(),
	)
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

func TestHealthCheck_Failure_DoublesTheIntervalUpToTheMaximum(t *testing.T) {
	cfg := HealthCheckConfig{
		Interval:    20 * time.Millisecond,
		MaxInterval: 80 * time.Millisecond,
		GiveUpAfter: time.Minute,
	}
	healthCheck := NewHealthCheck(t.Context(), newTestConnection(t), cfg, discardLogger())

	want := []time.Duration{40, 80, 80, 80}
	for i, expected := range want {
		healthCheck.recordFailure(errUnreachable)

		if got := healthCheck.interval; got != expected*time.Millisecond {
			t.Fatalf("Expected the interval to be %s after failure %d, got %s", expected*time.Millisecond, i+1, got)
		}
	}

	healthCheck.recordSuccess()

	if healthCheck.interval != cfg.Interval {
		t.Errorf("Expected recovery to restore the %s interval, got %s", cfg.Interval, healthCheck.interval)
	}
}

func TestNewHealthCheck_FallsBackToDefaultsForZeroValues(t *testing.T) {
	healthCheck := NewHealthCheck(t.Context(), newTestConnection(t), HealthCheckConfig{}, discardLogger())

	if healthCheck.cfg.Interval != DefaultHealthCheckInterval {
		t.Errorf("Expected the default interval, got %s", healthCheck.cfg.Interval)
	}
	if healthCheck.cfg.MaxInterval != DefaultHealthCheckMaxInterval {
		t.Errorf("Expected the default maximum interval, got %s", healthCheck.cfg.MaxInterval)
	}
	if healthCheck.cfg.GiveUpAfter != DefaultHealthCheckGiveUpAfter {
		t.Errorf("Expected the default give up duration, got %s", healthCheck.cfg.GiveUpAfter)
	}
}

// A cap below the interval would otherwise have the keep-alive back off to
// probing more often than it was asked to.
func TestNewHealthCheck_RaisesAMaximumIntervalBelowTheInterval(t *testing.T) {
	cfg := HealthCheckConfig{Interval: time.Minute, MaxInterval: time.Second}
	healthCheck := NewHealthCheck(t.Context(), newTestConnection(t), cfg, discardLogger())

	if healthCheck.cfg.MaxInterval != cfg.Interval {
		t.Errorf("Expected the maximum interval to be raised to %s, got %s", cfg.Interval, healthCheck.cfg.MaxInterval)
	}
}

// These settings are deliberately misaligned: doubling alone would probe at 0, 200,
// 600 and 1400ms, giving up 400ms late.
func TestHealthCheck_Start_TakesALastProbeOnTheGiveUpBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	const giveUpAfter = time.Second

	cfg := HealthCheckConfig{
		Interval:    100 * time.Millisecond,
		MaxInterval: 800 * time.Millisecond,
		GiveUpAfter: giveUpAfter,
	}

	connection := newTestConnection(t)
	connection.unreachable(t)
	healthCheck := NewHealthCheck(ctx, connection, cfg, discardLogger())

	started := time.Now()
	go healthCheck.Start()

	select {
	case <-healthCheck.Errors():
		lasted := time.Since(started)
		if lasted < giveUpAfter {
			t.Errorf("Expected it to tolerate the outage for %s, it gave up after %s", giveUpAfter, lasted)
		}
		if lasted > giveUpAfter+cfg.MaxInterval/2 {
			t.Errorf("Expected it to give up on the boundary, it waited out a backed-off interval: %s", lasted)
		}
	case <-ctx.Done():
		t.Fatal("Expected a fatal error, got none")
	}
}

// testConfig pins the maximum interval to the interval, so repeated failures keep
// probing at a rate the test can assert against.
func testConfig(interval, giveUpAfter time.Duration) HealthCheckConfig {
	return HealthCheckConfig{Interval: interval, MaxInterval: interval, GiveUpAfter: giveUpAfter}
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
