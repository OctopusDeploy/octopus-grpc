package connection

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type mockHealthClient struct {
	checkFunc func(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error)
}

func (m *mockHealthClient) Check(
	ctx context.Context,
	in *grpc_health_v1.HealthCheckRequest,
	opts ...grpc.CallOption,
) (*grpc_health_v1.HealthCheckResponse, error) {
	return m.checkFunc(ctx, in, opts...)
}

func (m *mockHealthClient) List(
	_ context.Context,
	_ *grpc_health_v1.HealthListRequest,
	_ ...grpc.CallOption,
) (*grpc_health_v1.HealthListResponse, error) {
	return nil, nil
}

func (m *mockHealthClient) Watch(
	_ context.Context,
	_ *grpc_health_v1.HealthCheckRequest,
	_ ...grpc.CallOption,
) (grpc_health_v1.Health_WatchClient, error) {
	return nil, nil
}

func servingResponse() (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func notServingResponse() (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING}, nil
}

func TestKeepAlive_Start_SuccessfulChecks(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()

	var calls atomic.Int32
	client := &mockHealthClient{
		checkFunc: func(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
			calls.Add(1)
			return servingResponse()
		},
	}

	keepalive := NewHealthCheck(ctx, client, 100*time.Millisecond, 1, discardLogger())
	go keepalive.Start()
	<-ctx.Done()

	if calls.Load() == 0 {
		t.Error("expected at least one health check call")
	}
	select {
	case err := <-keepalive.Errors():
		t.Errorf("unexpected error: %v", err)
	default:
	}
}

func TestKeepAlive_Start_SendsDownEventOnFirstFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	client := &mockHealthClient{
		checkFunc: func(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
			return nil, errors.New("connection refused")
		},
	}

	// maxConsecutiveFailures = 10 — should not reach fatal before first DOWN event
	keepalive := NewHealthCheck(ctx, client, 50*time.Millisecond, 10, discardLogger())
	go keepalive.Start()

	select {
	case event := <-keepalive.Events():
		if event != Down {
			t.Errorf("expected Down, got %v", event)
		}
	case <-keepalive.Errors():
		t.Error("expected DOWN event before fatal error")
	case <-ctx.Done():
		t.Error("timed out waiting for DOWN event")
	}
}

func TestKeepAlive_Start_SendsDownEventOnNotServingResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	client := &mockHealthClient{
		checkFunc: func(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
			return notServingResponse()
		},
	}

	// maxConsecutiveFailures = 10 — should not reach fatal before first DOWN event
	keepalive := NewHealthCheck(ctx, client, 50*time.Millisecond, 10, discardLogger())
	go keepalive.Start()

	select {
	case event := <-keepalive.Events():
		if event != Down {
			t.Errorf("expected Down, got %v", event)
		}
	case <-keepalive.Errors():
		t.Error("expected DOWN event before fatal error")
	case <-ctx.Done():
		t.Error("timed out waiting for DOWN event")
	}
}

func TestKeepAlive_Start_DoesNotResendDownOnSubsequentFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	calls := 0
	client := &mockHealthClient{
		checkFunc: func(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
			calls++
			return nil, errors.New("connection refused")
		},
	}

	// maxConsecutiveFailures = 5, interval fast enough to get several ticks
	keepalive := NewHealthCheck(ctx, client, 30*time.Millisecond, 5, discardLogger())
	go keepalive.Start()

	// Collect events until fatal
	var events []Transition
	for {
		select {
		case event := <-keepalive.Events():
			events = append(events, event)
		case <-keepalive.Errors():
			// Fatal received — check collected events
			downCount := 0
			for _, e := range events {
				if e == Down {
					downCount++
				}
			}
			if downCount != 1 {
				t.Errorf("expected exactly 1 DOWN event, got %d", downCount)
			}
			return
		case <-ctx.Done():
			t.Error("timed out")
			return
		}
	}
}

func TestKeepAlive_Start_SendsUpEventOnRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	calls := 0
	client := &mockHealthClient{
		checkFunc: func(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
			calls++
			if calls <= 2 {
				return nil, errors.New("transient error")
			}
			return servingResponse()
		},
	}

	keepalive := NewHealthCheck(ctx, client, 50*time.Millisecond, 10, discardLogger())
	go keepalive.Start()

	// First event must be DOWN
	select {
	case event := <-keepalive.Events():
		if event != Down {
			t.Fatalf("expected first event to be Down, got %v", event)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for DOWN event")
	}

	// Second event must be UP (recovery)
	select {
	case event := <-keepalive.Events():
		if event != Up {
			t.Errorf("expected second event to be Up, got %v", event)
		}
	case err := <-keepalive.Errors():
		t.Errorf("unexpected fatal error: %v", err)
	case <-ctx.Done():
		t.Error("timed out waiting for UP event after recovery")
	}
}

func TestKeepAlive_Start_FatalAfterMaxConsecutiveFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	client := &mockHealthClient{
		checkFunc: func(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
			return nil, errors.New("connection refused")
		},
	}

	keepalive := NewHealthCheck(ctx, client, 50*time.Millisecond, 1, discardLogger())
	go keepalive.Start()

	select {
	case err := <-keepalive.Errors():
		if err == nil {
			t.Error("expected non-nil fatal error")
		}
	case <-ctx.Done():
		t.Error("timed out waiting for fatal error after consecutive failures")
	}
}

func TestKeepAlive_Start_ResetsConsecutiveFailuresOnSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	calls := 0
	client := &mockHealthClient{
		checkFunc: func(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
			calls++
			if calls <= 2 {
				return nil, errors.New("transient error")
			}
			return servingResponse()
		},
	}

	keepalive := NewHealthCheck(ctx, client, 50*time.Millisecond, 5, discardLogger())
	go keepalive.Start()

	time.Sleep(500 * time.Millisecond)
	cancel()

	select {
	case err := <-keepalive.Errors():
		t.Errorf("unexpected fatal error after recovery: %v", err)
	default:
	}
}

func TestKeepAlive_Start_StopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	client := &mockHealthClient{
		checkFunc: func(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
			return servingResponse()
		},
	}

	keepalive := NewHealthCheck(ctx, client, 100*time.Millisecond, 1, discardLogger())
	done := make(chan struct{})
	go func() {
		keepalive.Start()
		close(done)
	}()

	cancel()
	select {
	case <-done:
		// Start() returned — success
	case <-time.After(500 * time.Millisecond):
		t.Error("Start() did not return after context cancellation")
	}
}

func TestNewKeepAlive_ZeroIntervalUsesDefault(t *testing.T) {
	ctx := t.Context()
	client := &mockHealthClient{
		checkFunc: func(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
			return servingResponse()
		},
	}
	keepalive := NewHealthCheck(ctx, client, 0, 1, discardLogger())
	if keepalive.interval != DefaultHealthCheckInterval {
		t.Errorf("expected default interval %v, got %v", DefaultHealthCheckInterval, keepalive.interval)
	}
}

func TestNewKeepalive_ZeroMaxConsecutiveFailuresUsesDefault(t *testing.T) {
	ctx := t.Context()
	client := &mockHealthClient{
		checkFunc: func(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
			return servingResponse()
		},
	}
	keepalive := NewHealthCheck(ctx, client, 20, 0, discardLogger())
	if keepalive.maxConsecutiveFailures != DefaultHealthCheckMaxFailures {
		t.Errorf(
			"expected default max consecutive failures %v, got %v",
			DefaultHealthCheckMaxFailures,
			keepalive.maxConsecutiveFailures,
		)
	}
}

// Up is an edge, not a heartbeat. With no preceding Down there is nothing to
// recover from, and a caller that acts on every event -- as the gateway's loop
// does -- would restart its subscribers on every successful probe.
func TestKeepAlive_Start_SendsNoUpWithoutAPrecedingDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()

	client := &mockHealthClient{
		checkFunc: func(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
			return servingResponse()
		},
	}

	keepalive := NewHealthCheck(ctx, client, 50*time.Millisecond, 3, discardLogger())
	go keepalive.Start()

	select {
	case transition := <-keepalive.Events():
		t.Fatalf("Expected no transition while every probe succeeds, got %s", transition)
	case <-ctx.Done():
	}
}

// maxConsecutiveFailures is the number of failures that ends the process, not the
// number it survives. An off-by-one here buys an extra interval of downtime before
// Kubernetes gets the chance to restart the pod.
func TestKeepAlive_Start_GoesFatalOnTheMaximumFailureNotTheOneAfter(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	const maxFailures = 3

	var checks atomic.Int32
	client := &mockHealthClient{
		checkFunc: func(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
			checks.Add(1)
			return nil, errors.New("connection refused")
		},
	}

	keepalive := NewHealthCheck(ctx, client, 20*time.Millisecond, maxFailures, discardLogger())
	go keepalive.Start()

	select {
	case err := <-keepalive.Errors():
		if err == nil {
			t.Fatal("Expected a non-nil fatal error")
		}
		if got := checks.Load(); got != maxFailures {
			t.Errorf("Expected the fatal error after exactly %d failed checks, got %d", maxFailures, got)
		}
	case <-ctx.Done():
		t.Fatal("Expected a fatal error, got none")
	}
}
