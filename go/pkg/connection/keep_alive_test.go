package connection

import (
	"context"
	"errors"
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

	calls := 0
	client := &mockHealthClient{
		checkFunc: func(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
			calls++
			return servingResponse()
		},
	}

	keepalive := NewKeepAlive(ctx, client, 100*time.Millisecond, 1, discardLogger())
	go keepalive.Start()
	<-ctx.Done()

	if calls == 0 {
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
	keepalive := NewKeepAlive(ctx, client, 50*time.Millisecond, 10, discardLogger())
	go keepalive.Start()

	select {
	case event := <-keepalive.Events():
		if event != HealthEventDown {
			t.Errorf("expected HealthEventDown, got %v", event)
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
	keepalive := NewKeepAlive(ctx, client, 50*time.Millisecond, 10, discardLogger())
	go keepalive.Start()

	select {
	case event := <-keepalive.Events():
		if event != HealthEventDown {
			t.Errorf("expected HealthEventDown, got %v", event)
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
	keepalive := NewKeepAlive(ctx, client, 30*time.Millisecond, 5, discardLogger())
	go keepalive.Start()

	// Collect events until fatal
	var events []HealthEvent
	for {
		select {
		case event := <-keepalive.Events():
			events = append(events, event)
		case <-keepalive.Errors():
			// Fatal received — check collected events
			downCount := 0
			for _, e := range events {
				if e == HealthEventDown {
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

	keepalive := NewKeepAlive(ctx, client, 50*time.Millisecond, 10, discardLogger())
	go keepalive.Start()

	// First event must be DOWN
	select {
	case event := <-keepalive.Events():
		if event != HealthEventDown {
			t.Fatalf("expected first event to be HealthEventDown, got %v", event)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for DOWN event")
	}

	// Second event must be UP (recovery)
	select {
	case event := <-keepalive.Events():
		if event != HealthEventUp {
			t.Errorf("expected second event to be HealthEventUp, got %v", event)
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

	keepalive := NewKeepAlive(ctx, client, 50*time.Millisecond, 1, discardLogger())
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

	keepalive := NewKeepAlive(ctx, client, 50*time.Millisecond, 5, discardLogger())
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

	keepalive := NewKeepAlive(ctx, client, 100*time.Millisecond, 1, discardLogger())
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
	keepalive := NewKeepAlive(ctx, client, 0, 1, discardLogger())
	if keepalive.interval != DefaultKeepAliveInterval {
		t.Errorf("expected default interval %v, got %v", DefaultKeepAliveInterval, keepalive.interval)
	}
}

func TestNewKeepalive_ZeroMaxConsecutiveFailuresUsesDefault(t *testing.T) {
	ctx := t.Context()
	client := &mockHealthClient{
		checkFunc: func(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
			return servingResponse()
		},
	}
	keepalive := NewKeepAlive(ctx, client, 20, 0, discardLogger())
	if keepalive.maxConsecutiveFailures != DefaultKeepAliveMaxFailures {
		t.Errorf(
			"expected default max consecutive failures %v, got %v",
			DefaultKeepAliveMaxFailures,
			keepalive.maxConsecutiveFailures,
		)
	}
}
