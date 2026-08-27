package connection

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc/health/grpc_health_v1"
)

// These defaults should match the defaults in the chart's values.yaml file
const (
	DefaultKeepAliveInterval    = 30 * time.Second
	DefaultKeepAliveMaxFailures = 10
)

// HealthEvent signals a change in Octopus Server health observed by the keep-alive.
type HealthEvent int

const (
	// HealthEventDown is sent on the first health check failure.
	// Subscribers should be cancelled and wait for HealthEventUp before restarting.
	HealthEventDown HealthEvent = iota

	// HealthEventUp is sent when the health check recovers after a HealthEventDown.
	// Subscribers should be restarted.
	HealthEventUp
)

// KeepAlive sends periodic gRPC health check RPCs as application-level
// keep-alives. This keeps connections alive through load balancers that
// incorrectly respond to TCP-level gRPC keepalive frames.
//
// When health checks begin to fail it emits HealthEventDown so subscribers can
// be cancelled. When health recovers it emits HealthEventUp so subscribers can
// be restarted. If failures reach the configured maximum it sends a fatal error
// to Errors() and stops, allowing the process to exit and be restarted by
// Kubernetes.
type KeepAlive struct {
	ctx                    context.Context
	client                 grpc_health_v1.HealthClient
	interval               time.Duration
	logger                 *slog.Logger
	fatal                  chan error
	events                 chan HealthEvent
	maxConsecutiveFailures int
	consecutiveFailures    int
}

func NewKeepAlive(
	ctx context.Context,
	client grpc_health_v1.HealthClient,
	interval time.Duration,
	maxConsecutiveFailures int,
	logger *slog.Logger,
) *KeepAlive {
	if interval <= 0 {
		interval = DefaultKeepAliveInterval
	}

	if maxConsecutiveFailures <= 0 {
		maxConsecutiveFailures = DefaultKeepAliveMaxFailures
	}

	return &KeepAlive{
		ctx:                    ctx,
		client:                 client,
		interval:               interval,
		logger:                 logger,
		fatal:                  make(chan error, 1),
		events:                 make(chan HealthEvent, 2),
		maxConsecutiveFailures: maxConsecutiveFailures,
	}
}

// Start runs the keep-alive loop until ctx is canceled or consecutive failures
// exceed the limit. On the first failure it emits HealthEventDown; on recovery
// it emits HealthEventUp. If max consecutive failures is reached it sends a
// fatal error to Errors() and stops.
func (h *KeepAlive) Start() {
	h.logger.Info("Starting application keep alive",
		slog.Any("interval", h.interval),
		slog.Any("maxConsecutiveFailures", h.maxConsecutiveFailures),
	)

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			checkCtx, cancel := context.WithTimeout(h.ctx, h.interval)
			resp, err := h.client.Check(checkCtx, &grpc_health_v1.HealthCheckRequest{})
			cancel()

			if err != nil || resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
				h.consecutiveFailures++
				if h.consecutiveFailures == 1 {
					h.events <- HealthEventDown
					h.logger.Warn("keep alive check failed - cancelling subscribers",
						slog.Any("error", err),
						slog.Int("consecutiveFailures", h.consecutiveFailures),
					)
				} else {
					h.logger.Warn("keep alive check still failing",
						slog.Any("error", err),
						slog.Int("consecutiveFailures", h.consecutiveFailures),
					)
				}
				if h.consecutiveFailures >= h.maxConsecutiveFailures {
					h.fatal <- fmt.Errorf(
						"keep alive: %d consecutive failures, last error: %w",
						h.consecutiveFailures, err,
					)
					return
				}
				continue
			}

			if h.consecutiveFailures > 0 {
				h.consecutiveFailures = 0
				h.events <- HealthEventUp
				h.logger.Info("keep alive recovered - restarting subscribers")
			} else {
				h.logger.Debug("keep alive check succeeded")
			}
		case <-h.ctx.Done():
			return
		}
	}
}

func (h *KeepAlive) Events() <-chan HealthEvent {
	return h.events
}

func (h *KeepAlive) Errors() <-chan error {
	return h.fatal
}

func (h *KeepAlive) Context() context.Context {
	return h.ctx
}

func (h *KeepAlive) AwaitHealthRecovery(ctx context.Context) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case <-h.fatal:
			return false
		case event := <-h.events:
			if event == HealthEventUp {
				return true
			}
		}
	}
}
