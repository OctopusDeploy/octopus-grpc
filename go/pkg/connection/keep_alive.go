package connection

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc/health/grpc_health_v1"
)

// Defaults for callers that pass zero. Each consumer's chart sets its own values,
// so these are a floor rather than the configured behaviour anywhere.
const (
	DefaultKeepAliveInterval    = 30 * time.Second
	DefaultKeepAliveMaxFailures = 10
)

// Transition is a change in what the keep-alive believes about Octopus Server's
// health, sent once per change rather than once per probe. A caller that sees
// Down will not see it again until an Up has been sent in between.
type Transition int

const (
	// Down is sent on the first failed health check.
	// Subscribers should be cancelled and wait for Up before restarting.
	Down Transition = iota

	// Up is sent when health recovers after a Down.
	// Subscribers should be restarted.
	Up
)

func (t Transition) String() string {
	if t == Up {
		return "up"
	}

	return "down"
}

// KeepAlive sends periodic gRPC health check RPCs as application-level
// keep-alives. This keeps connections alive through load balancers that
// incorrectly respond to TCP-level gRPC keepalive frames.
//
// When health checks begin to fail it emits Down so subscribers can
// be cancelled. When health recovers it emits Up so subscribers can
// be restarted. If failures reach the configured maximum it sends a fatal error
// to Errors() and stops, allowing the process to exit and be restarted by
// Kubernetes.
type KeepAlive struct {
	ctx                    context.Context
	client                 grpc_health_v1.HealthClient
	interval               time.Duration
	logger                 *slog.Logger
	fatal                  chan error
	events                 chan Transition
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
		events:                 make(chan Transition, 2),
		maxConsecutiveFailures: maxConsecutiveFailures,
	}
}

// Start runs the keep-alive loop until ctx is canceled or consecutive failures
// exceed the limit. On the first failure it emits Down; on recovery
// it emits Up. If max consecutive failures is reached it sends a
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
					h.events <- Down
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
				h.events <- Up
				h.logger.Info("keep alive recovered - restarting subscribers")
			} else {
				h.logger.Debug("keep alive check succeeded")
			}
		case <-h.ctx.Done():
			return
		}
	}
}

func (h *KeepAlive) Events() <-chan Transition {
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
			if event == Up {
				return true
			}
		}
	}
}
