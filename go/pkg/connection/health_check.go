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
	DefaultHealthCheckInterval    = 30 * time.Second
	DefaultHealthCheckMaxInterval = 2 * time.Minute
	DefaultHealthCheckGiveUpAfter = 5 * time.Minute
)

// HealthCheckProbeTimeout is how long a single probe waits for an answer. It is
// deliberately not the interval: a probe that waited out a backed-off interval
// would report an outage minutes late.
const HealthCheckProbeTimeout = 15 * time.Second

// HealthCheckConfig is how often the keep-alive probes and how long it tolerates
// an outage before giving up.
type HealthCheckConfig struct {
	// Interval is the duration between probes while Octopus Server is answering.
	Interval time.Duration

	// MaxInterval is the maximum possible duration between probes. Each failed probe doubles the
	// interval up to this.
	MaxInterval time.Duration

	// GiveUpAfter is how long an outage may last, measured from the first failed
	// probe, before the keep-alive reports a fatal error and stops.
	GiveUpAfter time.Duration
}

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

// HealthCheck sends periodic gRPC health check RPCs as application-level
// keep-alives. This keeps connections alive through load balancers that
// incorrectly respond to TCP-level gRPC keepalive frames.
//
// When health checks begin to fail it emits Down so subscribers can
// be cancelled. When health recovers it emits Up so subscribers can
// be restarted. If an outage outlasts GiveUpAfter it sends a fatal error
// to Errors() and stops, allowing the process to exit and be restarted by
// Kubernetes.
type HealthCheck struct {
	ctx        context.Context
	connection Connection
	cfg        HealthCheckConfig
	logger     *slog.Logger
	fatal      chan error
	events     chan Transition

	interval    time.Duration
	outageStart time.Time
}

func NewHealthCheck(
	ctx context.Context,
	connection Connection,
	cfg HealthCheckConfig,
	logger *slog.Logger,
) *HealthCheck {
	cfg = cfg.withDefaults()

	return &HealthCheck{
		ctx:        ctx,
		connection: connection,
		cfg:        cfg,
		logger:     logger,
		fatal:      make(chan error, 1),
		events:     make(chan Transition, 2),
		interval:   cfg.Interval,
	}
}

// Start runs the keep-alive loop until ctx is cancelled or an outage outlasts
// GiveUpAfter. On the first failure it emits Down; on recovery it emits Up.
func (h *HealthCheck) Start() {
	h.logger.Info("Starting application keep alive",
		slog.Any("interval", h.cfg.Interval),
		slog.Any("maxInterval", h.cfg.MaxInterval),
		slog.Any("giveUpAfter", h.cfg.GiveUpAfter),
	)

	timer := time.NewTimer(h.interval)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			if !h.check() {
				return
			}

			timer.Reset(h.interval)
		case <-h.ctx.Done():
			return
		}
	}
}

func (h *HealthCheck) Events() <-chan Transition {
	return h.events
}

func (h *HealthCheck) Errors() <-chan error {
	return h.fatal
}

func (h *HealthCheck) Context() context.Context {
	return h.ctx
}

func (h *HealthCheck) AwaitRecovery(ctx context.Context) bool {
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

func (c HealthCheckConfig) withDefaults() HealthCheckConfig {
	if c.Interval <= 0 {
		c.Interval = DefaultHealthCheckInterval
	}

	if c.MaxInterval <= 0 {
		c.MaxInterval = DefaultHealthCheckMaxInterval
	}

	// A cap below the interval would back the keep-alive off to probing more
	// often than it was asked to.
	c.MaxInterval = max(c.MaxInterval, c.Interval)

	if c.GiveUpAfter <= 0 {
		c.GiveUpAfter = DefaultHealthCheckGiveUpAfter
	}

	return c
}

func (h *HealthCheck) check() bool {
	if err := h.probe(); err != nil {
		return h.recordFailure(err)
	}

	h.recordSuccess()

	return true
}

func (h *HealthCheck) probe() error {
	ctx, cancel := context.WithTimeout(h.ctx, HealthCheckProbeTimeout)
	defer cancel()

	resp, err := grpc_health_v1.NewHealthClient(h.connection.Get()).Check(
		ctx,
		&grpc_health_v1.HealthCheckRequest{},
	)
	if err != nil {
		return err
	}

	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("octopus server reported %s", resp.GetStatus())
	}

	return nil
}

func (h *HealthCheck) recordFailure(err error) bool {
	if h.outageStart.IsZero() {
		h.outageStart = time.Now()
		h.logger.Warn("keep alive check failed - cancelling subscribers", slog.Any("error", err))
		h.events <- Down
	} else {
		h.logger.Warn("keep alive check still failing",
			slog.Any("error", err),
			slog.Duration("outage", time.Since(h.outageStart)),
		)
	}

	if outage := time.Since(h.outageStart); outage >= h.cfg.GiveUpAfter {
		h.fatal <- fmt.Errorf(
			"keep alive: no answer from Octopus Server for %s, last error: %w",
			outage.Round(time.Second), err,
		)

		return false
	}

	// The address the current client resolved to may be the one that went away,
	// and probing it again would only keep confirming that.
	if rebuildErr := h.connection.Rebuild(); rebuildErr != nil {
		h.logger.Warn("Continuing on the existing connection", slog.Any("error", rebuildErr))
	}

	h.interval = h.nextInterval()

	return true
}

// nextInterval never reaches past the give-up boundary. Waiting a backed-off
// interval over it would throw away the last chance to recover.
func (h *HealthCheck) nextInterval() time.Duration {
	backedOff := min(h.interval*2, h.cfg.MaxInterval)
	remaining := time.Until(h.outageStart.Add(h.cfg.GiveUpAfter))

	return min(backedOff, remaining)
}

func (h *HealthCheck) recordSuccess() {
	if h.outageStart.IsZero() {
		h.logger.Debug("keep alive check succeeded")

		return
	}

	h.outageStart = time.Time{}
	h.interval = h.cfg.Interval
	h.logger.Info("keep alive recovered - restarting subscribers")
	h.events <- Up
}
