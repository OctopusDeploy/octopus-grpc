package connection

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
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

// HealthCheckConfig is how often the health check probes and how long it tolerates
// an outage before giving up.
type HealthCheckConfig struct {
	// Interval is the duration between probes while Octopus Server is answering.
	Interval time.Duration

	// MaxInterval is the maximum possible duration between probes. Each failed probe doubles the
	// interval up to this.
	MaxInterval time.Duration

	// GiveUpAfter is how long an outage may last, measured from the first failed
	// probe, before the health check reports a fatal error and stops.
	GiveUpAfter time.Duration
}

// snapshot is what the last probe observed, together with a count of how many
// times the health check has gone unhealthy.
//
// unhealthyCount is here because healthy on its own cannot express a flap. Going
// unhealthy breaks every stream open at the time and usually replaces the
// connection underneath them, so a Supervisor that was busy through one and found
// a healthy server afterwards still has to restart the subscribers. Comparing
// unhealthyCount against the value the subscribers were started under is how it
// tells "still up" from "up again".
type snapshot struct {
	healthy        bool
	unhealthyCount uint64
}

// HealthCheck sends periodic gRPC health check RPCs. The traffic they generate
// also keeps connections alive through load balancers that incorrectly respond
// to TCP-level gRPC keepalive frames.
//
// What it observes is not a caller's to handle. Pass it to a Supervisor, which
// stops the subscribers while Octopus Server is not answering and starts them
// again on the replacement connection. If an outage outlasts GiveUpAfter the
// Supervisor's Run returns a fatal error, allowing the process to exit and be
// restarted by Kubernetes.
type HealthCheck struct {
	ctx        context.Context
	connection Connection
	cfg        HealthCheckConfig
	logger     *slog.Logger
	fatal      chan error

	// The current snapshot, and the slot the next one goes in.
	mu             sync.Mutex
	healthy        bool
	unhealthyCount uint64
	updates        chan snapshot

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
		healthy:    true,
		updates:    make(chan snapshot, 1),
		interval:   cfg.Interval,
	}
}

// Start runs the health check loop until ctx is cancelled or an outage outlasts
// GiveUpAfter. A Supervisor calls this; a caller does not.
func (h *HealthCheck) Start() {
	h.logger.Info("Starting health checks",
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

func (h *HealthCheck) Context() context.Context {
	return h.ctx
}

func (c HealthCheckConfig) withDefaults() HealthCheckConfig {
	if c.Interval <= 0 {
		c.Interval = DefaultHealthCheckInterval
	}

	if c.MaxInterval <= 0 {
		c.MaxInterval = DefaultHealthCheckMaxInterval
	}

	// A cap below the interval would back the health check off to probing more
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
		h.logger.Warn("Health check failed - cancelling subscribers", slog.Any("error", err))
		h.setHealthy(false)
	} else {
		h.logger.Warn("Health check still failing",
			slog.Any("error", err),
			slog.Duration("outage", time.Since(h.outageStart)),
		)
	}

	if outage := time.Since(h.outageStart); outage >= h.cfg.GiveUpAfter {
		h.fatal <- fmt.Errorf(
			"health check: no answer from Octopus Server for %s, last error: %w",
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
		h.logger.Debug("Health check succeeded")

		return
	}

	h.outageStart = time.Time{}
	h.interval = h.cfg.Interval
	h.logger.Info("Health check recovered - restarting subscribers")
	h.setHealthy(true)
}

// snapshot reports what the last probe observed.
func (h *HealthCheck) snapshot() snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()

	return snapshot{healthy: h.healthy, unhealthyCount: h.unhealthyCount}
}

func (h *HealthCheck) setHealthy(healthy bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.healthy == healthy {
		return
	}

	h.healthy = healthy
	if !healthy {
		h.unhealthyCount++
	}

	h.publish(snapshot{healthy: h.healthy, unhealthyCount: h.unhealthyCount})
}

// publish leaves the newest snapshot in the one-slot channel, discarding an unread
// one. Called with the lock held; every send here is non-blocking, so a Supervisor
// busy stopping subscribers can never stall a probe -- and the give-up error,
// which is sent after this point, can never end up stuck behind it.
func (h *HealthCheck) publish(latest snapshot) {
	for {
		select {
		case h.updates <- latest:
			return
		default:
			select {
			case <-h.updates:
			default:
			}
		}
	}
}
