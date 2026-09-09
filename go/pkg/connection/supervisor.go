package connection

import (
	"context"
	"log/slog"
)

// Subscribers is the work a caller runs against a Connection: streams and loops
// that hold a client and do not outlive it being replaced.
//
// Start returns once the work is running, and returns an error only if it could
// not be started at all. Wait blocks until the goroutines a cancelled Start left
// behind have exited, so a second set is never started over a live one.
type Subscribers interface {
	Start(ctx context.Context) error
	Wait()
}

// Supervisor keeps Subscribers running across outages, because a subscriber's own
// retries cannot tell a broken stream from an absent server, and because an outage
// usually replaces the connection underneath it.
//
// The health check reports to the Supervisor and to nobody else. A caller that
// held the signal itself had to drain every transition to keep the probe loop
// moving, and had to work out for itself that a server answering again is not the
// same as a server that never stopped.
type Supervisor struct {
	healthCheck *HealthCheck
	subscribers Subscribers
	logger      *slog.Logger

	cancel       context.CancelFunc
	startedUnder uint64
}

func NewSupervisor(
	healthCheck *HealthCheck,
	subscribers Subscribers,
	logger *slog.Logger,
) *Supervisor {
	return &Supervisor{healthCheck: healthCheck, subscribers: subscribers, logger: logger}
}

// Start starts the subscribers and returns what starting them returned. Call it
// before Run, and treat a failure as a failure to start: the first start is
// synchronous so a caller can fail its own startup on a bad connection rather than
// meeting it later as a failed restart.
func (s *Supervisor) Start(ctx context.Context) error {
	return s.start(ctx)
}

// Run supervises the subscribers Start began, until ctx is cancelled or an outage
// outlasts GiveUpAfter. It returns the error the health check gave up with, the
// error a restart failed with, or nil once ctx is cancelled. One Run per
// Supervisor.
func (s *Supervisor) Run(ctx context.Context) error {
	go s.healthCheck.Start()

	defer s.stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-s.healthCheck.fatal:
			return err
		case latest := <-s.healthCheck.updates:
			if err := s.reconcile(ctx, latest); err != nil {
				return err
			}
		}
	}
}

// reconcile acts on where health stands rather than on what changed, so a snapshot
// that overwrote an unread one costs nothing: an outage the Supervisor was busy
// through still arrives as an outage count it has not started under.
func (s *Supervisor) reconcile(ctx context.Context, latest snapshot) error {
	if !latest.healthy {
		s.logger.Warn("Octopus Server stopped answering, stopping subscribers")
		s.stop()

		return nil
	}

	if latest.outages == s.startedUnder {
		return nil
	}

	s.logger.Info("Octopus Server is answering again, restarting subscribers")
	s.stop()

	return s.start(ctx)
}

// start records the outage count before starting rather than after. An outage that
// begins while the subscribers are starting would otherwise be recorded as one
// they had already been started under, and they would hold a closed connection for
// good.
func (s *Supervisor) start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.startedUnder = s.healthCheck.snapshot().outages

	if err := s.subscribers.Start(runCtx); err != nil {
		cancel()

		return err
	}

	return nil
}

func (s *Supervisor) stop() {
	if s.cancel == nil {
		return
	}

	s.cancel()
	s.subscribers.Wait()
	s.cancel = nil
}
