package connection

import (
	"fmt"
	"log/slog"
	"sync"

	"google.golang.org/grpc"
)

// Connection is a gRPC client for an Octopus Server that can be replaced.
//
// Replacing is the point. A ClientConn resolves its target when it is built and
// only re-resolves when a subchannel drops out of READY. When an Octopus Cloud
// instance moves, the address it used to be at usually keeps answering, so the
// client stays READY and never looks again -- reconnecting, retrying and waiting
// all fail to help, because they all reuse the resolution. Building a new client
// is the only thing that forces a fresh one.
type Connection interface {
	// Get returns the current client.
	//
	// Resolve it per use and do not memoise anything built from it. A service stub
	// captures the ClientConn it was built with, so a stub kept across a
	// replacement goes on talking to a connection that has already been closed --
	// silently, since a stub has no idea its connection was retired.
	Get() *grpc.ClientConn

	// Rebuild replaces the client, closing the one it replaced. On failure the
	// existing client is left in place: a connection that might work beats none.
	Rebuild() error

	Close() error
}

type replaceableConnection struct {
	mu     sync.RWMutex
	client *grpc.ClientConn

	cfg    Config
	logger *slog.Logger
	dial   func(Config, *slog.Logger) (*grpc.ClientConn, error)
}

func New(cfg Config, logger *slog.Logger) (Connection, error) {
	return newConnection(cfg, logger, Dial)
}

func (c *replaceableConnection) Get() *grpc.ClientConn {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.client
}

func (c *replaceableConnection) Rebuild() error {
	replacement, err := c.dial(c.cfg, c.logger)
	if err != nil {
		return fmt.Errorf("failed to replace the connection: %w", err)
	}

	replacement.Connect()

	c.mu.Lock()
	previous := c.client
	c.client = replacement
	c.mu.Unlock()

	c.closeQuietly(previous)

	return nil
}

func (c *replaceableConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil {
		return nil
	}

	err := c.client.Close()
	c.client = nil

	return err
}

func newConnection(
	cfg Config,
	logger *slog.Logger,
	dial func(Config, *slog.Logger) (*grpc.ClientConn, error),
) (*replaceableConnection, error) {
	client, err := dial(cfg, logger)
	if err != nil {
		return nil, err
	}

	return &replaceableConnection{client: client, cfg: cfg, logger: logger, dial: dial}, nil
}

// closeQuietly logs rather than returns. The replacement is already in place by
// this point, so failing to close the old client is a leak worth knowing about
// but not a reason to tell the caller the rebuild failed.
func (c *replaceableConnection) closeQuietly(client *grpc.ClientConn) {
	if client == nil {
		return
	}

	if err := client.Close(); err != nil {
		c.logger.Debug("Closing the replaced connection failed", slog.Any("error", err))
	}
}
