// Package grpcerror decides what a failed RPC to Octopus Server means for the
// caller: reconnect, stop cleanly, or end the process.
//
// It lives here because lobster-watcher and octopus-argocd-gateway each had
// their own copy of the answer, and the copies disagreed -- a dropped stream
// reported as Internal was a reconnect in one and a dead pod in the other.
package grpcerror

import (
	"context"
	"errors"
	"io"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Action is what the caller should do about an error.
type Action int

const (
	// Retry means reconnect and carry on. The stream broke or Octopus Server is
	// not answering, and neither says the work itself was wrong.
	Retry Action = iota

	// Stop means end this stream and leave it ended, without reporting an error.
	// Either something asked for it to stop, or the server does not offer it.
	Stop

	// Fatal means report the error and let the process exit. Nothing the caller
	// can do will help, so being restarted is the honest response.
	Fatal
)

func (a Action) String() string {
	switch a {
	case Retry:
		return "retry"
	case Stop:
		return "stop"
	default:
		return "fatal"
	}
}

// Classify decides what to do about err.
//
// It errs towards Retry. A connection that has gone bad is repairable -- the
// keep-alive rebuilds it, which is what picks up an Octopus Cloud instance that
// has moved -- so treating a broken stream as fatal throws away a pod for
// something that fixes itself.
func Classify(err error) Action {
	if err == nil {
		return Stop
	}

	// A cancelled context arrives as a bare error rather than a status, so it
	// would otherwise fall through to Unknown and end the process.
	if errors.Is(err, context.Canceled) {
		return Stop
	}

	if errors.Is(err, io.EOF) {
		return Retry
	}

	grpcErr := status.Convert(err)
	switch grpcErr.Code() {
	case codes.OK:
		return Stop
	case codes.Unavailable, codes.Aborted:
		return Retry
	case codes.Internal:
		return classifyInternal(grpcErr.Message())
	case codes.Canceled, codes.Unimplemented:
		return Stop
	default:
		return Fatal
	}
}

// classifyInternal separates a connection that broke from a server that is
// broken. gRPC reports both as Internal and only the message tells them apart.
func classifyInternal(message string) Action {
	for _, brokenTransport := range []string{"EOF", "RST_STREAM"} {
		if strings.Contains(message, brokenTransport) {
			return Retry
		}
	}

	return Fatal
}
