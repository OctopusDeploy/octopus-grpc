package grpcerror

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Action
	}{
		{
			name: "a server that is not answering is worth reconnecting to",
			err:  status.Error(codes.Unavailable, "connection refused"),
			want: Retry,
		},
		{
			name: "an aborted call is worth reconnecting to",
			err:  status.Error(codes.Aborted, "aborted"),
			want: Retry,
		},
		{
			name: "the end of a stream is worth reconnecting to",
			err:  io.EOF,
			want: Retry,
		},
		{
			name: "an EOF wrapped by a caller is still the end of a stream",
			err:  fmt.Errorf("receiving events: %w", io.EOF),
			want: Retry,
		},
		{
			name: "gRPC reports a dropped stream as an internal EOF",
			err:  status.Error(codes.Internal, "unexpected EOF"),
			want: Retry,
		},
		{
			name: "gRPC reports a reset stream as an internal RST_STREAM",
			err:  status.Error(codes.Internal, "stream terminated by RST_STREAM with error code: INTERNAL_ERROR"),
			want: Retry,
		},
		{
			name: "an internal error that is not a broken stream is the server being broken",
			err:  status.Error(codes.Internal, "something broke server side"),
			want: Fatal,
		},
		{
			name: "a cancelled call was asked to stop",
			err:  status.Error(codes.Canceled, "context canceled"),
			want: Stop,
		},
		{
			name: "a cancelled context arrives without a status and still means stop",
			err:  context.Canceled,
			want: Stop,
		},
		{
			name: "a cancelled context wrapped by a caller still means stop",
			err:  fmt.Errorf("subscriber ending: %w", context.Canceled),
			want: Stop,
		},
		{
			name: "a method the server does not offer will not start working",
			err:  status.Error(codes.Unimplemented, "unknown method"),
			want: Stop,
		},
		{
			name: "bad credentials will not fix themselves",
			err:  status.Error(codes.Unauthenticated, "token rejected"),
			want: Fatal,
		},
		{
			name: "an unrecognised error is reported rather than retried in silence",
			err:  errors.New("something else entirely"),
			want: Fatal,
		},
		{
			name: "no error is nothing to act on",
			err:  nil,
			want: Stop,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := Classify(test.err); got != test.want {
				t.Errorf("Expected %s, got %s", test.want, got)
			}
		})
	}
}
