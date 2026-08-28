package connection

import (
	"time"

	"google.golang.org/grpc/keepalive"

	_ "embed"
)

//go:embed GrpcServiceConfig.json
var grpcServiceConfigFile string

func ServiceConfig() string {
	return grpcServiceConfigFile
}

// MaxWindowSize returns the maximum window size for gRPC streams.
// The value is calculated as 1MB + 10 bytes of overhead based on the maximum message receive size configured on Octopus Server
// Reason for setting an explicit window size rather than relying on dynamic window sizing is that it can lead to streams ending unexpectedly during high load situations.
func MaxWindowSize() int32 {
	windowOverhead := 10
	return int32(1*1024*1024 + windowOverhead)
}

// KeepAliveParams returns the keepalive parameters for gRPC connections. These values match what is configured on Octopus Server and should be kept in sync.
func KeepAliveParams() keepalive.ClientParameters {
	return keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             60 * time.Second,
		PermitWithoutStream: true,
	}
}
