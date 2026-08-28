# octopus-grpc

Shared libraries for connecting to Octopus Server from the Kubernetes Monitor &amp; Argo CD Gateway.

| Directory | Contents                     |
|-----------|------------------------------|
| `go/`     | The Go module                |
| `dotnet/` | The .NET libraries           |

## Go

The module lives in a subdirectory, so its path carries that subdirectory too:

```go
import "github.com/OctopusDeploy/octopus-grpc/go/pkg/certificate"
```

```
go get github.com/OctopusDeploy/octopus-grpc/go
```

Releases are tagged `go/vX.Y.Z`. The prefix is not decoration: the Go module proxy only
resolves a subdirectory module from tags named after that subdirectory.
