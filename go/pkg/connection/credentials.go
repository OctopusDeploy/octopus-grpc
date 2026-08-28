package connection

import (
	"context"
	"fmt"

	"google.golang.org/grpc/credentials"
)

var (
	_ credentials.PerRPCCredentials = BearerCredentials{}
	_ credentials.PerRPCCredentials = PlaintextBearerCredentials{}
)

// BearerCredentials authenticates as an Octopus Argo CD gateway: a client id
// alongside a bearer token.
type BearerCredentials struct {
	ClientID string
	Token    string
}

func NewBearerCredentials(clientID string, token string) BearerCredentials {
	return BearerCredentials{ClientID: clientID, Token: token}
}

func (c BearerCredentials) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{
		"client-id":     c.ClientID,
		"authorization": fmt.Sprintf("Bearer %s", c.Token),
	}, nil
}

func (c BearerCredentials) RequireTransportSecurity() bool {
	return true
}

// PlaintextBearerCredentials sends what BearerCredentials sends, to a server
// running with TLS disabled.
//
// It is a separate type because gRPC refuses to build a client at all when
// per-RPC credentials requiring transport security are paired with an insecure
// transport. Naming the two postings separately keeps that refusal a choice made
// at the call site rather than a flag that has to agree with TLSConfig.
type PlaintextBearerCredentials struct {
	BearerCredentials
}

func NewPlaintextBearerCredentials(clientID string, token string) PlaintextBearerCredentials {
	return PlaintextBearerCredentials{BearerCredentials: NewBearerCredentials(clientID, token)}
}

func (PlaintextBearerCredentials) RequireTransportSecurity() bool {
	return false
}
