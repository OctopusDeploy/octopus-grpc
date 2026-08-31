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

type BearerCredentials struct {
	ClientID string
	Token    string
}

func (c BearerCredentials) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return bearerMetadata(c.ClientID, c.Token), nil
}

func (c BearerCredentials) RequireTransportSecurity() bool {
	return true
}

type PlaintextBearerCredentials struct {
	ClientID string
	Token    string
}

func (c PlaintextBearerCredentials) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return bearerMetadata(c.ClientID, c.Token), nil
}

func (PlaintextBearerCredentials) RequireTransportSecurity() bool {
	return false
}

func bearerMetadata(clientID string, token string) map[string]string {
	return map[string]string{
		"client-id":     clientID,
		"authorization": fmt.Sprintf("Bearer %s", token),
	}
}
