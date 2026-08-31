package connection

import (
	"context"
	"testing"

	"google.golang.org/grpc/credentials"
)

func TestBearerCredentials_PopulateTheHeadersOctopusServerAuthenticatesOn(t *testing.T) {
	tests := []struct {
		name  string
		creds credentials.PerRPCCredentials
	}{
		{name: "BearerCredentials", creds: BearerCredentials{ClientID: "gateway-1", Token: "tok"}},
		{name: "PlaintextBearerCredentials", creds: PlaintextBearerCredentials{ClientID: "gateway-1", Token: "tok"}},
	}

	want := map[string]string{
		"client-id":     "gateway-1",
		"authorization": "Bearer tok",
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.creds.GetRequestMetadata(context.Background())
			if err != nil {
				t.Fatalf("Expected metadata, got %v", err)
			}
			assertMetadata(t, got, want)
		})
	}
}

// PlaintextBearerCredentials is the only reason these credentials travel over a
// connection without TLS. gRPC refuses to build the client at all rather than risk
// sending the token in the clear, so the choice of type is load-bearing: a plaintext
// deployment does not get a degraded client, it gets no client.
func TestBearerCredentials_AreRefusedOnAnInsecureTransport(t *testing.T) {
	addr, _ := startHealthServer(t)

	_, err := Dial(Config{
		ServerURL:   addr,
		TLS:         TLSConfig{Plaintext: true},
		Credentials: BearerCredentials{ClientID: "gateway-1", Token: "tok"},
	}, discardLogger())
	if err == nil {
		t.Fatal("Expected gRPC to refuse credentials requiring transport security on an insecure transport")
	}
}

func assertMetadata(t *testing.T, got, want map[string]string) {
	t.Helper()

	if len(got) != len(want) {
		t.Errorf("Expected %d headers, got %v", len(want), got)
	}

	for key, value := range want {
		if got[key] != value {
			t.Errorf("Expected header %q to be %q, got %q", key, value, got[key])
		}
	}
}
