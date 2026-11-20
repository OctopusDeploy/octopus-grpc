package certificate

import (
	"crypto/sha1"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"strings"
)

func VerifyServerCertificate(serverCert *x509.Certificate, expectedThumbprint string) error {
	thumbprint := sha1.Sum(serverCert.Raw)
	thumbprintStr := strings.ToUpper(hex.EncodeToString(thumbprint[:]))
	if thumbprintStr != strings.ToUpper(expectedThumbprint) {
		return ErrUnexpectedServerCertificate(expectedThumbprint, thumbprintStr)
	}

	return nil
}

func ParseCertificates(rawCerts [][]byte) ([]*x509.Certificate, error) {
	if len(rawCerts) == 0 {
		return nil, fmt.Errorf("no raw certificate data provided")
	}

	// Make the certificates from the raw bytes
	certs := make([]*x509.Certificate, len(rawCerts))
	for i, asn1Data := range rawCerts {
		cert, err := x509.ParseCertificate(asn1Data)
		if err != nil {
			return nil, fmt.Errorf("failed to parse certificate: %w", err)
		}
		certs[i] = cert
	}
	return certs, nil
}

// VerifyServerCertificateWithRoot checks that the leaf chains to rootCertificates
// and is valid for serverName, as standard TLS verification would. serverName is
// required because x509 skips the hostname check when it is empty.
func VerifyServerCertificateWithRoot(
	certs []*x509.Certificate,
	rootCertificates *x509.CertPool,
	serverName string,
) error {
	if len(certs) == 0 {
		return fmt.Errorf("no server certificate provided")
	}
	if serverName == "" {
		return fmt.Errorf("a server name is required to verify the server certificate")
	}

	opts := x509.VerifyOptions{
		Roots:         rootCertificates,
		Intermediates: x509.NewCertPool(),
		DNSName:       serverName,
	}

	for i := 1; i < len(certs); i++ {
		opts.Intermediates.AddCert(certs[i])
	}

	_, err := certs[0].Verify(opts)
	return err
}
