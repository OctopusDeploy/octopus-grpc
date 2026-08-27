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

func VerifyServerCertificateWithRoot(certs []*x509.Certificate, rootCertificates *x509.CertPool) error {
	// Attempt to verify the server certificate against system CAs
	opts := x509.VerifyOptions{
		Roots:         rootCertificates,
		Intermediates: x509.NewCertPool(),
		DNSName:       "",
	}

	// Add any intermediate certs to the pool
	for i := 1; i < len(certs); i++ {
		opts.Intermediates.AddCert(certs[i])
	}

	// Try system CA validation
	if _, err := certs[0].Verify(opts); err == nil {
		return nil
	} else {
		return err
	}
}
