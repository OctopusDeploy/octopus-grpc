package certificate

import (
	"bytes"
	"crypto/x509"
	"io"
	"log/slog"
	"testing"
)

var initialRootCertPool *x509.CertPool

func postTestTeardown() {
	rootCertificatePool = nil
}

func TestMain(m *testing.M) {
	systemCertPool, err := x509.SystemCertPool()
	if err != nil {
		initialRootCertPool = x509.NewCertPool()
	} else {
		initialRootCertPool = systemCertPool
	}

	m.Run()
}

func TestInitializeCertificatePool_ReadsCertFilesAndAppendsThemToCertPool(t *testing.T) {
	// arrange
	defer postTestTeardown()

	// act
	err := InitializeCertificatePool("testdata/certs", discardLogger())
	if err != nil {
		t.Fatalf("Expected no error initializing cert pool, got %v", err)
	}

	pool, err := GetRootCertificatePool()
	if err != nil {
		t.Fatalf("Expected no error getting root CA bundle, got %v", err)
	}

	// assert
	// There is 4 total unique certs in the testdata/certs and we don't care about any system certs
	totalCerts := len(pool.Subjects()) - len(initialRootCertPool.Subjects())
	if totalCerts != 4 {
		t.Errorf("Expected 4 certificates, got %v", len(pool.Subjects()))
	}
}

func TestInitializeCertificatePool_OnlyReadsPemFiles(t *testing.T) {
	// arrange
	defer postTestTeardown()

	// act
	err := InitializeCertificatePool("testdata/certs/not_a_real_cert.txt", discardLogger())
	if err != nil {
		t.Fatalf("Expected no error initializing cert pool, got %v", err)
	}

	pool, err := GetRootCertificatePool()
	if err != nil {
		t.Fatalf("Expected no error getting root CA bundle, got %v", err)
	}

	// assert
	// There is 4 total unique certs in the testdata/certs and we don't care about any system certs
	totalCerts := len(pool.Subjects()) - len(initialRootCertPool.Subjects())
	if totalCerts != 0 {
		t.Errorf("Expected 0 certificates, got %v", len(pool.Subjects()))
	}
}

func TestInitializeCertificatePool_OnlyReadsPemFilesOnce(t *testing.T) {
	// arrange
	defer postTestTeardown()

	// act
	err := InitializeCertificatePool("testdata/certs", discardLogger())
	if err != nil {
		t.Fatalf("Expected no error initializing cert pool, got %v", err)
	}

	pool, err := GetRootCertificatePool()
	if err != nil {
		t.Fatalf("Expected no error getting root CA bundle, got %v", err)
	}

	err = InitializeCertificatePool("testdata/certs/gen", discardLogger())
	if err != nil {
		t.Fatalf("Expected no error initializing cert pool for the second time, got %v", err)
	}
	poolUpdated, err := GetRootCertificatePool()
	if err != nil {
		t.Fatalf("Expected no error getting root CA bundle, got %v", err)
	}

	// assert
	if !pool.Equal(poolUpdated) {
		t.Errorf("Expected certificate pools to be the same")
	}
}

func TestRootCertificatePool_ReturnsPemEncodedRoot(t *testing.T) {
	// arrange
	defer postTestTeardown()

	rootCas, err := getCertificates(rootCaFilePath)
	if err != nil {
		t.Fatalf("Expected no error getting root CA certificate, got %v", err)
	}

	// act
	err = InitializeCertificatePool("testdata/certs/rootCA.cert.pem", discardLogger())
	if err != nil {
		t.Fatalf("Expected no error initializing cert pool, got %v", err)
	}
	pool, err := GetRootCertificatePool()
	if err != nil {
		t.Fatalf("Expected no error getting root CA bundle, got %v", err)
	}

	// assert
	found := false
	rootCa := rootCas[0] // Get the first certificate as the root CA
	for _, subj := range pool.Subjects() {
		if bytes.Compare(subj, rootCa.RawSubject) == 0 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected root CA certificate to be in the pool, but it was not found")
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
