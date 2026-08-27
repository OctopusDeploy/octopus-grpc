package certificate

import (
	"crypto/sha1"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"go.uber.org/multierr"
)

var (
	rootCertificatePool *x509.CertPool
	certPoolMutex       sync.RWMutex
)

func GetRootCertificatePool() (*x509.CertPool, error) {
	certPoolMutex.RLock()
	defer certPoolMutex.RUnlock()

	if rootCertificatePool == nil {
		return x509.SystemCertPool()
	}

	return rootCertificatePool.Clone(), nil
}

func InitializeCertificatePool(additionalRootBundlePath string, logger *slog.Logger) error {
	certPoolMutex.Lock()
	defer certPoolMutex.Unlock()

	// Already initialized
	if rootCertificatePool != nil {
		return nil
	}

	rootCertificates, err := x509.SystemCertPool()
	if err != nil {
		rootCertificates = x509.NewCertPool()
	}
	rootCertificatePool = rootCertificates

	var errors []error

	absPath, err := filepath.Abs(additionalRootBundlePath)
	if err != nil {
		return err
	}

	certificateCount := 0
	err = filepath.Walk(absPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if strings.ToLower(filepath.Ext(path)) != ".pem" {
			return nil
		}

		certData, readErr := os.ReadFile(path)
		if readErr != nil {
			errors = append(errors, readErr)
			return nil
		}

		if !rootCertificatePool.AppendCertsFromPEM(certData) {
			errors = append(errors, fmt.Errorf("failed to parse PEM certificate(s) in file: %s", path))
		} else {
			thumbprint := sha1.Sum(certData)
			thumbprintStr := strings.ToUpper(hex.EncodeToString(thumbprint[:]))
			logger.Info("Certificate added to pool", "thumbprint", thumbprintStr, "file", path)
			certificateCount++
		}

		return nil
	})
	if err != nil {
		return err
	}

	if len(errors) > 0 {
		return multierr.Combine(errors...)
	}

	logger.Info("Successfully initialized root certificate pool", "certificateCount", certificateCount)
	return nil
}
