package certificate

import (
	"errors"
	"fmt"
)

func ErrUnexpectedServerCertificate(expectedThumbprint string, serverThumbprint string) error {
	return errors.New(
		fmt.Sprintf(
			"unable to establish a secure connection to the server, as the server's certificate thumbprint '%s' does not match the expected thumbprint '%s'",
			serverThumbprint,
			expectedThumbprint,
		),
	)
}
