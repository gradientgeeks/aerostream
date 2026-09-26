package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

// ServerCredentials builds gRPC server transport credentials from the TLS
// config. When CAFile is set, mTLS is enforced (clients must present a cert
// signed by that CA). Returns nil credentials when TLS is disabled.
func (t TLSConfig) ServerCredentials() (credentials.TransportCredentials, error) {
	if !t.Enabled {
		return nil, nil
	}
	if t.CertFile == "" || t.KeyFile == "" {
		return nil, fmt.Errorf("tls.enabled is true but cert_file/key_file are not set")
	}

	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server keypair: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if t.CAFile != "" {
		caPEM, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("failed to parse CA file %q", t.CAFile)
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return credentials.NewTLS(tlsCfg), nil
}
