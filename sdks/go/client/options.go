package client

import (
	"crypto/tls"
	"time"
)

// Options holds configuration for connection, producer, and consumer.
type Options struct {
	Address      string
	AuthToken    string
	TLSConfig    *tls.Config
	Timeout      time.Duration
	KeepAlive    time.Duration
	TCPNoDelay   bool
	MaxRetries   int
	RetryBackoff time.Duration
}

// Option configures client settings.
type Option func(*Options)

// DefaultOptions returns standard configuration defaults.
func DefaultOptions() Options {
	return Options{
		Timeout:      10 * time.Second,
		KeepAlive:    30 * time.Second,
		TCPNoDelay:   true,
		MaxRetries:   3,
		RetryBackoff: 100 * time.Millisecond,
	}
}

// WithAuthToken sets the bearer token for Command 0 authentication.
func WithAuthToken(token string) Option {
	return func(o *Options) {
		o.AuthToken = token
	}
}

// WithTLS configures TLS encryption.
func WithTLS(cfg *tls.Config) Option {
	return func(o *Options) {
		o.TLSConfig = cfg
	}
}

// WithTimeout sets connection and network I/O timeout.
func WithTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.Timeout = d
	}
}

// WithRetry configures max retry attempts and base backoff duration.
func WithRetry(maxRetries int, backoff time.Duration) Option {
	return func(o *Options) {
		o.MaxRetries = maxRetries
		o.RetryBackoff = backoff
	}
}

// WithKeepAlive sets the TCP keep-alive interval.
func WithKeepAlive(d time.Duration) Option {
	return func(o *Options) {
		o.KeepAlive = d
	}
}

// WithTCPNoDelay enables or disables TCP_NODELAY.
func WithTCPNoDelay(enabled bool) Option {
	return func(o *Options) {
		o.TCPNoDelay = enabled
	}
}
