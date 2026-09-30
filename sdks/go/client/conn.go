package client

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/gradientgeeks/aerostream-sdk/go"
	"github.com/gradientgeeks/aerostream-sdk/go/protocol"
)

// Conn manages a thread-safe TCP/TLS connection with auto-reconnect and auth.
type Conn struct {
	addr    string
	opts    Options
	mu      sync.Mutex
	netConn net.Conn
	closed  bool
}

// Dial creates a new Conn and performs connection and authentication.
func Dial(addr string, opts Options) (*Conn, error) {
	c := &Conn{
		addr: addr,
		opts: opts,
	}
	ctx := context.Background()
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}
	if err := c.connect(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// Close closes the underlying network connection and marks Conn closed.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return c.closeSocketLocked()
}

func (c *Conn) closeSocketLocked() error {
	if c.netConn != nil {
		err := c.netConn.Close()
		c.netConn = nil
		return err
	}
	return nil
}

// IsClosed reports whether the connection has been closed by the caller.
func (c *Conn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *Conn) connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectLocked(ctx)
}

func (c *Conn) connectLocked(ctx context.Context) error {
	if c.closed {
		return aerostream.ErrClosed
	}
	if c.netConn != nil {
		return nil
	}

	dialer := &net.Dialer{
		Timeout:   c.opts.Timeout,
		KeepAlive: c.opts.KeepAlive,
	}

	rawConn, err := dialer.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return errors.Join(aerostream.ErrConnectionRefused, err)
	}

	if tcpConn, ok := rawConn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(c.opts.TCPNoDelay)
	}

	var conn net.Conn = rawConn
	if c.opts.TLSConfig != nil {
		tlsConn := tls.Client(rawConn, c.opts.TLSConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = rawConn.Close()
			return err
		}
		conn = tlsConn
	}

	if c.opts.AuthToken != "" {
		if c.opts.Timeout > 0 {
			_ = conn.SetDeadline(time.Now().Add(c.opts.Timeout))
		}
		authReq := protocol.EncodeAuthRequest(nil, c.opts.AuthToken)
		if _, err := conn.Write(authReq); err != nil {
			_ = conn.Close()
			return err
		}
		if err := protocol.DecodeAuthResponse(conn); err != nil {
			_ = conn.Close()
			return err
		}
		_ = conn.SetDeadline(time.Time{})
	}

	c.netConn = conn
	return nil
}

// RoundTrip sends a request and executes response decoding under connection lock.
func (c *Conn) RoundTrip(ctx context.Context, req []byte, respFn func(r io.Reader) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var lastErr error
	backoff := c.opts.RetryBackoff

	for attempt := 0; attempt <= c.opts.MaxRetries; attempt++ {
		if c.closed {
			return aerostream.ErrClosed
		}

		if err := c.connectLocked(ctx); err != nil {
			lastErr = err
			if !c.sleepBackoff(ctx, backoff) {
				return ctx.Err()
			}
			backoff *= 2
			continue
		}

		var deadline time.Time
		if d, ok := ctx.Deadline(); ok {
			deadline = d
		} else if c.opts.Timeout > 0 {
			deadline = time.Now().Add(c.opts.Timeout)
		}
		if !deadline.IsZero() {
			_ = c.netConn.SetDeadline(deadline)
		}

		if _, err := c.netConn.Write(req); err != nil {
			_ = c.closeSocketLocked()
			lastErr = err
			if !c.sleepBackoff(ctx, backoff) {
				return ctx.Err()
			}
			backoff *= 2
			continue
		}

		if err := respFn(c.netConn); err != nil {
			if errors.Is(err, aerostream.ErrOutOfOrderSequence) ||
				errors.Is(err, aerostream.ErrAuthFailed) ||
				errors.Is(err, aerostream.ErrEmpty) ||
				errors.Is(err, aerostream.ErrInvalidMagic) {
				return err
			}
			_ = c.closeSocketLocked()
			lastErr = err
			if !c.sleepBackoff(ctx, backoff) {
				return ctx.Err()
			}
			backoff *= 2
			continue
		}

		return nil
	}

	if lastErr != nil {
		return lastErr
	}
	return aerostream.ErrTimeout
}

func (c *Conn) sleepBackoff(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
