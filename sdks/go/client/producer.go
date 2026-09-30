package client

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/gradientgeeks/aerostream-sdk/go"
	"github.com/gradientgeeks/aerostream-sdk/go/protocol"
)

// Producer publishes records to AeroStream topics and partitions.
type Producer interface {
	Produce(ctx context.Context, topic string, partition uint32, payload []byte) (int64, error)
	ProduceBatch(ctx context.Context, topic string, partition uint32, payloads [][]byte) ([]int64, error)
	Close() error
}

// ProducerConfig contains tuning parameters for a Producer.
type ProducerConfig struct {
	MaxInFlight  int
	MaxRetries   int
	RetryBackoff time.Duration
	BatchSize    int
	BatchLinger  time.Duration
}

// ProducerOption configures Producer settings.
type ProducerOption func(*ProducerConfig)

// WithProducerMaxInFlight sets maximum concurrent in-flight requests.
func WithProducerMaxInFlight(limit int) ProducerOption {
	return func(c *ProducerConfig) {
		if limit > 0 {
			c.MaxInFlight = limit
		}
	}
}

// WithProducerRetries configures retry count and base backoff.
func WithProducerRetries(retries int, backoff time.Duration) ProducerOption {
	return func(c *ProducerConfig) {
		c.MaxRetries = retries
		c.RetryBackoff = backoff
	}
}

// WithProducerBatchSize configures batch capacity for bulk produces.
func WithProducerBatchSize(size int) ProducerOption {
	return func(c *ProducerConfig) {
		if size > 0 {
			c.BatchSize = size
		}
	}
}

// WithProducerLinger configures batch linger delay.
func WithProducerLinger(linger time.Duration) ProducerOption {
	return func(c *ProducerConfig) {
		c.BatchLinger = linger
	}
}

type producerImpl struct {
	conn   *Conn
	cfg    ProducerConfig
	sem    chan struct{}
	closed bool
	mu     sync.Mutex
}

// NewProducer instantiates a Producer with the provided options.
func NewProducer(conn *Conn, opts ...ProducerOption) Producer {
	cfg := ProducerConfig{
		MaxInFlight:  256,
		MaxRetries:   3,
		RetryBackoff: 50 * time.Millisecond,
		BatchSize:    100,
		BatchLinger:  5 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	return &producerImpl{
		conn: conn,
		cfg:  cfg,
		sem:  make(chan struct{}, cfg.MaxInFlight),
	}
}

func (p *producerImpl) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

// Produce sends a single payload with backpressure and retry handling.
func (p *producerImpl) Produce(ctx context.Context, topic string, partition uint32, payload []byte) (int64, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return -1, aerostream.ErrClosed
	}
	p.mu.Unlock()

	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-ctx.Done():
		return -1, ctx.Err()
	}

	req, err := protocol.EncodeProduceRequest(nil, topic, partition, payload)
	if err != nil {
		return -1, err
	}

	var offset uint64
	var lastErr error
	backoff := p.cfg.RetryBackoff

	for attempt := 0; attempt <= p.cfg.MaxRetries; attempt++ {
		err := p.conn.RoundTrip(ctx, req, func(r io.Reader) error {
			off, decErr := protocol.DecodeProduceResponse(r)
			if decErr != nil {
				return decErr
			}
			offset = off
			return nil
		})

		if err == nil {
			return int64(offset), nil
		}

		if errors.Is(err, aerostream.ErrOutOfOrderSequence) ||
			errors.Is(err, aerostream.ErrAuthFailed) ||
			errors.Is(err, aerostream.ErrClosed) {
			return -1, err
		}

		lastErr = err
		if attempt < p.cfg.MaxRetries {
			select {
			case <-ctx.Done():
				return -1, ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
			}
		}
	}

	return -1, lastErr
}

// ProduceBatch sequentially appends a collection of payloads.
func (p *producerImpl) ProduceBatch(ctx context.Context, topic string, partition uint32, payloads [][]byte) ([]int64, error) {
	offsets := make([]int64, len(payloads))
	for i, payload := range payloads {
		off, err := p.Produce(ctx, topic, partition, payload)
		if err != nil {
			return offsets[:i], err
		}
		offsets[i] = off
	}
	return offsets, nil
}
