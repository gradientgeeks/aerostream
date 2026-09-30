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

// Consumer reads records from AeroStream topics and partitions.
type Consumer interface {
	Fetch(ctx context.Context, topic string, partition uint32, startOffset int64, maxBytes uint32, maxWait time.Duration) ([]aerostream.Record, error)
	Messages(ctx context.Context) <-chan aerostream.Record
	Close() error
}

// ConsumerConfig contains parameters for consumption and streaming.
type ConsumerConfig struct {
	Topic       string
	Partition   uint32
	StartOffset int64
	MaxBytes    uint32
	MaxWait     time.Duration
	BufferSize  int
}

// ConsumerOption configures Consumer settings.
type ConsumerOption func(*ConsumerConfig)

// WithConsumerStartOffset sets the initial offset to begin reading from.
func WithConsumerStartOffset(offset int64) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.StartOffset = offset
	}
}

// WithConsumerMaxBytes sets the maximum batch payload size in bytes.
func WithConsumerMaxBytes(maxBytes uint32) ConsumerOption {
	return func(c *ConsumerConfig) {
		if maxBytes > 0 {
			c.MaxBytes = maxBytes
		}
	}
}

// WithConsumerMaxWait sets the long-polling timeout duration.
func WithConsumerMaxWait(maxWait time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		if maxWait > 0 {
			c.MaxWait = maxWait
		}
	}
}

// WithConsumerBufferSize sets the buffer capacity of the Messages channel.
func WithConsumerBufferSize(size int) ConsumerOption {
	return func(c *ConsumerConfig) {
		if size > 0 {
			c.BufferSize = size
		}
	}
}

type consumerImpl struct {
	conn   *Conn
	cfg    ConsumerConfig
	mu     sync.Mutex
	closed bool
}

// NewConsumer creates a Consumer subscribed to a specific topic and partition.
func NewConsumer(conn *Conn, topic string, partition uint32, opts ...ConsumerOption) Consumer {
	cfg := ConsumerConfig{
		Topic:       topic,
		Partition:   partition,
		StartOffset: 0,
		MaxBytes:    1048576,
		MaxWait:     500 * time.Millisecond,
		BufferSize:  256,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	return &consumerImpl{
		conn: conn,
		cfg:  cfg,
	}
}

func (c *consumerImpl) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

// Fetch executes a Command 4 long-polling request against the broker.
func (c *consumerImpl) Fetch(ctx context.Context, topic string, partition uint32, startOffset int64, maxBytes uint32, maxWait time.Duration) ([]aerostream.Record, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, aerostream.ErrClosed
	}
	c.mu.Unlock()

	maxWaitMs := uint32(maxWait.Milliseconds())
	req, err := protocol.EncodeMultiFetchRequest(nil, topic, partition, uint64(startOffset), maxBytes, maxWaitMs)
	if err != nil {
		return nil, err
	}

	var entries []protocol.Entry
	err = c.conn.RoundTrip(ctx, req, func(r io.Reader) error {
		res, decErr := protocol.DecodeMultiFetchResponse(r)
		if decErr != nil {
			return decErr
		}
		entries = res
		return nil
	})

	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, nil
	}

	now := time.Now().UTC()
	records := make([]aerostream.Record, len(entries))
	for i, e := range entries {
		records[i] = aerostream.Record{
			Topic:     topic,
			Partition: partition,
			Offset:    int64(e.Offset),
			Payload:   e.Payload,
			Timestamp: now,
		}
	}
	return records, nil
}

// Messages returns a streaming channel iterator consuming continuously.
func (c *consumerImpl) Messages(ctx context.Context) <-chan aerostream.Record {
	out := make(chan aerostream.Record, c.cfg.BufferSize)

	go func() {
		defer close(out)
		currOffset := c.cfg.StartOffset

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			c.mu.Lock()
			if c.closed {
				c.mu.Unlock()
				return
			}
			c.mu.Unlock()

			records, err := c.Fetch(ctx, c.cfg.Topic, c.cfg.Partition, currOffset, c.cfg.MaxBytes, c.cfg.MaxWait)
			if err != nil {
				if errors.Is(err, aerostream.ErrClosed) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
					continue
				}
			}

			if len(records) == 0 {
				continue
			}

			for _, rec := range records {
				select {
				case <-ctx.Done():
					return
				case out <- rec:
					currOffset = rec.Offset + 1
				}
			}
		}
	}()

	return out
}
