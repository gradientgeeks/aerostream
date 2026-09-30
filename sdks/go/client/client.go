package client

import (
	"context"
	"time"

	"github.com/gradientgeeks/aerostream-sdk/go"
)

// Client provides access to AeroStream messaging operations.
type Client interface {
	Producer(opts ...ProducerOption) Producer
	Consumer(topic string, partition uint32, opts ...ConsumerOption) Consumer
	Produce(ctx context.Context, topic string, partition uint32, payload []byte) (int64, error)
	Fetch(ctx context.Context, topic string, partition uint32, startOffset int64, maxBytes uint32, maxWait time.Duration) ([]aerostream.Record, error)
	Close() error
}

type clientImpl struct {
	conn            *Conn
	defaultProducer Producer
}

// NewClient establishes an authenticated connection to the AeroStream broker.
func NewClient(addr string, opts ...Option) (Client, error) {
	cfg := DefaultOptions()
	for _, opt := range opts {
		opt(&cfg)
	}

	conn, err := Dial(addr, cfg)
	if err != nil {
		return nil, err
	}

	return &clientImpl{
		conn:            conn,
		defaultProducer: NewProducer(conn),
	}, nil
}

// Producer returns a configured Producer instance.
func (c *clientImpl) Producer(opts ...ProducerOption) Producer {
	if len(opts) == 0 {
		return c.defaultProducer
	}
	return NewProducer(c.conn, opts...)
}

// Consumer creates a Consumer instance for a specific topic and partition.
func (c *clientImpl) Consumer(topic string, partition uint32, opts ...ConsumerOption) Consumer {
	return NewConsumer(c.conn, topic, partition, opts...)
}

// Produce sends a payload synchronously using the default producer.
func (c *clientImpl) Produce(ctx context.Context, topic string, partition uint32, payload []byte) (int64, error) {
	return c.defaultProducer.Produce(ctx, topic, partition, payload)
}

// Fetch reads records from the broker using the provided offsets and limits.
func (c *clientImpl) Fetch(ctx context.Context, topic string, partition uint32, startOffset int64, maxBytes uint32, maxWait time.Duration) ([]aerostream.Record, error) {
	cons := NewConsumer(c.conn, topic, partition)
	return cons.Fetch(ctx, topic, partition, startOffset, maxBytes, maxWait)
}

// Close terminates default producers and the underlying connection.
func (c *clientImpl) Close() error {
	_ = c.defaultProducer.Close()
	return c.conn.Close()
}
