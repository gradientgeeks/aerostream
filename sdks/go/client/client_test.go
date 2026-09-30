package client_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gradientgeeks/aerostream-sdk/go"
	"github.com/gradientgeeks/aerostream-sdk/go/client"
	"github.com/gradientgeeks/aerostream-sdk/go/protocol"
)

type storedMessage struct {
	offset  int64
	payload []byte
}

type mockBroker struct {
	listener net.Listener
	mu       sync.Mutex
	messages map[string]map[uint32][]storedMessage
	token    string
	conns    []net.Conn
	closed   bool
}

func startMockBroker(t *testing.T, token string) *mockBroker {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock listener: %v", err)
	}

	b := &mockBroker{
		listener: ln,
		messages: make(map[string]map[uint32][]storedMessage),
		token:    token,
	}

	go b.acceptLoop()
	return b
}

func (b *mockBroker) addr() string {
	return b.listener.Addr().String()
}

func (b *mockBroker) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	_ = b.listener.Close()
	for _, c := range b.conns {
		_ = c.Close()
	}
	b.conns = nil
}

func (b *mockBroker) acceptLoop() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			return
		}
		b.mu.Lock()
		b.conns = append(b.conns, conn)
		b.mu.Unlock()
		go b.handleConn(conn)
	}
}

func (b *mockBroker) handleConn(conn net.Conn) {
	defer conn.Close()
	authenticated := b.token == ""

	for {
		var hdr [7]byte
		if _, err := io.ReadFull(conn, hdr[:]); err != nil {
			return
		}
		if !protocol.IsValidMagic(hdr[0], hdr[1]) {
			return
		}
		cmd := hdr[2]
		bodyLen := binary.BigEndian.Uint32(hdr[3:7])

		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}

		if cmd == protocol.CmdAuth {
			tokenStr := string(body)
			if b.token == "" || tokenStr == b.token {
				authenticated = true
				_, _ = conn.Write([]byte{protocol.Magic0, protocol.Magic1, protocol.StatusOk})
			} else {
				_, _ = conn.Write([]byte{protocol.Magic0, protocol.Magic1, protocol.StatusAuthFailed})
				return
			}
			continue
		}

		if !authenticated {
			_, _ = conn.Write([]byte{protocol.Magic0, protocol.Magic1, protocol.StatusAuthFailed})
			return
		}

		switch cmd {
		case protocol.CmdProduce:
			if len(body) < 10 {
				return
			}
			topLen := binary.BigEndian.Uint16(body[0:2])
			topic := string(body[2 : 2+topLen])
			part := binary.BigEndian.Uint32(body[2+topLen : 6+topLen])
			payLen := binary.BigEndian.Uint32(body[6+topLen : 10+topLen])
			payload := body[10+topLen : 10+int(topLen)+int(payLen)]

			// Check for special test payload simulating OutOfOrder
			if string(payload) == "TRIGGER_OUT_OF_ORDER" {
				_, _ = conn.Write([]byte{protocol.Magic0, protocol.Magic1, protocol.StatusOutOfOrderSequence})
				continue
			}

			b.mu.Lock()
			if _, ok := b.messages[topic]; !ok {
				b.messages[topic] = make(map[uint32][]storedMessage)
			}
			list := b.messages[topic][part]
			offset := int64(len(list))
			msgCopy := make([]byte, len(payload))
			copy(msgCopy, payload)
			b.messages[topic][part] = append(list, storedMessage{offset: offset, payload: msgCopy})
			b.mu.Unlock()

			var resp [11]byte
			resp[0] = protocol.Magic0
			resp[1] = protocol.Magic1
			resp[2] = protocol.StatusOk
			binary.BigEndian.PutUint64(resp[3:11], uint64(offset))
			_, _ = conn.Write(resp[:])

		case protocol.CmdFetch:
			if len(body) < 18 {
				return
			}
			topLen := binary.BigEndian.Uint16(body[0:2])
			topic := string(body[2 : 2+topLen])
			part := binary.BigEndian.Uint32(body[2+topLen : 6+topLen])
			startOff := binary.BigEndian.Uint64(body[6+topLen : 14+topLen])

			b.mu.Lock()
			var msgData []byte
			if partMap, ok := b.messages[topic]; ok {
				if list, ok := partMap[part]; ok {
					for _, m := range list {
						if m.offset == int64(startOff) {
							msgData = m.payload
							break
						}
					}
				}
			}
			b.mu.Unlock()

			if len(msgData) == 0 {
				_, _ = conn.Write([]byte{protocol.Magic0, protocol.Magic1, protocol.StatusEmpty})
			} else {
				var resp []byte
				resp = append(resp, protocol.Magic0, protocol.Magic1, protocol.StatusData)
				resp = binary.BigEndian.AppendUint32(resp, uint32(len(msgData)))
				resp = append(resp, msgData...)
				_, _ = conn.Write(resp)
			}

		case protocol.CmdMultiFetch:
			if len(body) < 22 {
				return
			}
			topLen := binary.BigEndian.Uint16(body[0:2])
			topic := string(body[2 : 2+topLen])
			part := binary.BigEndian.Uint32(body[2+topLen : 6+topLen])
			startOff := binary.BigEndian.Uint64(body[6+topLen : 14+topLen])

			b.mu.Lock()
			var available []storedMessage
			if partMap, ok := b.messages[topic]; ok {
				if list, ok := partMap[part]; ok {
					for _, m := range list {
						if m.offset >= int64(startOff) {
							available = append(available, m)
						}
					}
				}
			}
			b.mu.Unlock()

			if len(available) == 0 {
				_, _ = conn.Write([]byte{protocol.Magic0, protocol.Magic1, protocol.StatusEmpty})
				continue
			}

			var resp []byte
			resp = append(resp, protocol.Magic0, protocol.Magic1, protocol.StatusData)
			resp = binary.BigEndian.AppendUint32(resp, uint32(len(available)))
			for _, m := range available {
				resp = binary.BigEndian.AppendUint64(resp, uint64(m.offset))
				resp = binary.BigEndian.AppendUint32(resp, uint32(len(m.payload)))
			}
			for _, m := range available {
				resp = append(resp, m.payload...)
			}
			_, _ = conn.Write(resp)

		default:
			return
		}
	}
}

func TestClientAuthAndProduce(t *testing.T) {
	broker := startMockBroker(t, "test-secret-token")
	defer broker.close()

	// Connect with invalid token
	_, err := client.NewClient(broker.addr(), client.WithAuthToken("wrong-token"), client.WithTimeout(1*time.Second))
	if !errors.Is(err, aerostream.ErrAuthFailed) {
		t.Fatalf("expected ErrAuthFailed, got: %v", err)
	}

	// Connect with valid token
	cli, err := client.NewClient(broker.addr(), client.WithAuthToken("test-secret-token"))
	if err != nil {
		t.Fatalf("failed to connect with valid token: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Produce 3 records
	for i := 0; i < 3; i++ {
		off, err := cli.Produce(ctx, "telemetry", 0, []byte("metric-payload"))
		if err != nil {
			t.Fatalf("failed produce %d: %v", i, err)
		}
		if off != int64(i) {
			t.Fatalf("expected offset %d, got %d", i, off)
		}
	}
}

func TestOutOfOrderSequenceError(t *testing.T) {
	broker := startMockBroker(t, "")
	defer broker.close()

	cli, err := client.NewClient(broker.addr())
	if err != nil {
		t.Fatalf("new client err: %v", err)
	}
	defer cli.Close()

	_, err = cli.Produce(context.Background(), "test", 0, []byte("TRIGGER_OUT_OF_ORDER"))
	if !errors.Is(err, aerostream.ErrOutOfOrderSequence) {
		t.Fatalf("expected ErrOutOfOrderSequence, got: %v", err)
	}
}

func TestConsumerFetchAndMessagesStream(t *testing.T) {
	broker := startMockBroker(t, "")
	defer broker.close()

	cli, err := client.NewClient(broker.addr())
	if err != nil {
		t.Fatalf("new client err: %v", err)
	}
	defer cli.Close()

	topic := "events"
	partition := uint32(0)

	// Produce 5 messages
	for i := 0; i < 5; i++ {
		payload := []byte("event-body")
		_, err := cli.Produce(context.Background(), topic, partition, payload)
		if err != nil {
			t.Fatalf("produce err: %v", err)
		}
	}

	// Fetch via Consumer
	consumer := cli.Consumer(topic, partition, client.WithConsumerStartOffset(0))
	defer consumer.Close()

	records, err := consumer.Fetch(context.Background(), topic, partition, 0, 65536, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("fetch err: %v", err)
	}
	if len(records) != 5 {
		t.Fatalf("expected 5 records, got %d", len(records))
	}

	// Stream via Messages()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgChan := consumer.Messages(ctx)
	var count int
	for rec := range msgChan {
		if rec.Offset != int64(count) {
			t.Fatalf("offset mismatch in stream: expected %d, got %d", count, rec.Offset)
		}
		if !bytes.Equal(rec.Payload, []byte("event-body")) {
			t.Fatalf("payload mismatch in stream")
		}
		count++
		if count == 5 {
			cancel()
			break
		}
	}

	if count != 5 {
		t.Fatalf("expected to stream 5 messages, got %d", count)
	}
}

func TestProducerBatchAndConcurrency(t *testing.T) {
	broker := startMockBroker(t, "")
	defer broker.close()

	cli, err := client.NewClient(broker.addr())
	if err != nil {
		t.Fatalf("client error: %v", err)
	}
	defer cli.Close()

	producer := cli.Producer(client.WithProducerMaxInFlight(64))

	// Batch produce
	payloads := [][]byte{
		[]byte("b1"),
		[]byte("b2"),
		[]byte("b3"),
	}
	offsets, err := producer.ProduceBatch(context.Background(), "batch-top", 0, payloads)
	if err != nil {
		t.Fatalf("produce batch error: %v", err)
	}
	if len(offsets) != 3 {
		t.Fatalf("expected 3 offsets, got %d", len(offsets))
	}

	// Concurrent produce
	const concurrency = 20
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := producer.Produce(context.Background(), "concurrent-top", 0, []byte("data"))
			if err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent produce failed: %v", err)
		}
	}
}

func TestAutoReconnect(t *testing.T) {
	broker := startMockBroker(t, "reconnect-token")

	cli, err := client.NewClient(broker.addr(),
		client.WithAuthToken("reconnect-token"),
		client.WithRetry(3, 50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer cli.Close()

	// Initial produce
	_, err = cli.Produce(context.Background(), "top", 0, []byte("p1"))
	if err != nil {
		t.Fatalf("produce 1 failed: %v", err)
	}

	// Close broker and restart on same port to verify reconnect
	addr := broker.addr()
	broker.close()

	time.Sleep(50 * time.Millisecond)

	// Restart broker at same address
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed to re-bind listener: %v", err)
	}
	newBroker := &mockBroker{
		listener: ln,
		messages: make(map[string]map[uint32][]storedMessage),
		token:    "reconnect-token",
	}
	go newBroker.acceptLoop()
	defer newBroker.close()

	// Client should auto-reconnect and re-authenticate seamlessly
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	off, err := cli.Produce(ctx, "top", 0, []byte("p2"))
	if err != nil {
		t.Fatalf("reconnected produce failed: %v", err)
	}
	if off != 0 {
		t.Fatalf("expected offset 0 on fresh broker, got %d", off)
	}
}
