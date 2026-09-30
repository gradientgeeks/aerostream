package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gradientgeeks/aerostream-sdk/go/client"
	"github.com/gradientgeeks/aerostream-sdk/go/protocol"
)

func main() {
	brokerAddr := flag.String("broker", "127.0.0.1:9091", "AeroStream broker address")
	authToken := flag.String("token", "default-token", "Authentication bearer token")
	topic := flag.String("topic", "events.telemetry", "Target topic name")
	partition := flag.Uint("partition", 0, "Target partition index")
	useMock := flag.Bool("mock", false, "Start in-process mock server if true")
	flag.Parse()

	addr := *brokerAddr
	if *useMock || !isReachable(addr) {
		log.Printf("Starting embedded mock AeroStream broker on localhost...")
		addr = runMockBroker(*authToken)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cli, err := client.NewClient(
		addr,
		client.WithAuthToken(*authToken),
		client.WithTimeout(5*time.Second),
		client.WithRetry(3, 100*time.Millisecond),
	)
	if err != nil {
		log.Fatalf("Failed to initialize AeroStream client: %v", err)
	}
	defer cli.Close()

	producer := cli.Producer(
		client.WithProducerMaxInFlight(128),
		client.WithProducerRetries(3, 50*time.Millisecond),
	)
	defer producer.Close()

	for i := 1; i <= 5; i++ {
		payload := []byte(fmt.Sprintf(`{"sensor_id": "sensor-alpha", "reading": %d, "timestamp": %d}`, i*10, time.Now().UnixNano()))
		offset, err := producer.Produce(ctx, *topic, uint32(*partition), payload)
		if err != nil {
			log.Fatalf("Produce error for record %d: %v", i, err)
		}
		fmt.Printf("[PRODUCER] Sent message #%d -> offset: %d\n", i, offset)
	}

	consumer := cli.Consumer(
		*topic,
		uint32(*partition),
		client.WithConsumerStartOffset(0),
		client.WithConsumerMaxWait(500*time.Millisecond),
	)
	defer consumer.Close()

	streamCtx, cancelStream := context.WithTimeout(ctx, 3*time.Second)
	defer cancelStream()

	fmt.Println("[CONSUMER] Streaming records using Messages(ctx)...")
	msgChan := consumer.Messages(streamCtx)

	count := 0
	for record := range msgChan {
		count++
		fmt.Printf("[CONSUMER] Received record [offset: %d]: %s\n", record.Offset, string(record.Payload))
		if count >= 5 {
			break
		}
	}
	fmt.Printf("[SUMMARY] Successfully produced and consumed %d records\n", count)
}

func isReachable(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func runMockBroker(token string) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("Mock broker listener failed: %v", err)
	}

	type msg struct {
		offset  uint64
		payload []byte
	}
	var messages []msg

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				authOk := token == ""
				for {
					var hdr [7]byte
					if _, err := conn.Read(hdr[:]); err != nil {
						return
					}
					cmd := hdr[2]
					bodyLen := int(hdr[3])<<24 | int(hdr[4])<<16 | int(hdr[5])<<8 | int(hdr[6])
					body := make([]byte, bodyLen)
					if _, err := conn.Read(body); err != nil {
						return
					}

					if cmd == protocol.CmdAuth {
						if token == "" || string(body) == token {
							authOk = true
							_, _ = conn.Write([]byte{protocol.Magic0, protocol.Magic1, protocol.StatusOk})
						} else {
							_, _ = conn.Write([]byte{protocol.Magic0, protocol.Magic1, protocol.StatusAuthFailed})
							return
						}
						continue
					}
					if !authOk {
						_, _ = conn.Write([]byte{protocol.Magic0, protocol.Magic1, protocol.StatusAuthFailed})
						return
					}

					switch cmd {
					case protocol.CmdProduce:
						topLen := int(body[0])<<8 | int(body[1])
						payLen := int(body[6+topLen])<<24 | int(body[7+topLen])<<16 | int(body[8+topLen])<<8 | int(body[9+topLen])
						payload := body[10+topLen : 10+topLen+payLen]
						off := uint64(len(messages))
						cpy := make([]byte, len(payload))
						copy(cpy, payload)
						messages = append(messages, msg{offset: off, payload: cpy})

						var resp [11]byte
						resp[0] = protocol.Magic0
						resp[1] = protocol.Magic1
						resp[2] = protocol.StatusOk
						for i := 0; i < 8; i++ {
							resp[3+i] = byte(off >> ((7 - i) * 8))
						}
						_, _ = conn.Write(resp[:])

					case protocol.CmdMultiFetch:
						if len(messages) == 0 {
							_, _ = conn.Write([]byte{protocol.Magic0, protocol.Magic1, protocol.StatusEmpty})
							continue
						}
						var resp []byte
						resp = append(resp, protocol.Magic0, protocol.Magic1, protocol.StatusData)
						count := uint32(len(messages))
						resp = append(resp, byte(count>>24), byte(count>>16), byte(count>>8), byte(count))
						for _, m := range messages {
							for i := 0; i < 8; i++ {
								resp = append(resp, byte(m.offset>>((7-i)*8)))
							}
							plen := uint32(len(m.payload))
							resp = append(resp, byte(plen>>24), byte(plen>>16), byte(plen>>8), byte(plen))
						}
						for _, m := range messages {
							resp = append(resp, m.payload...)
						}
						_, _ = conn.Write(resp)
					}
				}
			}(c)
		}
	}()

	return ln.Addr().String()
}
