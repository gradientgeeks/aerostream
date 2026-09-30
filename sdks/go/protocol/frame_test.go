package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/gradientgeeks/aerostream-sdk/go"
)

func TestMagicValidation(t *testing.T) {
	if !IsValidMagic(Magic0, Magic1) {
		t.Fatal("expected valid magic")
	}
	if IsValidMagic(0x00, 0x01) || IsValidMagic(0xAE, 0x02) {
		t.Fatal("expected invalid magic")
	}
}

func TestEncodeHeader(t *testing.T) {
	buf := EncodeHeader(nil, CmdProduce, 42)
	if len(buf) != HeaderLen {
		t.Fatalf("expected header length %d, got %d", HeaderLen, len(buf))
	}
	if buf[0] != Magic0 || buf[1] != Magic1 {
		t.Fatalf("magic mismatch: %x %x", buf[0], buf[1])
	}
	if buf[2] != CmdProduce {
		t.Fatalf("cmd mismatch: %d", buf[2])
	}
	bodyLen := binary.BigEndian.Uint32(buf[3:7])
	if bodyLen != 42 {
		t.Fatalf("body length mismatch: %d", bodyLen)
	}
}

func TestEncodeAuthRequest(t *testing.T) {
	token := "secret-bearer-token"
	buf := EncodeAuthRequest(nil, token)

	if len(buf) != HeaderLen+len(token) {
		t.Fatalf("unexpected len: %d", len(buf))
	}
	if buf[2] != CmdAuth {
		t.Fatalf("cmd mismatch: %d", buf[2])
	}
	bodyLen := binary.BigEndian.Uint32(buf[3:7])
	if int(bodyLen) != len(token) {
		t.Fatalf("body length mismatch: %d", bodyLen)
	}
	if string(buf[HeaderLen:]) != token {
		t.Fatalf("token mismatch: %s", string(buf[HeaderLen:]))
	}
}

func TestDecodeAuthResponse(t *testing.T) {
	// Success case
	okResp := []byte{Magic0, Magic1, StatusOk}
	if err := DecodeAuthResponse(bytes.NewReader(okResp)); err != nil {
		t.Fatalf("unexpected auth err: %v", err)
	}

	// Auth failed case
	failResp := []byte{Magic0, Magic1, StatusAuthFailed}
	if err := DecodeAuthResponse(bytes.NewReader(failResp)); !errors.Is(err, aerostream.ErrAuthFailed) {
		t.Fatalf("expected ErrAuthFailed, got: %v", err)
	}

	// Bad magic
	badMagic := []byte{0x00, 0x01, StatusOk}
	if err := DecodeAuthResponse(bytes.NewReader(badMagic)); !errors.Is(err, aerostream.ErrInvalidMagic) {
		t.Fatalf("expected ErrInvalidMagic, got: %v", err)
	}
}

func TestProduceRequestAndResponse(t *testing.T) {
	topic := "events"
	partition := uint32(2)
	payload := []byte("hello aerostream world")

	reqBytes, err := EncodeProduceRequest(nil, topic, partition, payload)
	if err != nil {
		t.Fatalf("unexpected error encoding produce: %v", err)
	}

	if reqBytes[2] != CmdProduce {
		t.Fatalf("cmd mismatch: %d", reqBytes[2])
	}
	body := reqBytes[HeaderLen:]
	topLen := binary.BigEndian.Uint16(body[0:2])
	if int(topLen) != len(topic) {
		t.Fatalf("topic len mismatch: %d", topLen)
	}
	parsedTopic := string(body[2 : 2+topLen])
	if parsedTopic != topic {
		t.Fatalf("topic mismatch: %s", parsedTopic)
	}
	parsedPart := binary.BigEndian.Uint32(body[2+topLen : 6+topLen])
	if parsedPart != partition {
		t.Fatalf("partition mismatch: %d", parsedPart)
	}
	payLen := binary.BigEndian.Uint32(body[6+topLen : 10+topLen])
	if int(payLen) != len(payload) {
		t.Fatalf("payload len mismatch: %d", payLen)
	}
	parsedPayload := body[10+int(topLen) : 10+int(topLen)+int(payLen)]
	if !bytes.Equal(parsedPayload, payload) {
		t.Fatalf("payload content mismatch")
	}

	// Success response: [AE, 01, 00, offset(8)]
	var respOk []byte
	respOk = append(respOk, Magic0, Magic1, StatusOk)
	respOk = binary.BigEndian.AppendUint64(respOk, 123456789)
	off, err := DecodeProduceResponse(bytes.NewReader(respOk))
	if err != nil || off != 123456789 {
		t.Fatalf("expected offset 123456789, got %d, err: %v", off, err)
	}

	// OutOfOrder response: [AE, 01, 45]
	respOOO := []byte{Magic0, Magic1, StatusOutOfOrderSequence}
	_, err = DecodeProduceResponse(bytes.NewReader(respOOO))
	if !errors.Is(err, aerostream.ErrOutOfOrderSequence) {
		t.Fatalf("expected ErrOutOfOrderSequence, got: %v", err)
	}
}

func TestFetchRequestAndResponse(t *testing.T) {
	topic := "sensors"
	partition := uint32(0)
	startOffset := uint64(500)
	maxBytes := uint32(65536)

	reqBytes, err := EncodeFetchRequest(nil, topic, partition, startOffset, maxBytes)
	if err != nil {
		t.Fatalf("encode fetch err: %v", err)
	}
	if reqBytes[2] != CmdFetch {
		t.Fatalf("cmd mismatch: %d", reqBytes[2])
	}

	// Response Data: [AE, 01, 02, bytes_to_read(4), data...]
	sampleData := []byte("stream-data-chunk")
	var respData []byte
	respData = append(respData, Magic0, Magic1, StatusData)
	respData = binary.BigEndian.AppendUint32(respData, uint32(len(sampleData)))
	respData = append(respData, sampleData...)

	data, err := DecodeFetchResponse(bytes.NewReader(respData))
	if err != nil || !bytes.Equal(data, sampleData) {
		t.Fatalf("unexpected fetch data response: %v, got %s", err, string(data))
	}

	// Response Empty: [AE, 01, 01]
	respEmpty := []byte{Magic0, Magic1, StatusEmpty}
	_, err = DecodeFetchResponse(bytes.NewReader(respEmpty))
	if !errors.Is(err, aerostream.ErrEmpty) {
		t.Fatalf("expected ErrEmpty, got %v", err)
	}
}

func TestMultiFetchRequestAndResponse(t *testing.T) {
	topic := "telemetry"
	partition := uint32(1)
	startOffset := uint64(10)
	maxBytes := uint32(1048576)
	maxWaitMs := uint32(500)

	reqBytes, err := EncodeMultiFetchRequest(nil, topic, partition, startOffset, maxBytes, maxWaitMs)
	if err != nil {
		t.Fatalf("encode multi fetch err: %v", err)
	}
	if reqBytes[2] != CmdMultiFetch {
		t.Fatalf("cmd mismatch: %d", reqBytes[2])
	}

	// Multi response with 2 entries
	p1 := []byte("payload-one")
	p2 := []byte("payload-two-longer")
	var resp []byte
	resp = append(resp, Magic0, Magic1, StatusData)
	resp = binary.BigEndian.AppendUint32(resp, 2) // 2 entries
	// entry 1: offset 10, len len(p1)
	resp = binary.BigEndian.AppendUint64(resp, 10)
	resp = binary.BigEndian.AppendUint32(resp, uint32(len(p1)))
	// entry 2: offset 11, len len(p2)
	resp = binary.BigEndian.AppendUint64(resp, 11)
	resp = binary.BigEndian.AppendUint32(resp, uint32(len(p2)))
	// concatenated data
	resp = append(resp, p1...)
	resp = append(resp, p2...)

	entries, err := DecodeMultiFetchResponse(bytes.NewReader(resp))
	if err != nil {
		t.Fatalf("decode multi fetch err: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Offset != 10 || !bytes.Equal(entries[0].Payload, p1) {
		t.Fatalf("entry 0 mismatch: off=%d, payload=%s", entries[0].Offset, string(entries[0].Payload))
	}
	if entries[1].Offset != 11 || !bytes.Equal(entries[1].Payload, p2) {
		t.Fatalf("entry 1 mismatch: off=%d, payload=%s", entries[1].Offset, string(entries[1].Payload))
	}

	// Multi response Empty
	emptyResp := []byte{Magic0, Magic1, StatusEmpty}
	emptyEntries, err := DecodeMultiFetchResponse(bytes.NewReader(emptyResp))
	if err != nil || emptyEntries != nil {
		t.Fatalf("expected nil entries and nil err for empty response, got: %v, %v", emptyEntries, err)
	}
}
