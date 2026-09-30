package protocol

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/gradientgeeks/aerostream-sdk/go"
)

const (
	Magic0 byte = 0xAE
	Magic1 byte = 0x01

	HeaderLen = 7

	CmdAuth         byte = 0
	CmdProduce      byte = 1
	CmdFetch        byte = 2
	CmdReplicaFetch byte = 3
	CmdMultiFetch   byte = 4

	StatusOk                 byte = 0
	StatusEmpty              byte = 1
	StatusData               byte = 2
	StatusAuthFailed         byte = 3
	StatusOutOfOrderSequence byte = 45
)

// Entry holds an offset and raw payload extracted from a multi-fetch response.
type Entry struct {
	Offset  uint64
	Payload []byte
}

// IsValidMagic checks whether the 2-byte prefix matches the AeroStream magic number.
func IsValidMagic(b0, b1 byte) bool {
	return b0 == Magic0 && b1 == Magic1
}

// EncodeHeader writes a 7-byte AeroStream frame header into dst.
func EncodeHeader(dst []byte, cmd byte, bodyLen uint32) []byte {
	return append(dst,
		Magic0,
		Magic1,
		cmd,
		byte(bodyLen>>24),
		byte(bodyLen>>16),
		byte(bodyLen>>8),
		byte(bodyLen),
	)
}

// EncodeAuthRequest encodes Command 0 (AUTH) frame into dst.
func EncodeAuthRequest(dst []byte, token string) []byte {
	tokenBytes := []byte(token)
	dst = EncodeHeader(dst, CmdAuth, uint32(len(tokenBytes)))
	return append(dst, tokenBytes...)
}

// EncodeProduceRequest encodes Command 1 (PRODUCE) frame into dst.
func EncodeProduceRequest(dst []byte, topic string, partition uint32, payload []byte) ([]byte, error) {
	topicBytes := []byte(topic)
	if len(topicBytes) > 65535 {
		return nil, aerostream.ErrTopicTooLong
	}
	bodyLen := 2 + len(topicBytes) + 4 + 4 + len(payload)
	dst = EncodeHeader(dst, CmdProduce, uint32(bodyLen))

	dst = binary.BigEndian.AppendUint16(dst, uint16(len(topicBytes)))
	dst = append(dst, topicBytes...)
	dst = binary.BigEndian.AppendUint32(dst, partition)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(payload)))
	dst = append(dst, payload...)
	return dst, nil
}

// EncodeFetchRequest encodes Command 2 (CONSUMER FETCH) frame into dst.
func EncodeFetchRequest(dst []byte, topic string, partition uint32, startOffset uint64, maxBytes uint32) ([]byte, error) {
	topicBytes := []byte(topic)
	if len(topicBytes) > 65535 {
		return nil, aerostream.ErrTopicTooLong
	}
	bodyLen := 2 + len(topicBytes) + 4 + 8 + 4
	dst = EncodeHeader(dst, CmdFetch, uint32(bodyLen))

	dst = binary.BigEndian.AppendUint16(dst, uint16(len(topicBytes)))
	dst = append(dst, topicBytes...)
	dst = binary.BigEndian.AppendUint32(dst, partition)
	dst = binary.BigEndian.AppendUint64(dst, startOffset)
	dst = binary.BigEndian.AppendUint32(dst, maxBytes)
	return dst, nil
}

// EncodeMultiFetchRequest encodes Command 4 (MULTI-ENTRY LONG-POLL FETCH) frame into dst.
func EncodeMultiFetchRequest(dst []byte, topic string, partition uint32, startOffset uint64, maxBytes uint32, maxWaitMs uint32) ([]byte, error) {
	topicBytes := []byte(topic)
	if len(topicBytes) > 65535 {
		return nil, aerostream.ErrTopicTooLong
	}
	bodyLen := 2 + len(topicBytes) + 4 + 8 + 4 + 4
	dst = EncodeHeader(dst, CmdMultiFetch, uint32(bodyLen))

	dst = binary.BigEndian.AppendUint16(dst, uint16(len(topicBytes)))
	dst = append(dst, topicBytes...)
	dst = binary.BigEndian.AppendUint32(dst, partition)
	dst = binary.BigEndian.AppendUint64(dst, startOffset)
	dst = binary.BigEndian.AppendUint32(dst, maxBytes)
	dst = binary.BigEndian.AppendUint32(dst, maxWaitMs)
	return dst, nil
}

// ReadResponseStatus reads the 3-byte prefix [magic(2), status(1)].
func ReadResponseStatus(r io.Reader) (byte, error) {
	var buf [3]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	if !IsValidMagic(buf[0], buf[1]) {
		return 0, aerostream.ErrInvalidMagic
	}
	return buf[2], nil
}

// DecodeAuthResponse reads and validates Command 0 response.
func DecodeAuthResponse(r io.Reader) error {
	status, err := ReadResponseStatus(r)
	if err != nil {
		return err
	}
	if status == StatusAuthFailed {
		return aerostream.ErrAuthFailed
	}
	if status != StatusOk {
		return aerostream.ErrUnexpectedStatus
	}
	return nil
}

// DecodeProduceResponse reads and validates Command 1 response.
func DecodeProduceResponse(r io.Reader) (uint64, error) {
	status, err := ReadResponseStatus(r)
	if err != nil {
		return 0, err
	}
	if status == StatusOutOfOrderSequence {
		return 0, aerostream.ErrOutOfOrderSequence
	}
	if status == StatusAuthFailed {
		return 0, aerostream.ErrAuthFailed
	}
	if status != StatusOk {
		return 0, aerostream.ErrUnexpectedStatus
	}

	var offBuf [8]byte
	if _, err := io.ReadFull(r, offBuf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(offBuf[:]), nil
}

// DecodeFetchResponse reads and decodes Command 2 single-range fetch response.
func DecodeFetchResponse(r io.Reader) ([]byte, error) {
	status, err := ReadResponseStatus(r)
	if err != nil {
		return nil, err
	}
	if status == StatusEmpty {
		return nil, aerostream.ErrEmpty
	}
	if status == StatusAuthFailed {
		return nil, aerostream.ErrAuthFailed
	}
	if status != StatusData {
		return nil, aerostream.ErrUnexpectedStatus
	}

	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	bytesToRead := binary.BigEndian.Uint32(lenBuf[:])
	data := make([]byte, bytesToRead)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

// DecodeMultiFetchResponse reads and decodes Command 4 multi-entry fetch response.
func DecodeMultiFetchResponse(r io.Reader) ([]Entry, error) {
	status, err := ReadResponseStatus(r)
	if err != nil {
		return nil, err
	}
	if status == StatusEmpty {
		return nil, nil
	}
	if status == StatusAuthFailed {
		return nil, aerostream.ErrAuthFailed
	}
	if status != StatusData {
		return nil, aerostream.ErrUnexpectedStatus
	}

	var countBuf [4]byte
	if _, err := io.ReadFull(r, countBuf[:]); err != nil {
		return nil, err
	}
	entryCount := binary.BigEndian.Uint32(countBuf[:])
	if entryCount == 0 {
		return nil, nil
	}

	type meta struct {
		offset uint64
		length uint32
	}
	metaList := make([]meta, entryCount)
	var totalLen uint64
	var metaBuf [12]byte

	for i := uint32(0); i < entryCount; i++ {
		if _, err := io.ReadFull(r, metaBuf[:]); err != nil {
			return nil, err
		}
		off := binary.BigEndian.Uint64(metaBuf[0:8])
		length := binary.BigEndian.Uint32(metaBuf[8:12])
		metaList[i] = meta{offset: off, length: length}
		totalLen += uint64(length)
	}

	if totalLen > 1<<30 {
		return nil, errors.New("aerostream: response body exceeds 1GB limit")
	}

	data := make([]byte, totalLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	entries := make([]Entry, entryCount)
	var curr uint32
	for i, m := range metaList {
		entries[i] = Entry{
			Offset:  m.offset,
			Payload: data[curr : curr+m.length],
		}
		curr += m.length
	}

	return entries, nil
}
