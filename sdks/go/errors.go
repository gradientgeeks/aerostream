package aerostream

import "errors"

// Sentinel errors returned by the AeroStream client and protocol codec.
var (
	ErrAuthFailed         = errors.New("aerostream: authentication failed")
	ErrOutOfOrderSequence = errors.New("aerostream: out of order sequence number")
	ErrClosed             = errors.New("aerostream: client or connection is closed")
	ErrTimeout            = errors.New("aerostream: operation timed out")
	ErrInvalidMagic       = errors.New("aerostream: invalid magic bytes (expected 0xAE 0x01)")
	ErrEmpty              = errors.New("aerostream: no data available")
	ErrUnexpectedStatus   = errors.New("aerostream: unexpected protocol status")
	ErrTopicTooLong       = errors.New("aerostream: topic name exceeds 65535 bytes")
	ErrPayloadTooLarge    = errors.New("aerostream: payload exceeds maximum allowable size")
	ErrBufferTooSmall     = errors.New("aerostream: target buffer too small")
	ErrConnectionRefused  = errors.New("aerostream: failed to connect to broker")
)
