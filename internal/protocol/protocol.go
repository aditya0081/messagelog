// Package protocol is the broker's binary wire format over long-lived TCP.
//
// Every message is a frame: a big-endian uint32 size (excluding itself)
// followed by the payload.
//
//	request payload:  uint16 apiKey | uint32 correlationID | body
//	response payload: uint32 correlationID | uint16 errorCode | string errorMessage | body
//
// The response body is present only when errorCode is ErrNone. Correlation
// ids let a client keep many requests in flight on one connection and match
// responses that come back out of order.
//
// Field encodings: string = uint16 length + bytes, bytes = uint32 length +
// bytes, bool = one byte (0 or 1), integers big-endian.
//
// Each body is a struct implementing Body. Strings must be at most
// MaxStringLen bytes; encoding a longer one is a programming error and
// panics, so callers validate user input (topic and group names) first.
package protocol

import (
	"errors"
	"fmt"
	"math"
)

// MaxStringLen is the longest string the uint16 length prefix can carry.
const MaxStringLen = math.MaxUint16

var (
	// ErrMalformed means a payload does not match its expected layout:
	// too short, trailing bytes, or a length that runs past the end.
	ErrMalformed = errors.New("protocol: malformed message")
	// ErrFrameTooLarge means a frame's size prefix exceeds the reader's limit.
	ErrFrameTooLarge = errors.New("protocol: frame too large")
)

// APIKey says which request a frame carries.
type APIKey uint16

const (
	APICreateTopic        APIKey = 1
	APIMetadata           APIKey = 2
	APIInitProducer       APIKey = 3
	APIPublish            APIKey = 4
	APIConsume            APIKey = 5
	APICommit             APIKey = 6
	APIGetCommittedOffset APIKey = 7
)

// ErrorCode is the outcome of a request, sent in every response.
type ErrorCode uint16

const (
	ErrNone ErrorCode = iota
	ErrUnknown
	ErrUnknownTopicOrPartition
	ErrOffsetOutOfRange
	ErrOutOfOrderSequence
	ErrMessageTooLarge
	ErrTopicExists
	ErrInvalidRequest
	ErrShuttingDown
	ErrInvalidTopic
)

var errorCodeNames = [...]string{
	ErrNone:                    "None",
	ErrUnknown:                 "Unknown",
	ErrUnknownTopicOrPartition: "UnknownTopicOrPartition",
	ErrOffsetOutOfRange:        "OffsetOutOfRange",
	ErrOutOfOrderSequence:      "OutOfOrderSequence",
	ErrMessageTooLarge:         "MessageTooLarge",
	ErrTopicExists:             "TopicExists",
	ErrInvalidRequest:          "InvalidRequest",
	ErrShuttingDown:            "ShuttingDown",
	ErrInvalidTopic:            "InvalidTopic",
}

func (c ErrorCode) String() string {
	if int(c) < len(errorCodeNames) {
		return errorCodeNames[c]
	}
	return fmt.Sprintf("ErrorCode(%d)", uint16(c))
}

// Body is implemented by every request and response body.
type Body interface {
	AppendTo(b []byte) []byte
	DecodeFrom(b []byte) error // errors on short/trailing bytes
}
