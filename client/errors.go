package client

import (
	"fmt"

	"messagelog/internal/protocol"
)

// Error is an error code returned by the broker.
type Error struct {
	Code    protocol.ErrorCode
	Message string
}

func (e *Error) Error() string {
	if e.Message == "" {
		return "messagelog: " + e.Code.String()
	}
	return fmt.Sprintf("messagelog: %s: %s", e.Code, e.Message)
}

// Is matches any *Error with the same code, so callers can write
// errors.Is(err, client.ErrUnknownTopicOrPartition).
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

// Sentinels for errors.Is, one per broker error code.
var (
	ErrUnknown                 = &Error{Code: protocol.ErrUnknown}
	ErrUnknownTopicOrPartition = &Error{Code: protocol.ErrUnknownTopicOrPartition}
	ErrOffsetOutOfRange        = &Error{Code: protocol.ErrOffsetOutOfRange}
	ErrOutOfOrderSequence      = &Error{Code: protocol.ErrOutOfOrderSequence}
	ErrMessageTooLarge         = &Error{Code: protocol.ErrMessageTooLarge}
	ErrTopicExists             = &Error{Code: protocol.ErrTopicExists}
	ErrInvalidRequest          = &Error{Code: protocol.ErrInvalidRequest}
	ErrShuttingDown            = &Error{Code: protocol.ErrShuttingDown}
	ErrInvalidTopic            = &Error{Code: protocol.ErrInvalidTopic}
)

// checkWireLen rejects a string too long for the wire format's uint16
// length prefix, before it is encoded (the encoder panics on one). Whether
// the name is otherwise valid is the broker's call.
func checkWireLen(kind, s string) error {
	if len(s) > protocol.MaxStringLen {
		return fmt.Errorf("%w: %s is %d bytes, max %d", ErrInvalidRequest, kind, len(s), protocol.MaxStringLen)
	}
	return nil
}
