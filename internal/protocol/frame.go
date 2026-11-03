package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

const sizePrefixLen = 4

// Request is a parsed request frame. Body is still encoded: the server picks
// the body type from APIKey and calls its DecodeFrom.
type Request struct {
	APIKey        APIKey
	CorrelationID uint32
	Body          []byte // raw
}

// Response is a parsed response frame. Body is still encoded and is empty
// unless Error is ErrNone.
type Response struct {
	CorrelationID uint32
	Error         ErrorCode
	Message       string
	Body          []byte // raw
}

// ReadFrame reads one frame and returns its payload (without the size
// prefix). A frame larger than maxSize is rejected before anything is
// allocated. It returns io.EOF unwrapped when the stream ends cleanly
// between frames, so a server can tell a closed connection from a broken one.
func ReadFrame(r io.Reader, maxSize uint32) ([]byte, error) {
	var prefix [sizePrefixLen]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("protocol: read frame size: %w", err)
	}
	size := binary.BigEndian.Uint32(prefix[:])
	if size > maxSize {
		return nil, fmt.Errorf("%w: %d bytes, max %d", ErrFrameTooLarge, size, maxSize)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("protocol: read frame payload: %w", err)
	}
	return payload, nil
}

// WriteRequest encodes and writes one request frame in a single Write.
// body may be nil for APIs without a request body.
func WriteRequest(w io.Writer, apiKey APIKey, corrID uint32, body Body) error {
	b := make([]byte, sizePrefixLen, 64)
	b = binary.BigEndian.AppendUint16(b, uint16(apiKey))
	b = binary.BigEndian.AppendUint32(b, corrID)
	if body != nil {
		b = body.AppendTo(b)
	}
	return writeFrame(w, b)
}

// WriteResponse encodes and writes one response frame in a single Write.
// body is sent only when code is ErrNone, and may be nil. msg longer than
// MaxStringLen is cut, since it is only diagnostic text.
func WriteResponse(w io.Writer, corrID uint32, code ErrorCode, msg string, body Body) error {
	b := appendResponseHeader(make([]byte, sizePrefixLen, 64), corrID, code, msg)
	if code == ErrNone && body != nil {
		b = body.AppendTo(b)
	}
	return writeFrame(w, b)
}

// ConsumeResponseHeader returns the frame bytes up to (and including) the
// records length prefix, with the frame size already accounting for
// recordsLen bytes that the caller then streams straight from the segment file.
//
// Header + records is byte-for-byte what WriteResponse produces for a
// successful ConsumeResponse, so clients decode it with ConsumeResponse.
func ConsumeResponseHeader(corrID uint32, highWatermark int64, recordsLen uint32) []byte {
	b := appendResponseHeader(make([]byte, sizePrefixLen, 32), corrID, ErrNone, "")
	b = appendInt64(b, highWatermark)
	b = binary.BigEndian.AppendUint32(b, recordsLen)
	binary.BigEndian.PutUint32(b, uint32(len(b)-sizePrefixLen)+recordsLen)
	return b
}

// ParseRequest splits a request payload (from ReadFrame) into its header and
// raw body. It does not check that APIKey is known, so the server can still
// answer an unknown key using the correlation id.
func ParseRequest(frame []byte) (Request, error) {
	r := reader{buf: frame}
	req := Request{APIKey: APIKey(r.uint16()), CorrelationID: r.uint32()}
	req.Body = r.rest()
	if r.err != nil {
		return Request{}, fmt.Errorf("protocol: request header: %w", r.err)
	}
	return req, nil
}

// ParseResponse splits a response payload (from ReadFrame) into its header
// and raw body.
func ParseResponse(frame []byte) (Response, error) {
	r := reader{buf: frame}
	resp := Response{CorrelationID: r.uint32(), Error: ErrorCode(r.uint16()), Message: r.string()}
	resp.Body = r.rest()
	if r.err != nil {
		return Response{}, fmt.Errorf("protocol: response header: %w", r.err)
	}
	if resp.Error != ErrNone && len(resp.Body) > 0 {
		return Response{}, fmt.Errorf("%w: error response %s carries a body", ErrMalformed, resp.Error)
	}
	return resp, nil
}

func appendResponseHeader(b []byte, corrID uint32, code ErrorCode, msg string) []byte {
	if len(msg) > MaxStringLen {
		msg = msg[:MaxStringLen]
	}
	b = binary.BigEndian.AppendUint32(b, corrID)
	b = binary.BigEndian.AppendUint16(b, uint16(code))
	return appendString(b, msg)
}

// writeFrame fills in the size prefix reserved at the start of frame and
// writes the whole frame at once, so concurrent writers that serialize on
// Write never interleave partial frames.
func writeFrame(w io.Writer, frame []byte) error {
	binary.BigEndian.PutUint32(frame, uint32(len(frame)-sizePrefixLen))
	if _, err := w.Write(frame); err != nil {
		return fmt.Errorf("protocol: write frame: %w", err)
	}
	return nil
}
