package protocol

import (
	"encoding/binary"
	"fmt"
)

// Encoding helpers. Each appends one field to b and returns the extended slice.

func appendInt32(b []byte, v int32) []byte { return binary.BigEndian.AppendUint32(b, uint32(v)) }
func appendInt64(b []byte, v int64) []byte { return binary.BigEndian.AppendUint64(b, uint64(v)) }

func appendBool(b []byte, v bool) []byte {
	if v {
		return append(b, 1)
	}
	return append(b, 0)
}

func appendString(b []byte, s string) []byte {
	if len(s) > MaxStringLen {
		panic(fmt.Sprintf("protocol: string of %d bytes exceeds MaxStringLen", len(s)))
	}
	b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

func appendBytes(b []byte, v []byte) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(v)))
	return append(b, v...)
}

// appendSlice writes an int32 count followed by each item.
func appendSlice[T any](b []byte, items []T, appendItem func([]byte, T) []byte) []byte {
	b = appendInt32(b, int32(len(items)))
	for _, item := range items {
		b = appendItem(b, item)
	}
	return b
}

// reader decodes fields from a byte slice. The first failure is sticky: later
// reads return zero values, so a decoder reads every field and checks the
// error once at the end via finish.
//
// Lengths and counts are checked against the bytes actually left before
// anything is allocated, so a bad length cannot trigger a huge allocation.
type reader struct {
	buf []byte
	err error
}

func (r *reader) fail(format string, args ...any) {
	if r.err == nil {
		r.err = fmt.Errorf("%w: %s", ErrMalformed, fmt.Sprintf(format, args...))
	}
}

// next consumes n bytes, or fails if fewer remain.
func (r *reader) next(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n > len(r.buf) {
		r.fail("need %d bytes, have %d", n, len(r.buf))
		return nil
	}
	p := r.buf[:n]
	r.buf = r.buf[n:]
	return p
}

func (r *reader) uint16() uint16 {
	if p := r.next(2); p != nil {
		return binary.BigEndian.Uint16(p)
	}
	return 0
}

func (r *reader) uint32() uint32 {
	if p := r.next(4); p != nil {
		return binary.BigEndian.Uint32(p)
	}
	return 0
}

func (r *reader) int32() int32 { return int32(r.uint32()) }

func (r *reader) int64() int64 {
	if p := r.next(8); p != nil {
		return int64(binary.BigEndian.Uint64(p))
	}
	return 0
}

func (r *reader) bool() bool {
	p := r.next(1)
	if p == nil {
		return false
	}
	if p[0] > 1 {
		r.fail("bool byte %d", p[0])
	}
	return p[0] == 1
}

func (r *reader) string() string {
	return string(r.next(int(r.uint16())))
}

// bytes returns a slice aliasing the input (nil when empty), so large values
// such as consumed records are not copied.
func (r *reader) bytes() []byte {
	n := r.uint32()
	if uint64(n) > uint64(len(r.buf)) {
		r.fail("bytes length %d, have %d", n, len(r.buf))
		return nil
	}
	if p := r.next(int(n)); len(p) > 0 {
		return p
	}
	return nil
}

// rest consumes and returns everything left (nil when empty).
func (r *reader) rest() []byte {
	if r.err != nil || len(r.buf) == 0 {
		return nil
	}
	p := r.buf
	r.buf = nil
	return p
}

// finish reports the first error, or trailing bytes if decoding succeeded
// without consuming the whole input.
func (r *reader) finish() error {
	if r.err == nil && len(r.buf) > 0 {
		r.fail("%d trailing bytes", len(r.buf))
	}
	return r.err
}

// readSlice reads an int32 count and then that many items. minItemSize is
// the smallest encoding of one item; it bounds the count by the bytes left.
func readSlice[T any](r *reader, minItemSize int, readItem func(*reader) T) []T {
	n := r.int32()
	if r.err != nil {
		return nil
	}
	if n < 0 || int64(n)*int64(minItemSize) > int64(len(r.buf)) {
		r.fail("count %d does not fit in %d bytes", n, len(r.buf))
		return nil
	}
	var items []T
	for range n {
		items = append(items, readItem(r))
	}
	return items
}
