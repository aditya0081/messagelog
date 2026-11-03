// Package record defines the on-disk (and on-wire) format of a single message.
//
// Layout, big-endian, 40-byte fixed header followed by the body:
//
//	offset       field        size
//	 0           Offset       8   position of the record in its partition
//	 8           Timestamp    8   unix milliseconds, set by the broker
//	16           Length       4   body length in bytes
//	20           CRC          4   CRC32-C of every other byte of the record
//	24           ProducerID   8   idempotent producer id, or NoProducerID
//	32           Sequence     8   per (producer, partition) sequence number
//	40           Body         Length bytes, opaque to the broker
//
// The CRC covers bytes [0:20] and [24:end], i.e. everything except itself,
// so a torn write anywhere in the record is detected.
package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	// HeaderSize is the fixed size of every record header.
	HeaderSize = 40

	// MaxBodySize caps a single message. Anything larger on disk is treated
	// as corruption during recovery.
	MaxBodySize = 1 << 20 // 1 MB

	// NoProducerID marks a record from a producer that did not ask for
	// idempotence. Such records skip the duplicate check.
	NoProducerID int64 = -1
)

var (
	// ErrShort means the buffer ends before the record does (torn write).
	ErrShort = errors.New("record: incomplete record")
	// ErrTooLarge means the header claims a body bigger than MaxBodySize.
	ErrTooLarge = errors.New("record: body too large")
	// ErrCorrupt means the CRC does not match the bytes.
	ErrCorrupt = errors.New("record: crc mismatch")
)

// castagnoli is hardware-accelerated on amd64 and arm64.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Record is one message in a partition log.
type Record struct {
	Offset     int64
	Timestamp  int64 // unix milliseconds
	ProducerID int64
	Sequence   int64
	Body       []byte
}

// Size is the number of bytes the record takes once encoded.
func (r Record) Size() int { return HeaderSize + len(r.Body) }

// Header is the decoded fixed-size prefix of a record.
type Header struct {
	Offset     int64
	Timestamp  int64
	Length     uint32
	CRC        uint32
	ProducerID int64
	Sequence   int64
}

// RecordSize is the full encoded size of the record this header starts.
func (h Header) RecordSize() int { return HeaderSize + int(h.Length) }

// Append encodes r onto the end of dst and returns the extended slice.
func Append(dst []byte, r Record) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, HeaderSize)...)
	dst = append(dst, r.Body...)

	b := dst[start:]
	binary.BigEndian.PutUint64(b[0:8], uint64(r.Offset))
	binary.BigEndian.PutUint64(b[8:16], uint64(r.Timestamp))
	binary.BigEndian.PutUint32(b[16:20], uint32(len(r.Body)))
	binary.BigEndian.PutUint64(b[24:32], uint64(r.ProducerID))
	binary.BigEndian.PutUint64(b[32:40], uint64(r.Sequence))
	binary.BigEndian.PutUint32(b[20:24], checksum(b))
	return dst
}

// DecodeHeader parses the fixed header at the start of b. It checks only the
// length bound, not the CRC; use Decode when the body is available too.
func DecodeHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, ErrShort
	}
	h := Header{
		Offset:     int64(binary.BigEndian.Uint64(b[0:8])),
		Timestamp:  int64(binary.BigEndian.Uint64(b[8:16])),
		Length:     binary.BigEndian.Uint32(b[16:20]),
		CRC:        binary.BigEndian.Uint32(b[20:24]),
		ProducerID: int64(binary.BigEndian.Uint64(b[24:32])),
		Sequence:   int64(binary.BigEndian.Uint64(b[32:40])),
	}
	if h.Length > MaxBodySize {
		return Header{}, ErrTooLarge
	}
	return h, nil
}

// Decode parses and validates the first record in b and returns it together
// with the number of bytes it occupied. The returned Body aliases b.
//
// Validation order matches crash recovery: header present, length sane,
// body present, CRC matches. The body is never interpreted before the CRC
// check passes.
func Decode(b []byte) (Record, int, error) {
	h, err := DecodeHeader(b)
	if err != nil {
		return Record{}, 0, err
	}
	size := h.RecordSize()
	if len(b) < size {
		return Record{}, 0, ErrShort
	}
	if checksum(b[:size]) != h.CRC {
		return Record{}, 0, ErrCorrupt
	}
	return Record{
		Offset:     h.Offset,
		Timestamp:  h.Timestamp,
		ProducerID: h.ProducerID,
		Sequence:   h.Sequence,
		Body:       b[HeaderSize:size],
	}, size, nil
}

// DecodeAll parses a run of back-to-back records, such as a consume
// response. It fails on the first invalid record.
func DecodeAll(b []byte) ([]Record, error) {
	var out []Record
	for len(b) > 0 {
		r, n, err := Decode(b)
		if err != nil {
			return out, err
		}
		out = append(out, r)
		b = b[n:]
	}
	return out, nil
}

// checksum computes the CRC of a full encoded record, skipping the CRC field.
func checksum(rec []byte) uint32 {
	crc := crc32.Update(0, castagnoli, rec[0:20])
	return crc32.Update(crc, castagnoli, rec[24:])
}
