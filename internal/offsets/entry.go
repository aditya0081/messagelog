package offsets

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
)

// Entry layout, big-endian:
//
//	uint32 length   payload size in bytes
//	uint32 crc      CRC32-C of the payload
//	payload:
//	  uint16 len + group bytes
//	  uint16 len + topic bytes
//	  int32  partition
//	  int64  offset
const (
	entryHeaderSize = 8

	// maxNameLen bounds group and topic so they fit their uint16 length prefix.
	maxNameLen = math.MaxUint16

	// maxPayloadSize is the largest payload a valid entry can have. A bigger
	// length on disk can only be corruption.
	maxPayloadSize = 2 + maxNameLen + 2 + maxNameLen + 4 + 8
)

// errBadEntry marks a torn or corrupt entry: replay stops there and the file
// is truncated to the entries before it.
var errBadEntry = errors.New("offsets: torn or corrupt entry")

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// entrySize is the encoded size of an entry for k.
func entrySize(k Key) int64 {
	return int64(entryHeaderSize + 2 + len(k.Group) + 2 + len(k.Topic) + 4 + 8)
}

// appendEntry encodes one commit onto dst. k's group and topic must be at
// most maxNameLen bytes (Commit checks).
func appendEntry(dst []byte, k Key, offset int64) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, entryHeaderSize)...)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(k.Group)))
	dst = append(dst, k.Group...)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(k.Topic)))
	dst = append(dst, k.Topic...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(k.Partition))
	dst = binary.BigEndian.AppendUint64(dst, uint64(offset))

	payload := dst[start+entryHeaderSize:]
	binary.BigEndian.PutUint32(dst[start:], uint32(len(payload)))
	binary.BigEndian.PutUint32(dst[start+4:], crc32.Checksum(payload, castagnoli))
	return dst
}

// readEntry reads the next entry from r and returns it with its encoded size.
// It returns io.EOF at a clean end of file and errBadEntry for a torn or
// corrupt entry, checking in the same order as partition recovery: header
// present, length sane, payload present, CRC matches.
func readEntry(r *bufio.Reader) (Key, int64, int64, error) {
	var header [entryHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if err == io.ErrUnexpectedEOF { // part of a header: torn
			err = errBadEntry
		}
		return Key{}, 0, 0, err // io.EOF here is a clean end
	}
	length := binary.BigEndian.Uint32(header[0:4])
	if length > maxPayloadSize {
		return Key{}, 0, 0, errBadEntry
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			err = errBadEntry
		}
		return Key{}, 0, 0, err
	}
	if crc32.Checksum(payload, castagnoli) != binary.BigEndian.Uint32(header[4:8]) {
		return Key{}, 0, 0, errBadEntry
	}
	k, offset, err := decodePayload(payload)
	if err != nil {
		return Key{}, 0, 0, err
	}
	return k, offset, int64(entryHeaderSize + length), nil
}

// decodePayload parses a payload whose CRC already matched. The bounds checks
// still matter: a CRC collision on garbage must not cause a panic.
func decodePayload(p []byte) (Key, int64, error) {
	group, p, ok := cutString(p)
	if !ok {
		return Key{}, 0, errBadEntry
	}
	topic, p, ok := cutString(p)
	if !ok || len(p) != 4+8 {
		return Key{}, 0, errBadEntry
	}
	k := Key{Group: group, Topic: topic, Partition: int32(binary.BigEndian.Uint32(p[0:4]))}
	return k, int64(binary.BigEndian.Uint64(p[4:12])), nil
}

// cutString splits a uint16-length-prefixed string off the front of p.
func cutString(p []byte) (string, []byte, bool) {
	if len(p) < 2 {
		return "", nil, false
	}
	n := int(binary.BigEndian.Uint16(p))
	p = p[2:]
	if len(p) < n {
		return "", nil, false
	}
	return string(p[:n]), p[n:], true
}
