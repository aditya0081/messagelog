package segment

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

// indexEntrySize is one on-disk entry: relative offset (uint32) + byte position (uint32).
const indexEntrySize = 8

type indexEntry struct {
	relOffset uint32 // offset - segment base
	position  uint32 // byte position of that record in the .log file
}

// index is a segment's sparse offset → position index (DESIGN "Sparse index").
// It is kept fully in memory and mirrored to the .index file, which only ever
// grows except during recovery.
//
// The writer goroutine appends while readers look up, so entries are guarded
// by a RWMutex; both critical sections are a few instructions long.
type index struct {
	base int64
	file *os.File

	mu      sync.RWMutex
	entries []indexEntry
}

// openIndex opens (or creates) the index file at path and loads its entries.
// Entries are trusted only while they increase and point inside a log of
// logSize bytes; anything after the first bad entry (e.g. a torn final write)
// is cut off, on disk too.
func openIndex(path string, base, logSize int64) (*index, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open index: %w", err)
	}
	raw, err := io.ReadAll(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("read index %s: %w", path, err)
	}
	x := &index{base: base, file: f, entries: decodeEntries(raw, logSize)}
	if err := x.truncateFile(); err != nil {
		f.Close()
		return nil, err
	}
	return x, nil
}

// decodeEntries parses raw index bytes, stopping at the first entry that is
// incomplete, not increasing, or beyond the end of the log.
func decodeEntries(raw []byte, logSize int64) []indexEntry {
	var entries []indexEntry
	for len(raw) >= indexEntrySize {
		e := indexEntry{
			relOffset: binary.BigEndian.Uint32(raw[0:4]),
			position:  binary.BigEndian.Uint32(raw[4:8]),
		}
		if int64(e.position) >= logSize {
			break
		}
		if n := len(entries); n > 0 && (e.relOffset <= entries[n-1].relOffset || e.position <= entries[n-1].position) {
			break
		}
		entries = append(entries, e)
		raw = raw[indexEntrySize:]
	}
	return entries
}

// append adds an entry for the record at offset/position. Callers add entries
// in increasing order.
func (x *index) append(offset, position int64) error {
	e := indexEntry{relOffset: uint32(offset - x.base), position: uint32(position)}
	var buf [indexEntrySize]byte
	binary.BigEndian.PutUint32(buf[0:4], e.relOffset)
	binary.BigEndian.PutUint32(buf[4:8], e.position)

	// Only the writer goroutine mutates entries, so reading len here is safe.
	at := int64(len(x.entries)) * indexEntrySize
	if _, err := x.file.WriteAt(buf[:], at); err != nil {
		return fmt.Errorf("write index entry: %w", err)
	}
	x.mu.Lock()
	x.entries = append(x.entries, e)
	x.mu.Unlock()
	return nil
}

// floor returns the entry with the greatest offset <= offset. With no such
// entry it returns the start of the segment (base, position 0).
func (x *index) floor(offset int64) (entryOffset, position int64) {
	rel := offset - x.base
	x.mu.RLock()
	defer x.mu.RUnlock()
	i := sort.Search(len(x.entries), func(i int) bool {
		return int64(x.entries[i].relOffset) > rel
	}) - 1
	if i < 0 {
		return x.base, 0
	}
	return x.base + int64(x.entries[i].relOffset), int64(x.entries[i].position)
}

// last returns the newest entry, or the start of the segment if there is none.
func (x *index) last() (entryOffset, position int64) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	if len(x.entries) == 0 {
		return x.base, 0
	}
	e := x.entries[len(x.entries)-1]
	return x.base + int64(e.relOffset), int64(e.position)
}

// truncateAt drops every entry pointing at position or later. Recovery uses
// it before re-validating the log from position.
func (x *index) truncateAt(position int64) error {
	x.mu.Lock()
	keep := sort.Search(len(x.entries), func(i int) bool {
		return int64(x.entries[i].position) >= position
	})
	x.entries = x.entries[:keep]
	x.mu.Unlock()
	return x.truncateFile()
}

// truncateFile makes the file hold exactly the in-memory entries.
func (x *index) truncateFile() error {
	size := int64(len(x.entries)) * indexEntrySize
	if err := x.file.Truncate(size); err != nil {
		return fmt.Errorf("truncate index: %w", err)
	}
	return nil
}

func (x *index) sync() error {
	if err := x.file.Sync(); err != nil {
		return fmt.Errorf("fsync index: %w", err)
	}
	return nil
}

func (x *index) close() error {
	return x.file.Close()
}
