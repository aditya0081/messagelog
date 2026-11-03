// Package segment manages one slice of a partition log: a .log file of
// back-to-back records (see package record) plus its sparse .index file.
//
// Files are named by the segment's base offset, 20 digits zero padded, so a
// directory listing sorts in offset order (DESIGN "Message Storage").
//
// Concurrency: one writer goroutine calls Append, Sync, Recover and
// TruncateTail. Any number
// of readers may call Locate and ReadHeader at the same time; they use
// positioned reads (pread) so they never share a file offset with the writer.
// Readers must only ask for records the writer has already finished writing.
package segment

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"messagelog/internal/fsutil"
	"messagelog/internal/record"
)

const (
	logSuffix   = ".log"
	indexSuffix = ".index"
	nameDigits  = 20
)

// ErrClosed is returned by reads on a segment that was closed or removed,
// e.g. by retention while a reader was still looking at it.
var ErrClosed = errors.New("segment: closed")

// Segment is one .log file and its sparse index.
type Segment struct {
	base          int64
	logPath       string
	indexPath     string
	log           *os.File
	index         *index
	indexInterval int

	// Updated by the writer; atomic because readers and retention peek at them.
	size          atomic.Int64
	lastTimestamp atomic.Int64

	// Writer-only state.
	nextOffset      int64
	bytesSinceIndex int
	buf             []byte // encode scratch space, reused across appends
}

// List returns the base offsets of all segments in dir, oldest first.
func List(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("list segments: %w", err)
	}
	var bases []int64
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), logSuffix)
		if !ok || len(name) != nameDigits {
			continue
		}
		base, err := strconv.ParseInt(name, 10, 64)
		if err != nil {
			continue
		}
		bases = append(bases, base)
	}
	slices.Sort(bases)
	return bases, nil
}

// Create makes a new, empty segment starting at base. An index entry is
// added roughly every indexInterval bytes.
func Create(dir string, base int64, indexInterval int) (*Segment, error) {
	s := newSegment(dir, base, indexInterval)
	f, err := os.OpenFile(s.logPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create segment: %w", err)
	}
	s.log = f
	if s.index, err = openIndex(s.indexPath, base, 0); err != nil {
		f.Close()
		return nil, err
	}
	s.nextOffset = base
	// Make the new directory entries durable, or a crash could lose the files.
	if err := fsutil.SyncDir(dir); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// Open opens an existing segment and loads its index. It finds the next
// offset and newest timestamp by reading headers after the last index entry.
//
// That is exact for segments that were fsynced before the last checkpoint.
// Segments that may have a torn tail must then be checked with Recover.
func Open(dir string, base int64, indexInterval int) (*Segment, error) {
	s := newSegment(dir, base, indexInterval)
	f, err := os.OpenFile(s.logPath, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open segment: %w", err)
	}
	s.log = f
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat segment: %w", err)
	}
	if s.index, err = openIndex(s.indexPath, base, info.Size()); err != nil {
		f.Close()
		return nil, err
	}
	tail, err := s.readTail(info.Size())
	if err != nil {
		s.Close()
		return nil, err
	}
	s.nextOffset = tail.nextOffset
	s.lastTimestamp.Store(tail.lastTimestamp)
	s.bytesSinceIndex = tail.bytesSinceIndex
	s.size.Store(info.Size())
	return s, nil
}

func newSegment(dir string, base int64, indexInterval int) *Segment {
	name := fmt.Sprintf("%0*d", nameDigits, base)
	return &Segment{
		base:          base,
		logPath:       filepath.Join(dir, name+logSuffix),
		indexPath:     filepath.Join(dir, name+indexSuffix),
		indexInterval: indexInterval,
	}
}

// Base is the offset of the first record in the segment.
func (s *Segment) Base() int64 { return s.base }

// Path is the .log file path; readers open it themselves for zero-copy sends.
func (s *Segment) Path() string { return s.logPath }

// Size is the number of bytes of records in the segment.
func (s *Segment) Size() int64 { return s.size.Load() }

// LastTimestamp is the timestamp (unix ms) of the newest record, 0 if empty.
// Retention uses it to decide whether the whole segment has expired.
func (s *Segment) LastTimestamp() int64 { return s.lastTimestamp.Load() }

// NextOffset is the offset the next appended record gets. Writer-only.
func (s *Segment) NextOffset() int64 { return s.nextOffset }

// Append writes r at the end of the segment. r.Offset must be NextOffset().
// The data reaches the OS page cache; call Sync to make it durable.
func (s *Segment) Append(r record.Record) error {
	s.buf = record.Append(s.buf[:0], r)
	pos := s.size.Load()
	if _, err := s.log.WriteAt(s.buf, pos); err != nil {
		return fmt.Errorf("append to %s: %w", s.logPath, err)
	}
	return s.track(r.Offset, r.Timestamp, pos, len(s.buf))
}

// track updates in-memory state for a record that now sits at pos, adding a
// sparse index entry once indexInterval bytes have passed since the last one.
func (s *Segment) track(offset, timestamp, pos int64, size int) error {
	if s.bytesSinceIndex >= s.indexInterval {
		if err := s.index.append(offset, pos); err != nil {
			return err
		}
		s.bytesSinceIndex = 0
	}
	s.bytesSinceIndex += size
	s.nextOffset = offset + 1
	s.lastTimestamp.Store(timestamp)
	s.size.Store(pos + int64(size))
	return nil
}

// ReadHeader reads the record header at byte position pos.
func (s *Segment) ReadHeader(pos int64) (record.Header, error) {
	var buf [record.HeaderSize]byte
	if _, err := s.log.ReadAt(buf[:], pos); err != nil {
		if errors.Is(err, os.ErrClosed) {
			return record.Header{}, ErrClosed
		}
		return record.Header{}, fmt.Errorf("read header at %d in %s: %w", pos, s.logPath, err)
	}
	return record.DecodeHeader(buf[:])
}

// Locate returns the byte position of the record with the given offset
// (DESIGN "Consume flow" steps 3-4): nearest lower index entry, then a
// forward scan that reads only headers. offset must already be written.
func (s *Segment) Locate(offset int64) (int64, error) {
	cur, pos := s.index.floor(offset)
	for cur < offset {
		h, err := s.ReadHeader(pos)
		if err != nil {
			return 0, err
		}
		if h.Offset != cur {
			return 0, fmt.Errorf("%s: expected offset %d at position %d, found %d", s.logPath, cur, pos, h.Offset)
		}
		pos += int64(h.RecordSize())
		cur++
	}
	return pos, nil
}

// tail is what a header scan from the last index entry finds out.
type tail struct {
	nextOffset      int64 // offset after the last record seen
	lastTimestamp   int64 // timestamp of the last record seen, 0 if none
	bytesSinceIndex int   // bytes from the last index entry to the end of the scan
}

// readTail reads headers from the last index entry up to byte limit. Those
// bytes are trusted, so the scan just stops at anything that does not look
// like a record. It changes nothing.
func (s *Segment) readTail(limit int64) (tail, error) {
	next, pos := s.index.last()
	start := pos
	t := tail{nextOffset: next}
	for pos+record.HeaderSize <= limit {
		h, err := s.ReadHeader(pos)
		if errors.Is(err, record.ErrTooLarge) {
			break
		}
		if err != nil {
			return tail{}, err
		}
		end := pos + int64(h.RecordSize())
		if h.Offset != t.nextOffset || end > limit {
			break
		}
		t.nextOffset = h.Offset + 1
		t.lastTimestamp = h.Timestamp
		pos = end
	}
	t.bytesSinceIndex = int(pos - start)
	return t, nil
}

// Recover validates every record from byte position onwards (DESIGN "Crash
// Recovery"): full header, sane length, full body, matching CRC, and offsets
// counting up from nextOffset. The segment then ends at the last good record:
// Size, NextOffset and the index cover exactly the good records, and both
// files are fsynced (after a process crash, unsynced writes still sit in the
// page cache and are kept if valid). visit sees each good header in order, so
// the caller can rebuild producer state.
//
// torn reports bytes after the last good record. Recover leaves them in the
// file, because only the caller knows whether they are a torn write to cut
// off (TruncateTail) or corruption to report.
//
// (position, nextOffset) must be a durable record boundary: a checkpoint, or
// (0, Base()).
func (s *Segment) Recover(position, nextOffset int64, visit func(record.Header)) (torn bool, err error) {
	if err := s.index.truncateAt(position); err != nil {
		return false, err
	}
	// The prefix is trusted: only lastTimestamp and the index spacing counter
	// are restored from it. nextOffset comes from the caller.
	prefix, err := s.readTail(position)
	if err != nil {
		return false, err
	}
	s.nextOffset = nextOffset
	s.lastTimestamp.Store(prefix.lastTimestamp)
	s.bytesSinceIndex = prefix.bytesSinceIndex
	s.size.Store(position)

	info, err := s.log.Stat()
	if err != nil {
		return false, fmt.Errorf("stat segment: %w", err)
	}
	if err := s.scanFrom(position, info.Size(), visit); err != nil {
		return false, err
	}
	return s.Size() < info.Size(), s.Sync()
}

// scanFrom reads records from position up to fileSize and adds each fully
// valid one to the segment as if it had just been appended: it updates
// NextOffset, Size and LastTimestamp, appends sparse index entries, and
// passes the header to visit. It stops quietly at the first record that is
// torn, corrupt or out of sequence; only I/O errors are returned.
func (s *Segment) scanFrom(position, fileSize int64, visit func(record.Header)) error {
	r := bufio.NewReaderSize(io.NewSectionReader(s.log, position, fileSize-position), 64<<10)
	var buf []byte
	pos := position
	for {
		var (
			h   record.Header
			err error
		)
		h, buf, err = readRecord(r, buf)
		if errors.Is(err, io.EOF) || isBadRecord(err) || (err == nil && h.Offset != s.nextOffset) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("scan %s at %d: %w", s.logPath, pos, err)
		}
		if err := s.track(h.Offset, h.Timestamp, pos, h.RecordSize()); err != nil {
			return err
		}
		visit(h)
		pos += int64(h.RecordSize())
	}
}

// TruncateTail cuts the log file at Size(), dropping what Recover found
// after the last good record, and fsyncs it.
func (s *Segment) TruncateTail() error {
	if err := s.log.Truncate(s.Size()); err != nil {
		return fmt.Errorf("truncate %s: %w", s.logPath, err)
	}
	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", s.logPath, err)
	}
	return nil
}

// readRecord reads and fully validates the next record from r, reusing buf.
// It returns io.EOF only when r is exhausted exactly at a record boundary.
func readRecord(r io.Reader, buf []byte) (record.Header, []byte, error) {
	buf = slices.Grow(buf[:0], record.HeaderSize)[:record.HeaderSize]
	if _, err := io.ReadFull(r, buf); err != nil {
		return record.Header{}, buf, err
	}
	h, err := record.DecodeHeader(buf)
	if err != nil {
		return record.Header{}, buf, err
	}
	buf = slices.Grow(buf, int(h.Length))[:h.RecordSize()]
	if _, err := io.ReadFull(r, buf[record.HeaderSize:]); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return record.Header{}, buf, err
	}
	if _, _, err := record.Decode(buf); err != nil {
		return record.Header{}, buf, err
	}
	return h, buf, nil
}

// isBadRecord reports whether err means "the log is torn or corrupt here",
// as opposed to an I/O failure.
func isBadRecord(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, record.ErrShort) ||
		errors.Is(err, record.ErrTooLarge) ||
		errors.Is(err, record.ErrCorrupt)
}

// Sync fsyncs the log and then its index.
func (s *Segment) Sync() error {
	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", s.logPath, err)
	}
	return s.index.sync()
}

// Close closes both files without syncing them.
func (s *Segment) Close() error {
	return errors.Join(s.log.Close(), s.index.close())
}

// Remove closes the segment and deletes its files. In-flight readers get
// ErrClosed. The .log goes first: an orphaned .index is harmless, a .log
// without its index is merely slower to read. The caller fsyncs the
// directory (fsutil.SyncDir) once it has removed a batch of segments.
func (s *Segment) Remove() error {
	closeErr := s.Close()
	if err := os.Remove(s.logPath); err != nil {
		return fmt.Errorf("remove segment: %w", err)
	}
	if err := os.Remove(s.indexPath); err != nil {
		return fmt.Errorf("remove index: %w", err)
	}
	return closeErr
}
