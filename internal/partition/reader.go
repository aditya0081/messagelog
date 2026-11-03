package partition

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"sort"

	"messagelog/internal/segment"
)

// Chunk is a byte range of one segment file holding whole records. It is a
// recipe for sending, not the data itself, so the bytes can go from page
// cache to socket without passing through user space.
//
// A non-empty chunk holds its own open handle on the segment file, opened by
// Read. Retention may delete the segment right after, but an open file
// survives the unlink, so a chunk that Read returned can always be sent in
// full. The caller must Close every chunk. Use a chunk (and its copies) from
// one goroutine at a time: WriteTo moves the handle's file offset.
type Chunk struct {
	Path        string // segment file
	Position    int64  // byte position of the first record
	Size        int64  // bytes; 0 = empty
	FirstOffset int64
	NextOffset  int64 // offset after the last record in the chunk

	file *os.File // nil for an empty chunk
}

// WriteTo copies the chunk to w. It does not close the chunk.
//
// Passing io.Copy an *io.LimitedReader around an *os.File lets
// (*net.TCPConn).ReadFrom recognise it and use sendfile(2) on Linux: zero
// copy, the broker never touches the bytes (DESIGN "Networking").
func (c Chunk) WriteTo(w io.Writer) (int64, error) {
	if c.Size == 0 {
		return 0, nil
	}
	if c.file == nil {
		return 0, errNoFile
	}
	if _, err := c.file.Seek(c.Position, io.SeekStart); err != nil {
		return 0, fmt.Errorf("seek chunk: %w", err)
	}
	n, err := io.Copy(w, io.LimitReader(c.file, c.Size))
	if err == nil && n < c.Size {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

// Bytes reads the chunk into memory. Meant for tests and debugging. It does
// not close the chunk.
func (c Chunk) Bytes() ([]byte, error) {
	if c.Size == 0 {
		return nil, nil
	}
	if c.file == nil {
		return nil, errNoFile
	}
	buf := make([]byte, c.Size)
	if _, err := c.file.ReadAt(buf, c.Position); err != nil {
		return nil, fmt.Errorf("read chunk: %w", err)
	}
	return buf, nil
}

// Close releases the chunk's file handle. It is safe to call more than once
// and on an empty or zero Chunk.
func (c Chunk) Close() error {
	if c.file == nil {
		return nil
	}
	if err := c.file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return fmt.Errorf("close chunk: %w", err)
	}
	return nil
}

var errNoFile = errors.New("partition: chunk has no open file (not returned by Read?)")

// Read returns up to maxRecords records (and roughly maxBytes, but always at
// least one record if any is available) starting at offset, never past the
// high watermark, never spanning two segments. maxRecords < 1 counts as 1.
// offset == HighWatermark → empty Chunk, nil error.
// offset < LogStartOffset or > HighWatermark → ErrOffsetOutOfRange.
//
// Lock-free on data (DESIGN "Consume flow"): the high watermark is an atomic
// load, and only headers are read, with positioned reads.
func (p *Partition) Read(offset int64, maxRecords, maxBytes int) (Chunk, error) {
	if p.closed.Load() {
		return Chunk{}, ErrClosed
	}
	// If retention deletes the segment after this check, segmentFor, the
	// header reads or opening the file notice and report ErrOffsetOutOfRange.
	start, hw := p.LogStartOffset(), p.HighWatermark()
	if offset < start || offset > hw {
		return Chunk{}, ErrOffsetOutOfRange
	}
	if offset == hw {
		return Chunk{FirstOffset: offset, NextOffset: offset}, nil
	}
	seg, segEnd, ok := p.segmentFor(offset)
	if !ok {
		return Chunk{}, ErrOffsetOutOfRange
	}
	chunk, err := readChunk(seg, offset, min(hw, segEnd), max(maxRecords, 1), maxBytes)
	if errors.Is(err, segment.ErrClosed) {
		return Chunk{}, p.closedReadErr()
	}
	if err != nil {
		return Chunk{}, err
	}
	// Open the file now, not when the chunk is sent: from here on retention
	// can no longer take the bytes away.
	chunk.file, err = os.Open(chunk.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return Chunk{}, p.closedReadErr()
	}
	if err != nil {
		return Chunk{}, fmt.Errorf("open chunk: %w", err)
	}
	return chunk, nil
}

// segmentFor finds the segment holding offset and the offset where the
// next segment starts (math.MaxInt64 for the active one).
func (p *Partition) segmentFor(offset int64) (seg *segment.Segment, end int64, ok bool) {
	p.segMu.RLock()
	defer p.segMu.RUnlock()
	segs := p.segments
	i := sort.Search(len(segs), func(i int) bool { return segs[i].Base() > offset }) - 1
	if i < 0 {
		return nil, 0, false
	}
	end = math.MaxInt64
	if i+1 < len(segs) {
		end = segs[i+1].Base()
	}
	return segs[i], end, true
}

// readChunk builds a chunk of records [offset, end) from seg, stopping at
// maxRecords or when the next record would push it past maxBytes.
func readChunk(seg *segment.Segment, offset, end int64, maxRecords, maxBytes int) (Chunk, error) {
	pos, err := seg.Locate(offset)
	if err != nil {
		return Chunk{}, err
	}
	var size int64
	next := offset
	for next < end && next-offset < int64(maxRecords) {
		h, err := seg.ReadHeader(pos + size)
		if err != nil {
			return Chunk{}, err
		}
		if h.Offset != next {
			return Chunk{}, fmt.Errorf("%s: expected offset %d at position %d, found %d", seg.Path(), next, pos+size, h.Offset)
		}
		recSize := int64(h.RecordSize())
		if size > 0 && size+recSize > int64(maxBytes) {
			break
		}
		size += recSize
		next++
	}
	return Chunk{Path: seg.Path(), Position: pos, Size: size, FirstOffset: offset, NextOffset: next}, nil
}

// closedReadErr explains a read whose segment went away under it: either
// the partition was closed, or retention deleted the segment mid-read.
func (p *Partition) closedReadErr() error {
	if p.closed.Load() {
		return ErrClosed
	}
	return ErrOffsetOutOfRange
}

// Wait blocks until HighWatermark() > offset or ctx is done (returns
// ctx.Err()). It returns ErrClosed if the partition closes first.
func (p *Partition) Wait(ctx context.Context, offset int64) error {
	for {
		// Grab the channel before checking, so an advance in between still
		// wakes us: the writer stores the watermark, then closes the channel.
		changed := *p.hwChanged.Load()
		if p.HighWatermark() > offset {
			return nil
		}
		if p.closed.Load() {
			return ErrClosed
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// advanceHighWatermark makes records below hw readable and wakes waiters.
func (p *Partition) advanceHighWatermark(hw int64) {
	p.highWatermark.Store(hw)
	p.notifyWaiters()
}

// notifyWaiters wakes every Wait call by closing the current channel.
func (p *Partition) notifyWaiters() {
	next := make(chan struct{})
	close(*p.hwChanged.Swap(&next))
}
