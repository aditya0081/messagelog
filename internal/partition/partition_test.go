package partition

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"messagelog/internal/record"
	"messagelog/internal/segment"
)

func TestAppendReadRoundTrip(t *testing.T) {
	cfg := testConfig(t)
	clock := newFakeClock()
	cfg.Now = clock.Now
	p := mustOpen(t, cfg)

	appendBodies(t, p, 0, 20)
	assertLog(t, p, 20)

	c, err := p.Read(5, 3, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	recs := decodeChunk(t, c)
	if len(recs) != 3 || recs[0].Offset != 5 || c.FirstOffset != 5 || c.NextOffset != 8 {
		t.Fatalf("Read(5, 3): %d records, chunk %+v", len(recs), c)
	}
	if recs[0].Timestamp != clock.Now().UnixMilli() || recs[0].ProducerID != record.NoProducerID {
		t.Fatalf("record header: %+v", recs[0])
	}
}

func TestReadBounds(t *testing.T) {
	p := mustOpen(t, testConfig(t))
	appendBodies(t, p, 0, 5)

	c, err := p.Read(5, 10, 1<<20)
	if err != nil || c.Size != 0 || c.NextOffset != 5 {
		t.Fatalf("Read at high watermark = %+v, %v; want empty chunk", c, err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close on an empty chunk: %v", err)
	}
	if _, err := p.Read(6, 10, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("Read past high watermark: %v", err)
	}
	if _, err := p.Read(-1, 10, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("Read below start: %v", err)
	}
}

func TestReadMaxBytesReturnsAtLeastOneRecord(t *testing.T) {
	p := mustOpen(t, testConfig(t))
	appendBodies(t, p, 0, 5)

	c, err := p.Read(0, 10, 1)
	if err != nil || c.NextOffset != 1 || c.Size != int64(recordSize) {
		t.Fatalf("Read with tiny maxBytes = %+v, %v; want one record", c, err)
	}
	c.Close()
	c, err = p.Read(0, 10, 2*recordSize)
	if err != nil || c.NextOffset != 2 {
		t.Fatalf("Read with room for two = %+v, %v", c, err)
	}
	c.Close()
}

func TestSegmentRollAndReopen(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentBytes = int64(2 * recordSize) // two records per segment
	p := mustOpen(t, cfg)

	appendBodies(t, p, 0, 7)
	bases, err := segment.List(cfg.Dir)
	if err != nil || len(bases) != 4 {
		t.Fatalf("segments = %v, %v; want 4", bases, err)
	}
	c, err := p.Read(0, 100, 1<<20)
	if err != nil || c.NextOffset != 2 {
		t.Fatalf("Read(0) = %+v, %v; a chunk must stop at the segment end", c, err)
	}
	c.Close()
	if _, chunks := readAll(t, p); chunks != 4 {
		t.Fatalf("read took %d chunks, want one per segment (4)", chunks)
	}

	p = reopen(t, p)
	assertLog(t, p, 7)
	appendBodies(t, p, 7, 9)
	assertLog(t, p, 9)
}

func TestReadFromMiddleOfSegmentUsesIndex(t *testing.T) {
	cfg := testConfig(t)
	cfg.IndexIntervalBytes = 2 * recordSize
	p := mustOpen(t, cfg)
	appendBodies(t, p, 0, 100)
	if fileSize(t, indexPath(cfg.Dir, 0)) == 0 {
		t.Fatal("expected sparse index entries on disk")
	}

	for _, off := range []int64{1, 37, 38, 99} {
		c, err := p.Read(off, 1, 1<<20)
		if err != nil {
			t.Fatalf("Read(%d): %v", off, err)
		}
		recs := decodeChunk(t, c)
		if len(recs) != 1 || recs[0].Offset != off || string(recs[0].Body) != string(body(int(off))) {
			t.Fatalf("Read(%d) returned %+v", off, recs)
		}
	}
}

func TestChunkWriteToMatchesBytes(t *testing.T) {
	p := mustOpen(t, testConfig(t))
	appendBodies(t, p, 0, 10)
	c, err := p.Read(2, 5, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	want, err := c.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	for range 2 { // WriteTo can be repeated: it seeks to Position each time
		var got bytes.Buffer
		n, err := c.WriteTo(&got)
		if err != nil || n != c.Size || !bytes.Equal(got.Bytes(), want) {
			t.Fatalf("WriteTo wrote %d bytes, err %v; differs from Bytes", n, err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestZeroChunkCloseIsSafe(t *testing.T) {
	if err := (Chunk{}).Close(); err != nil {
		t.Fatal(err)
	}
}

func TestIdempotentProducer(t *testing.T) {
	p := mustOpen(t, testConfig(t))
	const pid = 42

	for seq := int64(0); seq < 8; seq++ {
		if res := appendSeq(p, pid, seq); res.Err != nil || res.Offset != seq || res.Duplicate {
			t.Fatalf("seq %d: %+v", seq, res)
		}
	}
	t.Run("duplicate in window re-acks original offset", func(t *testing.T) {
		if res := appendSeq(p, pid, 6); res.Err != nil || res.Offset != 6 || !res.Duplicate {
			t.Fatalf("dup seq 6: %+v", res)
		}
	})
	t.Run("duplicate older than window re-acks -1", func(t *testing.T) {
		if res := appendSeq(p, pid, 1); res.Err != nil || res.Offset != -1 || !res.Duplicate {
			t.Fatalf("old dup seq 1: %+v", res)
		}
	})
	t.Run("gap is rejected", func(t *testing.T) {
		if res := appendSeq(p, pid, 10); !errors.Is(res.Err, ErrOutOfOrderSeq) {
			t.Fatalf("gap: %+v", res)
		}
	})
	t.Run("negative sequence is rejected", func(t *testing.T) {
		for _, pid := range []int64{pid, 77} { // known and new producer
			if res := appendSeq(p, pid, -1); !errors.Is(res.Err, ErrOutOfOrderSeq) {
				t.Fatalf("producer %d seq -1: %+v", pid, res)
			}
		}
	})
	t.Run("new producer must start at 0", func(t *testing.T) {
		if res := appendSeq(p, 99, 3); !errors.Is(res.Err, ErrOutOfOrderSeq) {
			t.Fatalf("new producer seq 3: %+v", res)
		}
	})
	t.Run("no producer id skips dedup", func(t *testing.T) {
		for range 2 {
			res := p.AppendSync(context.Background(), AppendRequest{ProducerID: record.NoProducerID, Sequence: 0, Body: body(0)})
			if res.Err != nil || res.Duplicate {
				t.Fatalf("no-pid append: %+v", res)
			}
		}
	})
	if hw := p.HighWatermark(); hw != 10 {
		t.Fatalf("high watermark = %d, want 10 (duplicates must not be written)", hw)
	}
}

func TestMessageTooLarge(t *testing.T) {
	p := mustOpen(t, testConfig(t))
	res := p.AppendSync(context.Background(), AppendRequest{ProducerID: record.NoProducerID, Body: make([]byte, record.MaxBodySize+1)})
	if !errors.Is(res.Err, ErrMessageTooLarge) {
		t.Fatalf("got %+v", res)
	}
}

func TestAckAfterFsyncHidesUnflushedData(t *testing.T) {
	cfg := testConfig(t)
	cfg.FlushInterval = time.Hour   // only FlushBytes triggers a flush
	cfg.FlushBytes = 2 * recordSize // ...on the second distinct record
	p := mustOpen(t, cfg)

	acks := make(chan AppendResult, 3)
	ack := func(r AppendResult) { acks <- r }
	p.Append(AppendRequest{ProducerID: 1, Sequence: 0, Body: body(0)}, ack)
	p.Append(AppendRequest{ProducerID: 1, Sequence: 0, Body: body(0)}, ack) // duplicate of unflushed data

	// A gap is rejected without waiting for a flush; since the queue is
	// FIFO, once it returns the writer has handled both requests above.
	if res := appendSeq(p, 2, 5); !errors.Is(res.Err, ErrOutOfOrderSeq) {
		t.Fatalf("gap probe: %+v", res)
	}
	if fileSize(t, segmentPath(cfg.Dir, 0)) != int64(recordSize) {
		t.Fatal("record should already be written to the file")
	}
	if len(acks) != 0 || p.HighWatermark() != 0 {
		t.Fatalf("before fsync: %d acks, high watermark %d; want none", len(acks), p.HighWatermark())
	}
	if c, err := p.Read(0, 10, 1<<20); err != nil || c.Size != 0 {
		t.Fatalf("Read before fsync = %+v, %v; want empty", c, err)
	} else {
		c.Close()
	}

	p.Append(AppendRequest{ProducerID: 1, Sequence: 1, Body: body(1)}, ack) // reaches FlushBytes
	want := []AppendResult{{Offset: 0}, {Offset: 0, Duplicate: true}, {Offset: 1}}
	for i, w := range want {
		if got := <-acks; got != w {
			t.Fatalf("ack %d = %+v, want %+v", i, got, w)
		}
	}
	if p.HighWatermark() != 2 {
		t.Fatalf("high watermark after fsync = %d, want 2", p.HighWatermark())
	}
}

func TestAckBeforeFsyncMakesDataReadableImmediately(t *testing.T) {
	cfg := testConfig(t)
	cfg.AckMode = AckBeforeFsync
	cfg.FlushInterval = time.Hour
	p := mustOpen(t, cfg)

	appendBodies(t, p, 0, 3) // would hang if acks waited for the hour-long flush
	assertLog(t, p, 3)
	p = reopen(t, p) // Close still flushes
	assertLog(t, p, 3)
}

// TestReadNeverAheadOfAck checks that every record a reader sees has
// already been acked to its producer, in both ack modes.
func TestReadNeverAheadOfAck(t *testing.T) {
	for _, mode := range []AckMode{AckAfterFsync, AckBeforeFsync} {
		t.Run(map[AckMode]string{AckAfterFsync: "after fsync", AckBeforeFsync: "before fsync"}[mode], func(t *testing.T) {
			cfg := testConfig(t)
			cfg.AckMode = mode
			p := mustOpen(t, cfg)

			const n = 300
			var acked [n]atomic.Bool
			go func() {
				for i := range n {
					p.Append(AppendRequest{ProducerID: record.NoProducerID, Body: body(i)}, func(r AppendResult) {
						acked[r.Offset].Store(true)
					})
				}
			}()

			for off := int64(0); off < n; {
				if err := p.Wait(context.Background(), off); err != nil {
					t.Fatal(err)
				}
				c, err := p.Read(off, 50, 1<<20)
				if err != nil {
					t.Fatal(err)
				}
				for _, r := range decodeChunk(t, c) {
					if !acked[r.Offset].Load() {
						t.Fatalf("offset %d readable before its ack", r.Offset)
					}
				}
				off = c.NextOffset
			}
		})
	}
}

func TestWait(t *testing.T) {
	t.Run("returns when data arrives", func(t *testing.T) {
		p := mustOpen(t, testConfig(t))
		errc := make(chan error, 1)
		go func() { errc <- p.Wait(context.Background(), 0) }()
		appendBodies(t, p, 0, 1)
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	})
	t.Run("returns ctx error on timeout", func(t *testing.T) {
		p := mustOpen(t, testConfig(t))
		appendBodies(t, p, 0, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if err := p.Wait(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Wait = %v, want DeadlineExceeded", err)
		}
	})
	t.Run("returns ErrClosed on Close", func(t *testing.T) {
		p := mustOpen(t, testConfig(t))
		errc := make(chan error, 1)
		go func() { errc <- p.Wait(context.Background(), 0) }()
		p.Close()
		if err := <-errc; !errors.Is(err, ErrClosed) {
			t.Fatalf("Wait = %v, want ErrClosed", err)
		}
	})
}

func TestCloseIsIdempotentAndRejectsAppends(t *testing.T) {
	p := mustOpen(t, testConfig(t))
	appendBodies(t, p, 0, 2)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if res := appendSeq(p, 1, 0); !errors.Is(res.Err, ErrClosed) {
		t.Fatalf("append after Close: %+v", res)
	}
	if _, err := p.Read(0, 1, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Read after Close: %v", err)
	}
	if _, err := os.Stat(checkpointPath(p.cfg.Dir)); err != nil {
		t.Fatalf("Close should leave a checkpoint: %v", err)
	}
}
