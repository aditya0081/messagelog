package partition

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"messagelog/internal/record"
	"messagelog/internal/segment"
)

func assertSegments(t *testing.T, dir string, want string) {
	t.Helper()
	bases, err := segment.List(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(bases); got != want {
		t.Fatalf("segments on disk = %s, want %s", got, want)
	}
}

func TestRetentionByAge(t *testing.T) {
	clock := newFakeClock()
	cfg := testConfig(t)
	cfg.Now = clock.Now
	cfg.SegmentBytes = int64(2 * recordSize)
	cfg.RetentionAge = time.Hour
	p := mustOpen(t, cfg)

	appendBodies(t, p, 0, 4) // segments 0 and 2, both stamped "now"
	clock.Advance(2 * time.Hour)
	appendBodies(t, p, 4, 5) // rolls: segment 4 is active and fresh

	if err := p.EnforceRetention(); err != nil {
		t.Fatal(err)
	}
	assertSegments(t, cfg.Dir, "[4]")
	if p.LogStartOffset() != 4 {
		t.Fatalf("LogStartOffset = %d, want 4", p.LogStartOffset())
	}
	if _, err := p.Read(3, 1, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("Read below log start: %v", err)
	}
	if c, err := p.Read(4, 1, 1<<20); err != nil || c.FirstOffset != 4 {
		t.Fatalf("Read(4) = %+v, %v", c, err)
	} else {
		c.Close()
	}

	clock.Advance(24 * time.Hour) // the active segment is expired too...
	if err := p.EnforceRetention(); err != nil {
		t.Fatal(err)
	}
	assertSegments(t, cfg.Dir, "[4]") // ...but never deleted

	p = reopen(t, p)
	if p.LogStartOffset() != 4 || p.HighWatermark() != 5 {
		t.Fatalf("after reopen: start=%d hw=%d", p.LogStartOffset(), p.HighWatermark())
	}
}

func TestRetentionBySize(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentBytes = int64(2 * recordSize)
	cfg.RetentionBytes = int64(5 * recordSize)
	p := mustOpen(t, cfg)

	appendBodies(t, p, 0, 10) // 5 full segments, 10 records
	if err := p.EnforceRetention(); err != nil {
		t.Fatal(err)
	}
	// Oldest first until at most 5 records' worth remain: 3 segments go.
	assertSegments(t, cfg.Dir, "[6 8]")
	if p.LogStartOffset() != 6 {
		t.Fatalf("LogStartOffset = %d, want 6", p.LogStartOffset())
	}
	recs, _ := readAll(t, p)
	if len(recs) != 4 || recs[0].Offset != 6 {
		t.Fatalf("read %d records starting at %d", len(recs), recs[0].Offset)
	}
}

// TestReadRacingRetention checks readers get ErrOffsetOutOfRange, never a
// panic or garbage, when the segment they read is deleted under them.
func TestReadRacingRetention(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentBytes = int64(2 * recordSize)
	cfg.RetentionBytes = int64(2 * recordSize)
	p := mustOpen(t, cfg)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 100 {
			req := AppendRequest{ProducerID: record.NoProducerID, Body: body(i)}
			if res := p.AppendSync(context.Background(), req); res.Err != nil {
				t.Error(res.Err)
				return
			}
			if err := p.EnforceRetention(); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for {
		select {
		case <-done:
			return
		default:
		}
		off := p.LogStartOffset()
		c, err := p.Read(off, 10, 1<<20)
		if errors.Is(err, ErrOffsetOutOfRange) {
			continue
		}
		if err != nil {
			t.Fatalf("Read(%d): %v", off, err)
		}
		recs := decodeChunk(t, c) // must succeed even if the segment is gone by now
		if len(recs) > 0 && recs[0].Offset != off {
			t.Fatalf("chunk starts at %d, want %d", recs[0].Offset, off)
		}
	}
}

// TestChunkSurvivesRetention: a chunk holds the segment file open, so the
// bytes Read promised can still be sent after retention deletes the file.
func TestChunkSurvivesRetention(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentBytes = int64(2 * recordSize)
	cfg.RetentionBytes = int64(2 * recordSize)
	p := mustOpen(t, cfg)
	appendBodies(t, p, 0, 5) // segments 0, 2, 4

	c, err := p.Read(0, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := p.EnforceRetention(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(segmentPath(cfg.Dir, 0)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("segment 0 should be deleted: %v", err)
	}

	var buf bytes.Buffer
	if n, err := c.WriteTo(&buf); err != nil || n != int64(2*recordSize) {
		t.Fatalf("WriteTo after retention = %d, %v; want %d bytes", n, err, 2*recordSize)
	}
	recs, err := record.DecodeAll(buf.Bytes())
	if err != nil || len(recs) != 2 || string(recs[1].Body) != string(body(1)) {
		t.Fatalf("decoded %d records, err %v", len(recs), err)
	}
	if _, err := p.Read(0, 10, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("Read after retention: %v, want ErrOffsetOutOfRange", err)
	}
}

// TestReadOfVanishedSegmentFileIsOutOfRange covers retention deleting the
// file between Read finding the segment and opening it.
func TestReadOfVanishedSegmentFileIsOutOfRange(t *testing.T) {
	p := mustOpen(t, testConfig(t))
	appendBodies(t, p, 0, 3)
	if err := os.Remove(segmentPath(p.cfg.Dir, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Read(0, 10, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("Read = %v, want ErrOffsetOutOfRange", err)
	}
}

// TestRetentionExpiresProducers: once retention has deleted all of a
// producer's records it is forgotten (Kafka's rule), so the producer table
// and the checkpoint do not grow forever.
func TestRetentionExpiresProducers(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentBytes = int64(2 * recordSize)
	cfg.RetentionBytes = int64(2 * recordSize)
	p := mustOpen(t, cfg)

	const oldPID, livePID = 7, 8
	for seq := int64(0); seq < 2; seq++ { // fills segment 0
		if res := appendSeq(p, oldPID, seq); res.Err != nil {
			t.Fatal(res.Err)
		}
	}
	appendBodies(t, p, 2, 4) // segment 2
	if res := appendSeq(p, livePID, 0); res.Err != nil {
		t.Fatal(res.Err)
	}
	if err := p.EnforceRetention(); err != nil { // deletes segments 0 and 2
		t.Fatal(err)
	}
	if err := p.Close(); err != nil { // writes a checkpoint
		t.Fatal(err)
	}

	cp, ok, err := readCheckpoint(cfg.Dir)
	if err != nil || !ok {
		t.Fatalf("readCheckpoint: %v, %v", ok, err)
	}
	if _, found := cp.Producers[oldPID]; found {
		t.Fatalf("producer %d survived retention of all its records", oldPID)
	}
	if _, found := cp.Producers[livePID]; !found {
		t.Fatalf("producer %d with live records was expired", livePID)
	}

	p = mustOpen(t, cfg)
	if res := appendSeq(p, oldPID, 2); !errors.Is(res.Err, ErrOutOfOrderSeq) {
		t.Fatalf("expired producer continuing at seq 2: %+v, want ErrOutOfOrderSeq", res)
	}
	if res := appendSeq(p, oldPID, 0); res.Err != nil || res.Duplicate {
		t.Fatalf("expired producer restarting at seq 0: %+v", res)
	}
}
