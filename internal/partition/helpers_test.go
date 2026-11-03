package partition

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"messagelog/internal/record"
)

// crashForTest simulates the process dying: the writer stops on the spot
// and files are closed without a final flush or checkpoint. Whatever was
// already written stays in the files, exactly like a real process crash.
func (p *Partition) crashForTest() {
	p.closeOnce.Do(func() {
		p.closeMu.Lock()
		p.closed.Store(true)
		p.closeMu.Unlock()
		close(p.writer.abort)
		<-p.writerDone
		p.closeErr = p.stopBackground()
	})
}

// testConfig is a fast config: tiny flush interval, background jobs that
// effectively never fire on their own, and a first checkpoint only on the
// first flush (lastCheckpoint starts at the zero time).
func testConfig(t *testing.T) Config {
	return Config{
		Dir:                t.TempDir(),
		FlushInterval:      time.Millisecond,
		RetentionCheck:     time.Hour,
		CheckpointInterval: time.Hour,
	}
}

func mustOpen(t *testing.T, cfg Config) *Partition {
	t.Helper()
	p, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// reopen closes p cleanly and opens the same directory again.
func reopen(t *testing.T, p *Partition) *Partition {
	t.Helper()
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return mustOpen(t, p.cfg)
}

// crashAndReopen crashes p, lets tamper edit the files, and opens again.
func crashAndReopen(t *testing.T, p *Partition, tamper func(dir string)) *Partition {
	t.Helper()
	p.crashForTest()
	if tamper != nil {
		tamper(p.cfg.Dir)
	}
	return mustOpen(t, p.cfg)
}

func body(i int) []byte { return fmt.Appendf(nil, "message-%04d", i) }

// recordSize is the encoded size of a record carrying body(i).
var recordSize = record.HeaderSize + len(body(0))

// appendBodies appends body(from)..body(to-1) without dedup and waits until
// they are readable (the writer acks a batch just before it advances the
// high watermark).
func appendBodies(t *testing.T, p *Partition, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		res := p.AppendSync(context.Background(), AppendRequest{ProducerID: record.NoProducerID, Body: body(i)})
		if res.Err != nil || res.Offset != int64(i) {
			t.Fatalf("append %d: offset=%d err=%v", i, res.Offset, res.Err)
		}
	}
	if to > from {
		if err := p.Wait(context.Background(), int64(to-1)); err != nil {
			t.Fatalf("wait for offset %d: %v", to-1, err)
		}
	}
}

func appendSeq(p *Partition, pid, seq int64) AppendResult {
	return p.AppendSync(context.Background(), AppendRequest{ProducerID: pid, Sequence: seq, Body: body(int(seq))})
}

// decodeChunk returns the records in c and closes it.
func decodeChunk(t *testing.T, c Chunk) []record.Record {
	t.Helper()
	defer c.Close()
	b, err := c.Bytes()
	if err != nil {
		t.Fatalf("chunk bytes: %v", err)
	}
	recs, err := record.DecodeAll(b)
	if err != nil {
		t.Fatalf("decode chunk: %v", err)
	}
	return recs
}

// readAll reads every record from LogStartOffset to HighWatermark and
// returns them with the number of chunks it took.
func readAll(t *testing.T, p *Partition) (recs []record.Record, chunks int) {
	t.Helper()
	for off := p.LogStartOffset(); off < p.HighWatermark(); chunks++ {
		c, err := p.Read(off, 1000, 1<<20)
		if err != nil {
			t.Fatalf("Read(%d): %v", off, err)
		}
		recs = append(recs, decodeChunk(t, c)...)
		off = c.NextOffset
	}
	return recs, chunks
}

// assertLog checks the log holds exactly body(0)..body(n-1) at offsets 0..n-1.
func assertLog(t *testing.T, p *Partition, n int) {
	t.Helper()
	if hw := p.HighWatermark(); hw != int64(n) {
		t.Fatalf("high watermark = %d, want %d", hw, n)
	}
	recs, _ := readAll(t, p)
	if len(recs) != n {
		t.Fatalf("read %d records, want %d", len(recs), n)
	}
	for i, r := range recs {
		if r.Offset != int64(i) || string(r.Body) != string(body(i)) {
			t.Fatalf("record %d: offset=%d body=%q", i, r.Offset, r.Body)
		}
	}
}

func segmentPath(dir string, base int64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.log", base))
}

func indexPath(dir string, base int64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.index", base))
}

func checkpointPath(dir string) string {
	return filepath.Join(dir, checkpointFile)
}

func appendToFile(t *testing.T, path string, data []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

// fakeClock is a settable Config.Now.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
