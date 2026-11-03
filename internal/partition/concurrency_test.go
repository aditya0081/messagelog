package partition

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"messagelog/internal/record"
)

// TestConcurrentAppendersAndReaders runs producers and tailing consumers at
// once (with segment rolls) and checks every reader sees one gap-free log in
// which each producer's records are in sequence order. Run under -race.
func TestConcurrentAppendersAndReaders(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentBytes = 4096
	cfg.IndexIntervalBytes = 256
	p := mustOpen(t, cfg)

	const producers, perProducer, readers = 4, 250, 3
	const total = producers * perProducer

	var wg sync.WaitGroup
	for pid := int64(1); pid <= producers; pid++ {
		wg.Go(func() {
			for seq := int64(0); seq < perProducer; seq++ {
				res := p.AppendSync(context.Background(), AppendRequest{
					ProducerID: pid, Sequence: seq, Body: fmt.Appendf(nil, "p%d-s%d", pid, seq),
				})
				if res.Err != nil {
					t.Errorf("producer %d seq %d: %v", pid, seq, res.Err)
					return
				}
			}
		})
	}
	for range readers {
		wg.Go(func() { tailLog(t, p, total) })
	}
	wg.Wait()
}

// tailLog follows the log with Wait + Read until it has seen n records.
func tailLog(t *testing.T, p *Partition, n int64) {
	nextSeq := map[int64]int64{}
	for off := int64(0); off < n; {
		if err := p.Wait(context.Background(), off); err != nil {
			t.Error(err)
			return
		}
		c, err := p.Read(off, 37, 1000)
		if err != nil {
			t.Errorf("Read(%d): %v", off, err)
			return
		}
		b, err := c.Bytes()
		c.Close()
		if err != nil {
			t.Error(err)
			return
		}
		recs, err := record.DecodeAll(b)
		if err != nil {
			t.Errorf("decode at %d: %v", off, err)
			return
		}
		for _, r := range recs {
			want := fmt.Sprintf("p%d-s%d", r.ProducerID, nextSeq[r.ProducerID])
			if r.Offset != off || string(r.Body) != want {
				t.Errorf("offset %d: got offset %d body %q, want %q", off, r.Offset, r.Body, want)
				return
			}
			nextSeq[r.ProducerID]++
			off++
		}
	}
}
