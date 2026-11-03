package server

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"messagelog/client"
)

// publishOutcome is what one Publish call in the chaos test returned.
type publishOutcome struct {
	value string
	res   client.PublishResult
	err   error
}

// Many goroutines share one Producer while a proxy kills every connection
// every 20-60ms (and still delivers the killed connections' last bytes up
// to 30ms late), and 10% of the calls give up after a few milliseconds.
// Nothing may be written twice, every acked message must be in the log once
// at its acked offset, each goroutine's acked messages must be in its call
// order, and the producer id must never change (no message failed for good).
func TestChaosExactlyOnce(t *testing.T) {
	tb := startBroker(t, t.TempDir(), "127.0.0.1:0")
	createTopic(t, tb.addr, "orders")
	px := newFaultProxy(t, tb.addr, 3*time.Millisecond)
	p := newProducerWith(t, px.addr(), client.ProducerConfig{RetryBackoff: 2 * time.Millisecond})

	stopKilling := make(chan struct{})
	killerDone := make(chan struct{})
	go func() {
		defer close(killerDone)
		for {
			select {
			case <-stopKilling:
				return
			case <-time.After(time.Duration(20+rand.IntN(40)) * time.Millisecond):
				px.killAll(time.Duration(rand.IntN(30)) * time.Millisecond)
			}
		}
	}()

	const goroutines, perGoroutine = 16, 150
	outcomes := make([][]publishOutcome, goroutines)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range perGoroutine {
				value := fmt.Sprintf("g%d-%d", g, i)
				timeout := 30 * time.Second
				if rand.IntN(10) == 0 {
					timeout = time.Duration(rand.IntN(8)) * time.Millisecond
				}
				ctx, cancel := context.WithTimeout(t.Context(), timeout)
				res, err := p.Publish(ctx, "orders", []byte(value))
				cancel()
				outcomes[g] = append(outcomes[g], publishOutcome{value, res, err})
			}
		})
	}
	wg.Wait()
	close(stopKilling)
	<-killerDone

	// Messages are resolved in order, so once this one is acked every
	// earlier one is resolved too, abandoned ones included.
	mustPublish(t, p, "orders", "last")

	records := readAll(t, newConsumer(t, tb.addr), "orders")
	count := map[string]int{}
	offsetOf := map[string]int64{}
	for _, r := range records {
		count[string(r.Body)]++
		offsetOf[string(r.Body)] = r.Offset
		if r.ProducerID != records[0].ProducerID {
			t.Errorf("offset %d: producer id %d, want %d throughout", r.Offset, r.ProducerID, records[0].ProducerID)
		}
	}

	var acked, failed int
	for g, calls := range outcomes {
		lastOffset := int64(-1)
		for _, o := range calls {
			if count[o.value] > 1 {
				t.Errorf("%s is in the log %d times", o.value, count[o.value])
			}
			if o.err != nil {
				failed++
				continue
			}
			acked++
			if count[o.value] != 1 {
				t.Errorf("acked %s (%+v) is in the log %d times", o.value, o.res, count[o.value])
				continue
			}
			at := offsetOf[o.value]
			if o.res.Offset != -1 && o.res.Offset != at {
				t.Errorf("%s acked at offset %d but is at %d", o.value, o.res.Offset, at)
			}
			if at < lastOffset {
				t.Errorf("g%d: %s at offset %d, before an earlier acked message at %d", g, o.value, at, lastOffset)
			}
			lastOffset = at
		}
	}
	t.Logf("%d records; %d publishes acked, %d gave up", len(records), acked, failed)
}
