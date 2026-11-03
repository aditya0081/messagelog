package server

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"messagelog/client"
)

// A Publish whose caller gives up only stops waiting. The message keeps its
// place and is delivered in the background, so younger messages are not
// stuck behind a gap and the producer id does not change.
func TestAbandonedPublishIsStillDelivered(t *testing.T) {
	tb := startBroker(t, t.TempDir(), "127.0.0.1:0")
	createTopic(t, tb.addr, "orders")
	px := newFaultProxy(t, tb.addr, 0)
	p := newProducerWith(t, px.addr(), client.ProducerConfig{})
	mustPublish(t, p, "orders", "warmup")

	px.blackhole.Store(true) // A and B are lost on the wire
	ctxA, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	errA := make(chan error, 1)
	go func() { _, err := p.Publish(ctxA, "orders", []byte("A")); errA <- err }()
	time.Sleep(20 * time.Millisecond)
	errB := make(chan error, 1)
	go func() { _, err := p.Publish(testCtx(t), "orders", []byte("B")); errB <- err }()
	if err := <-errA; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish A = %v, want DeadlineExceeded", err)
	}

	px.killAll(0)
	px.blackhole.Store(false)
	start := time.Now()
	mustPublish(t, p, "orders", "C")
	if took := time.Since(start); took > time.Second {
		t.Errorf("Publish C took %v after an abandoned publish, want well under 1s", took)
	}
	if err := <-errB; err != nil {
		t.Fatalf("Publish B: %v", err)
	}

	// C was acked after A and B (in-order delivery), so the log is final.
	records := readAll(t, newConsumer(t, tb.addr), "orders")
	if got, want := bodies(records), []string{"warmup", "A", "B", "C"}; !slices.Equal(got, want) {
		t.Fatalf("log = %q, want %q", got, want)
	}
	for _, r := range records {
		if r.ProducerID != records[0].ProducerID {
			t.Fatalf("producer id changed from %d to %d at offset %d", records[0].ProducerID, r.ProducerID, r.Offset)
		}
	}
}

// A message that cannot be delivered within DeliveryTimeout fails. The
// next message hits the sequence gap it left (OutOfOrderSequence), so the
// stream takes a new producer id and renumbers from 0 instead of stalling.
func TestDeliveryTimeoutStartsNewProducerID(t *testing.T) {
	tb := startBroker(t, t.TempDir(), "127.0.0.1:0")
	createTopic(t, tb.addr, "orders")
	px := newFaultProxy(t, tb.addr, 0)
	p := newProducerWith(t, px.addr(), client.ProducerConfig{
		RetryBackoff:    10 * time.Millisecond,
		DeliveryTimeout: 300 * time.Millisecond,
	})
	mustPublish(t, p, "orders", "warmup")

	px.blackhole.Store(true)
	if _, err := p.Publish(testCtx(t), "orders", []byte("lost")); !errors.Is(err, client.ErrDeliveryTimeout) {
		t.Fatalf("Publish while blackholed = %v, want ErrDeliveryTimeout", err)
	}
	px.killAll(0)
	px.blackhole.Store(false)

	mustPublish(t, p, "orders", "after")
	records := readAll(t, newConsumer(t, tb.addr), "orders")
	if got, want := bodies(records), []string{"warmup", "after"}; !slices.Equal(got, want) {
		t.Fatalf("log = %q, want %q", got, want)
	}
	if after := records[1]; after.ProducerID == records[0].ProducerID || after.Sequence != 0 {
		t.Fatalf("after the failure: producer %d seq %d, want a new producer id at seq 0 (old id %d)",
			after.ProducerID, after.Sequence, records[0].ProducerID)
	}
}

// A ctx that is already done means nothing is sent at all.
func TestPublishWithDoneContextSendsNothing(t *testing.T) {
	tb := startBroker(t, t.TempDir(), "127.0.0.1:0")
	createTopic(t, tb.addr, "orders")
	p := newProducer(t, tb.addr)
	mustPublish(t, p, "orders", "first")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := p.Publish(ctx, "orders", []byte("ghost")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish with a cancelled ctx = %v, want context.Canceled", err)
	}
	mustPublish(t, p, "orders", "second") // acked after anything sent before it

	if got, want := bodies(readAll(t, newConsumer(t, tb.addr), "orders")), []string{"first", "second"}; !slices.Equal(got, want) {
		t.Fatalf("log = %q, want %q", got, want)
	}
}

// byValue routes a message to the partition named by its first byte ('0', '1', ...).
type byValue struct{}

func (byValue) Partition(_ string, value []byte, _ int) int32 { return int32(value[0] - '0') }

// Right after a broker restart the producer's connection still looks
// healthy and fails on first use. The Metadata lookup and InitProducer must
// retry on a new connection instead of failing the Publish.
func TestProducerRetriesOnDeadConnection(t *testing.T) {
	tb := startBroker(t, t.TempDir(), "127.0.0.1:0")
	if err := dial(t, tb.addr).CreateTopic(testCtx(t), "orders", 2); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	px := newFaultProxy(t, tb.addr, 0)
	p := newProducerWith(t, px.addr(), client.ProducerConfig{Partitioner: byValue{}, RetryBackoff: 10 * time.Millisecond})

	px.waitForLinks(t, 1)
	px.killSilently()
	mustPublish(t, p, "orders", "0-first") // Metadata hits the dead connection

	px.waitForLinks(t, 1)
	px.killSilently()
	mustPublish(t, p, "orders", "1-first") // partition 1's InitProducer hits it
}
