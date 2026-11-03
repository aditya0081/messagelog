package server

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"messagelog/client"
	"messagelog/internal/protocol"
	"messagelog/internal/record"
)

// readAll reads a partition from offset 0 up to its high watermark.
func readAll(t *testing.T, c *client.Consumer, topic string) []record.Record {
	t.Helper()
	var all []record.Record
	for offset := int64(0); ; {
		records, hw, err := c.Consume(testCtx(t), topic, 0, offset, 1000, 0)
		if err != nil {
			t.Fatalf("Consume(%s, %d): %v", topic, offset, err)
		}
		all = append(all, records...)
		offset += int64(len(records))
		if offset >= hw {
			return all
		}
	}
}

func TestPublishConsumeMetadata(t *testing.T) {
	tb := startBroker(t, t.TempDir(), "127.0.0.1:0")
	createTopic(t, tb.addr, "orders")
	p := newProducer(t, tb.addr)

	const n = 20
	for i := range n {
		res, err := p.Publish(testCtx(t), "orders", fmt.Appendf(nil, "msg-%d", i))
		if err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
		if res.Offset != int64(i) || res.Duplicate {
			t.Fatalf("Publish %d = %+v, want offset %d", i, res, i)
		}
	}

	records := readAll(t, newConsumer(t, tb.addr), "orders")
	if len(records) != n {
		t.Fatalf("read %d records, want %d", len(records), n)
	}
	for i, r := range records {
		if r.Offset != int64(i) || string(r.Body) != fmt.Sprintf("msg-%d", i) {
			t.Fatalf("record %d = offset %d %q", i, r.Offset, r.Body)
		}
	}

	md, err := dial(t, tb.addr).Metadata(testCtx(t))
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if len(md.Brokers) != 1 || md.Brokers[0].ID != 1 {
		t.Fatalf("brokers = %+v, want broker 1", md.Brokers)
	}
	want := protocol.TopicInfo{Name: "orders", Partitions: []protocol.PartitionInfo{{Number: 0, LeaderID: 1}}}
	if len(md.Topics) != 1 || fmt.Sprint(md.Topics[0]) != fmt.Sprint(want) {
		t.Fatalf("topics = %+v, want %+v", md.Topics, want)
	}
}

// Many goroutines share one Producer, so requests are pipelined on one
// connection. Every publish must be acked once, and the log must hold the
// producer's sequences in order.
func TestPipelinedProducer(t *testing.T) {
	tb := startBroker(t, t.TempDir(), "127.0.0.1:0")
	createTopic(t, tb.addr, "orders")
	p := newProducer(t, tb.addr)

	const goroutines, perGoroutine = 8, 100
	offsets := make(chan int64, goroutines*perGoroutine)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range perGoroutine {
				res, err := p.Publish(testCtx(t), "orders", fmt.Appendf(nil, "g%d-%d", g, i))
				if err != nil {
					t.Errorf("Publish: %v", err)
					return
				}
				offsets <- res.Offset
			}
		})
	}
	wg.Wait()
	close(offsets)

	seen := map[int64]bool{}
	for o := range offsets {
		if seen[o] {
			t.Fatalf("offset %d acked twice", o)
		}
		seen[o] = true
	}
	for o := range int64(goroutines * perGoroutine) {
		if !seen[o] {
			t.Fatalf("offset %d never acked", o)
		}
	}

	records := readAll(t, newConsumer(t, tb.addr), "orders")
	for i := 1; i < len(records); i++ {
		prev, cur := records[i-1], records[i]
		if cur.ProducerID != prev.ProducerID || cur.Sequence <= prev.Sequence {
			t.Fatalf("offset %d: producer %d seq %d after producer %d seq %d", cur.Offset, cur.ProducerID, cur.Sequence, prev.ProducerID, prev.Sequence)
		}
	}
}

func initProducer(t *testing.T, conn *client.Conn) int64 {
	t.Helper()
	var resp protocol.InitProducerResponse
	if err := conn.Do(testCtx(t), protocol.APIInitProducer, nil, &resp); err != nil {
		t.Fatalf("InitProducer: %v", err)
	}
	return resp.ProducerID
}

func rawPublish(t *testing.T, conn *client.Conn, pid, seq int64, value string) (protocol.PublishResponse, error) {
	t.Helper()
	req := &protocol.PublishRequest{Topic: "orders", ProducerID: pid, Sequence: seq, Value: []byte(value)}
	var resp protocol.PublishResponse
	err := conn.Do(testCtx(t), protocol.APIPublish, req, &resp)
	return resp, err
}

func mustRawPublish(t *testing.T, conn *client.Conn, pid, seq int64, want protocol.PublishResponse) {
	t.Helper()
	got, err := rawPublish(t, conn, pid, seq, fmt.Sprintf("seq-%d", seq))
	if err != nil || got != want {
		t.Fatalf("publish seq %d = (%+v, %v), want %+v", seq, got, err, want)
	}
}

func TestIdempotentPublishOverWire(t *testing.T) {
	tb := startBroker(t, t.TempDir(), "127.0.0.1:0")
	createTopic(t, tb.addr, "orders")
	conn := dial(t, tb.addr)
	pid := initProducer(t, conn)

	mustRawPublish(t, conn, pid, 0, protocol.PublishResponse{Offset: 0})
	mustRawPublish(t, conn, pid, 0, protocol.PublishResponse{Offset: 0, Duplicate: true})
	if _, err := rawPublish(t, conn, pid, 2, "gap"); !errors.Is(err, client.ErrOutOfOrderSequence) {
		t.Fatalf("publish after a gap = %v, want OutOfOrderSequence", err)
	}

	if records := readAll(t, newConsumer(t, tb.addr), "orders"); len(records) != 1 {
		t.Fatalf("log has %d records, want 1", len(records))
	}
}

func TestLongPoll(t *testing.T) {
	tb := startBroker(t, t.TempDir(), "127.0.0.1:0")
	createTopic(t, tb.addr, "orders")
	c := newConsumer(t, tb.addr)

	t.Run("returns when data arrives", func(t *testing.T) {
		type result struct {
			records []record.Record
			err     error
		}
		done := make(chan result, 1)
		go func() {
			records, _, err := c.Consume(testCtx(t), "orders", 0, 0, 10, 10*time.Second)
			done <- result{records, err}
		}()
		if _, err := newProducer(t, tb.addr).Publish(testCtx(t), "orders", []byte("late")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		r := <-done
		if r.err != nil || len(r.records) != 1 || string(r.records[0].Body) != "late" {
			t.Fatalf("Consume = (%v, %v), want the late record", r.records, r.err)
		}
	})

	t.Run("times out empty", func(t *testing.T) {
		start := time.Now()
		records, hw, err := c.Consume(testCtx(t), "orders", 0, 1, 10, 100*time.Millisecond)
		if err != nil || len(records) != 0 || hw != 1 {
			t.Fatalf("Consume = (%v, %d, %v), want no records, hw 1", records, hw, err)
		}
		if waited := time.Since(start); waited < 100*time.Millisecond {
			t.Fatalf("returned after %v, want a 100ms wait", waited)
		}
	})
}

// Data, committed offsets and producer dedup state all survive a restart.
func TestRestartKeepsState(t *testing.T) {
	tb := startBroker(t, t.TempDir(), "127.0.0.1:0")
	createTopic(t, tb.addr, "orders")
	conn := dial(t, tb.addr)
	pid := initProducer(t, conn)
	for seq := range int64(3) {
		mustRawPublish(t, conn, pid, seq, protocol.PublishResponse{Offset: seq})
	}
	if err := newConsumer(t, tb.addr).Commit(testCtx(t), "billing", "orders", 0, 2); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	tb = tb.restart()
	conn = dial(t, tb.addr)
	c := newConsumer(t, tb.addr)

	if records := readAll(t, c, "orders"); len(records) != 3 {
		t.Fatalf("after restart: %d records, want 3", len(records))
	}
	offset, found, err := c.CommittedOffset(testCtx(t), "billing", "orders", 0)
	if err != nil || !found || offset != 2 {
		t.Fatalf("CommittedOffset = (%d, %v, %v), want (2, true)", offset, found, err)
	}
	if _, found, _ := c.CommittedOffset(testCtx(t), "other", "orders", 0); found {
		t.Fatal("unknown group has a committed offset")
	}
	mustRawPublish(t, conn, pid, 2, protocol.PublishResponse{Offset: 2, Duplicate: true})
	mustRawPublish(t, conn, pid, 3, protocol.PublishResponse{Offset: 3})
}

func TestErrors(t *testing.T) {
	tb := startBroker(t, t.TempDir(), "127.0.0.1:0")
	createTopic(t, tb.addr, "orders")
	c := newConsumer(t, tb.addr)

	if _, _, err := c.Consume(testCtx(t), "missing", 0, 0, 10, 0); !errors.Is(err, client.ErrUnknownTopicOrPartition) {
		t.Errorf("consume unknown topic = %v, want UnknownTopicOrPartition", err)
	}
	if _, err := newProducer(t, tb.addr).Publish(testCtx(t), "missing", []byte("x")); !errors.Is(err, client.ErrUnknownTopicOrPartition) {
		t.Errorf("publish unknown topic = %v, want UnknownTopicOrPartition", err)
	}
	if _, _, err := c.Consume(testCtx(t), "orders", 0, 5, 10, 0); !errors.Is(err, client.ErrOffsetOutOfRange) {
		t.Errorf("consume beyond the high watermark = %v, want OffsetOutOfRange", err)
	}
	if err := dial(t, tb.addr).CreateTopic(testCtx(t), "orders", 1); !errors.Is(err, client.ErrTopicExists) {
		t.Errorf("create existing topic = %v, want TopicExists", err)
	}
	if err := dial(t, tb.addr).CreateTopic(testCtx(t), "bad/name", 1); !errors.Is(err, client.ErrInvalidTopic) {
		t.Errorf("create bad topic name = %v, want InvalidTopic", err)
	}
}

// rawBody sends arbitrary bytes as a request body.
type rawBody []byte

func (b rawBody) AppendTo(dst []byte) []byte { return append(dst, b...) }
func (b rawBody) DecodeFrom([]byte) error    { return nil }

// A garbled request gets InvalidRequest under its correlation id, and the
// connection keeps working.
func TestMalformedRequestKeepsConnection(t *testing.T) {
	tb := startBroker(t, t.TempDir(), "127.0.0.1:0")
	nc, err := net.Dial("tcp", tb.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer nc.Close()

	roundTrip := func(apiKey protocol.APIKey, corrID uint32, body protocol.Body) protocol.Response {
		t.Helper()
		if err := protocol.WriteRequest(nc, apiKey, corrID, body); err != nil {
			t.Fatalf("write: %v", err)
		}
		frame, err := protocol.ReadFrame(nc, 1<<20)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		resp, err := protocol.ParseResponse(frame)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		return resp
	}

	cases := []struct {
		name   string
		apiKey protocol.APIKey
		corrID uint32
		body   protocol.Body
		want   protocol.ErrorCode
	}{
		{"truncated publish", protocol.APIPublish, 7, rawBody{0, 5, 'a'}, protocol.ErrInvalidRequest},
		{"truncated consume", protocol.APIConsume, 8, rawBody{1}, protocol.ErrInvalidRequest},
		{"unknown api key", 99, 9, nil, protocol.ErrInvalidRequest},
		{"valid request afterwards", protocol.APIMetadata, 10, &protocol.MetadataRequest{}, protocol.ErrNone},
	}
	for _, c := range cases {
		resp := roundTrip(c.apiKey, c.corrID, c.body)
		if resp.CorrelationID != c.corrID || resp.Error != c.want {
			t.Errorf("%s: got corrID %d %s (%s), want corrID %d %s", c.name, resp.CorrelationID, resp.Error, resp.Message, c.corrID, c.want)
		}
	}
}

func TestGroupConsumerResumes(t *testing.T) {
	tb := startBroker(t, t.TempDir(), "127.0.0.1:0")
	createTopic(t, tb.addr, "orders")
	p := newProducer(t, tb.addr)
	for i := range 10 {
		if _, err := p.Publish(testCtx(t), "orders", fmt.Appendf(nil, "m%d", i)); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	first := newConsumer(t, tb.addr)
	g, err := first.Group(testCtx(t), "billing", "orders", 0)
	if err != nil {
		t.Fatalf("Group: %v", err)
	}
	g.Limit = 4
	records, err := g.Poll(testCtx(t))
	if err != nil || len(records) != 4 {
		t.Fatalf("Poll = (%d records, %v), want 4", len(records), err)
	}
	if err := g.Commit(testCtx(t)); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	first.Close()

	g, err = newConsumer(t, tb.addr).Group(testCtx(t), "billing", "orders", 0)
	if err != nil {
		t.Fatalf("Group after reconnect: %v", err)
	}
	records, err = g.Poll(testCtx(t))
	if err != nil || len(records) != 6 || records[0].Offset != 4 {
		t.Fatalf("Poll after reconnect = (%v, %v), want offsets 4..9", records, err)
	}
}

// The broker restarts while several goroutines publish through one
// Producer. Every publish must still succeed, and every message must be in
// the log exactly once.
func TestProducerSurvivesBrokerRestart(t *testing.T) {
	tb := startBroker(t, t.TempDir(), "127.0.0.1:0")
	createTopic(t, tb.addr, "orders")
	p := newProducer(t, tb.addr)

	const goroutines, perGoroutine = 4, 50
	acked := make(chan struct{}, goroutines*perGoroutine)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range perGoroutine {
				if _, err := p.Publish(testCtx(t), "orders", fmt.Appendf(nil, "g%d-%d", g, i)); err != nil {
					t.Errorf("Publish g%d-%d: %v", g, i, err)
					return
				}
				acked <- struct{}{}
			}
		})
	}
	for range goroutines * perGoroutine / 4 {
		<-acked // let a quarter through, then restart mid-stream
	}
	tb = tb.restart()
	wg.Wait()

	records := readAll(t, newConsumer(t, tb.addr), "orders")
	seen := map[string]int{}
	for _, r := range records {
		seen[string(r.Body)]++
	}
	for g := range goroutines {
		for i := range perGoroutine {
			if v := fmt.Sprintf("g%d-%d", g, i); seen[v] != 1 {
				t.Errorf("%s is in the log %d times, want 1", v, seen[v])
			}
		}
	}
	if len(records) != goroutines*perGoroutine {
		t.Errorf("log has %d records, want %d", len(records), goroutines*perGoroutine)
	}
}
