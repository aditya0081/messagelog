package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"messagelog/internal/protocol"
	"messagelog/internal/record"
)

func TestInFlightBudget(t *testing.T) {
	b := newInFlightBudget(2, 100)
	ctx := testCtx(t)
	if !b.acquire(ctx, 60) || !b.acquire(ctx, 40) {
		t.Fatal("acquire within the budget failed")
	}
	if b.tryAcquire(1) {
		t.Fatal("acquired past the byte and request limits")
	}

	got := make(chan bool)
	go func() { got <- b.acquire(ctx, 50) }()
	b.release(40) // 60 bytes, 1 request held: 50 more does not fit yet
	select {
	case <-got:
		t.Fatal("acquired 50 bytes with only 40 free")
	case <-time.After(20 * time.Millisecond):
	}
	b.release(60)
	if !<-got {
		t.Fatal("acquire failed after room was released")
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if b.acquire(cancelled, 100) {
		t.Fatal("acquire with no room and a cancelled ctx succeeded")
	}
}

// Clients that pipeline large publishes and never read the acks must not
// stall other clients of the same partition (the partition writer never
// blocks on them), and Close must still finish.
func TestStalledReaderDoesNotBlockOthers(t *testing.T) {
	tb := startBroker(t, t.TempDir(), "127.0.0.1:0")
	createTopic(t, tb.addr, "orders")

	for range 2 {
		nc, err := net.Dial("tcp", tb.addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { nc.Close() })
		nc.(*net.TCPConn).SetReadBuffer(4096)
		go func() {
			big := &protocol.PublishRequest{Topic: "orders", ProducerID: record.NoProducerID, Value: make([]byte, 64<<10)}
			var frame bytes.Buffer
			for i := range 500 {
				frame.Reset()
				protocol.WriteRequest(&frame, protocol.APIPublish, uint32(i+1), big)
				if _, err := nc.Write(frame.Bytes()); err != nil {
					return
				}
			}
		}()
	}
	time.Sleep(300 * time.Millisecond) // let them fill their budgets

	p := newProducer(t, tb.addr)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for i := range 20 {
		if _, err := p.Publish(ctx, "orders", []byte("x")); err != nil {
			t.Fatalf("publish %d while others stall: %v", i, err)
		}
	}

	closed := make(chan struct{})
	go func() { tb.srv.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Server.Close hung on stalled connections")
	}
}

func TestMaxConns(t *testing.T) {
	tb := startBrokerWith(t, t.TempDir(), "127.0.0.1:0", Options{MaxConns: 1})
	first := dial(t, tb.addr)
	if _, err := first.Metadata(testCtx(t)); err != nil {
		t.Fatalf("Metadata on the first connection: %v", err)
	}

	second, err := net.Dial("tcp", tb.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer second.Close()
	second.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := second.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("read on the connection over the limit = %v, want EOF (closed by the server)", err)
	}

	if _, err := first.Metadata(testCtx(t)); err != nil {
		t.Fatalf("Metadata on the first connection afterwards: %v", err)
	}
}

// The server closes a connection that sends nothing for IdleTimeout, and a
// Producer reconnects on its own afterwards.
func TestIdleTimeout(t *testing.T) {
	tb := startBrokerWith(t, t.TempDir(), "127.0.0.1:0", Options{IdleTimeout: 100 * time.Millisecond})
	createTopic(t, tb.addr, "orders")
	p := newProducer(t, tb.addr)
	mustPublish(t, p, "orders", "before")

	idle, err := net.Dial("tcp", tb.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer idle.Close()
	start := time.Now()
	idle.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := idle.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("read on an idle connection = %v, want EOF (closed by the server)", err)
	}
	if waited := time.Since(start); waited < 100*time.Millisecond {
		t.Fatalf("idle connection closed after %v, want at least 100ms", waited)
	}

	mustPublish(t, p, "orders", "after") // its connection was idle too
}
