package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"messagelog/client"
	"messagelog/internal/broker"
	"messagelog/internal/partition"
	"messagelog/internal/record"
)

// testBroker is a broker + server on a real loopback listener.
type testBroker struct {
	t    *testing.T
	dir  string
	addr string
	opts Options

	broker   *broker.Broker
	srv      *Server
	served   chan error
	stopOnce sync.Once
}

// startBroker serves the data in dir on addr ("127.0.0.1:0" for any port)
// with default server options.
func startBroker(t *testing.T, dir, addr string) *testBroker {
	t.Helper()
	return startBrokerWith(t, dir, addr, Options{})
}

func startBrokerWith(t *testing.T, dir, addr string, opts Options) *testBroker {
	t.Helper()
	b, err := broker.Open(broker.Config{
		DataDir:  dir,
		BrokerID: 1,
		Partition: func(dir string) partition.Config {
			cfg := partition.DefaultConfig(dir)
			cfg.FlushInterval = time.Millisecond
			return cfg
		},
	})
	if err != nil {
		t.Fatalf("broker.Open: %v", err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		b.Close()
		t.Fatalf("listen: %v", err)
	}
	tb := &testBroker{
		t:      t,
		dir:    dir,
		addr:   ln.Addr().String(),
		opts:   opts,
		broker: b,
		srv:    New(b, slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelWarn})), opts),
		served: make(chan error, 1),
	}
	go func() { tb.served <- tb.srv.Serve(ln) }()
	t.Cleanup(tb.stop)
	return tb
}

// stop shuts down the server, then the broker, like cmd/broker does.
func (tb *testBroker) stop() {
	tb.stopOnce.Do(func() {
		tb.srv.Close()
		if err := <-tb.served; !errors.Is(err, ErrServerClosed) {
			tb.t.Errorf("Serve returned %v, want ErrServerClosed", err)
		}
		if err := tb.broker.Close(); err != nil {
			tb.t.Errorf("broker.Close: %v", err)
		}
	})
}

// restart stops the broker and starts it again on the same data and address.
func (tb *testBroker) restart() *testBroker {
	tb.stop()
	return startBrokerWith(tb.t, tb.dir, tb.addr, tb.opts)
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func dial(t *testing.T, addr string) *client.Conn {
	t.Helper()
	c, err := client.Dial(testCtx(t), addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func createTopic(t *testing.T, addr, name string) {
	t.Helper()
	if err := dial(t, addr).CreateTopic(testCtx(t), name, 1); err != nil {
		t.Fatalf("CreateTopic(%s): %v", name, err)
	}
}

func newProducer(t *testing.T, addr string) *client.Producer {
	t.Helper()
	return newProducerWith(t, addr, client.ProducerConfig{RetryBackoff: 10 * time.Millisecond})
}

func newProducerWith(t *testing.T, addr string, cfg client.ProducerConfig) *client.Producer {
	t.Helper()
	p, err := client.NewProducer(testCtx(t), addr, cfg)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func newConsumer(t *testing.T, addr string) *client.Consumer {
	t.Helper()
	c, err := client.NewConsumer(testCtx(t), addr)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// mustPublish publishes value and fails the test on error.
func mustPublish(t *testing.T, p *client.Producer, topic, value string) client.PublishResult {
	t.Helper()
	res, err := p.Publish(testCtx(t), topic, []byte(value))
	if err != nil {
		t.Fatalf("Publish(%s): %v", value, err)
	}
	return res
}

// bodies returns the record bodies in log order.
func bodies(records []record.Record) []string {
	var out []string
	for _, r := range records {
		out = append(out, string(r.Body))
	}
	return out
}
