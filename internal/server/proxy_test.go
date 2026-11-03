package server

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// faultProxy forwards TCP between clients and a broker and injects faults
// into the client→broker direction:
//   - latency: bytes reach the broker this much later;
//   - blackhole: while set, bytes from clients are dropped;
//   - killAll: closes every connection. The client side closes at once,
//     but bytes already read from the client still reach the broker up to
//     lateFor later, so the broker may run requests the client gave up on.
//   - killSilently: closes the broker side of every connection but leaves
//     the client side open until the client sends something. The client
//     sees a healthy connection that fails on first use, like one whose
//     broker just restarted.
type faultProxy struct {
	ln        net.Listener
	upstream  string
	latency   time.Duration
	blackhole atomic.Bool

	mu    sync.Mutex
	links []*proxyLink
}

type proxyLink struct {
	client, broker net.Conn
	silent         atomic.Bool // killed silently: keep the client side open
	killOnce       sync.Once
}

// delayedChunk is client bytes waiting to be forwarded to the broker.
type delayedChunk struct {
	due  time.Time
	data []byte
}

func newFaultProxy(t *testing.T, upstream string, latency time.Duration) *faultProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	px := &faultProxy{ln: ln, upstream: upstream, latency: latency}
	t.Cleanup(func() {
		ln.Close()
		px.killAll(0)
	})
	go px.acceptLoop()
	return px
}

func (px *faultProxy) addr() string { return px.ln.Addr().String() }

func (px *faultProxy) acceptLoop() {
	for {
		client, err := px.ln.Accept()
		if err != nil {
			return
		}
		broker, err := net.Dial("tcp", px.upstream)
		if err != nil {
			client.Close()
			continue
		}
		l := &proxyLink{client: client, broker: broker}
		px.mu.Lock()
		px.links = append(px.links, l)
		px.mu.Unlock()

		toBroker := make(chan delayedChunk, 1<<12)
		go px.readFromClient(l, toBroker)
		go writeToBroker(l, toBroker)
		go func() {
			io.Copy(client, broker)
			if !l.silent.Load() {
				client.Close()
			}
		}()
	}
}

func (px *faultProxy) readFromClient(l *proxyLink, toBroker chan<- delayedChunk) {
	defer close(toBroker)
	buf := make([]byte, 32<<10)
	for {
		n, err := l.client.Read(buf)
		if l.silent.Load() {
			l.client.Close() // the dead connection fails on first use
			return
		}
		if n > 0 && !px.blackhole.Load() {
			toBroker <- delayedChunk{time.Now().Add(px.latency), append([]byte(nil), buf[:n]...)}
		}
		if err != nil {
			return
		}
	}
}

func writeToBroker(l *proxyLink, toBroker <-chan delayedChunk) {
	for chunk := range toBroker {
		time.Sleep(time.Until(chunk.due))
		if _, err := l.broker.Write(chunk.data); err != nil {
			return
		}
	}
	l.broker.Close()
}

// takeLinks removes and returns every current link.
func (px *faultProxy) takeLinks() []*proxyLink {
	px.mu.Lock()
	defer px.mu.Unlock()
	links := px.links
	px.links = nil
	return links
}

func (px *faultProxy) killAll(lateFor time.Duration) {
	for _, l := range px.takeLinks() {
		l.killOnce.Do(func() {
			l.client.Close()
			time.AfterFunc(lateFor, func() { l.broker.Close() })
		})
	}
}

// waitForLinks waits until the proxy has set up n connections: a client's
// Dial returns before the proxy has accepted and linked it.
func (px *faultProxy) waitForLinks(t *testing.T, n int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		px.mu.Lock()
		linked := len(px.links)
		px.mu.Unlock()
		if linked >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("proxy has %d connections, want %d", linked, n)
		}
	}
}

func (px *faultProxy) killSilently() {
	for _, l := range px.takeLinks() {
		l.killOnce.Do(func() {
			l.silent.Store(true)
			l.broker.Close()
		})
	}
}
