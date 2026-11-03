// Package client talks to a messagelog broker.
//
//   - Conn is one TCP connection with many requests in flight, matched to
//     their responses by correlation id.
//   - Producer publishes with idempotent sequence numbers, pipelines
//     requests, and reconnects + resends in the background on connection
//     loss without creating duplicates.
//   - Consumer reads, commits and looks up committed offsets; GroupConsumer
//     resumes a consumer group from its committed offset.
package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"messagelog/internal/protocol"
)

const (
	// maxResponseFrame bounds a response frame. The largest is a consume
	// response: about 1 MiB of records (the broker's per-read cap) plus at
	// most one more record of up to 1 MiB.
	maxResponseFrame = 4 << 20

	// writeTimeout bounds writing one request, so a broker that stopped
	// reading cannot block a sender forever. It is fixed instead of taken
	// from the caller's ctx: a timed-out write leaves half a frame behind,
	// which breaks the connection for every request on it, and one caller's
	// short deadline must not decide that for everyone.
	writeTimeout = 10 * time.Second
)

// ErrClosed is returned after Close.
var ErrClosed = errors.New("client: closed")

// Conn is one connection to a broker. It is safe for concurrent use: any
// number of goroutines can have requests in flight at once.
//
// Once a read or write fails, the Conn is broken for good: every pending
// and later request fails with that error (see Err). Callers that want to
// survive a broker restart dial a new Conn, as Producer and Consumer do.
type Conn struct {
	addr string
	nc   net.Conn

	writeMu sync.Mutex // one frame at a time

	mu      sync.Mutex
	nextID  uint32
	pending map[uint32]*call // written, response not yet delivered
	err     error            // why the connection broke; nil while healthy

	readerDone chan struct{}
}

// Dial connects to the broker at addr.
func Dial(ctx context.Context, addr string) (*Conn, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", addr, err)
	}
	c := &Conn{
		addr:       addr,
		nc:         nc,
		pending:    map[uint32]*call{},
		readerDone: make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

// Do sends a request and waits for its response, decoding the body into
// resp. Pass nil for APIs without a request or response body. Strings in
// req must fit protocol.MaxStringLen. A broker error comes back as *Error.
func (c *Conn) Do(ctx context.Context, apiKey protocol.APIKey, req, resp protocol.Body) error {
	call, err := c.send(apiKey, req)
	if err != nil {
		return err
	}
	return call.wait(ctx, resp)
}

// Err returns the error that broke the connection, or nil while it is healthy.
func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Close closes the connection; pending requests fail with ErrClosed.
func (c *Conn) Close() error {
	c.fail(ErrClosed)
	<-c.readerDone
	return nil
}

// call is one request that has been written and awaits its response.
type call struct {
	conn *Conn
	id   uint32
	// response receives the response (it has room for it, so delivery never
	// blocks), or is closed without one if the connection breaks first.
	response chan protocol.Response
}

// send registers a correlation id and writes the request. It does not wait
// for the response, which is what lets a Producer pipeline.
func (c *Conn) send(apiKey protocol.APIKey, req protocol.Body) (*call, error) {
	call, err := c.register()
	if err != nil {
		return nil, err
	}
	c.writeMu.Lock()
	c.nc.SetWriteDeadline(time.Now().Add(writeTimeout)) // fails only on a closed conn, which the write reports
	err = protocol.WriteRequest(c.nc, apiKey, call.id, req)
	c.writeMu.Unlock()
	if err != nil {
		c.fail(fmt.Errorf("client: write to %s: %w", c.addr, err))
		return nil, c.Err()
	}
	return call, nil
}

func (c *Conn) register() (*call, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	c.nextID++
	cl := &call{conn: c, id: c.nextID, response: make(chan protocol.Response, 1)}
	c.pending[cl.id] = cl
	return cl, nil
}

// wait blocks for the response and decodes it into body (see Do). If ctx
// ends first, the response is discarded when it arrives; but a response
// that is already there wins over the cancellation, since it is the truth.
func (cl *call) wait(ctx context.Context, body protocol.Body) error {
	select {
	case resp, ok := <-cl.response:
		return cl.decode(resp, ok, body)
	case <-ctx.Done():
	}
	select {
	case resp, ok := <-cl.response:
		return cl.decode(resp, ok, body)
	default:
		cl.conn.forget(cl.id)
		return ctx.Err()
	}
}

// decode turns a receive from cl.response into an error or a decoded body.
// received is false when the channel was closed: the connection broke.
func (cl *call) decode(resp protocol.Response, received bool, body protocol.Body) error {
	if !received {
		return cl.conn.Err()
	}
	if resp.Error != protocol.ErrNone {
		return &Error{Code: resp.Error, Message: resp.Message}
	}
	if body == nil {
		return nil
	}
	if err := body.DecodeFrom(resp.Body); err != nil {
		return fmt.Errorf("client: decode response: %w", err)
	}
	return nil
}

func (c *Conn) forget(id uint32) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

// readLoop delivers responses to their waiting calls until reading fails.
func (c *Conn) readLoop() {
	defer close(c.readerDone)
	for {
		frame, err := protocol.ReadFrame(c.nc, maxResponseFrame)
		if err != nil {
			c.fail(fmt.Errorf("client: read from %s: %w", c.addr, err))
			return
		}
		resp, err := protocol.ParseResponse(frame)
		if err != nil {
			c.fail(fmt.Errorf("client: read from %s: %w", c.addr, err))
			return
		}
		c.deliver(resp)
	}
}

func (c *Conn) deliver(resp protocol.Response) {
	c.mu.Lock()
	cl, ok := c.pending[resp.CorrelationID]
	delete(c.pending, resp.CorrelationID)
	c.mu.Unlock()
	if ok {
		cl.response <- resp // never blocks; the waiter may have given up already
	}
}

// fail marks the connection broken (keeping the first error), fails every
// pending call and closes the socket, which also stops readLoop.
func (c *Conn) fail(err error) {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
		for id, cl := range c.pending {
			close(cl.response)
			delete(c.pending, id)
		}
	}
	c.mu.Unlock()
	c.nc.Close()
}
