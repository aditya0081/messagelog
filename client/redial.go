package client

import (
	"context"
	"sync"

	"messagelog/internal/protocol"
)

// redialer holds the current connection and dials a new one once it
// breaks, so a Producer or Consumer outlives a broker restart.
type redialer struct {
	addr string

	mu     sync.Mutex
	conn   *Conn
	closed bool
}

// get returns a healthy connection, dialing if needed.
func (r *redialer) get(ctx context.Context) (*Conn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	if r.conn != nil && r.conn.Err() == nil {
		return r.conn, nil
	}
	if r.conn != nil {
		r.conn.Close() // already broken; this just reaps its reader
	}
	conn, err := Dial(ctx, r.addr)
	if err != nil {
		return nil, err
	}
	r.conn = conn
	return conn, nil
}

// do runs one request on the current connection.
func (r *redialer) do(ctx context.Context, apiKey protocol.APIKey, req, resp protocol.Body) error {
	conn, err := r.get(ctx)
	if err != nil {
		return err
	}
	return conn.Do(ctx, apiKey, req, resp)
}

func (r *redialer) close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	if r.conn == nil {
		return nil
	}
	return r.conn.Close()
}
