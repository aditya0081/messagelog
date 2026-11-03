// Package server exposes a broker over TCP using the binary protocol in
// package protocol.
//
// Each connection has one goroutine reading frames. Publish requests are
// handed to the broker right there in the read loop, so the order requests
// arrive on a connection is the order they are appended: a pipelining
// idempotent producer depends on that. Every other request runs in its own
// goroutine, because Consume may long-poll. Responses can therefore leave
// out of order; the client matches them up by correlation id.
//
// Memory and goroutines stay bounded however clients behave:
//   - each connection caps the requests it has read but not yet answered,
//     by count and by bytes. Once either is used up the read loop stops
//     reading, and TCP flow control pushes back on the client;
//   - the number of connections is capped;
//   - a connection that sends nothing for IdleTimeout is closed;
//   - a long-poll waits at most maxConsumeWait.
package server

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"messagelog/internal/broker"
)

// ErrServerClosed is returned by Serve after Close.
var ErrServerClosed = errors.New("server: closed")

// errTooManyConns rejects a connection over Options.MaxConns.
var errTooManyConns = errors.New("server: too many connections")

// Options tunes a Server. Zero fields take defaults.
type Options struct {
	// MaxConns caps open connections; one more is closed right after
	// accept. Default 1024.
	MaxConns int
	// MaxInFlightBytes caps the request bytes one connection has read but
	// not yet answered. It is raised to fit at least one largest request.
	// Default 64 MiB.
	MaxInFlightBytes int
	// IdleTimeout closes a connection that sends no request for this long.
	// Clients reconnect on their next request. Default 5 minutes.
	IdleTimeout time.Duration
}

func (o Options) withDefaults() Options {
	if o.MaxConns <= 0 {
		o.MaxConns = 1024
	}
	if o.MaxInFlightBytes <= 0 {
		o.MaxInFlightBytes = 64 << 20
	}
	o.MaxInFlightBytes = max(o.MaxInFlightBytes, maxRequestFrame)
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = 5 * time.Minute
	}
	return o
}

// Server accepts connections and serves requests against one broker.
type Server struct {
	broker *broker.Broker
	log    *slog.Logger
	opts   Options

	mu        sync.Mutex
	closed    bool
	listeners map[net.Listener]struct{}
	conns     map[*conn]struct{}
	connsWG   sync.WaitGroup // one per conn goroutine
}

// New returns a server for b that logs to log.
func New(b *broker.Broker, log *slog.Logger, opts Options) *Server {
	return &Server{
		broker:    b,
		log:       log,
		opts:      opts.withDefaults(),
		listeners: map[net.Listener]struct{}{},
		conns:     map[*conn]struct{}{},
	}
}

// Serve accepts connections on ln until Close is called or Accept fails.
// It always returns a non-nil error: ErrServerClosed after Close.
func (s *Server) Serve(ln net.Listener) error {
	if !s.trackListener(ln) {
		ln.Close()
		return ErrServerClosed
	}
	s.log.Info("serving", "addr", ln.Addr().String())
	for {
		nc, err := ln.Accept()
		if err != nil {
			if s.isClosed() {
				return ErrServerClosed
			}
			return fmt.Errorf("server: accept: %w", err)
		}
		c := newConn(s, nc)
		if err := s.trackConn(c); err != nil {
			nc.Close()
			if errors.Is(err, ErrServerClosed) {
				return err
			}
			s.log.Warn("connection rejected", "remote", nc.RemoteAddr().String(), "err", err)
			continue
		}
		go func() {
			defer s.untrackConn(c)
			c.serve()
		}()
	}
}

// Close stops accepting, closes every connection, and waits until each
// connection's in-flight requests have been answered or abandoned. The
// broker is left open: the caller closes it afterwards.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	for ln := range s.listeners {
		ln.Close()
	}
	for c := range s.conns {
		c.close()
	}
	s.mu.Unlock()

	s.connsWG.Wait()
	return nil
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Server) trackListener(ln net.Listener) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.listeners[ln] = struct{}{}
	return true
}

func (s *Server) trackConn(c *conn) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrServerClosed
	}
	if len(s.conns) >= s.opts.MaxConns {
		return fmt.Errorf("%w (max %d)", errTooManyConns, s.opts.MaxConns)
	}
	s.conns[c] = struct{}{}
	s.connsWG.Add(1)
	return nil
}

func (s *Server) untrackConn(c *conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
	s.connsWG.Done()
}
