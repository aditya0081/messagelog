package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"messagelog/internal/partition"
	"messagelog/internal/protocol"
	"messagelog/internal/record"
)

const (
	// maxRequestFrame fits the largest message plus generous room for the
	// rest of a Publish request (topic name and fixed fields).
	maxRequestFrame = record.MaxBodySize + 64<<10

	// maxInFlight caps unanswered requests per connection.
	maxInFlight = 256
)

// conn serves one client connection.
type conn struct {
	srv *Server
	nc  net.Conn
	log *slog.Logger

	// ctx is cancelled when the connection closes, ending long polls early.
	ctx    context.Context
	cancel context.CancelFunc

	// budget and inFlight both track the requests read but not yet
	// answered: budget to cap them, inFlight so serve can wait for them.
	budget   *inFlightBudget
	inFlight sync.WaitGroup

	// writeMu serializes writes so frames never interleave.
	writeMu sync.Mutex

	// acks carries publish results from the partition writer goroutine to
	// writeAcks. The writer must never block on a slow socket, and it never
	// does: every ack belongs to an unanswered request, the budget allows at
	// most maxInFlight of those, and the channel has room for that many.
	acks     chan publishAck
	acksDone chan struct{}
}

type publishAck struct {
	corrID    uint32
	frameSize int // returned to the budget once the ack is written
	result    partition.AppendResult
}

func newConn(s *Server, nc net.Conn) *conn {
	ctx, cancel := context.WithCancel(context.Background())
	return &conn{
		srv:      s,
		nc:       nc,
		log:      s.log.With("remote", nc.RemoteAddr().String()),
		ctx:      ctx,
		cancel:   cancel,
		budget:   newInFlightBudget(maxInFlight, s.opts.MaxInFlightBytes),
		acks:     make(chan publishAck, maxInFlight),
		acksDone: make(chan struct{}),
	}
}

// serve runs the connection until the client goes away or the server closes
// it, then waits for every in-flight request to finish.
func (c *conn) serve() {
	c.log.Debug("connection opened")
	go c.writeAcks()

	err := c.readRequests()
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed), errors.Is(err, context.Canceled):
		c.log.Debug("connection closed")
	case errors.Is(err, os.ErrDeadlineExceeded):
		c.log.Debug("closing idle connection")
	default:
		c.log.Warn("connection failed", "err", err)
	}

	// Close first: a client that stopped reading would otherwise block our
	// writes, and so this wait, forever. Answers still pending are dropped.
	c.close()
	c.inFlight.Wait()
	close(c.acks)
	<-c.acksDone
}

// close abandons the connection. Safe to call more than once.
func (c *conn) close() {
	c.cancel()
	c.nc.Close()
}

// readRequests reads and dispatches frames until reading fails or the
// connection closes.
func (c *conn) readRequests() error {
	for {
		frame, err := c.readFrame()
		if err != nil {
			return err
		}
		req, err := protocol.ParseRequest(frame)
		if err != nil {
			c.budget.release(len(frame))
			return err // too short to hold a correlation id, so nothing to answer
		}
		c.inFlight.Add(1)
		if req.APIKey == protocol.APIPublish {
			c.publish(req, len(frame)) // inline: arrival order = append order
		} else {
			go c.handle(req, len(frame))
		}
	}
}

// readFrame reads one request frame, as protocol.ReadFrame does, but
// reserves the frame in the in-flight budget between reading its size and
// allocating its payload. So a client that pipelines faster than it is
// answered is held back by TCP flow control instead of growing our memory.
// The reservation is returned by finish once the request is answered.
func (c *conn) readFrame() ([]byte, error) {
	var sizePrefix [4]byte
	if err := c.readFull(sizePrefix[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(sizePrefix[:])
	if size > maxRequestFrame {
		return nil, fmt.Errorf("%w: %d bytes, max %d", protocol.ErrFrameTooLarge, size, maxRequestFrame)
	}
	if !c.budget.acquire(c.ctx, int(size)) {
		return nil, c.ctx.Err()
	}
	frame := make([]byte, size)
	if err := c.readFull(frame); err != nil {
		c.budget.release(int(size))
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF // the client hung up mid-frame
		}
		return nil, err
	}
	return frame, nil
}

// readFull fills buf, giving up once the client has sent nothing for
// IdleTimeout.
func (c *conn) readFull(buf []byte) error {
	c.nc.SetReadDeadline(time.Now().Add(c.srv.opts.IdleTimeout)) // fails only on a closed conn, which the read reports
	_, err := io.ReadFull(c.nc, buf)
	return err
}

// finish marks one request, read as frameSize bytes, as answered.
func (c *conn) finish(frameSize int) {
	c.budget.release(frameSize)
	c.inFlight.Done()
}

// publish enqueues a Publish with the broker. The ack is written later by
// writeAcks. The decoded Value aliases the frame, which nothing else uses,
// so the partition can hold on to it until it acks.
func (c *conn) publish(req protocol.Request, frameSize int) {
	ack := publishAck{corrID: req.CorrelationID, frameSize: frameSize}
	var body protocol.PublishRequest
	if err := body.DecodeFrom(req.Body); err != nil {
		ack.result = partition.AppendResult{Offset: -1, Err: err}
		c.acks <- ack
		return
	}
	appendReq := partition.AppendRequest{ProducerID: body.ProducerID, Sequence: body.Sequence, Body: body.Value}
	c.srv.broker.Publish(body.Topic, body.Partition, appendReq, func(r partition.AppendResult) {
		ack.result = r
		c.acks <- ack
	})
}

// writeAcks writes publish acks. Acks that are already queued go out
// together in one write, which matters under load: a group commit acks a
// whole batch at once.
func (c *conn) writeAcks() {
	defer close(c.acksDone)
	var buf bytes.Buffer
	var batch []publishAck
	for ack := range c.acks {
		batch = c.takeQueuedAcks(append(batch[:0], ack))
		buf.Reset()
		for _, a := range batch {
			c.encodeAck(&buf, a)
		}
		c.write(buf.Bytes())
		for _, a := range batch {
			c.finish(a.frameSize)
		}
	}
}

// takeQueuedAcks appends the acks waiting in the channel, without blocking.
// The channel cannot be closed meanwhile: serve closes it only after every
// ack, including the ones taken here, is finished.
func (c *conn) takeQueuedAcks(batch []publishAck) []publishAck {
	for {
		select {
		case ack := <-c.acks:
			batch = append(batch, ack)
		default:
			return batch
		}
	}
}

func (c *conn) encodeAck(buf *bytes.Buffer, ack publishAck) {
	var body protocol.Body
	if ack.result.Err == nil {
		body = &protocol.PublishResponse{Offset: ack.result.Offset, Duplicate: ack.result.Duplicate}
	}
	c.encodeResponse(buf, ack.corrID, body, ack.result.Err)
}

// handle answers every request except Publish, in its own goroutine.
func (c *conn) handle(req protocol.Request, frameSize int) {
	defer c.finish(frameSize)
	if req.APIKey == protocol.APIConsume {
		c.consume(req)
		return
	}
	body, err := c.srv.dispatch(req)
	var buf bytes.Buffer
	c.encodeResponse(&buf, req.CorrelationID, body, err)
	c.write(buf.Bytes())
}

// encodeResponse encodes a success (err == nil) or the error's code.
func (c *conn) encodeResponse(buf *bytes.Buffer, corrID uint32, body protocol.Body, err error) {
	code := errorCode(err)
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	if code == protocol.ErrUnknown {
		c.log.Error("request failed", "err", err)
	}
	protocol.WriteResponse(buf, corrID, code, msg, body) // a bytes.Buffer never fails
}

// write sends complete frames. A failed write means the connection is
// broken, so it is closed and the read loop ends.
func (c *conn) write(frames []byte) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if _, err := c.nc.Write(frames); err != nil {
		c.close()
	}
}
