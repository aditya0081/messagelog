package server

import (
	"bytes"
	"time"

	"messagelog/internal/partition"
	"messagelog/internal/protocol"
)

// maxConsumeWait caps a long-poll, so a request cannot hold its goroutine
// and in-flight slot for long.
const maxConsumeWait = 30 * time.Second

// consume answers a Consume request, long-polling in the broker if the
// client is caught up, and streams the records straight from the segment file.
func (c *conn) consume(req protocol.Request) {
	var body protocol.ConsumeRequest
	if err := body.DecodeFrom(req.Body); err != nil {
		c.writeError(req.CorrelationID, err)
		return
	}
	maxWait := min(time.Duration(max(body.MaxWaitMs, 0))*time.Millisecond, maxConsumeWait)
	chunk, hw, err := c.srv.broker.Consume(c.ctx, body.Topic, body.Partition, body.Offset, int(body.Limit), maxWait)
	if err != nil {
		c.writeError(req.CorrelationID, err)
		return
	}
	defer chunk.Close() // the chunk holds the segment open, so retention cannot delete it mid-send
	c.sendChunk(req.CorrelationID, hw, chunk)
}

func (c *conn) writeError(corrID uint32, err error) {
	var buf bytes.Buffer
	c.encodeResponse(&buf, corrID, nil, err)
	c.write(buf.Bytes())
}

// sendChunk writes a consume response: the header from protocol, then the
// records copied by chunk.WriteTo straight onto the socket (not through a
// buffer), which uses sendfile(2) for a *net.TCPConn on Linux. The write
// lock covers both, so no other frame can land between them.
func (c *conn) sendChunk(corrID uint32, hw int64, chunk partition.Chunk) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	header := protocol.ConsumeResponseHeader(corrID, hw, uint32(chunk.Size))
	_, err := c.nc.Write(header)
	if err == nil {
		_, err = chunk.WriteTo(c.nc)
	}
	if err != nil {
		// A frame may be half-sent: the stream cannot be resynced.
		c.log.Debug("consume response cut short", "err", err)
		c.close()
	}
}
