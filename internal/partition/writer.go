package partition

import (
	"fmt"
	"time"

	"messagelog/internal/record"
	"messagelog/internal/segment"
)

// writer is the single goroutine that owns the active segment (DESIGN
// "Concurrency": one writer per partition keeps ordering without locks).
//
// It implements group commit: each request is written straight away, but
// fsync runs once per batch, when FlushInterval has passed since the first
// unflushed write or FlushBytes have piled up. While it fsyncs, new requests
// wait in the queue and form the next batch.
type writer struct {
	p         *Partition
	cfg       Config
	active    *segment.Segment
	producers producerState

	nextOffset     int64
	unflushedBytes int
	pending        []pendingAck // AckAfterFsync: acks waiting for the next fsync
	flushTimer     *time.Timer  // armed while unflushedBytes > 0
	lastCheckpoint time.Time

	// failed is set by the first I/O error; every later request gets it.
	failed error
	// shutdownErr is the result of the final flush + checkpoint, read by
	// Close after the writer goroutine has exited.
	shutdownErr error
	// abort makes run return at once without flushing. Only tests close it,
	// to simulate a crash.
	abort chan struct{}
}

type pendingAck struct {
	done   func(AppendResult)
	result AppendResult
}

func newWriter(p *Partition, active *segment.Segment, producers producerState) *writer {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	return &writer{
		p:          p,
		cfg:        p.cfg,
		active:     active,
		producers:  producers,
		nextOffset: active.NextOffset(),
		flushTimer: timer,
		abort:      make(chan struct{}),
	}
}

// run processes requests until the queue is closed (Close) or abort is closed.
func (w *writer) run() {
	for {
		select {
		case req, ok := <-w.p.queue:
			if !ok {
				w.shutdownErr = w.shutdown()
				return
			}
			w.handle(req)
		case <-w.flushTimer.C:
			w.flush()
		case <-w.abort:
			return
		}
	}
}

// handle runs DESIGN "Publish flow" steps 3-5 for one request.
func (w *writer) handle(req appendRequest) {
	if w.failed != nil {
		req.done(AppendResult{Offset: -1, Err: w.failed})
		return
	}
	if len(req.Body) > record.MaxBodySize {
		req.done(AppendResult{Offset: -1, Err: ErrMessageTooLarge})
		return
	}
	if req.ProducerID != record.NoProducerID {
		dup, offset, err := w.producers.check(req.ProducerID, req.Sequence)
		if err != nil {
			req.done(AppendResult{Offset: -1, Err: err})
			return
		}
		if dup {
			w.ackDuplicate(req.done, AppendResult{Offset: offset, Duplicate: true})
			return
		}
	}
	w.write(req)
}

// write appends the record, rolling the segment first if it would not fit.
func (w *writer) write(req appendRequest) {
	rec := record.Record{
		Offset:     w.nextOffset,
		Timestamp:  w.cfg.Now().UnixMilli(),
		ProducerID: req.ProducerID,
		Sequence:   req.Sequence,
		Body:       req.Body,
	}
	if w.needsRoll(rec.Size()) {
		if err := w.roll(); err != nil {
			w.takeOffline(err)
			req.done(AppendResult{Offset: -1, Err: w.failed})
			return
		}
	}
	if err := w.active.Append(rec); err != nil {
		w.takeOffline(err)
		req.done(AppendResult{Offset: -1, Err: w.failed})
		return
	}
	if rec.ProducerID != record.NoProducerID {
		w.producers.record(rec.ProducerID, rec.Sequence, rec.Offset)
	}
	w.nextOffset++
	if w.unflushedBytes == 0 {
		w.flushTimer.Reset(w.cfg.FlushInterval)
	}
	w.unflushedBytes += rec.Size()

	if w.cfg.AckMode == AckBeforeFsync {
		req.done(AppendResult{Offset: rec.Offset})
		w.p.advanceHighWatermark(w.nextOffset)
	} else {
		w.pending = append(w.pending, pendingAck{req.done, AppendResult{Offset: rec.Offset}})
	}
	if w.unflushedBytes >= w.cfg.FlushBytes {
		w.flush()
	}
}

// ackDuplicate acks a duplicate, which writes nothing. If unflushed data
// exists, the original may be in it, so in AckAfterFsync mode the ack waits
// for the same fsync: never ack data that is not durable yet.
func (w *writer) ackDuplicate(done func(AppendResult), result AppendResult) {
	if w.cfg.AckMode == AckAfterFsync && w.unflushedBytes > 0 {
		w.pending = append(w.pending, pendingAck{done, result})
		return
	}
	done(result)
}

// needsRoll reports whether a record of size bytes would overflow the active
// segment. An empty segment always takes the record, however big.
func (w *writer) needsRoll(size int) bool {
	used := w.active.Size()
	return used > 0 && used+int64(size) > w.cfg.SegmentBytes
}

// roll closes the active segment for writing and starts a new one named by
// the next offset (DESIGN "Publish flow" step 6). The old segment is
// fsynced first so it never needs recovery again.
func (w *writer) roll() error {
	w.flush()
	if w.failed != nil {
		return w.failed
	}
	next, err := segment.Create(w.cfg.Dir, w.nextOffset, w.cfg.IndexIntervalBytes)
	if err != nil {
		return fmt.Errorf("roll segment: %w", err)
	}
	w.p.addSegment(next)
	w.active = next
	return w.writeCheckpoint()
}

// flush fsyncs the active segment, then acks the batch and makes it
// readable (DESIGN "Publish flow" steps 7-9).
func (w *writer) flush() {
	// Since Go 1.23, Stop guarantees no stale tick is delivered afterwards.
	w.flushTimer.Stop()
	if w.unflushedBytes == 0 || w.failed != nil {
		return
	}
	if err := w.active.Sync(); err != nil {
		w.takeOffline(err)
		return
	}
	w.unflushedBytes = 0
	if w.cfg.AckMode == AckAfterFsync {
		// Ack before advancing, so a reader never sees a record whose
		// producer has not been told it is safe.
		w.ackPending()
		w.p.advanceHighWatermark(w.nextOffset)
	}
	if w.cfg.Now().Sub(w.lastCheckpoint) >= w.cfg.CheckpointInterval {
		if err := w.writeCheckpoint(); err != nil {
			w.takeOffline(err)
		}
	}
}

func (w *writer) ackPending() {
	for _, a := range w.pending {
		a.done(a.result)
	}
	clear(w.pending) // drop callback references
	w.pending = w.pending[:0]
}

// writeCheckpoint records how far the log is known to be durable. It must
// only run right after a flush, when nothing is unflushed. It first forgets
// producers whose records retention has deleted, which keeps the producer
// table and the checkpoint from growing forever.
func (w *writer) writeCheckpoint() error {
	w.producers.expire(w.p.LogStartOffset())
	cp := checkpoint{
		SegmentBase: w.active.Base(),
		Position:    w.active.Size(),
		NextOffset:  w.nextOffset,
		Producers:   w.producers,
	}
	if err := writeCheckpoint(w.cfg.Dir, cp); err != nil {
		return err
	}
	w.lastCheckpoint = w.cfg.Now()
	return nil
}

// takeOffline records err as the partition's fatal error and fails every
// pending (written but unacked) request with it. Every later request gets
// the same error.
func (w *writer) takeOffline(err error) {
	if w.failed == nil {
		w.failed = fmt.Errorf("partition offline: %w", err)
	}
	for i := range w.pending {
		w.pending[i].result = AppendResult{Offset: -1, Err: w.failed}
	}
	w.ackPending()
}

// shutdown runs after the queue is drained: final flush and checkpoint.
func (w *writer) shutdown() error {
	w.flush()
	if w.failed != nil {
		return w.failed
	}
	return w.writeCheckpoint()
}
