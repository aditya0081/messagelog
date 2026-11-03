package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"messagelog/internal/protocol"
)

// maxInFlightPerPartition caps the messages a partition stream has written
// but not yet seen resolved. Further Publish calls wait in its queue.
const maxInFlightPerPartition = 256

// ErrDeliveryTimeout means a message was not acked within
// ProducerConfig.DeliveryTimeout. It may still be in the log.
var ErrDeliveryTimeout = errors.New("client: delivery timeout")

// errNoAck is the cause given when a message times out on a connection
// that still looks healthy.
var errNoAck = errors.New("no ack")

// partitionStream delivers one partition's messages in Publish order, each
// exactly once, under an idempotent (producer id, sequence) numbering.
//
// One goroutine, run, owns all of its state, so nothing here is locked.
// Publish hands a message over through queue and waits for the outcome on
// the message's own channel. A caller that gives up only stops waiting: its
// message keeps its sequence and is still delivered in the background. That
// is what keeps the numbering contiguous. If a give-up dropped a message, the
// broker would reject every younger one as a gap.
//
// The broker only accepts sequence last+1 for a producer id (older ones are
// re-acked as duplicates, newer ones rejected with OutOfOrderSequence). So
// the in-flight messages always have contiguous sequences under one producer
// id, are written in that order on one connection, and are resolved strictly
// oldest first. run reacts to one event at a time:
//
//	new message           number it (next sequence), write it, append it.
//	oldest acked          report the ack, pop it.
//	oldest failed: connection lost or broker shutting down
//	                      back off, redial if needed, and rewrite every
//	                      in-flight message unchanged. The broker re-acks
//	                      the ones it already has, so nothing is written twice.
//	oldest failed: OutOfOrderSequence
//	                      the broker lacks the previous sequence. Nothing older
//	                      is outstanding, so waiting cannot fix that: either an
//	                      older message failed for good without being written,
//	                      or the broker forgot this producer id after
//	                      retention deleted all its records. Take a new
//	                      producer id, renumber every in-flight message from
//	                      0, and rewrite them. (OutOfOrderSequence on a
//	                      younger message is never looked at while an older
//	                      one is outstanding, so it is never retried as is.)
//	oldest failed: any other broker error
//	                      report it, pop it. It was not written.
//	oldest past its delivery deadline
//	                      report ErrDeliveryTimeout, pop it. It may still land.
//	                      If it never does, the younger messages fail with
//	                      OutOfOrderSequence and get renumbered (see above).
//
// So the producer id only changes after a real failure, never because a
// caller gave up. One known limit: renumbering assumes no copy of a failed
// message is still in transit somewhere, more than DeliveryTimeout after it
// was sent. If one did land late, the messages after it could be written
// twice. Kafka closes that gap with producer epochs, which this broker does
// not have yet.
type partitionStream struct {
	producer *Producer // connection, config, close signal
	queue    chan *pendingPublish

	// Owned by run:
	haveProducerID bool  // false until the first InitProducer, and after OutOfOrderSequence
	producerID     int64 // valid while haveProducerID
	nextSeq        int64 // sequence for the next new message under producerID
	conn           *Conn // every in-flight message was last written on this connection
	inFlight       []*pendingPublish
}

// pendingPublish is one Publish call's message on its way to the broker.
type pendingPublish struct {
	wire     protocol.PublishRequest // ProducerID and Sequence are set by the stream
	caller   context.Context         // the Publish ctx; done before numbering → dropped unsent
	deadline time.Time               // when the stream stops trying (DeliveryTimeout after Publish)
	call     *call                   // the latest write of wire
	outcome  chan publishOutcome     // has room for the one outcome, so resolving never blocks
}

type publishOutcome struct {
	ack protocol.PublishResponse
	err error
}

func newPartitionStream(p *Producer) *partitionStream {
	return &partitionStream{producer: p, queue: make(chan *pendingPublish, maxInFlightPerPartition)}
}

func newPendingPublish(caller context.Context, topic string, part int32, value []byte, deadline time.Time) *pendingPublish {
	return &pendingPublish{
		wire:     protocol.PublishRequest{Topic: topic, Partition: part, Value: value},
		caller:   caller,
		deadline: deadline,
		outcome:  make(chan publishOutcome, 1),
	}
}

// deliver queues msg and waits for its outcome. If ctx ends first, only the
// wait is abandoned: msg is still delivered (see partitionStream).
func (s *partitionStream) deliver(ctx context.Context, msg *pendingPublish) (protocol.PublishResponse, error) {
	closed := s.producer.ctx.Done()
	select {
	case s.queue <- msg:
	case <-ctx.Done():
		return protocol.PublishResponse{}, ctx.Err() // never queued, so never sent
	case <-closed:
		return protocol.PublishResponse{}, ErrClosed
	}

	select {
	case out := <-msg.outcome:
		return out.ack, out.err
	case <-ctx.Done():
	case <-closed:
	}
	// An outcome that arrived at the same moment wins: it is the truth.
	select {
	case out := <-msg.outcome:
		return out.ack, out.err
	default:
	}
	if err := ctx.Err(); err != nil {
		return protocol.PublishResponse{}, err
	}
	return protocol.PublishResponse{}, ErrClosed
}

// run is the stream's goroutine; it returns when the producer closes.
//
// Between events every in-flight message has been written on s.conn and has
// a call to wait on, unless a retry was cut short because the producer is
// closing, which the loop condition checks first.
func (s *partitionStream) run() {
	deadline := time.NewTimer(time.Hour)
	defer deadline.Stop()
	for s.producer.ctx.Err() == nil {
		// A nil channel never fires, which switches its case off: no new
		// messages while the in-flight window is full, and nothing to wait
		// for while it is empty.
		var incoming <-chan *pendingPublish
		if len(s.inFlight) < maxInFlightPerPartition {
			incoming = s.queue
		}
		var oldestResponse <-chan protocol.Response
		var oldestExpired <-chan time.Time
		if len(s.inFlight) > 0 {
			oldest := s.inFlight[0]
			oldestResponse = oldest.call.response
			deadline.Reset(time.Until(oldest.deadline))
			oldestExpired = deadline.C
		}

		select {
		case <-s.producer.ctx.Done():
			return
		case msg := <-incoming:
			s.add(msg)
		case resp, received := <-oldestResponse:
			s.handleOldestResponse(resp, received)
		case <-oldestExpired:
			s.expireOverdue(errNoAck)
		}
	}
}

// add numbers msg and writes it, unless its caller already gave up or its
// deadline passed while it was queued: then it is dropped, never sent.
func (s *partitionStream) add(msg *pendingPublish) {
	if err := msg.caller.Err(); err != nil {
		msg.outcome <- publishOutcome{err: err}
		return
	}
	if !time.Now().Before(msg.deadline) {
		msg.outcome <- publishOutcome{err: fmt.Errorf("%w (still queued)", ErrDeliveryTimeout)}
		return
	}
	s.inFlight = append(s.inFlight, msg)
	if s.haveProducerID { // otherwise send numbers it along with the rest
		msg.wire.ProducerID, msg.wire.Sequence = s.producerID, s.nextSeq
		s.nextSeq++
	}
	if err := s.send(len(s.inFlight) - 1); err != nil {
		s.retryAfter(err)
	}
}

// handleOldestResponse acts on the response to the oldest in-flight
// message; received is false if its connection broke first.
func (s *partitionStream) handleOldestResponse(resp protocol.Response, received bool) {
	var ack protocol.PublishResponse
	err := s.inFlight[0].call.decode(resp, received, &ack)
	switch {
	case err == nil:
		s.resolveOldest(publishOutcome{ack: ack})
	case errors.Is(err, ErrOutOfOrderSequence):
		s.haveProducerID = false
		s.retryAfter(err)
	case isRetriable(err):
		s.retryAfter(err)
	default:
		s.resolveOldest(publishOutcome{err: err})
	}
}

// retryAfter gets the in-flight messages written again after cause stopped
// them. It backs off, then sends them all (taking a new producer id first if
// needed), and repeats until that works, every message has passed its
// delivery deadline, or the producer closes.
func (s *partitionStream) retryAfter(cause error) {
	for attempt := 0; ; attempt++ {
		s.expireOverdue(cause)
		if len(s.inFlight) == 0 || sleep(s.producer.ctx, s.producer.backoff(attempt)) != nil {
			return
		}
		if cause = s.send(0); cause == nil {
			return
		}
	}
}

// send writes s.inFlight[from:] in order. Without a usable producer id it
// first takes a new one and renumbers every in-flight message from 0, so
// they all need writing again.
func (s *partitionStream) send(from int) error {
	// Network calls here may take until the oldest message's deadline.
	ctx, cancel := context.WithDeadline(s.producer.ctx, s.inFlight[0].deadline)
	defer cancel()

	if !s.haveProducerID {
		if err := s.renumberUnderNewProducerID(ctx); err != nil {
			return err
		}
		from = 0
	}

	conn, err := s.producer.conn.get(ctx)
	if err != nil {
		return err
	}
	if conn != s.conn {
		// The older messages went out on the previous connection. Rewrite
		// them here first, or this one would arrive ahead of them as a gap.
		s.conn, from = conn, 0
	}
	for _, msg := range s.inFlight[from:] {
		if msg.call, err = conn.send(protocol.APIPublish, &msg.wire); err != nil {
			return err
		}
	}
	return nil
}

// renumberUnderNewProducerID takes a new producer id from the broker and
// numbers every in-flight message under it, from sequence 0.
func (s *partitionStream) renumberUnderNewProducerID(ctx context.Context) error {
	id, err := s.producer.initProducer(ctx)
	if err != nil {
		return err
	}
	s.producerID, s.haveProducerID = id, true
	for i, msg := range s.inFlight {
		msg.wire.ProducerID, msg.wire.Sequence = id, int64(i)
	}
	s.nextSeq = int64(len(s.inFlight))
	return nil
}

// expireOverdue fails the oldest messages whose delivery deadline has
// passed. Deadlines follow sequence order (up to the moment between taking
// the time and queueing), so checking the front is enough: a later overdue
// message is expired once it becomes the oldest.
func (s *partitionStream) expireOverdue(cause error) {
	for len(s.inFlight) > 0 && !time.Now().Before(s.inFlight[0].deadline) {
		s.resolveOldest(publishOutcome{err: fmt.Errorf("%w (last error: %v)", ErrDeliveryTimeout, cause)})
	}
}

func (s *partitionStream) resolveOldest(out publishOutcome) {
	s.inFlight[0].outcome <- out
	s.inFlight[0] = nil // let the message be collected
	s.inFlight = s.inFlight[1:]
}
