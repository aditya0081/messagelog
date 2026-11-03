package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"messagelog/internal/protocol"
	"messagelog/internal/record"
)

// Partitioner picks the partition for a message. Routing is client-side
// because the producer must know the partition before it can assign the
// per-(producer, partition) sequence number.
type Partitioner interface {
	Partition(topic string, value []byte, numPartitions int) int32
}

// SinglePartition sends everything to partition 0.
type SinglePartition struct{}

func (SinglePartition) Partition(string, []byte, int) int32 { return 0 }

// ProducerConfig tunes a Producer. Zero fields take defaults.
type ProducerConfig struct {
	Partitioner Partitioner // default SinglePartition{}

	// RetryBackoff is the pause before the first retry after a failure; it
	// doubles with each consecutive failure, up to maxBackoff. Default 50ms.
	RetryBackoff time.Duration

	// DeliveryTimeout bounds how long the producer keeps trying to get one
	// message acked, retries included, counted from Publish. Default 30s.
	DeliveryTimeout time.Duration
}

const maxBackoff = time.Second

// PublishResult is the broker's ack. Duplicate means the broker already had
// the message (a resend); Offset is then the original offset, or -1 if the
// broker no longer remembers it.
type PublishResult struct {
	Partition int32
	Offset    int64
	Duplicate bool
}

// Producer publishes messages exactly once, even across reconnects and
// broker restarts. It is safe for concurrent use, and concurrent Publish
// calls are pipelined on one connection.
//
// Each partition gets a partitionStream: a goroutine that numbers the
// partition's messages under an idempotent producer id, writes them in
// order, and resends them after a failure (see stream.go). The broker
// recognizes a sequence it already wrote and re-acks it instead of writing
// it twice.
type Producer struct {
	cfg  ProducerConfig
	conn *redialer

	// ctx is cancelled by Close, which stops every stream goroutine.
	ctx     context.Context
	cancel  context.CancelFunc
	senders sync.WaitGroup // one per stream goroutine

	mu              sync.Mutex
	streams         map[streamKey]*partitionStream
	partitionCounts map[string]int // cached Metadata answers, by topic
}

type streamKey struct {
	topic     string
	partition int32
}

// NewProducer connects to the broker at addr.
func NewProducer(ctx context.Context, addr string, cfg ProducerConfig) (*Producer, error) {
	if cfg.Partitioner == nil {
		cfg.Partitioner = SinglePartition{}
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 50 * time.Millisecond
	}
	if cfg.DeliveryTimeout <= 0 {
		cfg.DeliveryTimeout = 30 * time.Second
	}
	pctx, cancel := context.WithCancel(context.Background())
	p := &Producer{
		cfg:             cfg,
		conn:            &redialer{addr: addr},
		ctx:             pctx,
		cancel:          cancel,
		streams:         map[streamKey]*partitionStream{},
		partitionCounts: map[string]int{},
	}
	if _, err := p.conn.get(ctx); err != nil {
		cancel()
		return nil, err
	}
	return p, nil
}

// Publish sends value to topic and waits until the broker acks it.
//
// If ctx ends first, Publish stops waiting but the message is not taken
// back: it keeps its place in the partition's order and is still delivered
// in the background, for up to DeliveryTimeout. So an error from Publish
// means the message may or may not be in the log. Publishing it again by
// hand may then store it twice, as with Kafka. Only a ctx that is already
// done when Publish is called guarantees nothing is sent.
func (p *Producer) Publish(ctx context.Context, topic string, value []byte) (PublishResult, error) {
	if err := checkWireLen("topic", topic); err != nil {
		return PublishResult{}, err
	}
	if len(value) > record.MaxBodySize {
		return PublishResult{}, fmt.Errorf("%w: %d bytes, max %d", ErrMessageTooLarge, len(value), record.MaxBodySize)
	}
	if err := ctx.Err(); err != nil {
		return PublishResult{}, err
	}
	part, err := p.choosePartition(ctx, topic, value)
	if err != nil {
		return PublishResult{}, err
	}
	stream, err := p.stream(topic, part)
	if err != nil {
		return PublishResult{}, err
	}
	msg := newPendingPublish(ctx, topic, part, value, time.Now().Add(p.cfg.DeliveryTimeout))
	ack, err := stream.deliver(ctx, msg)
	if err != nil {
		return PublishResult{}, fmt.Errorf("client: publish to %s-%d: %w", topic, part, err)
	}
	return PublishResult{Partition: part, Offset: ack.Offset, Duplicate: ack.Duplicate}, nil
}

// Close stops every stream and closes the connection. Publish calls still
// waiting return ErrClosed; their messages may or may not have been written.
func (p *Producer) Close() error {
	p.mu.Lock()
	p.cancel() // under mu, so stream() never starts a goroutine after senders.Wait
	p.mu.Unlock()
	err := p.conn.close()
	p.senders.Wait()
	return err
}

// stream returns the partition's stream, starting it on first use.
func (p *Producer) stream(topic string, part int32) (*partitionStream, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx.Err() != nil {
		return nil, ErrClosed
	}
	key := streamKey{topic, part}
	s, ok := p.streams[key]
	if !ok {
		s = newPartitionStream(p)
		p.streams[key] = s
		p.senders.Go(s.run)
	}
	return s, nil
}

func (p *Producer) choosePartition(ctx context.Context, topic string, value []byte) (int32, error) {
	n, err := p.partitionCount(ctx, topic)
	if err != nil {
		return 0, err
	}
	part := p.cfg.Partitioner.Partition(topic, value, n)
	if part < 0 || int(part) >= n {
		return 0, fmt.Errorf("client: partitioner chose partition %d of %d", part, n)
	}
	return part, nil
}

// partitionCount asks the broker how many partitions topic has and caches
// the answer. An unknown topic counts as one partition: the broker either
// auto-creates it (partition 0 always exists then) or rejects the publish.
func (p *Producer) partitionCount(ctx context.Context, topic string) (int, error) {
	p.mu.Lock()
	n, ok := p.partitionCounts[topic]
	p.mu.Unlock()
	if ok {
		return n, nil
	}

	var md protocol.MetadataResponse
	req := &protocol.MetadataRequest{Topics: []string{topic}}
	if err := p.doWithRetry(ctx, protocol.APIMetadata, req, &md); err != nil {
		return 0, fmt.Errorf("client: metadata for %s: %w", topic, err)
	}
	if len(md.Topics) == 0 {
		return 1, nil
	}
	n = len(md.Topics[0].Partitions)
	p.mu.Lock()
	p.partitionCounts[topic] = n
	p.mu.Unlock()
	return n, nil
}

// initProducer asks the broker for a new producer id. It makes one attempt:
// its caller, partitionStream.retryAfter, does the retrying.
func (p *Producer) initProducer(ctx context.Context) (int64, error) {
	var resp protocol.InitProducerResponse
	if err := p.conn.do(ctx, protocol.APIInitProducer, nil, &resp); err != nil {
		return 0, fmt.Errorf("client: init producer: %w", err)
	}
	return resp.ProducerID, nil
}

// doWithRetry runs a request that is safe to repeat, retrying with backoff
// while the failure is retriable (see isRetriable), for at most
// DeliveryTimeout. Right after a broker restart the redialer may still hand
// out the dead connection once; the retry then dials a new one.
func (p *Producer) doWithRetry(ctx context.Context, apiKey protocol.APIKey, req, resp protocol.Body) error {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.DeliveryTimeout)
	defer cancel()
	for attempt := 0; ; attempt++ {
		err := p.conn.do(ctx, apiKey, req, resp)
		if err == nil || !isRetriable(err) {
			return err
		}
		if sleep(ctx, p.backoff(attempt)) != nil {
			return err // the last real failure says more than "deadline exceeded"
		}
	}
}

// isRetriable reports whether sending the same request again might work:
// the connection broke (any error that is not from the broker), or the
// broker is shutting down and will be back. Cancellation and Close are
// final, and so is every other broker error.
func isRetriable(err error) bool {
	var brokerErr *Error
	if errors.As(err, &brokerErr) {
		return brokerErr.Code == protocol.ErrShuttingDown
	}
	return !errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) &&
		!errors.Is(err, ErrClosed)
}

// backoff is the pause before retry number attempt (0-based) in a run of
// consecutive failures.
func (p *Producer) backoff(attempt int) time.Duration {
	return min(p.cfg.RetryBackoff<<min(attempt, 16), maxBackoff) // the cap on the shift avoids overflow
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
