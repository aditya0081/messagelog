// Package broker is the glue between the network layer and storage: it maps
// (topic, partition) to an open partition log, and owns the metadata store
// and the committed-offsets store. It knows nothing about sockets, so it can
// be tested and reused without a server.
//
// Data layout under Config.DataDir:
//
//	metadata.json                  topics, partitions, producer ids
//	__consumer_offsets/offsets.log committed offsets per group
//	logs/<topic>-<partition>/      one directory per partition log
package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"messagelog/internal/metadata"
	"messagelog/internal/offsets"
	"messagelog/internal/partition"
)

// maxConsumeBytes caps one consume response at roughly 1 MiB of records
// (partition.Read always returns at least one record, even a bigger one).
const maxConsumeBytes = 1 << 20

var (
	// ErrUnknownTopicOrPartition means the topic does not exist (and was not
	// auto-created) or has no partition with that number.
	ErrUnknownTopicOrPartition = errors.New("broker: unknown topic or partition")
	// ErrClosed is returned by calls that race with Close.
	ErrClosed = errors.New("broker: closed")
)

// Config configures a Broker.
type Config struct {
	DataDir           string
	BrokerID          int32
	AdvertisedAddr    string                            // returned in Metadata
	Partition         func(dir string) partition.Config // per-partition config; nil → partition.DefaultConfig
	AutoCreateTopics  bool                              // publishing to an unknown topic creates it
	DefaultPartitions int                               // for auto-create; default 1
}

type partitionKey struct {
	topic     string
	partition int32
}

// Broker serves every partition on this machine. It is safe for concurrent use.
type Broker struct {
	cfg     Config
	meta    metadata.Store
	offsets *offsets.Store

	// createMu serializes topic creation, so a concurrent auto-create never
	// sees a topic in metadata whose partitions are not open yet, and Close
	// never closes the stores under a creation in progress. Lock order:
	// createMu, then mu.
	createMu sync.Mutex

	mu         sync.RWMutex
	partitions map[partitionKey]*partition.Partition
	closed     bool
}

// Open loads metadata, replays the offsets log, and opens (= recovers) every partition.
func Open(cfg Config) (*Broker, error) {
	if cfg.DataDir == "" {
		return nil, errors.New("broker: Config.DataDir is empty")
	}
	if cfg.Partition == nil {
		cfg.Partition = partition.DefaultConfig
	}
	if cfg.DefaultPartitions <= 0 {
		cfg.DefaultPartitions = 1
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("broker: create data dir: %w", err)
	}

	meta, err := metadata.OpenFileStore(filepath.Join(cfg.DataDir, "metadata.json"), cfg.BrokerID)
	if err != nil {
		return nil, fmt.Errorf("broker: %w", err)
	}
	offs, err := offsets.Open(filepath.Join(cfg.DataDir, "__consumer_offsets"), offsets.Options{})
	if err != nil {
		meta.Close()
		return nil, fmt.Errorf("broker: %w", err)
	}
	b := &Broker{cfg: cfg, meta: meta, offsets: offs, partitions: map[partitionKey]*partition.Partition{}}
	for _, t := range meta.Topics() {
		if err := b.openTopic(t); err != nil {
			b.Close()
			return nil, err
		}
	}
	return b, nil
}

// CreateTopic registers a topic and opens its partitions. The metadata
// store rejects a partition count outside 1..metadata.MaxPartitions.
func (b *Broker) CreateTopic(name string, partitions int) (metadata.Topic, error) {
	b.createMu.Lock()
	defer b.createMu.Unlock()
	t, err := b.meta.CreateTopic(name, partitions)
	if err != nil {
		return metadata.Topic{}, err
	}
	// If this fails the topic stays in metadata, and the next Open retries.
	if err := b.openTopic(t); err != nil {
		return metadata.Topic{}, err
	}
	return t, nil
}

// Metadata returns this broker and the requested topics (all if topics is
// empty). Unknown topic names are left out.
func (b *Broker) Metadata(topics []string) ([]metadata.Broker, []metadata.Topic) {
	brokers := []metadata.Broker{{ID: b.cfg.BrokerID, Addr: b.cfg.AdvertisedAddr}}
	if len(topics) == 0 {
		return brokers, b.meta.Topics()
	}
	var found []metadata.Topic
	for _, name := range topics {
		if t, ok := b.meta.Topic(name); ok {
			found = append(found, t)
		}
	}
	return brokers, found
}

// InitProducer allocates a new idempotent producer id.
func (b *Broker) InitProducer() (int64, error) {
	id, err := b.meta.NextProducerID()
	if err != nil {
		return 0, fmt.Errorf("broker: init producer: %w", err)
	}
	return id, nil
}

// Publish is async and preserves call order per partition (the server calls
// it from the connection's read loop). done is called exactly once, possibly
// before Publish returns if the request is rejected up front.
func (b *Broker) Publish(topic string, partitionNum int32, req partition.AppendRequest, done func(partition.AppendResult)) {
	p, err := b.partition(topic, partitionNum, b.cfg.AutoCreateTopics)
	if err != nil {
		done(partition.AppendResult{Offset: -1, Err: err})
		return
	}
	p.Append(req, done)
}

// Consume reads up to limit records starting at offset. It returns the
// records as a chunk of the segment file (sent zero-copy by the server) and
// the partition's high watermark.
//
// If offset is the high watermark (the consumer is caught up), it long-polls
// up to maxWait for new data; if none arrives it returns an empty chunk, not
// an error. On success the caller must Close the chunk: it holds the
// segment file open.
//
// Consume never creates a topic, even with AutoCreateTopics: only publishing
// does, so a mistyped or failed read has no side effects.
func (b *Broker) Consume(ctx context.Context, topic string, partitionNum int32, offset int64, limit int, maxWait time.Duration) (partition.Chunk, int64, error) {
	p, err := b.partition(topic, partitionNum, false)
	if err != nil {
		return partition.Chunk{}, 0, err
	}
	if offset == p.HighWatermark() && maxWait > 0 {
		if err := waitForData(ctx, p, offset, maxWait); err != nil {
			return partition.Chunk{}, 0, err
		}
	}
	chunk, err := p.Read(offset, limit, maxConsumeBytes)
	if errors.Is(err, partition.ErrOffsetOutOfRange) {
		// Name the valid range, so a consumer that fell behind retention
		// knows where it can resume.
		return partition.Chunk{}, 0, fmt.Errorf("broker: consume %s-%d at %d: %w (log has offsets %d to %d)",
			topic, partitionNum, offset, err, p.LogStartOffset(), p.HighWatermark())
	}
	if err != nil {
		return partition.Chunk{}, 0, fmt.Errorf("broker: consume %s-%d at %d: %w", topic, partitionNum, offset, err)
	}
	return chunk, p.HighWatermark(), nil
}

// waitForData blocks until p has a record at offset or maxWait passes.
// Running out of time is the normal "no new data" outcome, so it is not an error.
func waitForData(ctx context.Context, p *partition.Partition, offset int64, maxWait time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()
	err := p.Wait(waitCtx, offset)
	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		return nil
	}
	return err
}

// Commit stores a consumer group's offset for an existing partition.
func (b *Broker) Commit(group, topic string, partitionNum int32, offset int64) error {
	if _, err := b.partition(topic, partitionNum, false); err != nil {
		return err
	}
	key := offsets.Key{Group: group, Topic: topic, Partition: partitionNum}
	if err := b.offsets.Commit(key, offset); err != nil {
		return fmt.Errorf("broker: commit: %w", err)
	}
	return nil
}

// CommittedOffset returns a group's last committed offset; found is false if
// the group never committed on this partition.
func (b *Broker) CommittedOffset(group, topic string, partitionNum int32) (offset int64, found bool, err error) {
	if _, err := b.partition(topic, partitionNum, false); err != nil {
		return 0, false, err
	}
	offset, found = b.offsets.Get(offsets.Key{Group: group, Topic: topic, Partition: partitionNum})
	return offset, found, nil
}

// Close closes every partition (flushing and checkpointing them), then the
// offsets and metadata stores. Calls racing with Close get ErrClosed or the
// partition's own ErrClosed.
func (b *Broker) Close() error {
	b.createMu.Lock() // wait out a topic creation in progress
	defer b.createMu.Unlock()
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	parts := b.partitions
	b.mu.Unlock()

	var errs []error
	for key, p := range parts {
		if err := p.Close(); err != nil {
			errs = append(errs, fmt.Errorf("broker: close %s-%d: %w", key.topic, key.partition, err))
		}
	}
	errs = append(errs, b.offsets.Close(), b.meta.Close())
	return errors.Join(errs...)
}

// partition looks up an open partition, creating its topic first if it does
// not exist and autoCreate is set.
func (b *Broker) partition(topic string, partitionNum int32, autoCreate bool) (*partition.Partition, error) {
	p, err := b.lookup(topic, partitionNum)
	if !errors.Is(err, ErrUnknownTopicOrPartition) || !autoCreate {
		return p, err
	}
	if _, ok := b.meta.Topic(topic); ok {
		return nil, err // the topic exists; the partition number is wrong
	}
	if _, err := b.CreateTopic(topic, b.cfg.DefaultPartitions); err != nil && !errors.Is(err, metadata.ErrTopicExists) {
		return nil, err
	}
	return b.lookup(topic, partitionNum)
}

func (b *Broker) lookup(topic string, partitionNum int32) (*partition.Partition, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, ErrClosed
	}
	p, ok := b.partitions[partitionKey{topic, partitionNum}]
	if !ok {
		return nil, fmt.Errorf("%w: %s-%d", ErrUnknownTopicOrPartition, topic, partitionNum)
	}
	return p, nil
}

// openTopic opens every partition of t and makes them visible to lookups.
func (b *Broker) openTopic(t metadata.Topic) error {
	for _, mp := range t.Partitions {
		dir := filepath.Join(b.cfg.DataDir, "logs", fmt.Sprintf("%s-%d", t.Name, mp.Number))
		p, err := partition.Open(b.cfg.Partition(dir))
		if err != nil {
			return fmt.Errorf("broker: open %s-%d: %w", t.Name, mp.Number, err)
		}
		if err := b.register(partitionKey{t.Name, mp.Number}, p); err != nil {
			p.Close()
			return err
		}
	}
	return nil
}

func (b *Broker) register(key partitionKey, p *partition.Partition) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	b.partitions[key] = p
	return nil
}
