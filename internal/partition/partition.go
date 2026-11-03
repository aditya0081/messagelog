// Package partition is the storage engine for one partition of a topic: an
// append-only log split into segments (package segment), written by a single
// goroutine and read lock-free by any number of consumers.
//
// The moving parts, mapped to docs/DESIGN.md:
//   - writer.go: the single writer, dedup check, group commit, segment roll
//     ("Publish flow").
//   - reader.go: Read, Chunk and Wait ("Consume flow", long-poll, zero-copy).
//   - recovery.go + checkpoint.go: repair of a torn tail on Open
//     ("Crash recovery flow").
//   - producerstate.go: idempotent producer sequences ("Exactly-once").
//   - retention.go: deleting expired segments ("Retention flow").
//
// Any I/O error in the writer takes the partition offline: every later
// append fails with that error until the partition is reopened, which runs
// recovery. Guessing which bytes made it to disk is not worth the risk.
package partition

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"messagelog/internal/fsutil"
	"messagelog/internal/segment"
)

// AckMode decides when a producer is acked and the data becomes readable.
type AckMode int

const (
	// AckAfterFsync acks and exposes records only once they are fsynced.
	AckAfterFsync AckMode = iota
	// AckBeforeFsync acks and exposes records right after write(); fsync
	// still runs on the group-commit timer. Faster, but a machine crash can
	// lose acked data (Kafka's default, relying on replication).
	AckBeforeFsync
)

// Config tunes a partition. Zero fields take the DefaultConfig values.
type Config struct {
	Dir                string        // partition folder, created if missing
	SegmentBytes       int64         // roll threshold, default 1 GiB
	IndexIntervalBytes int           // sparse index spacing, default 4096
	FlushInterval      time.Duration // group commit linger, default 2ms
	FlushBytes         int           // flush early once this many bytes are unflushed, default 1 MiB
	AckMode            AckMode
	RetentionAge       time.Duration    // default 7 * 24h
	RetentionBytes     int64            // default 1 TiB
	RetentionCheck     time.Duration    // how often the retention job runs, default 1m
	CheckpointInterval time.Duration    // default 1s
	QueueSize          int              // writer queue capacity, default 4096
	Now                func() time.Time // for tests; default time.Now
}

// DefaultConfig returns the production defaults for a partition in dir.
func DefaultConfig(dir string) Config {
	return Config{
		Dir:                dir,
		SegmentBytes:       1 << 30,
		IndexIntervalBytes: 4096,
		FlushInterval:      2 * time.Millisecond,
		FlushBytes:         1 << 20,
		AckMode:            AckAfterFsync,
		RetentionAge:       7 * 24 * time.Hour,
		RetentionBytes:     1 << 40,
		RetentionCheck:     time.Minute,
		CheckpointInterval: time.Second,
		QueueSize:          4096,
		Now:                time.Now,
	}
}

// withDefaults fills every zero field from DefaultConfig.
func (c Config) withDefaults() Config {
	d := DefaultConfig(c.Dir)
	setDefault(&c.SegmentBytes, d.SegmentBytes)
	setDefault(&c.IndexIntervalBytes, d.IndexIntervalBytes)
	setDefault(&c.FlushInterval, d.FlushInterval)
	setDefault(&c.FlushBytes, d.FlushBytes)
	setDefault(&c.RetentionAge, d.RetentionAge)
	setDefault(&c.RetentionBytes, d.RetentionBytes)
	setDefault(&c.RetentionCheck, d.RetentionCheck)
	setDefault(&c.CheckpointInterval, d.CheckpointInterval)
	setDefault(&c.QueueSize, d.QueueSize)
	if c.Now == nil {
		c.Now = d.Now
	}
	return c
}

func setDefault[T int | int64 | time.Duration](v *T, def T) {
	if *v <= 0 {
		*v = def
	}
}

func (c Config) validate() error {
	if c.Dir == "" {
		return errors.New("partition: Config.Dir is empty")
	}
	// Index entries store byte positions as uint32.
	if c.SegmentBytes > math.MaxUint32 {
		return fmt.Errorf("partition: SegmentBytes %d exceeds 4 GiB", c.SegmentBytes)
	}
	return nil
}

var (
	ErrClosed           = errors.New("partition: closed")
	ErrOutOfOrderSeq    = errors.New("partition: out of order sequence")
	ErrMessageTooLarge  = errors.New("partition: message too large")
	ErrOffsetOutOfRange = errors.New("partition: offset out of range")
	// ErrCorruptLog is returned by Open when a segment other than the last
	// has a bad record or the segments do not line up. Those segments were
	// fsynced before the next one was created, so this is not a torn write
	// that recovery may cut off: it needs an operator.
	ErrCorruptLog = errors.New("partition: corrupt log")
)

// AppendRequest is one message to append.
type AppendRequest struct {
	ProducerID int64 // record.NoProducerID to skip dedup
	Sequence   int64
	Body       []byte // must not be modified until done is called
}

// AppendResult is the outcome of an AppendRequest.
type AppendResult struct {
	Offset    int64 // -1 on error, or for a duplicate older than the dedup window
	Duplicate bool
	Err       error
}

type appendRequest struct {
	AppendRequest
	done func(AppendResult)
}

// Partition is one partition log. All methods are safe for concurrent use.
type Partition struct {
	cfg Config

	// closeMu orders Append's send against Close closing the queue, so a
	// send never hits a closed channel.
	closeMu   sync.RWMutex
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error

	queue      chan appendRequest
	writer     *writer
	writerDone chan struct{}

	retentionMu   sync.Mutex // one EnforceRetention at a time
	stopRetention chan struct{}
	retentionDone chan struct{}

	// segments is sorted by base offset; the last one is active. Readers
	// hold segMu only to pick a segment, never while doing I/O.
	segMu    sync.RWMutex
	segments []*segment.Segment

	highWatermark  atomic.Int64
	logStartOffset atomic.Int64
	// hwChanged is closed and replaced every time the high watermark moves,
	// waking every Wait call at once.
	hwChanged atomic.Pointer[chan struct{}]
}

// Open creates or recovers the partition and starts its writer and retention goroutines.
func Open(cfg Config) (*Partition, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := fsutil.MkdirAll(cfg.Dir); err != nil {
		return nil, fmt.Errorf("partition: %w", err)
	}
	state, err := recoverLog(cfg)
	if err != nil {
		return nil, fmt.Errorf("partition: recover %s: %w", cfg.Dir, err)
	}

	p := &Partition{
		cfg:           cfg,
		queue:         make(chan appendRequest, cfg.QueueSize),
		writerDone:    make(chan struct{}),
		stopRetention: make(chan struct{}),
		retentionDone: make(chan struct{}),
		segments:      state.segments,
	}
	active := state.segments[len(state.segments)-1]
	p.highWatermark.Store(active.NextOffset())
	p.logStartOffset.Store(state.segments[0].Base())
	hwChanged := make(chan struct{})
	p.hwChanged.Store(&hwChanged)
	p.writer = newWriter(p, active, state.producers)

	go p.runWriter()
	go p.runRetention()
	return p, nil
}

// Append enqueues a request in call order. done is called exactly once, from
// the writer goroutine, when the request is acked (or rejected); it must not
// block. The one exception: on a closed partition done is called right away
// with ErrClosed. Append blocks only when the queue is full.
func (p *Partition) Append(req AppendRequest, done func(AppendResult)) {
	p.closeMu.RLock()
	defer p.closeMu.RUnlock()
	if p.closed.Load() {
		done(AppendResult{Offset: -1, Err: ErrClosed})
		return
	}
	p.queue <- appendRequest{AppendRequest: req, done: done}
}

// AppendSync is Append + wait; for tests and simple callers.
func (p *Partition) AppendSync(ctx context.Context, req AppendRequest) AppendResult {
	result := make(chan AppendResult, 1)
	p.Append(req, func(r AppendResult) { result <- r })
	select {
	case r := <-result:
		return r
	case <-ctx.Done():
		return AppendResult{Offset: -1, Err: ctx.Err()}
	}
}

// HighWatermark is the next offset readers may see (exclusive).
func (p *Partition) HighWatermark() int64 { return p.highWatermark.Load() }

// LogStartOffset is the first offset still on disk.
func (p *Partition) LogStartOffset() int64 { return p.logStartOffset.Load() }

// Close stops accepting appends, drains the queue, flushes, writes a
// checkpoint, stops the background goroutines and closes the files.
// It is idempotent; every call returns the first call's result.
func (p *Partition) Close() error {
	p.closeOnce.Do(func() {
		p.closeMu.Lock()
		p.closed.Store(true)
		close(p.queue) // the writer drains what is left, then shuts down
		p.closeMu.Unlock()

		<-p.writerDone
		p.closeErr = errors.Join(p.writer.shutdownErr, p.stopBackground())
	})
	return p.closeErr
}

// runWriter runs the writer loop and signals when it has returned.
func (p *Partition) runWriter() {
	defer close(p.writerDone)
	p.writer.run()
}

// stopBackground stops retention, closes every segment file and wakes
// waiters. The writer must already have stopped.
func (p *Partition) stopBackground() error {
	close(p.stopRetention)
	<-p.retentionDone

	p.retentionMu.Lock()
	defer p.retentionMu.Unlock()
	p.segMu.Lock()
	defer p.segMu.Unlock()
	var errs []error
	for _, s := range p.segments {
		errs = append(errs, s.Close())
	}
	p.notifyWaiters()
	return errors.Join(errs...)
}

// addSegment publishes a newly rolled segment to readers.
func (p *Partition) addSegment(s *segment.Segment) {
	p.segMu.Lock()
	p.segments = append(p.segments, s)
	p.segMu.Unlock()
}
