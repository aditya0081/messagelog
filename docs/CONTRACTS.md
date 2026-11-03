# Implementation contracts

Read `docs/DESIGN.md` first. This file pins the decisions the design left
open and the exact exported APIs each package must provide, so packages can be
built in parallel. Internals are free; exported names/signatures are not.

Module: `messagelog`, Go 1.25. All builds/tests run in Docker via `make`
(`make test`, `make test-v P=./internal/partition/... T=TestX`). Never install
Go on the host.

## Decisions (beyond DESIGN.md)

| Topic | Decision |
|---|---|
| Header | 40 bytes, not 24: the 24 from the doc + ProducerID(8) + Sequence(8). See `internal/record`. CRC32-C over all bytes except the CRC field. |
| Topology | Single broker. Each topic has 1 partition for now, but every API carries a partition number and metadata stores partitions + a leader broker id, so multi-broker is additive. |
| Routing | **Client-side.** Idempotence needs a per-(producer, partition) sequence, so the producer must know the partition before it assigns a sequence. The client has a `Partitioner` interface; the only impl today returns partition 0. Wire Publish carries the partition. |
| Ack mode | Configurable. `AckAfterFsync` (default): group commit, ack + make readable only after fsync. `AckBeforeFsync`: ack and make readable right after write(); fsync still runs on the flush timer. |
| Group commit | The partition's single writer goroutine owns the files. It appends each request, and flushes (fsync + ack batch + advance high watermark) when `FlushInterval` has passed since the first unflushed append, or `FlushBytes` accumulated. While it fsyncs, new requests queue in the channel and form the next batch. (Simpler than a separate fsync goroutine: no file-handle sharing across roll.) |
| Readable offset | `highWatermark` = next offset readers may see (exclusive), an `atomic.Int64`. Readers never read past it. |
| Duplicate re-ack | Broker remembers the last 5 (seq → offset) per producer. A duplicate within that window re-acks with its original offset and `Duplicate=true`; older duplicates re-ack with offset -1 and `Duplicate=true`. First sequence of a new producer is 0; a negative sequence is rejected with `ErrOutOfOrderSeq`. `ProducerID == record.NoProducerID` skips dedup. A producer whose newest record is below `LogStartOffset` (retention deleted all its records) is forgotten when the next checkpoint is written (Kafka's rule); it is then a new producer again and must restart at sequence 0, so a client that keeps counting gets `OutOfOrderSequence` and should call InitProducer again. |
| Recovery checkpoint | Per-partition file `recovery.checkpoint` (written tmp + fsync + rename) with: segment base offset, byte position, next offset, and the producer-state snapshot. Written by the writer right after a flush at most every `CheckpointInterval`, on segment roll, and on Close. Recovery scans from that (segment, position) to the end of the log, so only the tail is scanned. No checkpoint → scan everything (an unparseable one is logged and treated as missing). Only the **last** segment may be truncated: every earlier one was fsynced before its successor was created, so a bad record or offset gap there makes `Open` fail with `ErrCorruptLog` instead of deleting acked data. |
| Segment files | `<20-digit zero-padded base offset>.log` and `.index` in the partition dir `<topic>-<partition>`. Index entry = relative offset uint32 + byte position uint32. Index entries are appended to the `.index` file as they are created and fsynced with the log. |
| Long-poll | `Partition.Wait(ctx, offset)` returns once highWatermark > offset or ctx is done. |
| Zero-copy | `Partition.Read` returns a `Chunk` (byte range + its own open fd on the segment file, opened by Read). `Chunk.WriteTo(w)` seeks to the start and does `io.Copy(w, io.LimitReader(f, size))`, which becomes sendfile(2) when `w` is a `*net.TCPConn` on Linux. Because Read opens the file, retention deleting the segment afterwards cannot break a send (an open fd survives unlink); a segment already gone at Read → `ErrOffsetOutOfRange`. **Every Chunk must be `Close`d.** The broker reads record headers only to find byte boundaries, never bodies. |
| Consumer groups | Just a name for now (no membership / numberOfConsumers until rebalancing is designed). |
| Producer ids | Allocated by metadata store, persisted, monotonically increasing from 1. |

## `internal/partition` (storage agent)

```go
type AckMode int
const (
    AckAfterFsync AckMode = iota // default
    AckBeforeFsync
)

type Config struct {
    Dir                string        // partition folder, created if missing
    SegmentBytes       int64         // roll threshold, default 1 GiB
    IndexIntervalBytes int           // sparse index spacing, default 4096
    FlushInterval      time.Duration // group commit linger, default 2ms
    FlushBytes         int           // flush early once this many bytes are unflushed, default 1 MiB
    AckMode            AckMode
    RetentionAge       time.Duration // default 7 * 24h
    RetentionBytes     int64         // default 1 TiB
    RetentionCheck     time.Duration // how often the retention job runs, default 1m
    CheckpointInterval time.Duration // default 1s
    QueueSize          int           // writer queue capacity, default 4096
    Now                func() time.Time // for tests; default time.Now
}
func DefaultConfig(dir string) Config

var (
    ErrClosed           = errors.New("partition: closed")
    ErrOutOfOrderSeq    = errors.New("partition: out of order sequence")
    ErrMessageTooLarge  = errors.New("partition: message too large")
    ErrOffsetOutOfRange = errors.New("partition: offset out of range")
    ErrCorruptLog       = errors.New("partition: corrupt log") // from Open only; needs an operator
)

type AppendRequest struct {
    ProducerID int64 // record.NoProducerID to skip dedup
    Sequence   int64
    Body       []byte
}

type AppendResult struct {
    Offset    int64
    Duplicate bool
    Err       error
}

// Open creates or recovers the partition and starts its writer and retention goroutines.
func Open(cfg Config) (*Partition, error)

// Append enqueues a request in call order. done is called exactly once, from
// the writer goroutine, when the request is acked (or rejected). It blocks
// only when the queue is full. Calls from one goroutine are appended in order.
func (p *Partition) Append(req AppendRequest, done func(AppendResult))

// AppendSync is Append + wait; for tests and simple callers.
func (p *Partition) AppendSync(ctx context.Context, req AppendRequest) AppendResult

func (p *Partition) HighWatermark() int64  // next readable offset (exclusive)
func (p *Partition) LogStartOffset() int64 // first offset still on disk

// Read returns up to maxRecords records (and roughly maxBytes, but always at
// least one record if any is available) starting at offset, never past the
// high watermark, never spanning two segments.
// offset == HighWatermark → empty Chunk, nil error.
// offset < LogStartOffset or > HighWatermark → ErrOffsetOutOfRange.
func (p *Partition) Read(offset int64, maxRecords, maxBytes int) (Chunk, error)

// Wait blocks until HighWatermark() > offset or ctx is done (returns ctx.Err()).
func (p *Partition) Wait(ctx context.Context, offset int64) error

// EnforceRetention deletes expired closed segments now. The background job calls it too.
func (p *Partition) EnforceRetention() error

// Close drains the queue, flushes, writes a checkpoint, and closes files.
func (p *Partition) Close() error

// A non-empty Chunk holds an open fd, opened by Read. The caller must Close
// it once sent (also on error paths). Use a chunk from one goroutine at a
// time: WriteTo moves the fd's offset.
type Chunk struct {
    Path        string // segment file (informational)
    Position    int64  // byte position of the first record
    Size        int64  // bytes; 0 = empty
    FirstOffset int64
    NextOffset  int64  // offset after the last record in the chunk
    // unexported: file *os.File
}
func (c Chunk) WriteTo(w io.Writer) (int64, error) // zero-copy-friendly; does not close; repeatable
func (c Chunk) Bytes() ([]byte, error)             // reads into memory (ReadAt); tests; does not close
func (c Chunk) Close() error                       // idempotent; safe on an empty or zero Chunk
```

## `internal/metadata` (meta agent)

```go
type Broker struct { ID int32; Addr string }

type Partition struct {
    ID       int64 // globally unique
    TopicID  int64
    Number   int32
    LeaderID int32 // broker id; always this broker for now
}

type Topic struct {
    ID         int64
    Name       string
    Partitions []Partition
}

var (
    ErrTopicExists  = errors.New("metadata: topic already exists") // also for a name equal ignoring case (strings.EqualFold)
    ErrInvalidTopic = errors.New("metadata: invalid topic name") // allowed: [a-zA-Z0-9._-], 1..200 chars, not starting with "__"
    ErrInvalidPartitionCount = errors.New("metadata: invalid partition count") // outside 1..MaxPartitions
)
const MaxPartitions = 1024

// Store is the metadata interface. Today a file; later backed by consensus.
type Store interface {
    CreateTopic(name string, numPartitions int) (Topic, error)
    Topic(name string) (Topic, bool)
    Topics() []Topic
    NextProducerID() (int64, error) // persisted before returning
    Close() error
}

// OpenFileStore loads (or creates) a JSON file; every change is written
// tmp + fsync + rename. brokerID becomes LeaderID of new partitions.
func OpenFileStore(path string, brokerID int32) (*FileStore, error)
```

## `internal/offsets` (meta agent)

```go
type Key struct {
    Group     string
    Topic     string
    Partition int32
}

type Options struct {
    CompactAfterBytes int64 // compact when the log exceeds this and is >2x live size; default 64 MiB
}

// Open replays the commit log in dir (file "offsets.log") into memory.
// A torn/corrupt tail is truncated, like partition recovery.
func Open(dir string, opts Options) (*Store, error)
// Commit is durable (fsync) before return; safe for concurrent use. A failed
// automatic compaction does not fail the commit (logged, retried once the
// log doubles). If the log can no longer be trusted (a failed append could
// not be cut back off, or the dir fsync after a compaction rename failed),
// Commit and Compact return that error until the store is reopened.
func (s *Store) Commit(k Key, offset int64) error
func (s *Store) Get(k Key) (offset int64, ok bool)
func (s *Store) Compact() error                    // rewrite with latest per key: tmp + fsync + rename
func (s *Store) Close() error
```

## `internal/fsutil`

Durability helpers shared by the storage packages:

```go
func SyncDir(dir string) error                   // fsync a directory (create/rename/unlink durable)
func MkdirAll(dir string) error                  // os.MkdirAll + fsync the parent
func WriteFileAtomic(path string, data []byte) error // tmp + fsync + rename + SyncDir
```

## `internal/protocol` (meta agent)

Framing: `uint32 size | payload` (size excludes itself). Big-endian.
- Request payload: `uint16 apiKey | uint32 correlationID | body`
- Response payload: `uint32 correlationID | uint16 errorCode | string errorMessage | body` (body present only if errorCode == 0)
- `string` = uint16 length + bytes; `bytes` = uint32 length + bytes; bool = 1 byte.

| apiKey | Request body | Response body |
|---|---|---|
| 1 CreateTopic | name string, partitions int32 | – |
| 2 Metadata | count int32, topics []string (0 = all) | brokers: count int32, {id int32, addr string}; topics: count int32, {name string, partitions: count int32, {number int32, leaderID int32}} |
| 3 InitProducer | – | producerID int64 |
| 4 Publish | topic string, partition int32, producerID int64, sequence int64, value bytes | offset int64, duplicate bool |
| 5 Consume | topic string, partition int32, offset int64, limit int32, maxWaitMs int32 | highWatermark int64, records bytes (raw back-to-back records) |
| 6 Commit | group string, topic string, partition int32, offset int64 | – |
| 7 GetCommittedOffset | group string, topic string, partition int32 | offset int64, found bool |

Error codes: 0 None, 1 Unknown, 2 UnknownTopicOrPartition, 3 OffsetOutOfRange,
4 OutOfOrderSequence, 5 MessageTooLarge, 6 TopicExists, 7 InvalidRequest,
8 ShuttingDown, 9 InvalidTopic.

Go API shape (names fixed):

```go
type APIKey uint16
const ( APICreateTopic APIKey = 1; APIMetadata = 2; APIInitProducer = 3; APIPublish = 4; APIConsume = 5; APICommit = 6; APIGetCommittedOffset = 7 )

type ErrorCode uint16
const ( ErrNone ErrorCode = iota; ErrUnknown; ErrUnknownTopicOrPartition; ErrOffsetOutOfRange; ErrOutOfOrderSequence; ErrMessageTooLarge; ErrTopicExists; ErrInvalidRequest; ErrShuttingDown; ErrInvalidTopic )
func (c ErrorCode) String() string

// One struct per request/response body, e.g.
type PublishRequest struct { Topic string; Partition int32; ProducerID, Sequence int64; Value []byte }
type PublishResponse struct { Offset int64; Duplicate bool }
type ConsumeRequest struct { Topic string; Partition int32; Offset int64; Limit, MaxWaitMs int32 }
type ConsumeResponse struct { HighWatermark int64; Records []byte }
// ...CreateTopicRequest, MetadataRequest/Response (+ BrokerInfo, TopicInfo, PartitionInfo),
// InitProducerResponse, CommitRequest, GetCommittedOffsetRequest/Response.
// Each body type implements:
type Body interface {
    AppendTo(b []byte) []byte
    DecodeFrom(b []byte) error // errors on short/trailing bytes
}

type Request struct { APIKey APIKey; CorrelationID uint32; Body []byte /* raw */ }
type Response struct { CorrelationID uint32; Error ErrorCode; Message string; Body []byte /* raw */ }

func ReadFrame(r io.Reader, maxSize uint32) ([]byte, error)
func WriteRequest(w io.Writer, apiKey APIKey, corrID uint32, body Body) error  // body may be nil
func WriteResponse(w io.Writer, corrID uint32, code ErrorCode, msg string, body Body) error
func ParseRequest(frame []byte) (Request, error)
func ParseResponse(frame []byte) (Response, error)

// ConsumeResponseHeader returns the frame bytes up to (and including) the
// records length prefix, with the frame size already accounting for
// recordsLen bytes that the caller then streams straight from the segment file.
func ConsumeResponseHeader(corrID uint32, highWatermark int64, recordsLen uint32) []byte
```

## `internal/broker` — glue, no networking

Data layout under `DataDir`: `metadata.json`, `__consumer_offsets/offsets.log`,
`logs/<topic>-<partition>/` (one partition dir per DESIGN.md).

```go
type Config struct {
    DataDir           string
    BrokerID          int32
    AdvertisedAddr    string                             // returned in Metadata
    Partition         func(dir string) partition.Config  // per-partition config; nil → partition.DefaultConfig
    AutoCreateTopics  bool                               // publish to an unknown topic creates it (consume never does)
    DefaultPartitions int                                // for auto-create; default 1
}

var ErrUnknownTopicOrPartition = errors.New("broker: unknown topic or partition")

// Open loads metadata, replays the offsets log, and opens (= recovers) every partition.
func Open(cfg Config) (*Broker, error)
func (b *Broker) CreateTopic(name string, partitions int) (metadata.Topic, error)
func (b *Broker) Metadata(topics []string) ([]metadata.Broker, []metadata.Topic) // empty = all
func (b *Broker) InitProducer() (int64, error)
// Publish is async and preserves call order per partition (the server calls it from the connection's read loop).
func (b *Broker) Publish(topic string, partition int32, req partition.AppendRequest, done func(partition.AppendResult))
// Consume long-polls up to maxWait when offset == high watermark. It never
// auto-creates. An out-of-range offset's error names the valid range.
func (b *Broker) Consume(ctx context.Context, topic string, partition int32, offset int64, limit int, maxWait time.Duration) (partition.Chunk, int64, error)
func (b *Broker) Commit(group, topic string, partition int32, offset int64) error
func (b *Broker) CommittedOffset(group, topic string, partition int32) (int64, bool, error)
func (b *Broker) Close() error // waits out a topic creation in progress (createMu)
```

## `internal/server` — TCP

```go
type Options struct {
    MaxConns         int           // open connections; one more is closed on accept. Default 1024
    MaxInFlightBytes int           // request bytes read but unanswered, per connection. Default 64 MiB (≥ one max frame)
    IdleTimeout      time.Duration // close a connection that sends nothing this long. Default 5m
}
func New(b *broker.Broker, log *slog.Logger, opts Options) *Server
func (s *Server) Serve(ln net.Listener) error // ErrServerClosed after Close
func (s *Server) Close() error
```

- One goroutine reads frames per connection. **Publish is dispatched inline** (just an enqueue via `Broker.Publish`) so per-connection request order = append order, which pipelined idempotent producers rely on. Every other request runs in its own goroutine (Consume may long-poll).
- Per-connection in-flight budget: at most 256 unanswered requests **and** `MaxInFlightBytes` of their frames. The read loop reserves a frame's bytes after reading its 4-byte size and before allocating its payload, so a client that pipelines faster than it is answered is held back by TCP flow control, never buffered. Once a budget is used up the read loop stops reading.
- Publish acks go from the partition writer's callback through a channel sized to the in-flight count cap to one ack-writer goroutine per connection, so **the partition writer never blocks** on a slow client socket. The ack writer batches queued acks into one write.
- Responses may complete out of order; correlation IDs match them up. All writes to a conn go through one mutex.
- Consume response: write `protocol.ConsumeResponseHeader` then `chunk.WriteTo(conn)` directly on the `*net.TCPConn` (not through a bufio.Writer) so Linux uses sendfile. Hold the write mutex for both. `maxWaitMs` is clamped to 30s.
- Errors map to protocol error codes (partition/metadata/broker errors → codes).
- `Close()` graceful: stop accepting, close conns, wait for handlers. The broker is closed by the caller.

## `client`

- `Conn`: one TCP connection, a reader goroutine dispatching responses by correlation ID, many in-flight requests. `Do(ctx, apiKey, req Body, resp Body) error`; protocol error codes surface as `*client.Error{Code, Message}` with `errors.Is` support for sentinel codes. A response that has already arrived wins over a ctx that ends at the same time. Each write has a fixed 10s deadline (not the ctx's: a timed-out write leaves half a frame and breaks the connection for every request on it).
- `Partitioner` interface `Partition(topic string, value []byte, numPartitions int) int32`; `SinglePartition{}` returns 0.
- `Producer` (`ProducerConfig{Partitioner, RetryBackoff, DeliveryTimeout}`, defaults 50ms backoff, 30s delivery timeout):
  - One **partition stream** per (topic, partition): a goroutine that owns the stream's producer id, sequence counter, connection and in-flight queue (≤ 256). `Publish` queues the message and waits for its outcome or its ctx. **A caller that gives up stops waiting, not sending**: its message keeps its sequence and is delivered in the background for up to `DeliveryTimeout`. A ctx already done when `Publish` is called sends nothing; a message whose caller gave up before the stream numbered it is dropped unsent.
  - In-flight messages have contiguous sequences under one producer id, are written in order on one connection, and are resolved strictly oldest first. Oldest acked → report. Connection lost / `ShuttingDown` → back off, redial, rewrite every in-flight message unchanged (the broker re-acks duplicates). Other broker error → report (not written). Past its deadline → `ErrDeliveryTimeout` (maybe written).
  - **Producer id reset rule:** only when the *oldest* in-flight message gets `OutOfOrderSequence`. Since nothing older is outstanding, that means the previous sequence is gone for good (an older message failed, or the broker expired the producer after retention). The stream then calls InitProducer and renumbers every in-flight message from 0. So ids change only after a real failure, never because a caller gave up, and `OutOfOrderSequence` is never retried as is. Known limit: if a copy of a failed message were still in transit more than `DeliveryTimeout` later, messages after it could be written twice (Kafka's producer epochs close this; not implemented).
  - InitProducer and the Metadata partition-count lookup retry with the same backoff and redial (a just-restarted broker's dead connection fails once, then a new one is dialed).
  - **An error from `Publish` means "maybe written"**; publishing again by hand can duplicate (as with Kafka).
- `Consumer`: `Consume`, `Commit`, `CommittedOffset`; plus a small group helper that starts from the committed offset (0 if none), returns decoded `[]record.Record` from `Poll(ctx)`, and `Commit(ctx)`s its position. If retention deleted the position, `Poll` returns `ErrOffsetOutOfRange` (the message names the valid range) until the caller moves with `SetPosition`.

## `cmd/broker`, `cmd/mlctl`
- broker flags: `-addr -advertise -data -id -ack (fsync|async) -flush-interval -segment-bytes -retention -auto-create -max-conns -idle-timeout`; SIGINT/SIGTERM → graceful shutdown.
- mlctl subcommands: `create-topic`, `topics`, `publish TOPIC MSG...`, `consume TOPIC [-offset -limit -follow]`, `commit GROUP TOPIC OFFSET`, `committed GROUP TOPIC`, `bench [-n -c -size]` (prints throughput + p50/p99 publish latency; stops at the first error; `-size` 0..1 MiB).
