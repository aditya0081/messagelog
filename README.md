# messagelog

A Kafka-like durable message log, single broker, in Go. Producers publish to
topic partitions, consumers read at their own pace and replay, consumer groups
commit their progress. Publishing is exactly-once (idempotent producer), acks
come after a group-commit fsync, and reads go from page cache to socket with
sendfile.

## Docs

Start with the design, then read in this order:

| Doc | What it explains |
|---|---|
| [Design](docs/DESIGN.md) | The original single-machine design this repo implements, plus a design → code map |
| [Architecture](docs/architecture.md) | Packages, goroutine ownership, life of a publish and a consume, on-disk layout |
| [Storage](docs/storage.md) | Record format, segments, sparse index lookup, zero-copy read path, retention |
| [Durability and recovery](docs/durability-and-recovery.md) | Group commit, ack modes, high watermark, checkpoint, crash recovery |
| [Exactly-once](docs/exactly-once.md) | Idempotent producer: ids, sequences, dedup, background redelivery, the chaos test |
| [Protocol](docs/protocol.md) | Binary wire format, APIs, error codes, pipelining, server limits |
| [Consumers and offsets](docs/consumers-and-offsets.md) | Consume + long-poll, group consumer, committed offsets store, metadata store |
| [Running and testing](docs/running-and-testing.md) | Make targets, demo session, flags, bench, what each test proves |
| [Contracts](docs/CONTRACTS.md) | Pinned decisions and exact package APIs |

## Layout

| Package | Role |
|---|---|
| `internal/record` | On-disk/on-wire record: 40-byte header + body, CRC32-C |
| `internal/segment` | One segment file + its sparse offset index |
| `internal/partition` | A partition log: single writer, group commit, dedup, recovery, retention, zero-copy reads |
| `internal/metadata` | Topics, partitions, producer ids in one JSON file |
| `internal/offsets` | Committed consumer-group offsets: append-only log + in-memory map, compaction |
| `internal/protocol` | Binary wire format: framing, correlation ids, request/response bodies |
| `internal/broker` | Glue: (topic, partition) → partition log, long-poll, commits. No networking |
| `internal/server` | TCP server: pipelined requests, in-flight caps (count + bytes), connection cap, idle timeout, sendfile consume path |
| `client` | `Conn`, idempotent `Producer` (per-partition background sender, reconnect + resend), `Consumer`, `GroupConsumer` |
| `cmd/broker` | The broker binary |
| `cmd/mlctl` | CLI: topics, publish, consume, commit, bench |

## Run

Everything runs in Docker (`golang:1.25`); nothing to install.

```sh
make test                 # all tests with -race
make broker               # start the broker in a container (data in ./data)
make cli ARGS="topics"    # run mlctl against it
make broker-stop
```

## Demo

```text
$ make cli ARGS="create-topic orders"
created orders with 1 partition(s)
$ make cli ARGS="publish orders hello world"
partition=0 offset=0
partition=0 offset=1
$ make cli ARGS="consume orders"
offset=0 time=2026-11-02T16:26:58.26Z hello
offset=1 time=2026-11-02T16:26:58.264Z world
$ make cli ARGS="commit billing orders 2"
committed billing orders-0 at 2
$ make cli ARGS="committed billing orders"
2
$ make cli ARGS="bench -n 50000 -c 128"
published 50000 x 1024 B with 128 publishers in 3.523s
throughput: 14191 msgs/s, 14.53 MB/s
latency:    p50 9.268ms, p99 11.904ms, max 15.097ms
```

(Bench numbers are from Docker on a laptop with `-ack fsync`: every ack waits
for an fsync, so throughput scales with how many publishes share one.)

## Key decisions

| Topic | Decision |
|---|---|
| Record header | 40 bytes: offset, timestamp, length, CRC32-C, producer id, sequence |
| Durability | Group commit: the partition's single writer fsyncs every ~2 ms (or 1 MiB), then acks the batch. `-ack async` acks before fsync |
| Readable offset | Atomic high watermark; readers never see unacked data |
| Exactly-once | Per (producer id, partition) sequence; broker drops `seq <= last`, rejects gaps. A per-partition sender goroutine resends the same sequences in order after a reconnect, in the background, for up to `DeliveryTimeout` (30s), even if the `Publish` caller stopped waiting. So an error from `Publish` means "maybe written": publishing again by hand can duplicate, as with Kafka |
| Routing | Client-side partitioner, because the sequence is per partition |
| Ordering on the wire | Server appends Publish requests in the connection's read loop, so pipelined requests keep their order |
| Long-poll | Consume at the high watermark waits up to `maxWait` for new data |
| Zero-copy | Consume response = header + `sendfile` straight from the segment file |
| Recovery | Checkpoint (segment, position, producer state); only the tail after it is CRC-scanned and truncated |
| Committed offsets | Separate append-only log, compacted, replayed on start |
| Retention | Whole segments deleted by age (7 days) or size (1 TiB) |

## Known limitations

- Each partition stream gets its own producer id; there are no producer epochs yet.
- The broker remembers the last 5 sequences per producer, but the client can have up to 256 in flight. After a reconnect, resends older than that window are still not written twice, but come back with `Offset = -1`.
- The sparse index is fully in memory (about 2 GiB for a full 1 TiB partition), and reads do one `pread` per record header.
- No API returns a partition's log start offset; `GroupConsumer.SetPosition` recovers from `OffsetOutOfRange`.

## Next: replication

Leader/follower partitions with a replicated high watermark, leader election
and log truncation on failover, and metadata through consensus instead of a
JSON file. Then partition assignment and rebalancing for consumer groups.
