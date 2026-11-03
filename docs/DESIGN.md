# Distributed Message Log — Single Machine Design

> This is the original design this repo implements, kept as written.
> Decisions it left open are pinned in [CONTRACTS.md](CONTRACTS.md); where each
> part lives in the code is mapped in [From design to code](#from-design-to-code) at the bottom.

## Problem
Design a durable, distributed message log (like Kafka). Producers publish to topics split into partitions; consumers read in order at their own pace and can replay. Consumer groups track their own progress. Retention: 7 days or 1 TB per partition. Exactly-once from producer to log. Scale: 2M writes/sec, 10M reads/sec, 1 KB avg messages, p99 publish < 10 ms, survive loss of 2 machines.

This doc covers the single machine stage. Replication and multi-broker routing come next.

## API
- publish(topic, message) → ack with offset. Partition routing (key-based or broker-chosen) to be decided later.
- consume(topic, partition, offset, limit) → messages starting at offset. No consumer group needed; the consumer says where to read.
- commit(topic, partition, consumerGroup, offset) → stores group progress.
- getCommittedOffset(topic, partition, consumerGroup) → last committed offset, used on restart/rebalance.

## Entities
- Message: id (may be dropped, offset acts as id), content, createdAt.
- Topic: id, name.
- Partition: id (globally unique), topicId, partitionNumber, nextOffset.
- ConsumerGroup: id, name, numberOfConsumers.
- CommittedOffset: partitionId, consumerGroupId, offset. Owned by group + partition, not by an individual consumer, so it survives rebalances.

## Message Storage
- Append-only log per partition: ordering for free, sequential writes.
- Partition folder named topic-partitionNumber, split into segments of ~1 GB.
- Each segment is named by its first offset. Binary search segment names to find the right file.
- Retention: delete a whole segment once its newest message is older than 7 days (or size limit exceeded).

### Record layout (fixed 24-byte header + body)
| Field | Size |
|---|---|
| Offset | 8 bytes |
| Timestamp | 8 bytes |
| Length | 4 bytes |
| CRC32 | 4 bytes |
| Content | Length bytes |

Also add producerId and sequenceNumber to the header for idempotency.

### Sparse index
- One index file per segment: (offset → byte position), one entry every ~4 KB, kept in memory.
- Lookup: binary search segments → binary search index for nearest lower entry → seek → scan forward using each record's length.

## Other Data
- Committed offsets: one internal append-only log keyed by (group, partition), with an in-memory map of latest offsets. Log compaction keeps only the latest record per key. Rebuild map from log on restart.
- Topic/partition metadata: small file + in-memory copy (needs consensus once multi-broker).

## Durability: fsync
- write() lands in OS page cache; a background thread fsyncs.
- Group commit: fsync every 2–5 ms (or when enough bytes accumulate); ack all producers in that batch afterwards. Keeps p99 within 10 ms.
- Alternative (Kafka's default): ack before fsync, rely on replication for durability.

## Crash Recovery
- Each record carries a CRC32 of its bytes.
- On restart, scan only the last segment from the recovery-point checkpoint:
  1. If fewer than 24 bytes remain → truncate.
  2. If length > 1 MB or runs past end of file → truncate.
  3. Read body, recompute CRC; mismatch → truncate.
- Body is treated as opaque bytes, never parsed before validation. CRC32 is hardware-accelerated, so the scan is fast.

## Networking
- Custom binary protocol over long-lived TCP, not HTTP.
- Request/response with correlation IDs; multiple in-flight requests per connection.
- Consumers poll; broker may hold (long-poll) until data arrives.
- Read path: zero-copy from page cache to socket (sendfile); broker doesn't parse records.

## Concurrency
- Single writer per partition (thread + queue) preserves ordering; partitions write in parallel.
- Readers are lock-free: writer publishes last complete offset via an atomic variable; readers read only up to it.

## Exactly-once (Idempotent Producer)
- Content hashes fail (identical legit messages); idempotency keys are costly to store.
- Each producer gets a producerId and a per-partition sequence number.
- Broker keeps last sequence per (producer, partition):
  - seq == last + 1 → write, update.
  - seq <= last → duplicate retry; skip write, re-ack.
  - seq > last + 1 → gap; return error.
- Map is rebuilt after a crash by scanning record headers.

## End-to-end Flows
### Publish flow
1. Producer sends publish(topic, message) with producerId and sequence number over a long-lived TCP connection.
2. Request enters the partition's queue; a single writer thread processes it, preserving order.
3. Dedup check: seq == last + 1 → accept; seq <= last → duplicate, re-ack without writing; seq > last + 1 → gap error.
4. Writer assigns the partition's next offset and builds the 24-byte header (offset, timestamp, length, CRC32) plus producerId and sequence.
5. Record is appended to the active segment; a sparse index entry is added every ~4 KB.
6. When the segment reaches ~1 GB, it is closed and a new one starts, named by its first offset.
7. Background thread fsyncs every 2–5 ms (group commit).
8. After fsync, every producer in that batch gets an ack with its offset.
9. Writer advances the readable offset (atomic), making the messages visible to consumers.

### Consume flow
1. Consumer sends consume(topic, partition, offset, limit), long-polling if no new data.
2. Broker binary searches segment names to find the segment holding the offset.
3. Broker binary searches that segment's sparse index for the nearest lower entry and seeks to its byte position.
4. Broker scans forward a few records using each header's length to reach the exact offset.
5. Broker reads only up to the readable offset and sends bytes zero-copy from page cache to the socket, without parsing.

### Commit flow
1. After processing, consumer sends commit(topic, partition, consumerGroup, offset).
2. Broker appends the commit to the internal offsets log and updates the in-memory map.
3. Log compaction keeps only the latest offset per (group, partition).
4. On restart or rebalance, a consumer calls getCommittedOffset and resumes from there.

### Retention flow
1. Background job checks closed segments periodically.
2. A segment whose newest message is older than 7 days (or exceeds the size limit) is deleted whole, along with its index.

### Crash recovery flow
1. On restart, broker loads metadata and rebuilds the committed-offset map from the offsets log.
2. Broker scans the last segment from the recovery-point checkpoint, validating each header, length and CRC.
3. File is truncated at the first incomplete or corrupt record.
4. Producer sequence map is rebuilt from record headers, and the index is rebuilt for the scanned range.

## Next Steps
- Partition routing in publish.
- Replication: leader/follower, high-water mark, leader failure and truncation, avoiding split brain.
- Consumer group assignment and rebalancing.
- Atomic read-process-write (transactions).

---

## From design to code

| Design section | Implemented in | Explained in | Differences from this doc |
|---|---|---|---|
| API | [internal/protocol](../internal/protocol), [internal/server](../internal/server), [client](../client) | [protocol.md](protocol.md) | Adds `CreateTopic`, `Metadata`, `InitProducer`. Publish carries a partition number (client-side routing, see below). |
| Entities | [internal/metadata](../internal/metadata) | [consumers-and-offsets.md](consumers-and-offsets.md) | Offset is the message id. Consumer groups are just names until rebalancing is designed. |
| Message storage, segments | [internal/segment](../internal/segment), [internal/partition](../internal/partition) | [storage.md](storage.md) | Segment files are `<20-digit base offset>.log` / `.index`. |
| Record layout | [internal/record](../internal/record) | [storage.md](storage.md) | Header is 40 bytes: the 24 here + producerId + sequence. CRC32-C covers every byte except itself. |
| Sparse index | [internal/segment/index.go](../internal/segment/index.go) | [storage.md](storage.md) | As designed. Index lives in memory and in an `.index` file. |
| Committed offsets | [internal/offsets](../internal/offsets) | [consumers-and-offsets.md](consumers-and-offsets.md) | As designed; compaction rewrites the file atomically. |
| Durability, fsync | [internal/partition/writer.go](../internal/partition/writer.go) | [durability-and-recovery.md](durability-and-recovery.md) | The single writer goroutine also does the fsync (no separate fsync thread); requests that queue during an fsync form the next batch. Flush = 2 ms after the first unflushed write, or early at 1 MiB. Ack mode is a flag. |
| Crash recovery | [internal/partition/recovery.go](../internal/partition/recovery.go) | [durability-and-recovery.md](durability-and-recovery.md) | Header is 40 bytes, so the first rule is "fewer than 40 bytes". Scans from the checkpoint's segment to the end (usually just the last segment; everything if no checkpoint) and also stops at an out-of-sequence offset. The checkpoint stores a producer-state snapshot, so dedup state is rebuilt without scanning the whole log. Corruption in a non-last segment refuses to start instead of truncating. |
| Networking | [internal/server](../internal/server), [client/conn.go](../client/conn.go) | [protocol.md](protocol.md) | Adds server limits: connection cap, in-flight count + bytes per connection, idle timeout, long-poll capped at 30s. |
| Concurrency | [internal/partition](../internal/partition) | [architecture.md](architecture.md) | As designed. |
| Exactly-once | [internal/partition/producerstate.go](../internal/partition/producerstate.go), [client/stream.go](../client/stream.go) | [exactly-once.md](exactly-once.md) | Partition is chosen by the client, because the sequence is per partition. Broker remembers the last 5 sequences per producer for re-acks. |
| Retention | [internal/partition/retention.go](../internal/partition/retention.go) | [storage.md](storage.md) | As designed. |

Run it: [running-and-testing.md](running-and-testing.md). Big picture: [architecture.md](architecture.md).
