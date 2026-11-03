# Storage

**What this covers:** how bytes are laid out on disk and how the broker finds
them again. The record format, segments, the sparse index (with a worked
lookup), the zero-copy read path, and retention. For fsync, acks and crash
recovery see [durability-and-recovery.md](durability-and-recovery.md).

---

## The record

Every message is one record: a fixed **40-byte header** followed by the body.
Big-endian. The same bytes are used on disk and in consume responses, so the
broker never re-encodes anything ([record.go](../internal/record/record.go)).

| Bytes | Field | Size | Meaning |
|---|---|---|---|
| 0–7 | `Offset` | 8 | Position of the record in its partition |
| 8–15 | `Timestamp` | 8 | Unix milliseconds, set by the broker's writer |
| 16–19 | `Length` | 4 | Body length in bytes |
| 20–23 | `CRC` | 4 | CRC32-C of every other byte of the record |
| 24–31 | `ProducerID` | 8 | Idempotent producer id, or `NoProducerID` (-1) |
| 32–39 | `Sequence` | 8 | Per (producer, partition) sequence number |
| 40– | `Body` | `Length` | Opaque bytes, never parsed by the broker |

Limits: `MaxBodySize` is 1 MiB. A header claiming more is treated as corruption.

### What the CRC covers, and why

The CRC covers bytes `[0:20]` and `[24:end]`: everything except the CRC field itself
([`checksum`](../internal/record/record.go)).

- **Header included, not just the body.** A torn or garbled `Length` or `Offset`
  is caught, so recovery never trusts a bad length to jump to the "next" record.
- **ProducerID and Sequence included.** Dedup state is rebuilt from headers after a
  crash, so those fields must be trustworthy too.
- **Castagnoli polynomial** (CRC32-C) is hardware-accelerated on amd64 and arm64,
  which keeps a recovery scan cheap.
- **End-to-end check for free.** The consumer client decodes responses with
  [`record.DecodeAll`](../internal/record/record.go), which verifies each CRC, so
  corruption on disk or on the wire is caught by the reader.

`record.Decode` validates in the same order recovery does: header present →
length ≤ 1 MiB → body present → CRC matches. The body is never looked at before the CRC passes.

## Segments

A partition directory holds a sequence of **segments**, each one `.log` file
plus one `.index` file ([segment.go](../internal/segment/segment.go)).

| Property | Value |
|---|---|
| Name | Base offset, zero-padded to 20 digits: `00000000000000052113.log` |
| Ordering | `segment.List` parses names and sorts them; the last one is **active** |
| Writes | Only the active segment, only by the writer goroutine, with `WriteAt` |
| Size cap | `SegmentBytes`, default 1 GiB; rejected above 4 GiB (index positions are `uint32`) |

### Rolling

Before each write, [`writer.needsRoll`](../internal/partition/writer.go) checks
`used > 0 && used + recordSize > SegmentBytes`. An empty segment always takes
the record, so one oversized record never loops forever.

When a roll is needed, [`writer.roll`](../internal/partition/writer.go):

1. `flush()`: fsync the current segment and ack its pending batch.
2. `segment.Create` the new segment named by the next offset (`O_EXCL`, then fsync the directory).
3. `addSegment`: publish it to readers under `segMu`.
4. Write a recovery checkpoint pointing at the new segment, position 0.

Step 1 is what makes every closed segment fully durable, a fact crash recovery depends on.

## Sparse index

Scanning a 1 GiB segment for one offset would be slow, and indexing every record
would be big. So each segment indexes **roughly one record every 4 KiB** of log
(`IndexIntervalBytes`, default 4096) ([index.go](../internal/segment/index.go)).

### Entry format

| Bytes | Field | Type |
|---|---|---|
| 0–3 | `relOffset` = offset − segment base | `uint32` |
| 4–7 | `position` = byte position in the `.log` | `uint32` |

8 bytes per entry, big-endian. Entries are kept **fully in memory** (guarded by
an RWMutex) and mirrored to the `.index` file with `WriteAt`. `Segment.Sync`
fsyncs the `.log` and then the `.index`.

### When an entry is added

In [`Segment.track`](../internal/segment/segment.go), before counting each record:
if `bytesSinceIndex >= IndexIntervalBytes`, add an entry for *this* record and
reset the counter. Consequences:

- The first record of a segment never gets an entry; a lookup with no lower entry
  falls back to `(base, position 0)`.
- Entries are never more than ~4 KiB + one record apart.

### Lookup, step by step

`Partition.Read(offset)` finds its byte position in three steps:

1. **Pick the segment.** [`segmentFor`](../internal/partition/reader.go) binary-searches
   the sorted segment list for the first base `> offset`, then steps back one.
2. **Index floor.** [`index.floor`](../internal/segment/index.go) binary-searches the
   entries for the greatest `relOffset <= offset - base`.
3. **Header scan.** [`Segment.Locate`](../internal/segment/segment.go) reads one 40-byte
   header at a time from that position, checks `h.Offset` is the offset it expects, and
   jumps ahead by `40 + Length` until it reaches the target.

### Worked example: find offset 1234

Assume a test config with small segments, bases `0, 1000, 2000`, and every body
1024 bytes, so every record is **1064 bytes**.

**Step 1, segment.** First base `> 1234` is `2000` (index 2). Step back one →
segment `1000`, which holds offsets 1000–1999.

**Which entries exist?** The counter after records rel 0..3 reaches
4 × 1064 = 4256 ≥ 4096, so rel 4 gets an entry. Then every 4th record:

| relOffset | offset | position |
|---|---|---|
| 4 | 1004 | 4256 |
| 8 | 1008 | 8512 |
| … | … | … |
| 232 | 1232 | 246848 |
| 236 | 1236 | 251104 |

**Step 2, index floor.** Target rel = 1234 − 1000 = 234. Greatest entry ≤ 234 is
rel 232 → position **246848**.

**Step 3, header scan.**

| pread at | header says | next position |
|---|---|---|
| 246848 | offset 1232, length 1024 | 246848 + 1064 = 247912 |
| 247912 | offset 1233, length 1024 | 247912 + 1064 = 248976 |

`cur` is now 1234, so `Locate` returns **248976** (check: 234 × 1064 = 248976).
Cost: two binary searches in memory, two 40-byte `pread`s.

### Loading on startup

[`openIndex`](../internal/segment/index.go) reads the whole `.index` and trusts
entries only while they are complete, strictly increasing, and point inside the
log. Everything after the first bad entry (e.g. a torn final write) is cut off,
in memory and on disk. A missing entry only makes lookups scan a bit further.

## The read path

[`Partition.Read(offset, maxRecords, maxBytes)`](../internal/partition/reader.go)
returns a **`Chunk`**: not the bytes, but a recipe for sending them.

```go
type Chunk struct {
    Path        string // segment file
    Position    int64  // byte position of the first record
    Size        int64  // bytes; 0 = empty
    FirstOffset int64
    NextOffset  int64  // offset after the last record
    file *os.File      // its own open handle
}
```

### Rules

| Rule | How |
|---|---|
| **Never past the high watermark** | Reads `HighWatermark()` once (atomic) and stops at `min(hw, nextSegmentBase)` |
| **Never spans two segments** | `segmentFor` returns the next segment's base as the end. The consumer simply asks again from `NextOffset` |
| **At least one record** | `readChunk` stops when the *next* record would exceed `maxBytes`, but only once `size > 0`. A 1 MiB record is still returned with a smaller `maxBytes` |
| `offset == hw` | Empty chunk, no error (the broker long-polls before this) |
| `offset < LogStartOffset` or `> hw` | `ErrOffsetOutOfRange`; the broker adds the valid range to the message |
| `maxRecords < 1` | Treated as 1 |

The broker calls `Read` with `maxBytes = 1 MiB` (`maxConsumeBytes` in
[broker.go](../internal/broker/broker.go)) and `maxRecords` = the client's `limit`.

`readChunk` walks headers with `ReadHeader` (one `pread` each), checking every
offset is the one it expects. Only headers are read; bodies stay on disk.

### Zero-copy: page cache → socket

[`Chunk.WriteTo`](../internal/partition/reader.go) seeks its file to `Position`
and calls `io.Copy(w, io.LimitReader(file, Size))`. When `w` is a `*net.TCPConn`,
Go's `TCPConn.ReadFrom` spots the `*io.LimitedReader` around an `*os.File` and
uses **sendfile(2)** on Linux: the kernel copies straight from page cache to
the socket, and the bytes never enter the broker's memory.

The server makes that possible in [`sendChunk`](../internal/server/consume.go):
it writes the response header from `protocol.ConsumeResponseHeader`, then calls
`chunk.WriteTo(c.nc)` directly on the connection (no `bufio.Writer` in between),
holding `writeMu` across both so no other frame lands in the middle. If a send
fails halfway the connection is closed, since the stream cannot be resynced.

### Why each Chunk owns its own fd

`Read` opens the segment file itself (`os.Open`) before returning. Three reasons:

1. **Retention can't pull the rug.** Retention may delete the segment the moment
   `Read` returns. On POSIX an open file survives `unlink`, so a chunk that `Read`
   returned can always be sent in full.
2. **The segment's own handle is closed on removal.** `Segment.Remove` closes it,
   so a chunk can't borrow it.
3. **`WriteTo` moves the file offset** (it seeks). A private fd means concurrent
   consumers never fight over one offset.

So: every chunk must be `Close`d (the server `defer`s it), and a chunk is used by
one goroutine at a time. If the segment disappears *before* `Read` opens it,
`Read` returns `ErrOffsetOutOfRange` (or `ErrClosed` if the partition is closing).

## Retention

A background goroutine calls [`EnforceRetention`](../internal/partition/retention.go)
every `RetentionCheck` (default 1 minute). Errors are not fatal; the next tick retries.

| Setting | Default | A segment is expired when… |
|---|---|---|
| `RetentionAge` | 7 days | its **newest** record's timestamp is older than `now − RetentionAge` |
| `RetentionBytes` | 1 TiB | the partition's total size (all segments, active included) is over the limit |

How [`detachExpired`](../internal/partition/retention.go) decides, under `segMu`:

1. Sum the size of every segment.
2. Walk from the **oldest** segment, stopping before the active one. Stop at the
   first segment that is both young enough *and* the total is within `RetentionBytes`.
   Otherwise mark it expired and subtract its size.
3. Detach the expired prefix from the list and set `LogStartOffset` to the new first
   segment's base. New readers stop finding them right here.

Then, outside the lock: `Segment.Remove` each (close, delete `.log` then `.index`),
and fsync the directory once.

Properties:

- **Whole segments only, oldest first.** The log stays gap-free: valid offsets are
  always `[LogStartOffset, HighWatermark)`.
- **Never the active segment.** A quiet partition keeps its last segment (up to
  `SegmentBytes`) however old its records are.
- **`LogStartOffset` moves only forward.** A consumer behind it gets
  `OffsetOutOfRange`, with the valid range in the message, and must jump forward.
- **Producers are forgotten too.** At the next checkpoint, producers whose newest
  record is below `LogStartOffset` are dropped from the dedup table (see
  [durability-and-recovery.md](durability-and-recovery.md#idempotent-producer-state)).

## Limitations

- **In-memory index size.** One 8-byte entry per ~4 KiB of log. A full 1 TiB
  partition is 2^40 / 2^12 = 2^28 entries ≈ **2 GiB of RAM**, all read from disk
  at `Open`.
- **One `pread` per header.** `Locate` scans up to ~4 KiB of headers one syscall at a
  time, and `readChunk` does one per record in the chunk: about 1000 for a 1 MiB
  chunk of 1 KB messages. Reading a block and parsing headers out of it would cut that.
- **File descriptors.** Every segment keeps its `.log` and `.index` open, so 1 TiB of
  1 GiB segments is ~2048 fds per partition, plus one per in-flight chunk.
- **sendfile is Linux-specific** in this design's assumptions (the code comment and
  the Docker image both target Linux).
- **Retention by timestamp uses broker time.** A clock jump moves expiry with it.

## Where to look in the code

- [internal/record/record.go](../internal/record/record.go): `Append`, `DecodeHeader`, `Decode`, `DecodeAll`, `checksum`
- [internal/segment/segment.go](../internal/segment/segment.go): `List`, `Create`, `Open`, `Append`, `track`, `Locate`, `ReadHeader`, `Remove`
- [internal/segment/index.go](../internal/segment/index.go): `openIndex`, `decodeEntries`, `append`, `floor`
- [internal/partition/writer.go](../internal/partition/writer.go): `needsRoll`, `roll`
- [internal/partition/reader.go](../internal/partition/reader.go): `Read`, `segmentFor`, `readChunk`, `Chunk.WriteTo`, `Chunk.Close`
- [internal/partition/retention.go](../internal/partition/retention.go): `runRetention`, `EnforceRetention`, `detachExpired`
- [internal/server/consume.go](../internal/server/consume.go): `consume`, `sendChunk`
