# Running and testing

What this covers: how to build, run and poke at the broker with only Docker installed, every
`make` target, broker flag and `mlctl` command, how to read the benchmark, and which test proves
what.

---

## 1. Prerequisites

- **Docker** and **make**. That's all. Go is never installed on the host. Every Go command runs
  in a `golang:1.25` container ([Makefile](../Makefile)).
- On macOS any Docker runtime works, e.g. [colima](https://github.com/abiosoft/colima):
  `colima start`.
- The first run pulls `golang:1.25` and downloads modules. Two named volumes, `lld-gomod`
  (module cache) and `lld-gocache` (build cache), make later runs fast.

## 2. Make targets

Run `make` (or `make help`) to list them.

| Target | What it does |
|---|---|
| `make build` | `go build ./...` |
| `make vet` | `go vet ./...` |
| `make fmt` | `gofmt -w .` |
| `make test` | all tests, `-race -count=1` |
| `make test-v` | all tests verbose; filter with `P=<package pattern>` and `T=<-run regex>` |
| `make broker` | build and start the broker in a background container `messagelog-broker` on network `messagelog-net`, listening on `:9092`, data in `./data` |
| `make broker-stop` | `docker stop` it: SIGTERM, so it flushes and checkpoints every partition |
| `make cli ARGS="..."` | `go run ./cmd/mlctl -addr messagelog-broker:9092 $(ARGS)` in a container on the same network |

Notes:

- The broker's port is **not** published to the host. The CLI reaches it by container name on
  `messagelog-net`.
- `make broker` runs the binary with `exec`, so it's PID 1 and `docker stop`'s SIGTERM reaches it.
- `./data` is a bind mount of the repo directory, so data survives `broker-stop` / `broker`.
  To start fresh: `make broker-stop && rm -rf data`.
- `make cli` does a `go run` each time: the first call compiles, later ones hit the build cache.

## 3. Demo session

**Step 1: start the broker** and wait for it to listen (it compiles first).

```text
$ make broker
broker starting; logs: docker logs -f messagelog-broker
$ docker logs -f messagelog-broker
... level=INFO msg=serving addr=[::]:9092
```

**Step 2: create a topic and list topics.** `make broker` doesn't pass `-auto-create`, so
publishing to a missing topic fails with `UnknownTopicOrPartition`.

```text
$ make cli ARGS="create-topic orders"
created orders with 1 partition(s)
$ make cli ARGS="topics"
orders	partition=0	leader=1
```

**Step 3: publish.** One idempotent `Producer`, one line per acked message.

```text
$ make cli ARGS="publish orders hello world"
partition=0 offset=0
partition=0 offset=1
```

**Step 4: read it back.** Without `-follow` it stops at the high watermark.

```text
$ make cli ARGS="consume orders"
offset=0 time=2026-11-02T16:26:58.26Z hello
offset=1 time=2026-11-02T16:26:58.264Z world
```

**Step 5: watch a long-poll.** In one terminal, wait for exactly one new record:

```text
$ make cli ARGS="consume orders -offset 2 -follow -limit 1"
```

In a second terminal, `make cli ARGS="publish orders late"`. The first terminal prints
`offset=2 time=... late` and exits.

**Step 6: commit a consumer group's progress** (the next offset to read) and read it back.

```text
$ make cli ARGS="commit billing orders 2"
committed billing orders-0 at 2
$ make cli ARGS="committed billing orders"
2
$ make cli ARGS="committed nobody orders"
no committed offset
```

**Step 7: restart and check nothing was lost.**

```text
$ make broker-stop
$ make broker                           # wait for msg=serving again
$ make cli ARGS="consume orders"        # same records, same offsets
$ make cli ARGS="committed billing orders"
2
```

**Step 8: benchmark** (see §6).

```text
$ make cli ARGS="bench -n 50000 -c 128"
published 50000 x 1024 B with 128 publishers in 3.523s
throughput: 14191 msgs/s, 14.53 MB/s
latency:    p50 9.268ms, p99 11.904ms, max 15.097ms
```

## 4. Broker flags

From [cmd/broker/main.go](../cmd/broker/main.go). `make broker` only sets `-addr :9092 -data /src/data`.

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `:9092` | listen address |
| `-advertise` | value of `-addr` | address returned in Metadata |
| `-data` | `./data` | data directory |
| `-id` | `1` | broker id (becomes partitions' `LeaderID`) |
| `-ack` | `fsync` | `fsync`: ack after the group-commit fsync. `async`: ack right after `write()`; fsync still runs on the flush timer |
| `-flush-interval` | `2ms` | group-commit linger |
| `-segment-bytes` | `1073741824` (1 GiB) | segment roll size |
| `-retention` | `168h` (7 days) | delete closed segments older than this |
| `-auto-create` | `false` | publish to an unknown topic creates it (1 partition) |
| `-max-conns` | `1024` | max open client connections |
| `-idle-timeout` | `5m` | close connections that send nothing this long |

Not flags (code defaults): size retention 1 TiB, flush early at 1 MiB unflushed, checkpoint at
most every 1 s, index entry every 4 KiB, 64 MiB in-flight bytes per connection.

SIGINT/SIGTERM → graceful shutdown: close the server (stop accepting, close connections, wait for
handlers), then close the broker (flush + checkpoint every partition).

To try other flags, run the container yourself (same as `make broker`, different flags):

```sh
docker network create messagelog-net 2>/dev/null
docker run -d --rm --name messagelog-broker --network messagelog-net \
  -v "$PWD":/src -w /src -v lld-gomod:/go/pkg/mod -v lld-gocache:/root/.cache/go-build \
  golang:1.25 sh -c 'go build -o /tmp/broker ./cmd/broker && exec /tmp/broker -addr :9092 -data /src/data -ack async -auto-create'
```

## 5. mlctl commands

`mlctl [-addr host:port] <command> [args]`. `-addr` defaults to `localhost:9092`; `make cli`
sets it for you. Command flags may go before, between or after positional arguments.

| Command | Flags (default) | Prints |
|---|---|---|
| `create-topic TOPIC` | `-partitions` (1) | `created TOPIC with N partition(s)` |
| `topics` | | one line per partition: `name	partition=N	leader=ID` |
| `publish TOPIC MSG...` | | `partition=P offset=O` per message, in order |
| `consume TOPIC` | `-partition` (0), `-offset` (0), `-limit` (0 = no limit), `-follow` | `offset=O time=RFC3339 body` per record |
| `commit GROUP TOPIC OFFSET` | `-partition` (0) | `committed GROUP TOPIC-P at OFFSET` |
| `committed GROUP TOPIC` | `-partition` (0) | the offset, or `no committed offset` |
| `bench` | `-topic` (bench), `-n` (10000), `-c` (8), `-size` (1024, max 1 MiB) | 3 lines: duration, throughput, latency |

Details:

- `publish` always goes to **partition 0** (the producer's default `SinglePartition`), even on a
  multi-partition topic.
- `consume` fetches in batches of up to 500. With `-follow` each fetch long-polls up to 1 s and
  it never stops on its own unless `-limit` is reached.
- `bench` creates its topic (1 partition) if missing, then runs `-c` goroutines sharing **one**
  `Producer`, so up to `-c` publishes are pipelined on one connection. It stops at the first error.

## 6. Reading the bench numbers

With the default `-ack fsync`, **every ack waits for an fsync**. The partition writer writes each
publish at once, then fsyncs the batch 2 ms after the first unflushed write (or at 1 MiB) and acks
everyone in it. So:

- **Latency ≈ linger + fsync time.** It barely depends on load until the disk saturates.
- **Throughput ≈ publishes per fsync ÷ fsync time.** More concurrent publishers → bigger batches →
  more throughput at about the same latency. Check with Little's law on the README run:
  128 in flight ÷ 9.27 ms p50 ≈ 13.8k msgs/s, close to the measured 14.2k.
- The client keeps at most **256** messages in flight per partition, so `-c` above 256 only adds
  queueing in the client.

**On macOS the numbers are fsync-bound by Docker, not by the design.** `./data` is a bind mount
from the host into the Docker VM, and fsync on it is far slower than on a native Linux disk.
Experiments that make this visible:

| Try | Expect |
|---|---|
| raise `-c` (8 → 64 → 256) | throughput up, p50 roughly flat |
| `-ack async` (§4) | the fsync leaves the ack path: much lower latency |
| `-data /tmp/data` (inside the container, not the bind mount) | cheaper fsync; data is lost when the container stops |

## 7. Test map

All tests run with `-race`. Most use real files in `t.TempDir()` and real loopback sockets.

| Package | What the tests prove |
|---|---|
| [internal/record](../internal/record/record_test.go) | encode/decode round trip; CRC catches any damaged byte; truncated buffers are detected |
| [internal/segment](../internal/segment/segment_test.go) | append + sparse-index lookup + reopen; recovery finds a torn tail and rebuilds the index; a torn index entry is ignored |
| [internal/partition](../internal/partition/partition_test.go) | read bounds, ≥1 record per read, segment roll, zero-copy `Chunk` matches bytes; dedup rules; readers never see unacked data in either ack mode; long-poll `Wait` |
| [partition recovery](../internal/partition/recovery_test.go) | torn tail truncated; checkpoint means only the tail is scanned; a corrupt **closed** segment or a missing middle segment refuses to open; producer state survives restart |
| [partition retention](../internal/partition/retention_test.go) | deletion by age and size; readers racing deletion get `OffsetOutOfRange`, never garbage; an open `Chunk` survives deletion; expired producers are forgotten |
| [partition concurrency](../internal/partition/concurrency_test.go) | concurrent producers + tailing readers through segment rolls see one gap-free log, each producer in sequence order |
| [internal/offsets](../internal/offsets/offsets_test.go) | commits overwrite and survive reopen; torn tail truncated; compaction keeps latest and shrinks; failed compaction doesn't fail a commit; an unrepairable append stops writes |
| [internal/metadata](../internal/metadata/filestore_test.go) | create/list; name and partition-count rules (incl. case-insensitive clash); global partition ids; state survives reopen |
| [internal/fsutil](../internal/fsutil/fsutil_test.go) | atomic file replace, durable mkdir |
| [internal/protocol](../internal/protocol/protocol_test.go) | every body round-trips; short/trailing/bad-length input is rejected without big allocations; consume header == normal response encoding |
| [internal/broker](../internal/broker/broker_test.go) | unknown topic/partition errors; auto-create on publish only; long-poll times out empty |
| [internal/server](../internal/server/server_test.go) | end to end over TCP: pipelined producer, idempotent publish on the wire, long-poll, restart keeps data + offsets + dedup state, error codes, malformed request keeps the connection, group consumer resumes, producer survives a broker restart |
| [server limits](../internal/server/limits_test.go) | in-flight budget; a client that never reads acks can't stall others or `Close`; max conns; idle timeout + producer reconnects |
| [server producer](../internal/server/producer_test.go) | abandoned publish still delivered; delivery timeout → new producer id at seq 0; done ctx sends nothing; retries on a dead connection |
| [server chaos](../internal/server/chaos_test.go) | exactly-once under connection kills, late bytes and callers giving up (see [exactly-once.md](exactly-once.md#10-how-the-chaos-test-proves-it)) |

## 8. Running one test

```sh
# one test, verbose
make test-v P=./internal/server/... T=TestChaosExactlyOnce

# several, by regex
make test-v P=./internal/partition/... T='TestCrashRecovery|TestRetention'

# one package
make test-v P=./internal/offsets/...
```

`make` always passes `-count=1`. To hammer a flaky-looking test many times, call Docker directly:

```sh
docker run --rm -v "$PWD":/src -w /src -v lld-gomod:/go/pkg/mod -v lld-gocache:/root/.cache/go-build \
  golang:1.25 go test -race -count=20 -run TestChaosExactlyOnce ./internal/server/
```

---

## Where to look in the code

- [Makefile](../Makefile): every target and the Docker invocation
- [cmd/broker/main.go](../cmd/broker/main.go): `parseFlags`, `brokerConfig`, graceful shutdown in `run`
- [cmd/mlctl/main.go](../cmd/mlctl/main.go): `commands`, `parseArgs`, one function per subcommand
- [cmd/mlctl/bench.go](../cmd/mlctl/bench.go): `bench`, `ensureTopic`
- [internal/partition/partition.go](../internal/partition/partition.go): `DefaultConfig` (the non-flag defaults)
- [internal/server/helpers_test.go](../internal/server/helpers_test.go), [proxy_test.go](../internal/server/proxy_test.go): test broker and fault-injecting proxy
