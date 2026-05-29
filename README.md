# Kafka Broker

A from-scratch implementation of an Apache Kafka broker in Python, built on `asyncio`. It speaks the Kafka binary wire protocol over TCP and implements a focused subset of the broker APIs end to end — from low-level byte (de)serialization, through metadata parsing of the KRaft `__cluster_metadata` log, to a buffered, crash-durable storage layer.

## Project Overview

This broker implements the Kafka wire protocol directly against raw TCP sockets, with no Kafka libraries in between. It handles four API keys:

| API | Key | Purpose |
| --- | --- | --- |
| `ApiVersions` | 18 | Advertise supported APIs and version ranges to clients |
| `DescribeTopicPartitions` | 75 | Return topic/partition metadata |
| `Fetch` | 1 | Read records from a partition log |
| `Produce` | 0 | Append records to a partition log |

Cluster metadata (topics, partitions, replicas, leaders) is sourced from a KRaft-style `__cluster_metadata` log that is parsed at startup. Produced records are buffered in memory and periodically flushed to per-partition log files with `fsync` for durability, while reads transparently merge on-disk data with not-yet-flushed buffered data so consumers always see the latest writes.

The project began as a [CodeCrafters](https://codecrafters.io/challenges/kafka) "Build Your Own Kafka" challenge and has been extended with production-minded concerns: graceful shutdown, backpressure, timeouts, structured logging, and Prometheus metrics.

## Features

### Architectural design

- **Layered architecture** with clear separation of concerns:
  - **Transport layer** (`main.py`) — TCP accept loop, 4-byte length-prefix framing, correlation-ID handling, per-connection lifecycle.
  - **Dispatch / application layer** (`handlers.py`) — one handler per API key; parses requests, calls storage, builds responses.
  - **Protocol layer** (`protocol/`) — pure (de)serialization of Kafka primitives, isolated from business logic.
  - **Storage layer** (`storage.py`) — metadata parsing, buffered writes, merged reads, background flushing.
- **Typed request model** — raw bytes are parsed into `@dataclass` request objects (`messages.py`), so handlers work with structured data instead of byte offsets.
- **Stateless parsers** — parsing functions take a cursor-based `BufferReader` and return dataclasses, making them easy to test and compose.
- **Single source of configuration** — all tunables are module-level constants at the top of `main.py`.

### Scalability patterns

- **Fully non-blocking I/O** — the entire server runs on a single `asyncio` event loop; thousands of connections can be multiplexed without a thread per connection.
- **Connection concurrency limit** — a `Semaphore` (`MAX_CONCURRENT_CONNECTIONS`) bounds the number of in-flight connections, providing admission control under load.
- **Offloaded blocking work** — all filesystem I/O is dispatched via `asyncio.to_thread` so disk latency never stalls the event loop.
- **O(1) Fetch lookups** — alongside the by-name topic index, a `topics_by_uuid` index is built once at startup so `Fetch` (which addresses topics by UUID) resolves in constant time.

### Performance optimizations

- **Write buffering / batching** — produced records accumulate in per-partition in-memory `bytearray` buffers and are flushed to disk in batches (every `BUFFER_FLUSH_INTERVAL` seconds), amortizing `open`/`fsync` cost across many writes.
- **Bounded, chunked reads** — `read_partition_log` seeks to the requested offset and reads at most `max_bytes`, avoiding full-file loads.
- **Read-through buffer merge** — reads combine on-disk bytes with the live write buffer, so a consumer never has to wait for a flush to observe a recent produce.
- **Append-only `bytearray` response building** — `BufferWriter` accumulates into a single mutable buffer, avoiding repeated byte-string concatenation.

### Reliability patterns

- **Crash durability** — every flush writes, `flush()`es, and `fsync()`s the file descriptor so acknowledged data survives process/OS crashes.
- **No-loss flush failure handling** — if a flush fails, the affected data is *prepended* back onto the partition buffer so ordering is preserved and it is retried on the next flush cycle.
- **Graceful shutdown** — on `SIGINT`/`SIGTERM` the server stops accepting, waits up to `GRACEFUL_SHUTDOWN_TIMEOUT` for active connections to drain, force-cancels stragglers, stops the flush task, and performs a final buffer flush.
- **Per-operation timeouts** — `CLIENT_READ_TIMEOUT` and `CLIENT_WRITE_TIMEOUT` guard against slow/stuck clients holding connections open.
- **Bounded write retries** — failed socket writes are retried up to `MAX_WRITE_RETRIES` before the connection is abandoned.
- **Input validation / hardening** — message sizes are validated against `MAX_REQUEST_SIZE`, negative read lengths are rejected, and incomplete reads are handled cleanly to defend against malformed or truncated frames.
- **Fault isolation** — an exception while handling one request is logged and recorded as an error metric without tearing down the whole server.

### Observability patterns

- **Structured JSON logging** via `structlog` — every log line is machine-parseable and connection logs are bound with the client address for correlation.
- **Prometheus metrics** exposed on a dedicated HTTP port (`8000`):
  - `kafka_server_requests_total` — Counter, labels: `api_key`, `status`
  - `kafka_server_request_duration_seconds` — Histogram, label: `api_key`
  - `kafka_server_active_connections` — Gauge
  - `kafka_server_disk_flush_duration_seconds` — Histogram
- **Lifecycle event logging** — connection accept, shutdown signals, flushes, and timeouts are all logged as discrete events.

## System Architecture

### High-level system architecture

```
                         ┌──────────────────────────────────────────────┐
                         │                  Kafka Broker                  │
                         │                  (asyncio loop)                │
   Kafka clients         │                                                │
  ┌───────────┐  TCP     │   ┌────────────────┐      ┌────────────────┐  │
  │ producer  │◀────────▶│   │  client_handler │      │  RequestHandler │  │
  │ consumer  │  :9092   │   │  (framing +     │─────▶│  (dispatch by   │  │
  │ admin     │          │   │   correlation)  │      │   api_key)      │  │
  └───────────┘          │   └────────┬───────┘      └───────┬────────┘  │
                         │            │ semaphore             │           │
                         │            │ (admission)           ▼           │
                         │            │              ┌────────────────┐   │
                         │            │              │ protocol layer  │  │
                         │            │              │ reader / writer │  │
                         │            │              │ parser / msgs   │  │
                         │            │              └───────┬────────┘   │
                         │            │                      ▼            │
                         │            │              ┌────────────────┐   │
                         │            │              │    Storage      │  │
                         │            │              │ buffers + locks │  │
                         │            │              └───┬────────┬───┘   │
                         │   ┌────────▼─────────┐        │        │       │
                         │   │ flush_buffers_   │        │ to_thread      │
                         │   │ periodically     │────────┘        │       │
                         │   │ (background task)│                 ▼       │
                         │   └──────────────────┘        ┌────────────────┐
                         │                               │  Disk: per-    │
   Prometheus  ◀─────────│  metrics HTTP server :8000    │  partition logs│
                         │                               │  + KRaft meta  │
                         └───────────────────────────────└────────────────┘
```

### Request lifecycle

```
TCP bytes
   │
   ▼
client_handler                         (main.py)
   │  read 4-byte length prefix  ─────────────────────┐
   │  validate size (0 < n ≤ MAX_REQUEST_SIZE)        │ timeouts +
   │  read exactly N payload bytes                    │ shutdown-aware
   │  extract correlation_id = bytes[8:12]            │ waits
   │  extract api_key         = bytes[4:6]  ──────────┘
   ▼
RequestHandler.handle_*                (handlers.py)   dispatch by api_key
   │
   ▼
parser.parse_*                         (protocol/parser.py)
   │  BufferReader walks the bytes → typed dataclass (messages.py)
   ▼
Storage                                (storage.py)
   │  Produce → write_partition_log  → append to in-memory buffer
   │  Fetch   → read_partition_log   → disk (seek/read max_bytes) + buffer merge
   ▼
BufferWriter                           (protocol/writer.py)
   │  typed response fields → bytearray (compact strings/arrays, varints)
   ▼
client_handler
   │  resp = len(corr_id + body) ‖ correlation_id ‖ resp_body
   │  write + drain (retry up to MAX_WRITE_RETRIES)
   ▼
TCP bytes out

(asynchronously, every BUFFER_FLUSH_INTERVAL seconds)
flush_buffers_periodically → Storage.flush_buffers → write + flush + fsync to disk
```

## Project Structure

```
kafka-broker/
├── src/
│   ├── main.py              # Entry point: TCP server, framing, lifecycle,
│   │                        #   signal handling, metrics, background flush task
│   ├── handlers.py          # RequestHandler: one method per API key
│   ├── storage.py           # Storage: metadata parsing, buffered writes,
│   │                        #   merged reads, periodic flush
│   ├── utils.py             # Shared helpers (unsigned varint encoding)
│   └── protocol/
│       ├── reader.py        # BufferReader: cursor-based deserializer
│       ├── writer.py        # BufferWriter: appends to internal bytearray
│       ├── parser.py        # Stateless parsing functions (bytes → dataclass)
│       └── messages.py      # @dataclass request type definitions
├── requirements.txt
└── README.md
```

### Module responsibilities

- **`main.py`** — Owns the network. Implements 4-byte length-prefix framing, extracts the correlation ID and API key directly from the raw bytes, dispatches to the handler, prepends the correlation ID to the response, and writes it back. Also wires up the connection semaphore, signal handlers, graceful shutdown, the periodic flush task, and the Prometheus endpoint.
- **`handlers.py`** — `RequestHandler` has one `handle_*` method per API key. Each parses the request via the protocol layer, consults `Storage`, and builds the response body with a `BufferWriter`. Handlers return only `resp_body`; the correlation ID is prepended by the transport layer.
- **`storage.py`** — `Storage` parses the KRaft `__cluster_metadata` log into a `topics` dict (and a `topics_by_uuid` index), buffers produced records per `(topic, partition)`, flushes them durably, and serves reads by merging disk and buffer.
- **`protocol/`** — Self-contained (de)serialization. Compact strings/arrays use unsigned varints where the encoded value is `length + 1` (so `0` encodes null).

## Requirements

- **Python 3.10+** (the code uses modern typing syntax such as `dict[str, dict]` and `list[...]`).
- Python dependencies (see `requirements.txt`):
  - `structlog` — structured JSON logging
  - `prometheus_client` — metrics exposition
  - `mypy` — static type checking (development)

Install them with:

```bash
pip install -r requirements.txt
```

### Metadata prerequisite

`Fetch`, `Produce`, and `DescribeTopicPartitions` rely on cluster metadata read from a KRaft `__cluster_metadata` log at:

```
${LOG_FILES_DIR}/__cluster_metadata-0/00000000000000000000.log
```

If this file is absent, the broker still starts and serves requests, but with an empty topic set (unknown-topic responses). Partition logs live alongside it at `${LOG_FILES_DIR}/<topic>-<partition>/00000000000000000000.log`.

## Configuration

All tunables are module-level constants at the top of `src/main.py`:

| Constant | Default | Description |
| --- | --- | --- |
| `LOG_FILES_DIR` | `/tmp/kraft-combined-logs` | Root directory for all log files (metadata + partition logs) |
| `LOG_FILE_NAME` | `00000000000000000000.log` | Per-partition log file name |
| `CLIENT_READ_TIMEOUT` | `5` | Per-read timeout (seconds) |
| `CLIENT_WRITE_TIMEOUT` | `5` | Per-write/drain timeout (seconds) |
| `MAX_WRITE_RETRIES` | `3` | Max retries for a failed socket write |
| `MAX_CONCURRENT_CONNECTIONS` | `100` | Connection semaphore limit |
| `GRACEFUL_SHUTDOWN_TIMEOUT` | `10.0` | Drain window on `SIGINT`/`SIGTERM` (seconds) |
| `BUFFER_FLUSH_INTERVAL` | `10` | Seconds between background buffer flushes |
| `MAX_REQUEST_SIZE` | `1024` | Maximum accepted request payload size (bytes) |

Network ports are set in `main()`:

| Setting | Default | Description |
| --- | --- | --- |
| Broker host | `localhost` | TCP listen address |
| Broker port | `9092` | Kafka wire-protocol port |
| Metrics port | `8000` | Prometheus metrics HTTP endpoint |

## Running the Broker

Install dependencies and start the broker:

```bash
# Install dependencies
pip install -r requirements.txt

# Run the broker (listens on localhost:9092, metrics on :8000)
python -m src.main
```

Type check the source:

```bash
mypy src/
```

Once running, the broker:

- Accepts Kafka protocol connections on `localhost:9092`.
- Exposes Prometheus metrics at `http://localhost:8000/`.
- Emits structured JSON logs to stdout.

Shut it down with `Ctrl-C` (`SIGINT`) or `SIGTERM`; it will drain active connections and perform a final durable flush before exiting.
