# kafka-broker

A Python implementation of a Kafka broker built on `asyncio`, supporting the core Kafka wire protocol. Designed as a CodeCrafters learning project.

---

## Features

- **Kafka wire protocol** — implements the binary framing, compact encoding (unsigned varints), and the following APIs:
  - `ApiVersions` (key 18)
  - `DescribeTopicPartitions` (key 75)
  - `Fetch` (key 1)
  - `Produce` (key 0)
- **KRaft metadata** — reads topic and partition configuration from the `__cluster_metadata` log file (no ZooKeeper dependency)
- **Durable writes** — in-memory write buffer flushed to disk with `fsync` every 10 seconds and on shutdown
- **Graceful shutdown** — drains active connections before exiting, with a final buffer flush to prevent data loss
- **Observability** — structured JSON logging via `structlog` and Prometheus metrics on a dedicated port

---

## Project Structure

```
kafka-broker/
├── src/
│   ├── main.py           # Server bootstrap, connection loop, metrics, shutdown
│   ├── handlers.py       # RequestHandler — dispatches and processes each API
│   ├── storage.py        # Storage — metadata loading, partition read/write/flush
│   ├── utils.py          # encode_unsigned_varint helper
│   └── protocol/
│       ├── messages.py   # Dataclass definitions for all request types
│       ├── parser.py     # Stateless request parsers (bytes → dataclass)
│       ├── reader.py     # BufferReader — cursor-based binary deserializer
│       └── writer.py     # BufferWriter — binary serializer
├── requirements.txt
└── mypy.ini
```

---

## Requirements

- Python 3.11+
- `structlog`
- `prometheus-client`

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Running the Broker

```bash
python -m src.main
```

The broker listens on:
- **`localhost:9092`** — Kafka protocol (data plane)
- **`localhost:8000`** — Prometheus metrics (metrics plane)

### Expected log directories

The broker reads KRaft cluster metadata from:

```
/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
```

Partition logs are written to:

```
/tmp/kraft-combined-logs/<topic>-<partition>/00000000000000000000.log
```

These directories are created automatically on first write. To pre-seed metadata, place a valid KRaft `__cluster_metadata` log file at the path above before starting the broker.

---

## Configuration

All constants are defined at the top of `src/main.py`:

| Constant | Default | Description |
|----------|---------|-------------|
| `host_ip` | `localhost` | Bind address |
| `host_port` | `9092` | Kafka protocol port |
| `metrics_port` | `8000` | Prometheus metrics port |
| `MAX_CONCURRENT_CONNECTIONS` | `100` | Connection semaphore limit |
| `CLIENT_READ_TIMEOUT` | `5s` | Per-read timeout per connection |
| `CLIENT_WRITE_TIMEOUT` | `5s` | Per-write timeout per connection |
| `MAX_WRITE_RETRIES` | `3` | Response write retry attempts |
| `GRACEFUL_SHUTDOWN_TIMEOUT` | `10s` | Drain window before force-cancel |
| `BUFFER_FLUSH_INTERVAL` | `10s` | How often buffers are flushed to disk |

---

## Architecture

### Request lifecycle

```
TCP bytes in
  → client_handler          (4-byte length prefix framing)
    → RequestHandler         (dispatch by api_key)
      → parser.py            (bytes → typed request dataclass)
      → Storage              (read or write partition log)
      → BufferWriter         (typed response → bytes)
  → TCP bytes out
```

### Write path

Produce requests are buffered in memory (`bytearray` per `(topic, partition)`) and never touch disk synchronously. A background task flushes all buffers to disk with `fsync` every 10 seconds. On failure, unwritten data is prepended back to the buffer and retried next cycle.

### Read path

Fetch requests seek directly to `fetch_offset` in the partition log file and read up to `max_bytes`. Any data buffered but not yet flushed is appended transparently so reads are always up-to-date.

### Shutdown

On `SIGINT` or `SIGTERM`:
1. Stop accepting new connections
2. Wait up to 10 seconds for active connections to finish
3. Force-cancel any remaining connections
4. Perform a final buffer flush to disk

---

## Observability

### Structured logging

All log output is JSON via `structlog`, including `log_level`, `logger`, `timestamp`, and structured key-value fields:

```json
{"log_level": "info", "logger": "src.main", "timestamp": "2026-01-01T00:00:00Z", "event": "server_started", "host": "localhost", "port": 9092}
```

### Prometheus metrics

Scraped at `http://localhost:8000/metrics`:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `kafka_server_requests_total` | Counter | `api_key`, `status` | Total requests by API and outcome |
| `kafka_server_request_duration_seconds` | Histogram | `api_key` | Request latency per API |
| `kafka_server_active_connections` | Gauge | — | Current open connections |
| `kafka_server_disk_flush_duration_seconds` | Histogram | — | Buffer flush latency |

---

## Type Checking

```bash
mypy src/
```

`mypy.ini` enables `check_untyped_defs = True` for full coverage.
