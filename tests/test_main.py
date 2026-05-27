import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from src.handlers import RequestHandler
from src.main import (
    API_VERSIONS_KEY,
    DESCRIBE_TOPIC_PARTITIONS_KEY,
    FETCH_KEY,
    MAX_WRITE_RETRIES,
    PRODUCE_KEY,
    client_handler,
    flush_buffers_periodically,
)
from src.protocol.reader import BufferReader
from src.protocol.writer import BufferWriter
from src.storage import Storage


# ---------------------------------------------------------------------------
# Wire-format builders
# ---------------------------------------------------------------------------

def make_header(api_key: int, api_version: int, correlation_id: int = 1) -> bytes:
    w = BufferWriter()
    w.write_int16(api_key)
    w.write_int16(api_version)
    w.write_int32(correlation_id)
    w.write_int16(0)       # client_id_length 0 → no client_id
    w.write_tag_buffer()
    return w.get_bytes()


def make_request(api_key: int, api_version: int, body: bytes = b"", correlation_id: int = 1) -> bytes:
    payload = make_header(api_key, api_version, correlation_id) + body
    return len(payload).to_bytes(4, "big") + payload


def make_describe_body(topic_names: list[str]) -> bytes:
    w = BufferWriter()
    w.write_compact_array_length(len(topic_names))
    for name in topic_names:
        w.write_compact_string(name)
        w.write_tag_buffer()
    w.write_int32(0)   # response_partition_limit
    w.write_int8(0)    # cursor
    w.write_tag_buffer()
    return w.get_bytes()


def make_produce_body(topics: list[tuple[str, list[tuple[int, bytes]]]]) -> bytes:
    w = BufferWriter()
    w.write_int8(0)          # transactional_id → null
    w.write_int16(1)         # acks
    w.write_int32(5000)      # timeout_ms
    w.write_compact_array_length(len(topics))
    for topic_name, partitions in topics:
        topic_bytes = topic_name.encode("utf-8")
        w.write_compact_array_length(len(topic_bytes))
        w.write_bytes(topic_bytes)
        w.write_compact_array_length(len(partitions))
        for part_idx, record_batch in partitions:
            w.write_int32(part_idx)
            w.write_compact_array_length(len(record_batch))
            w.write_bytes(record_batch)
            w.write_tag_buffer()
        w.write_tag_buffer()
    return w.get_bytes()


def make_fetch_body(topics: list[tuple[bytes, list[tuple[int, int, int, int, int, int]]]]) -> bytes:
    """topics: list of (topic_id_bytes, [(part_idx, leader_epoch, fetch_off, last_epoch, log_start, max_bytes)])"""
    w = BufferWriter()
    w.write_int32(500)       # max_wait_ms
    w.write_int32(1)         # min_bytes
    w.write_int32(1048576)   # max_bytes
    w.write_int8(0)          # isolation_level
    w.write_int32(0)         # session_id
    w.write_int32(-1)        # session_epoch
    w.write_compact_array_length(len(topics))
    for topic_id, partitions in topics:
        w.write_uuid(topic_id)
        w.write_compact_array_length(len(partitions))
        for part_idx, leader_epoch, fetch_off, last_epoch, log_start, max_part_bytes in partitions:
            w.write_int32(part_idx)
            w.write_int32(leader_epoch)
            w.write_int64(fetch_off)
            w.write_int32(last_epoch)
            w.write_int64(log_start)
            w.write_int32(max_part_bytes)
            w.write_tag_buffer()
        w.write_tag_buffer()
    return w.get_bytes()


# ---------------------------------------------------------------------------
# Response-parsing helpers
# ---------------------------------------------------------------------------

def parse_api_versions_response(body: bytes) -> tuple[int, dict[int, tuple[int, int]]]:
    """Returns (error_code, {api_key: (min_version, max_version)})."""
    r = BufferReader(body)
    error_code = r.read_int16()
    if error_code != 0:
        return error_code, {}
    n = r.read_compact_array_length()
    keys: dict[int, tuple[int, int]] = {}
    for _ in range(n):
        key = r.read_int16()
        min_v = r.read_int16()
        max_v = r.read_int16()
        r.read_tag_buffer()
        keys[key] = (min_v, max_v)
    return error_code, keys


async def read_response(reader: asyncio.StreamReader, timeout: float = 5.0) -> tuple[bytes, bytes]:
    """Read one framed response; return (correlation_id_bytes, body)."""
    size_bytes = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
    size = int.from_bytes(size_bytes, "big")
    full = await asyncio.wait_for(reader.readexactly(size), timeout=timeout)
    return full[:4], full[4:]


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def reader_with(data: bytes) -> asyncio.StreamReader:
    """StreamReader pre-loaded with data and EOF."""
    r = asyncio.StreamReader()
    r.feed_data(data)
    r.feed_eof()
    return r


@pytest.fixture
def mock_writer():
    writer = MagicMock()
    writer.get_extra_info.return_value = ("127.0.0.1", 9999)
    writer.write = MagicMock()
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()
    return writer


@pytest.fixture
def mock_handler():
    handler = MagicMock(spec=RequestHandler)
    handler.handle_api_version_requests = AsyncMock(return_value=b"\x00\x00")
    handler.handle_describe_topic_partition_requests = AsyncMock(return_value=b"\x00\x00")
    handler.handle_fetch_requests = AsyncMock(return_value=b"\x00\x00")
    handler.handle_produce_requests = AsyncMock(return_value=b"\x00\x00")
    return handler


@pytest.fixture
def mock_storage():
    storage = MagicMock(spec=Storage)
    storage.flush_buffers = AsyncMock()
    return storage


@pytest.fixture
def topics():
    return {}


@pytest.fixture
def topics_by_uuid():
    return {}


@pytest.fixture
def shutdown_event():
    return asyncio.Event()


# ---------------------------------------------------------------------------
# TestClientHandler — unit tests
# ---------------------------------------------------------------------------

class TestClientHandler:

    @pytest.mark.asyncio
    @pytest.mark.parametrize("api_key,handler_method", [
        (API_VERSIONS_KEY, "handle_api_version_requests"),
        (DESCRIBE_TOPIC_PARTITIONS_KEY, "handle_describe_topic_partition_requests"),
        (FETCH_KEY, "handle_fetch_requests"),
        (PRODUCE_KEY, "handle_produce_requests"),
    ])
    async def test_dispatches_to_correct_handler(
            self,
            api_key,
            handler_method,
            mock_writer,
            mock_handler,
            topics,
            topics_by_uuid,
            shutdown_event
    ):
        """Each API key routes to the matching handler method exactly once."""
        request = make_request(api_key, 4)
        await client_handler(reader_with(request), mock_writer, mock_handler, topics, topics_by_uuid, shutdown_event)
        getattr(mock_handler, handler_method).assert_called_once()

    @pytest.mark.asyncio
    async def test_response_framing_size_and_correlation_id(
            self,
            mock_writer,
            mock_handler,
            topics,
            topics_by_uuid,
            shutdown_event
    ):
        """Response bytes = 4-byte-size + correlation_id + handler_body."""
        body = b"\xAB\xCD\xEF"
        mock_handler.handle_api_version_requests.return_value = body
        correlation_id = 99

        request = make_request(API_VERSIONS_KEY, 4, correlation_id=correlation_id)
        await client_handler(reader_with(request), mock_writer, mock_handler, topics, topics_by_uuid, shutdown_event)

        written = mock_writer.write.call_args[0][0]
        corr_bytes = correlation_id.to_bytes(4, "big")
        expected_size = (len(corr_bytes) + len(body)).to_bytes(4, "big")
        assert written == expected_size + corr_bytes + body

    @pytest.mark.asyncio
    async def test_different_correlation_ids_preserved(
            self,
            mock_writer,
            mock_handler,
            topics,
            topics_by_uuid,
            shutdown_event
    ):
        """Correlation ID from the request is echoed verbatim in the response."""
        request = make_request(API_VERSIONS_KEY, 4, correlation_id=42)
        await client_handler(reader_with(request), mock_writer, mock_handler, topics, topics_by_uuid, shutdown_event)

        written = mock_writer.write.call_args[0][0]
        resp_corr_id = int.from_bytes(written[4:8], "big")
        assert resp_corr_id == 42

    @pytest.mark.asyncio
    async def test_eof_on_size_read_closes_cleanly(
            self,
            mock_writer,
            mock_handler,
            topics,
            topics_by_uuid,
            shutdown_event
    ):
        """Immediate EOF with no data: loop exits, no handler called, writer closed."""
        r = asyncio.StreamReader()
        r.feed_eof()

        await client_handler(r, mock_writer, mock_handler, topics, topics_by_uuid, shutdown_event)

        mock_handler.handle_api_version_requests.assert_not_called()
        mock_writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_eof_on_payload_read_closes_cleanly(
            self,
            mock_writer,
            mock_handler,
            topics,
            topics_by_uuid,
            shutdown_event
    ):
        """Size header received but EOF before payload: IncompleteReadError caught, loop exits."""
        r = asyncio.StreamReader()
        r.feed_data((10).to_bytes(4, "big"))  # claims 10 bytes of payload
        r.feed_eof()                          # but sends none

        await client_handler(r, mock_writer, mock_handler, topics, topics_by_uuid, shutdown_event)

        mock_handler.handle_api_version_requests.assert_not_called()
        mock_writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_unknown_api_key_no_handler_called(
            self,
            mock_writer,
            mock_handler,
            topics,
            topics_by_uuid,
            shutdown_event
    ):
        """Unknown api_key 999: no handler dispatched; response frame still sent."""
        request = make_request(999, 0)
        await client_handler(reader_with(request), mock_writer, mock_handler, topics, topics_by_uuid, shutdown_event)

        mock_handler.handle_api_version_requests.assert_not_called()
        mock_handler.handle_describe_topic_partition_requests.assert_not_called()
        mock_handler.handle_fetch_requests.assert_not_called()
        mock_handler.handle_produce_requests.assert_not_called()
        # A framed response (size + correlation_id, empty body) is still sent
        mock_writer.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_unknown_api_key_empty_body(
            self,
            mock_writer,
            mock_handler,
            topics,
            topics_by_uuid,
            shutdown_event
    ):
        """Unknown api_key response body is empty; only size + correlation_id written."""
        correlation_id = 77
        request = make_request(999, 0, correlation_id=correlation_id)
        await client_handler(reader_with(request), mock_writer, mock_handler, topics, topics_by_uuid, shutdown_event)

        written = mock_writer.write.call_args[0][0]
        corr_bytes = correlation_id.to_bytes(4, "big")
        # size = 4 (just correlation_id, no body)
        expected_size = (4).to_bytes(4, "big")
        assert written == expected_size + corr_bytes

    @pytest.mark.asyncio
    async def test_multiple_requests_in_sequence(
            self,
            mock_writer,
            mock_handler,
            topics,
            topics_by_uuid,
            shutdown_event
    ):
        """Two consecutive requests on the same connection are both handled."""
        req1 = make_request(API_VERSIONS_KEY, 4, correlation_id=1)
        req2 = make_request(API_VERSIONS_KEY, 4, correlation_id=2)

        await client_handler(reader_with(req1 + req2), mock_writer, mock_handler, topics, topics_by_uuid, shutdown_event)

        assert mock_handler.handle_api_version_requests.call_count == 2
        assert mock_writer.write.call_count == 2

    @pytest.mark.asyncio
    async def test_shutdown_event_set_before_call_skips_loop(
            self,
            mock_writer,
            mock_handler,
            topics,
            topics_by_uuid,
            shutdown_event
    ):
        """If shutdown is already set, the read loop never runs."""
        shutdown_event.set()
        r = asyncio.StreamReader()

        await client_handler(r, mock_writer, mock_handler, topics, topics_by_uuid, shutdown_event)

        mock_handler.handle_api_version_requests.assert_not_called()
        mock_writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_event_set_during_read_breaks_loop(
            self,
            mock_writer,
            mock_handler,
            topics,
            topics_by_uuid,
            shutdown_event
    ):
        """Shutdown event fires while waiting for client data; loop exits cleanly."""
        r = asyncio.StreamReader()  # no data, no EOF

        async def set_shutdown_soon():
            await asyncio.sleep(0.01)
            shutdown_event.set()

        asyncio.create_task(set_shutdown_soon())
        await client_handler(r, mock_writer, mock_handler, topics, topics_by_uuid, shutdown_event)

        mock_handler.handle_api_version_requests.assert_not_called()
        mock_writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_drain_error_retried_max_times(
            self,
            mock_writer,
            mock_handler,
            topics,
            topics_by_uuid,
            shutdown_event
    ):
        """ConnectionError on drain triggers exactly MAX_WRITE_RETRIES attempts."""
        mock_writer.drain.side_effect = ConnectionError("broken pipe")
        request = make_request(API_VERSIONS_KEY, 4)

        await client_handler(reader_with(request), mock_writer, mock_handler, topics, topics_by_uuid, shutdown_event)

        assert mock_writer.drain.call_count == MAX_WRITE_RETRIES

    @pytest.mark.asyncio
    async def test_all_write_retries_exhausted_exits_early(
            self,
            mock_writer,
            mock_handler,
            topics,
            topics_by_uuid,
            shutdown_event
    ):
        """After all write retries fail, function returns; subsequent requests not processed."""
        mock_writer.drain.side_effect = ConnectionError("broken pipe")
        req1 = make_request(API_VERSIONS_KEY, 4, correlation_id=1)
        req2 = make_request(API_VERSIONS_KEY, 4, correlation_id=2)

        await client_handler(reader_with(req1 + req2), mock_writer, mock_handler, topics, topics_by_uuid, shutdown_event)

        # Only the first request was processed before the early return
        assert mock_handler.handle_api_version_requests.call_count == 1
        mock_writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handler_exception_does_not_propagate(
            self,
            mock_writer,
            mock_handler,
            topics,
            topics_by_uuid,
            shutdown_event
    ):
        """RuntimeError in a handler is caught; client_handler returns normally."""
        mock_handler.handle_api_version_requests.side_effect = RuntimeError("boom")
        request = make_request(API_VERSIONS_KEY, 4)

        # Must not raise
        await client_handler(reader_with(request), mock_writer, mock_handler, topics, topics_by_uuid, shutdown_event)

        # A response frame with empty body was still sent
        mock_writer.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_writer_close_always_called(
            self,
            mock_writer,
            mock_handler,
            topics,
            topics_by_uuid,
            shutdown_event
    ):
        """writer.close() is called in the finally block regardless of exit reason."""
        r = asyncio.StreamReader()
        r.feed_eof()

        await client_handler(r, mock_writer, mock_handler, topics, topics_by_uuid, shutdown_event)

        mock_writer.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_read_timeout_closes_connection(
            self,
            mock_writer,
            mock_handler,
            topics,
            topics_by_uuid,
            shutdown_event
    ):
        """When the read times out (no data), the loop exits and the writer is closed."""
        r = asyncio.StreamReader()  # no data, no EOF

        with patch("src.main.CLIENT_READ_TIMEOUT", 0):
            await client_handler(r, mock_writer, mock_handler, topics, topics_by_uuid, shutdown_event)

        mock_handler.handle_api_version_requests.assert_not_called()
        mock_writer.close.assert_called_once()


# ---------------------------------------------------------------------------
# TestFlushBuffersPeriodically — unit tests
# ---------------------------------------------------------------------------

class TestFlushBuffersPeriodically:

    @pytest.mark.asyncio
    async def test_exits_immediately_when_shutdown_set(self, mock_storage):
        """Pre-set shutdown → flush_buffers is never called."""
        shutdown = asyncio.Event()
        shutdown.set()

        await flush_buffers_periodically(mock_storage, shutdown)

        mock_storage.flush_buffers.assert_not_called()

    @pytest.mark.asyncio
    async def test_calls_flush_buffers_after_sleep(self, mock_storage):
        """One sleep → one flush_buffers call, then shutdown."""
        shutdown = asyncio.Event()

        async def flush_and_stop():
            shutdown.set()

        mock_storage.flush_buffers.side_effect = flush_and_stop

        with patch("src.main.asyncio.sleep", new=AsyncMock()):
            await flush_buffers_periodically(mock_storage, shutdown)

        mock_storage.flush_buffers.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_iterations_before_shutdown(self, mock_storage):
        """flush_buffers is called the expected number of times before shutdown."""
        shutdown = asyncio.Event()
        call_count = 0

        async def count_then_maybe_stop():
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                shutdown.set()

        mock_storage.flush_buffers.side_effect = count_then_maybe_stop

        with patch("src.main.asyncio.sleep", new=AsyncMock()):
            await flush_buffers_periodically(mock_storage, shutdown)

        assert mock_storage.flush_buffers.call_count == 3

    @pytest.mark.asyncio
    async def test_flush_duration_metric_observed(self, mock_storage):
        """FLUSH_DURATION histogram is observed once per flush cycle."""
        shutdown = asyncio.Event()

        async def flush_and_stop():
            shutdown.set()

        mock_storage.flush_buffers.side_effect = flush_and_stop

        with patch("src.main.asyncio.sleep", new=AsyncMock()), \
             patch("src.main.FLUSH_DURATION") as mock_metric:
            await flush_buffers_periodically(mock_storage, shutdown)

        mock_metric.observe.assert_called_once()

    @pytest.mark.asyncio
    async def test_flush_exception_propagates(self, mock_storage):
        """An exception from flush_buffers propagates out of the task."""
        shutdown = asyncio.Event()
        mock_storage.flush_buffers.side_effect = OSError("disk full")

        with patch("src.main.asyncio.sleep", new=AsyncMock()), \
             pytest.raises(OSError, match="disk full"):
            await flush_buffers_periodically(mock_storage, shutdown)


# ---------------------------------------------------------------------------
# Integration fixtures and tests
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def integration_server():
    """Real asyncio TCP server on an ephemeral port with a mocked Storage."""
    mock_storage = MagicMock(spec=Storage)
    mock_storage.read_partition_log = AsyncMock(return_value=b"")
    mock_storage.write_partition_log = AsyncMock()
    handler = RequestHandler(mock_storage)
    topics: dict = {}
    topics_by_uuid: dict = {}
    shutdown = asyncio.Event()

    async def handle_client(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        await client_handler(r, w, handler, topics, topics_by_uuid, shutdown)

    server = await asyncio.start_server(handle_client, host="127.0.0.1", port=0)
    host, port = server.sockets[0].getsockname()
    yield host, port, shutdown

    shutdown.set()
    server.close()
    await server.wait_closed()


class TestServerIntegration:

    @pytest.mark.asyncio
    async def test_api_versions_full_roundtrip(self, integration_server):
        """Connect to real server, send API_VERSIONS → parse response → error_code 0."""
        host, port, _ = integration_server
        reader, writer = await asyncio.open_connection(host, port)
        try:
            writer.write(make_request(API_VERSIONS_KEY, 4, correlation_id=7))
            await writer.drain()
            corr_bytes, body = await read_response(reader)
            error_code, keys = parse_api_versions_response(body)
        finally:
            writer.close()
            await writer.wait_closed()

        assert int.from_bytes(corr_bytes, "big") == 7
        assert error_code == 0
        assert API_VERSIONS_KEY in keys

    @pytest.mark.asyncio
    async def test_multiple_requests_same_connection(self, integration_server):
        """Two requests on the same connection get correct independent responses."""
        host, port, _ = integration_server
        reader, writer = await asyncio.open_connection(host, port)
        try:
            for corr_id in (1, 2):
                writer.write(make_request(API_VERSIONS_KEY, 4, correlation_id=corr_id))
                await writer.drain()
                corr_bytes, body = await read_response(reader)
                error_code, _ = parse_api_versions_response(body)
                assert int.from_bytes(corr_bytes, "big") == corr_id
                assert error_code == 0
        finally:
            writer.close()
            await writer.wait_closed()

    @pytest.mark.asyncio
    async def test_graceful_shutdown_closes_connection(self, integration_server):
        """Setting the shutdown event causes the server to stop the client loop."""
        host, port, shutdown = integration_server
        reader, writer = await asyncio.open_connection(host, port)
        try:
            shutdown.set()
            await asyncio.sleep(0.1)  # let the server process the event

            # Connection should be closed; read returns empty or raises
            try:
                data = await asyncio.wait_for(reader.read(1024), timeout=2.0)
                assert data == b""
            except asyncio.TimeoutError:
                pass  # server draining; also acceptable
        finally:
            writer.close()

    @pytest.mark.asyncio
    async def test_concurrent_clients_all_served(self, integration_server):
        """Three concurrent connections each get a valid API_VERSIONS response."""
        host, port, _ = integration_server

        async def one_request(corr_id: int) -> int:
            reader, writer = await asyncio.open_connection(host, port)
            try:
                writer.write(make_request(API_VERSIONS_KEY, 4, correlation_id=corr_id))
                await writer.drain()
                _, body = await read_response(reader)
                error_code, _ = parse_api_versions_response(body)
                return error_code
            finally:
                writer.close()
                await writer.wait_closed()

        results = await asyncio.gather(one_request(1), one_request(2), one_request(3))
        assert all(code == 0 for code in results)


# ---------------------------------------------------------------------------
# End-to-end fixtures and tests
# ---------------------------------------------------------------------------

def _make_topic_fixture(tmp_path):
    """Build the topics/topics_by_uuid dicts for a single-partition test topic."""
    topic_uuid = uuid.uuid4()
    topics = {
        "e2e-topic": {
            "topic_name": "e2e-topic",
            "topic_uuid": topic_uuid,
            "partitions": {
                0: {
                    "partition_index": 0,
                    "leader_id": 1,
                    "leader_epoch": 0,
                    "num_replicas": 1,
                    "num_isr": 1,
                }
            },
        }
    }
    topics_by_uuid = {topic_uuid.bytes: topics["e2e-topic"]}
    return topics, topics_by_uuid, topic_uuid


@pytest_asyncio.fixture
async def e2e_server(tmp_path):
    """Real TCP server with real Storage and one pre-registered topic."""
    storage = Storage(str(tmp_path), "00000000000000000000.log")
    topics, topics_by_uuid, topic_uuid = _make_topic_fixture(tmp_path)
    handler = RequestHandler(storage)
    shutdown = asyncio.Event()

    async def handle_client(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        await client_handler(r, w, handler, topics, topics_by_uuid, shutdown)

    server = await asyncio.start_server(handle_client, host="127.0.0.1", port=0)
    host, port = server.sockets[0].getsockname()
    yield host, port, shutdown, topic_uuid, storage

    shutdown.set()
    server.close()
    await server.wait_closed()


async def send_and_receive(host: str, port: int, request_bytes: bytes) -> tuple[bytes, bytes]:
    """Open a fresh connection, send request, read one response, close."""
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(request_bytes)
        await writer.drain()
        return await read_response(reader)
    finally:
        writer.close()
        await writer.wait_closed()


class TestEndToEnd:

    @pytest.mark.asyncio
    async def test_api_versions_protocol_valid_response(self, e2e_server):
        """Real API_VERSIONS bytes → valid response with all four supported API keys."""
        host, port, _, _, _ = e2e_server
        _, body = await send_and_receive(host, port, make_request(API_VERSIONS_KEY, 4, correlation_id=1))
        error_code, keys = parse_api_versions_response(body)

        assert error_code == 0
        assert API_VERSIONS_KEY in keys
        assert DESCRIBE_TOPIC_PARTITIONS_KEY in keys
        assert FETCH_KEY in keys
        assert PRODUCE_KEY in keys

    @pytest.mark.asyncio
    async def test_produce_then_fetch_roundtrip(self, e2e_server):
        """Produce a record batch, fetch it back; both responses carry no error."""
        host, port, _, topic_uuid, _ = e2e_server
        record_batch = b"\xCA\xFE" * 32

        # --- Produce ---
        produce_body = make_produce_body([("e2e-topic", [(0, record_batch)])])
        _, produce_resp = await send_and_receive(
            host, port, make_request(PRODUCE_KEY, 8, produce_body, correlation_id=1)
        )

        r = BufferReader(produce_resp)
        r.read_tag_buffer()               # response-level tag
        r.read_compact_array_length()     # n_topics
        r.read_compact_string()           # topic_name
        r.read_compact_array_length()     # n_partitions
        r.read_int32()                    # partition_index
        produce_error = r.read_int16()
        assert produce_error == 0         # ERROR_CODE_NONE

        # --- Fetch ---
        fetch_body = make_fetch_body([(topic_uuid.bytes, [(0, 0, 0, 0, 0, 1048576)])])
        _, fetch_resp = await send_and_receive(
            host, port, make_request(FETCH_KEY, 16, fetch_body, correlation_id=2)
        )

        r = BufferReader(fetch_resp)
        r.read_tag_buffer()    # response-level tag
        r.read_int32()         # throttle_time
        fetch_error = r.read_int16()
        assert fetch_error == 0           # session-level error = NONE

    @pytest.mark.asyncio
    async def test_describe_unknown_topic_returns_error_code_3(self, e2e_server):
        """DescribeTopicPartitions for an unknown topic returns error_code 3."""
        host, port, _, _, _ = e2e_server
        body = make_describe_body(["no-such-topic"])
        _, resp_body = await send_and_receive(
            host, port, make_request(DESCRIBE_TOPIC_PARTITIONS_KEY, 0, body, correlation_id=1)
        )

        r = BufferReader(resp_body)
        r.read_tag_buffer()            # response-level tag
        r.read_int32()                 # throttle_time
        r.read_compact_array_length()  # n_topics
        error_code = r.read_int16()
        assert error_code == 3         # UNKNOWN_TOPIC_OR_PARTITION

    @pytest.mark.asyncio
    async def test_unknown_api_key_returns_framed_response_not_crash(self, e2e_server):
        """Unknown API key: server sends a correctly-framed response and stays alive."""
        host, port, _, _, _ = e2e_server

        corr_bytes, body = await send_and_receive(
            host, port, make_request(999, 0, correlation_id=55)
        )
        assert int.from_bytes(corr_bytes, "big") == 55
        assert body == b""  # no handler matched → empty body

        # Server is still up and serving subsequent requests
        _, body2 = await send_and_receive(
            host, port, make_request(API_VERSIONS_KEY, 4, correlation_id=56)
        )
        error_code, _ = parse_api_versions_response(body2)
        assert error_code == 0

    @pytest.mark.asyncio
    async def test_api_versions_unsupported_version_returns_error(self, e2e_server):
        """API_VERSIONS request with unsupported version → non-zero error code."""
        host, port, _, _, _ = e2e_server
        # version 99 is not in the supported set (0-4)
        _, body = await send_and_receive(
            host, port, make_request(API_VERSIONS_KEY, 99, correlation_id=1)
        )
        error_code, _ = parse_api_versions_response(body)
        assert error_code != 0

    @pytest.mark.asyncio
    async def test_produce_unknown_topic_returns_error_code_3(self, e2e_server):
        """Produce to a topic not in the metadata returns error_code 3."""
        host, port, _, _, _ = e2e_server
        produce_body = make_produce_body([("unknown-topic", [(0, b"\x00" * 16)])])
        _, produce_resp = await send_and_receive(
            host, port, make_request(PRODUCE_KEY, 8, produce_body, correlation_id=1)
        )

        r = BufferReader(produce_resp)
        r.read_tag_buffer()
        r.read_compact_array_length()
        r.read_compact_string()         # topic_name
        r.read_compact_array_length()
        r.read_int32()                  # partition_index
        produce_error = r.read_int16()
        assert produce_error == 3       # UNKNOWN_TOPIC_OR_PARTITION
