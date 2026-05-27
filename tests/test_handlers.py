import uuid
from unittest.mock import AsyncMock, MagicMock
import pytest

from src.handlers import RequestHandler
from src.protocol.reader import BufferReader
from src.protocol.writer import BufferWriter
from src.storage import Storage


# ---------------------------------------------------------------------------
# Request builders  (wire format matches src/protocol/parser.py exactly)
# ---------------------------------------------------------------------------

def make_header(api_key: int, api_version: int, correlation_id: int = 1) -> bytes:
    w = BufferWriter()
    w.write_int16(api_key)
    w.write_int16(api_version)
    w.write_int32(correlation_id)
    w.write_int16(0)        # client_id_length 0 → no client_id
    w.write_tag_buffer()
    return w.get_bytes()


def make_request(api_key: int, api_version: int, body: bytes = b'', correlation_id: int = 1) -> bytes:
    payload = make_header(api_key, api_version, correlation_id) + body
    return len(payload).to_bytes(4, 'big') + payload


def make_describe_body(topic_names: list[str], partition_limit: int = 0) -> bytes:
    w = BufferWriter()
    w.write_compact_array_length(len(topic_names))
    for name in topic_names:
        w.write_compact_string(name)
        w.write_tag_buffer()
    w.write_int32(partition_limit)
    w.write_int8(0)         # cursor
    w.write_tag_buffer()
    return w.get_bytes()


def make_fetch_body(
    topics: list[tuple[bytes, list[tuple[int, int, int, int, int, int]]]],
    session_id: int = 0,
    session_epoch: int = -1,
) -> bytes:
    """topics: list of (topic_id_bytes, [(part_idx, leader_epoch, fetch_off, last_epoch, log_start, max_bytes)])"""
    w = BufferWriter()
    w.write_int32(500)       # max_wait_ms
    w.write_int32(1)         # min_bytes
    w.write_int32(1048576)   # max_bytes
    w.write_int8(0)          # isolation_level
    w.write_int32(session_id)
    w.write_int32(session_epoch)
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


def make_produce_body(
    topics: list[tuple[str, list[tuple[int, bytes]]]],
    acks: int = 1,
    timeout_ms: int = 5000,
) -> bytes:
    """topics: list of (topic_name, [(partition_index, record_batch_bytes)])"""
    w = BufferWriter()
    w.write_int8(0)          # transactional_id length 0 → null
    w.write_int16(acks)
    w.write_int32(timeout_ms)
    w.write_compact_array_length(len(topics))
    for topic_name, partitions in topics:
        topic_bytes = topic_name.encode("utf-8")
        w.write_compact_array_length(len(topic_bytes))   # varint(len+1)
        w.write_bytes(topic_bytes)                        # raw bytes (not compact_string)
        w.write_compact_array_length(len(partitions))
        for part_idx, record_batch in partitions:
            w.write_int32(part_idx)
            w.write_compact_array_length(len(record_batch))
            w.write_bytes(record_batch)
            w.write_tag_buffer()
        w.write_tag_buffer()
    return w.get_bytes()


# ---------------------------------------------------------------------------
# Response parsers  (used by tests to decode handler output)
# ---------------------------------------------------------------------------

def parse_api_versions_response(resp: bytes) -> tuple[int, dict[int, tuple[int, int]]]:
    """Returns (error_code, {api_key: (min_version, max_version)})."""
    r = BufferReader(resp)
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


def parse_describe_response(resp: bytes) -> list[dict]:
    """Returns list of {error, name, uuid, is_internal, partitions: [{error, index, leader_id, ...}]}."""
    r = BufferReader(resp)
    r.read_tag_buffer()     # response-level tag buffer
    r.read_int32()          # throttle_time
    n = r.read_compact_array_length()
    result = []
    for _ in range(n):
        t: dict = {
            'error': r.read_int16(),
            'name': r.read_compact_string(),
            'uuid': r.read_uuid(),
            'is_internal': r.read_int8(),
        }
        n_parts = r.read_compact_array_length()
        parts = []
        for _ in range(n_parts):
            p: dict = {
                'error': r.read_int16(),
                'index': r.read_int32(),
                'leader_id': r.read_int32(),
                'leader_epoch': r.read_int32(),
                'num_replicas': r.read_int32(),
            }
            r.read_int8()           # broker (hardcoded 1 in handler)
            p['num_isr'] = r.read_int32()
            r.read_int8()           # broker
            r.read_int8()           # elr
            r.read_int8()           # last_elr
            r.read_int8()           # offline_replicas
            r.read_tag_buffer()
            parts.append(p)
        t['partitions'] = parts
        r.read_int32()              # topic_authorized_operations
        r.read_tag_buffer()
        result.append(t)
    return result


def parse_fetch_response(resp: bytes) -> dict:
    """Returns {error, session_id, topics: [{id, partitions: [{index, error, records}]}]}."""
    r = BufferReader(resp)
    r.read_tag_buffer()
    r.read_int32()          # throttle_time
    error = r.read_int16()
    session_id = r.read_int32()
    n_topics = r.read_compact_array_length()
    topics = []
    for _ in range(n_topics):
        topic_id = r.read_uuid()
        n_parts = r.read_compact_array_length()
        parts = []
        for _ in range(n_parts):
            p: dict = {
                'index': r.read_int32(),
                'error': r.read_int16(),
            }
            r.read_int64()  # high_watermark
            r.read_int64()  # last_stable_offset
            r.read_int64()  # log_start_offset
            r.read_int8()   # aborted_transactions (compact array len)
            r.read_int32()  # preferred_read_replica
            records_len = r.read_compact_array_length()   # -1 when handler wrote varint(0)
            p['records'] = r.read_bytes(records_len) if records_len > 0 else b''
            r.read_int8()   # diverging_epoch
            r.read_int8()   # current_leader
            r.read_int8()   # snapshot_id
            r.read_tag_buffer()
            parts.append(p)
        r.read_tag_buffer()
        topics.append({'id': topic_id, 'partitions': parts})
    return {'error': error, 'session_id': session_id, 'topics': topics}


def parse_produce_response(resp: bytes) -> list[dict]:
    """Returns list of {name, partitions: [{index, error, base_offset, log_append_time, log_start_offset}]}."""
    r = BufferReader(resp)
    r.read_tag_buffer()
    n_topics = r.read_compact_array_length()
    result = []
    for _ in range(n_topics):
        t: dict = {'name': r.read_compact_string()}
        n_parts = r.read_compact_array_length()
        parts = []
        for _ in range(n_parts):
            p: dict = {
                'index': r.read_int32(),
                'error': r.read_int16(),
                'base_offset': r.read_int64(),
                'log_append_time': r.read_int64(),
                'log_start_offset': r.read_int64(),
            }
            r.read_int8()   # record_errors compact array length (1 = empty)
            r.read_int8()   # error_message
            r.read_tag_buffer()
            parts.append(p)
        t['partitions'] = parts
        r.read_tag_buffer()
        result.append(t)
    r.read_int32()          # throttle_time
    return result


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def topic_uuid() -> uuid.UUID:
    return uuid.UUID('12345678-1234-5678-1234-567812345678')


@pytest.fixture
def topics(topic_uuid: uuid.UUID) -> dict:
    return {
        "test-topic": {
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


@pytest.fixture
def topics_by_uuid(topic_uuid: uuid.UUID, topics: dict) -> dict:
    return {
        topic_uuid.bytes: {
            "topic_name": "test-topic",
            **topics["test-topic"],
        }
    }


def make_storage_mock(read_return: bytes = b'') -> MagicMock:
    storage = MagicMock(spec=Storage)
    storage.read_partition_log = AsyncMock(return_value=read_return)
    storage.write_partition_log = AsyncMock(return_value=None)
    return storage


@pytest.fixture
def handler() -> RequestHandler:
    return RequestHandler(make_storage_mock())


# ---------------------------------------------------------------------------
# TestApiVersionHandler
# ---------------------------------------------------------------------------

class TestApiVersionHandler:

    @pytest.mark.asyncio
    @pytest.mark.parametrize("version", [0, 1, 2, 3, 4])
    async def test_valid_versions_return_no_error(self, handler: RequestHandler, version: int):
        resp = await handler.handle_api_version_requests(make_request(18, version))
        error_code, _ = parse_api_versions_response(resp)
        assert error_code == 0

    @pytest.mark.asyncio
    async def test_version_5_returns_unknown_server_error(self, handler: RequestHandler):
        resp = await handler.handle_api_version_requests(make_request(18, 5))
        assert resp == b'\x00\x23'

    @pytest.mark.asyncio
    async def test_version_100_returns_unknown_server_error(self, handler: RequestHandler):
        resp = await handler.handle_api_version_requests(make_request(18, 100))
        assert resp == b'\x00\x23'

    @pytest.mark.asyncio
    async def test_response_contains_four_api_keys(self, handler: RequestHandler):
        resp = await handler.handle_api_version_requests(make_request(18, 4))
        _, keys = parse_api_versions_response(resp)
        assert len(keys) == 4

    @pytest.mark.asyncio
    async def test_api_versions_key_18_min_0_max_4(self, handler: RequestHandler):
        resp = await handler.handle_api_version_requests(make_request(18, 4))
        _, keys = parse_api_versions_response(resp)
        assert keys[18] == (0, 4)

    @pytest.mark.asyncio
    async def test_describe_topic_key_75_min_0_max_0(self, handler: RequestHandler):
        resp = await handler.handle_api_version_requests(make_request(18, 4))
        _, keys = parse_api_versions_response(resp)
        assert keys[75] == (0, 0)

    @pytest.mark.asyncio
    async def test_fetch_key_1_min_0_max_16(self, handler: RequestHandler):
        resp = await handler.handle_api_version_requests(make_request(18, 4))
        _, keys = parse_api_versions_response(resp)
        assert keys[1] == (0, 16)

    @pytest.mark.asyncio
    async def test_produce_key_0_min_0_max_11(self, handler: RequestHandler):
        resp = await handler.handle_api_version_requests(make_request(18, 4))
        _, keys = parse_api_versions_response(resp)
        assert keys[0] == (0, 11)

    @pytest.mark.asyncio
    async def test_throttle_time_is_zero(self, handler: RequestHandler):
        resp = await handler.handle_api_version_requests(make_request(18, 4))
        # Layout: 2(error) + 1(array len) + 4×7(keys: 2+2+2+1 each) = 31 bytes before throttle
        throttle = int.from_bytes(resp[31:35], 'big')
        assert throttle == 0


# ---------------------------------------------------------------------------
# TestDescribeTopicPartitionHandler
# ---------------------------------------------------------------------------

class TestDescribeTopicPartitionHandler:

    @pytest.mark.asyncio
    async def test_known_topic_returns_no_error(self, handler: RequestHandler, topics: dict):
        req = make_request(75, 0, make_describe_body(["test-topic"]))
        resp = await handler.handle_describe_topic_partition_requests(req, topics)
        result = parse_describe_response(resp)
        assert result[0]['error'] == 0

    @pytest.mark.asyncio
    async def test_unknown_topic_returns_error_3(self, handler: RequestHandler, topics: dict):
        req = make_request(75, 0, make_describe_body(["no-such-topic"]))
        resp = await handler.handle_describe_topic_partition_requests(req, topics)
        result = parse_describe_response(resp)
        assert result[0]['error'] == 3

    @pytest.mark.asyncio
    async def test_unknown_topic_uuid_is_all_zeros(self, handler: RequestHandler, topics: dict):
        req = make_request(75, 0, make_describe_body(["no-such-topic"]))
        resp = await handler.handle_describe_topic_partition_requests(req, topics)
        result = parse_describe_response(resp)
        assert result[0]['uuid'] == bytes(16)

    @pytest.mark.asyncio
    async def test_known_topic_uuid_matches_metadata(
        self, handler: RequestHandler, topics: dict, topic_uuid: uuid.UUID
    ):
        req = make_request(75, 0, make_describe_body(["test-topic"]))
        resp = await handler.handle_describe_topic_partition_requests(req, topics)
        result = parse_describe_response(resp)
        assert result[0]['uuid'] == topic_uuid.bytes

    @pytest.mark.asyncio
    async def test_known_topic_has_one_partition(self, handler: RequestHandler, topics: dict):
        req = make_request(75, 0, make_describe_body(["test-topic"]))
        resp = await handler.handle_describe_topic_partition_requests(req, topics)
        result = parse_describe_response(resp)
        assert len(result[0]['partitions']) == 1

    @pytest.mark.asyncio
    async def test_known_topic_partition_index_is_zero(self, handler: RequestHandler, topics: dict):
        req = make_request(75, 0, make_describe_body(["test-topic"]))
        resp = await handler.handle_describe_topic_partition_requests(req, topics)
        result = parse_describe_response(resp)
        assert result[0]['partitions'][0]['index'] == 0

    @pytest.mark.asyncio
    async def test_known_topic_leader_id_matches_metadata(self, handler: RequestHandler, topics: dict):
        req = make_request(75, 0, make_describe_body(["test-topic"]))
        resp = await handler.handle_describe_topic_partition_requests(req, topics)
        result = parse_describe_response(resp)
        assert result[0]['partitions'][0]['leader_id'] == 1

    @pytest.mark.asyncio
    async def test_unknown_topic_has_no_partitions(self, handler: RequestHandler, topics: dict):
        req = make_request(75, 0, make_describe_body(["no-such-topic"]))
        resp = await handler.handle_describe_topic_partition_requests(req, topics)
        result = parse_describe_response(resp)
        assert result[0]['partitions'] == []

    @pytest.mark.asyncio
    async def test_two_unknown_topics_both_get_error_3(self, handler: RequestHandler, topics: dict):
        req = make_request(75, 0, make_describe_body(["alpha", "beta"]))
        resp = await handler.handle_describe_topic_partition_requests(req, topics)
        result = parse_describe_response(resp)
        assert len(result) == 2
        assert all(t['error'] == 3 for t in result)

    @pytest.mark.asyncio
    async def test_topics_sorted_alphabetically_in_response(self, handler: RequestHandler, topics: dict):
        req = make_request(75, 0, make_describe_body(["zzz-topic", "aaa-topic"]))
        resp = await handler.handle_describe_topic_partition_requests(req, topics)
        result = parse_describe_response(resp)
        assert result[0]['name'] == 'aaa-topic'
        assert result[1]['name'] == 'zzz-topic'

    @pytest.mark.asyncio
    async def test_empty_topics_list_returns_empty_array(self, handler: RequestHandler, topics: dict):
        req = make_request(75, 0, make_describe_body([]))
        resp = await handler.handle_describe_topic_partition_requests(req, topics)
        result = parse_describe_response(resp)
        assert result == []

    @pytest.mark.asyncio
    async def test_mixed_known_and_unknown_topics(self, handler: RequestHandler, topics: dict):
        req = make_request(75, 0, make_describe_body(["test-topic", "missing"]))
        resp = await handler.handle_describe_topic_partition_requests(req, topics)
        result = parse_describe_response(resp)
        by_name = {t['name']: t for t in result}
        assert by_name["test-topic"]['error'] == 0
        assert by_name["missing"]['error'] == 3

    @pytest.mark.asyncio
    async def test_topic_with_multiple_partitions(self, topic_uuid: uuid.UUID):
        multi_topics = {
            "multi": {
                "topic_uuid": topic_uuid,
                "partitions": {
                    0: {"partition_index": 0, "leader_id": 1, "leader_epoch": 0, "num_replicas": 3, "num_isr": 3},
                    1: {"partition_index": 1, "leader_id": 2, "leader_epoch": 0, "num_replicas": 3, "num_isr": 2},
                },
            }
        }
        handler = RequestHandler(make_storage_mock())
        req = make_request(75, 0, make_describe_body(["multi"]))
        resp = await handler.handle_describe_topic_partition_requests(req, multi_topics)
        result = parse_describe_response(resp)
        assert len(result[0]['partitions']) == 2


# ---------------------------------------------------------------------------
# TestFetchHandler
# ---------------------------------------------------------------------------

class TestFetchHandler:

    @pytest.mark.asyncio
    async def test_known_topic_calls_read_partition_log(
        self, topics_by_uuid: dict, topic_uuid: uuid.UUID
    ):
        storage = make_storage_mock()
        handler = RequestHandler(storage)
        req = make_request(1, 16, make_fetch_body([(topic_uuid.bytes, [(0, 0, 0, 0, 0, 1024)])]))
        await handler.handle_fetch_requests(req, topics_by_uuid)
        storage.read_partition_log.assert_called_once()

    @pytest.mark.asyncio
    async def test_known_topic_storage_called_with_correct_args(
        self, topics_by_uuid: dict, topic_uuid: uuid.UUID
    ):
        storage = make_storage_mock()
        handler = RequestHandler(storage)
        req = make_request(1, 16, make_fetch_body([(topic_uuid.bytes, [(0, 0, 42, 0, 0, 512)])]))
        await handler.handle_fetch_requests(req, topics_by_uuid)
        storage.read_partition_log.assert_called_once_with("test-topic", 0, 42, 512)

    @pytest.mark.asyncio
    async def test_known_topic_returns_records_from_storage(
        self, topics_by_uuid: dict, topic_uuid: uuid.UUID
    ):
        record_data = b'\xAA\xBB\xCC\xDD'
        handler = RequestHandler(make_storage_mock(read_return=record_data))
        req = make_request(1, 16, make_fetch_body([(topic_uuid.bytes, [(0, 0, 0, 0, 0, 1024)])]))
        resp = await handler.handle_fetch_requests(req, topics_by_uuid)
        result = parse_fetch_response(resp)
        assert result['topics'][0]['partitions'][0]['records'] == record_data

    @pytest.mark.asyncio
    async def test_unknown_topic_uuid_returns_error_100(self, topics_by_uuid: dict):
        handler = RequestHandler(make_storage_mock())
        unknown = uuid.UUID('99999999-9999-9999-9999-999999999999')
        req = make_request(1, 16, make_fetch_body([(unknown.bytes, [(0, 0, 0, 0, 0, 1024)])]))
        resp = await handler.handle_fetch_requests(req, topics_by_uuid)
        result = parse_fetch_response(resp)
        assert result['topics'][0]['partitions'][0]['error'] == 100

    @pytest.mark.asyncio
    async def test_unknown_topic_does_not_call_storage(self, topics_by_uuid: dict):
        storage = make_storage_mock()
        handler = RequestHandler(storage)
        unknown = uuid.UUID('99999999-9999-9999-9999-999999999999')
        req = make_request(1, 16, make_fetch_body([(unknown.bytes, [(0, 0, 0, 0, 0, 1024)])]))
        await handler.handle_fetch_requests(req, topics_by_uuid)
        storage.read_partition_log.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_topics_list(self, topics_by_uuid: dict):
        handler = RequestHandler(make_storage_mock())
        req = make_request(1, 16, make_fetch_body([]))
        resp = await handler.handle_fetch_requests(req, topics_by_uuid)
        result = parse_fetch_response(resp)
        assert result['topics'] == []

    @pytest.mark.asyncio
    async def test_session_id_echoed_in_response(self, topics_by_uuid: dict):
        handler = RequestHandler(make_storage_mock())
        req = make_request(1, 16, make_fetch_body([], session_id=99999))
        resp = await handler.handle_fetch_requests(req, topics_by_uuid)
        result = parse_fetch_response(resp)
        assert result['session_id'] == 99999

    @pytest.mark.asyncio
    async def test_top_level_error_code_is_none(self, topics_by_uuid: dict):
        handler = RequestHandler(make_storage_mock())
        req = make_request(1, 16, make_fetch_body([]))
        resp = await handler.handle_fetch_requests(req, topics_by_uuid)
        result = parse_fetch_response(resp)
        assert result['error'] == 0

    @pytest.mark.asyncio
    async def test_two_partitions_calls_storage_twice(
        self, topics_by_uuid: dict, topic_uuid: uuid.UUID
    ):
        storage = make_storage_mock()
        handler = RequestHandler(storage)
        partitions = [(0, 0, 0, 0, 0, 512), (1, 0, 0, 0, 0, 512)]
        req = make_request(1, 16, make_fetch_body([(topic_uuid.bytes, partitions)]))
        await handler.handle_fetch_requests(req, topics_by_uuid)
        assert storage.read_partition_log.call_count == 2

    @pytest.mark.asyncio
    async def test_empty_storage_return_gives_empty_records(
        self, topics_by_uuid: dict, topic_uuid: uuid.UUID
    ):
        handler = RequestHandler(make_storage_mock(read_return=b''))
        req = make_request(1, 16, make_fetch_body([(topic_uuid.bytes, [(0, 0, 0, 0, 0, 1024)])]))
        resp = await handler.handle_fetch_requests(req, topics_by_uuid)
        result = parse_fetch_response(resp)
        assert result['topics'][0]['partitions'][0]['records'] == b''

    @pytest.mark.asyncio
    async def test_known_and_unknown_topics_in_same_request(
        self, topics_by_uuid: dict, topic_uuid: uuid.UUID
    ):
        storage = make_storage_mock(read_return=b'\x01\x02')
        handler = RequestHandler(storage)
        unknown = uuid.UUID('99999999-9999-9999-9999-999999999999')
        req = make_request(1, 16, make_fetch_body([
            (topic_uuid.bytes, [(0, 0, 0, 0, 0, 1024)]),
            (unknown.bytes, [(0, 0, 0, 0, 0, 1024)]),
        ]))
        resp = await handler.handle_fetch_requests(req, topics_by_uuid)
        result = parse_fetch_response(resp)
        by_id = {t['id']: t for t in result['topics']}
        assert by_id[topic_uuid.bytes]['partitions'][0]['error'] == 0
        assert by_id[unknown.bytes]['partitions'][0]['error'] == 100


# ---------------------------------------------------------------------------
# TestProduceHandler
# ---------------------------------------------------------------------------

class TestProduceHandler:

    @pytest.mark.asyncio
    async def test_valid_topic_and_partition_returns_no_error(
        self, handler: RequestHandler, topics: dict
    ):
        req = make_request(0, 11, make_produce_body([("test-topic", [(0, b'\x01\x02\x03')])]))
        resp = await handler.handle_produce_requests(req, topics)
        result = parse_produce_response(resp)
        assert result[0]['partitions'][0]['error'] == 0

    @pytest.mark.asyncio
    async def test_valid_partition_calls_write_partition_log(self, topics: dict):
        storage = make_storage_mock()
        handler = RequestHandler(storage)
        req = make_request(0, 11, make_produce_body([("test-topic", [(0, b'\x01\x02\x03')])]))
        await handler.handle_produce_requests(req, topics)
        storage.write_partition_log.assert_called_once()

    @pytest.mark.asyncio
    async def test_record_batch_bytes_passed_to_storage(self, topics: dict):
        storage = make_storage_mock()
        handler = RequestHandler(storage)
        batch = b'\xDE\xAD\xBE\xEF'
        req = make_request(0, 11, make_produce_body([("test-topic", [(0, batch)])]))
        await handler.handle_produce_requests(req, topics)
        storage.write_partition_log.assert_called_once_with("test-topic", 0, batch)

    @pytest.mark.asyncio
    async def test_valid_partition_base_offset_is_zero(self, handler: RequestHandler, topics: dict):
        req = make_request(0, 11, make_produce_body([("test-topic", [(0, b'\x01')])]))
        resp = await handler.handle_produce_requests(req, topics)
        result = parse_produce_response(resp)
        assert result[0]['partitions'][0]['base_offset'] == 0

    @pytest.mark.asyncio
    async def test_valid_partition_log_start_offset_is_zero(self, handler: RequestHandler, topics: dict):
        req = make_request(0, 11, make_produce_body([("test-topic", [(0, b'\x01')])]))
        resp = await handler.handle_produce_requests(req, topics)
        result = parse_produce_response(resp)
        assert result[0]['partitions'][0]['log_start_offset'] == 0

    @pytest.mark.asyncio
    async def test_unknown_topic_returns_error_3(self, handler: RequestHandler, topics: dict):
        req = make_request(0, 11, make_produce_body([("no-such-topic", [(0, b'\x01')])]))
        resp = await handler.handle_produce_requests(req, topics)
        result = parse_produce_response(resp)
        assert result[0]['partitions'][0]['error'] == 3

    @pytest.mark.asyncio
    async def test_unknown_topic_does_not_call_storage(self, topics: dict):
        storage = make_storage_mock()
        handler = RequestHandler(storage)
        req = make_request(0, 11, make_produce_body([("no-such-topic", [(0, b'\x01')])]))
        await handler.handle_produce_requests(req, topics)
        storage.write_partition_log.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_partition_returns_error_3(self, handler: RequestHandler, topics: dict):
        req = make_request(0, 11, make_produce_body([("test-topic", [(99, b'\x01')])]))
        resp = await handler.handle_produce_requests(req, topics)
        result = parse_produce_response(resp)
        assert result[0]['partitions'][0]['error'] == 3

    @pytest.mark.asyncio
    async def test_unknown_partition_does_not_call_storage(self, topics: dict):
        storage = make_storage_mock()
        handler = RequestHandler(storage)
        req = make_request(0, 11, make_produce_body([("test-topic", [(99, b'\x01')])]))
        await handler.handle_produce_requests(req, topics)
        storage.write_partition_log.assert_not_called()

    @pytest.mark.asyncio
    async def test_two_partitions_one_valid_one_invalid(self, topics: dict):
        storage = make_storage_mock()
        handler = RequestHandler(storage)
        req = make_request(0, 11, make_produce_body([("test-topic", [(0, b'\x01'), (99, b'\x02')])]))
        resp = await handler.handle_produce_requests(req, topics)
        result = parse_produce_response(resp)
        by_idx = {p['index']: p for p in result[0]['partitions']}
        assert by_idx[0]['error'] == 0
        assert by_idx[99]['error'] == 3
        storage.write_partition_log.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_partition_offsets_are_all_minus_one(self, handler: RequestHandler, topics: dict):
        req = make_request(0, 11, make_produce_body([("no-such-topic", [(0, b'\x01')])]))
        resp = await handler.handle_produce_requests(req, topics)
        result = parse_produce_response(resp)
        p = result[0]['partitions'][0]
        assert p['base_offset'] == -1
        assert p['log_append_time'] == -1
        assert p['log_start_offset'] == -1

    @pytest.mark.asyncio
    async def test_throttle_time_is_zero(self, handler: RequestHandler, topics: dict):
        req = make_request(0, 11, make_produce_body([("test-topic", [(0, b'\x01')])]))
        resp = await handler.handle_produce_requests(req, topics)
        # throttle_time is last 4 bytes before the final tag_buffer byte
        assert resp[-5:-1] == b'\x00\x00\x00\x00'

    @pytest.mark.asyncio
    async def test_response_topic_name_matches_request(self, handler: RequestHandler, topics: dict):
        req = make_request(0, 11, make_produce_body([("test-topic", [(0, b'\x01')])]))
        resp = await handler.handle_produce_requests(req, topics)
        result = parse_produce_response(resp)
        assert result[0]['name'] == 'test-topic'


# ---------------------------------------------------------------------------
# TestIntegration
# ---------------------------------------------------------------------------

LOG_FILE_NAME = "00000000000000000000.log"


class TestIntegration:

    @pytest.mark.asyncio
    async def test_produce_then_fetch_returns_same_bytes(
        self, tmp_path, topics: dict, topics_by_uuid: dict, topic_uuid: uuid.UUID
    ):
        storage = Storage(str(tmp_path), LOG_FILE_NAME)
        handler = RequestHandler(storage)

        record_batch = b'\xCA\xFE\xBA\xBE\x01\x02\x03\x04'
        produce_req = make_request(0, 11, make_produce_body([("test-topic", [(0, record_batch)])]))
        await handler.handle_produce_requests(produce_req, topics)

        fetch_req = make_request(1, 16, make_fetch_body([(topic_uuid.bytes, [(0, 0, 0, 0, 0, 65536)])]))
        resp = await handler.handle_fetch_requests(fetch_req, topics_by_uuid)
        result = parse_fetch_response(resp)
        assert result['topics'][0]['partitions'][0]['records'] == record_batch

    @pytest.mark.asyncio
    async def test_fetch_with_offset_returns_partial_data(
        self, tmp_path, topics: dict, topics_by_uuid: dict, topic_uuid: uuid.UUID
    ):
        storage = Storage(str(tmp_path), LOG_FILE_NAME)
        handler = RequestHandler(storage)

        # Produce 8 bytes, then flush so they land on disk
        data = b'\x01\x02\x03\x04\x05\x06\x07\x08'
        produce_req = make_request(0, 11, make_produce_body([("test-topic", [(0, data)])]))
        await handler.handle_produce_requests(produce_req, topics)
        await storage.flush_buffers()

        # Fetch from byte offset 4 — should get last 4 bytes
        fetch_req = make_request(1, 16, make_fetch_body([(topic_uuid.bytes, [(0, 0, 4, 0, 0, 65536)])]))
        resp = await handler.handle_fetch_requests(fetch_req, topics_by_uuid)
        result = parse_fetch_response(resp)
        assert result['topics'][0]['partitions'][0]['records'] == data[4:]

    @pytest.mark.asyncio
    async def test_produce_to_unknown_topic_does_not_create_file(self, tmp_path, topics: dict):
        storage = Storage(str(tmp_path), LOG_FILE_NAME)
        handler = RequestHandler(storage)

        req = make_request(0, 11, make_produce_body([("ghost-topic", [(0, b'\xAB\xCD')])]))
        await handler.handle_produce_requests(req, topics)

        assert not (tmp_path / "ghost-topic-0").exists()
