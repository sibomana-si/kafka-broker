from src.protocol.reader import BufferReader
from src.protocol.writer import BufferWriter
from src.protocol.parser import (
    parse_request_header,
    parse_describe_topic_partitions_request,
    parse_fetch_request,
    parse_produce_request,
)
from src.protocol.messages import (
    RequestHeader,
    DescribeTopicPartitionsRequest,
    DescribeTopicPartitionsRequestTopic,
    FetchRequest,
    FetchRequestTopic,
    FetchRequestPartition,
    ProduceRequest,
    ProduceRequestTopic,
    ProduceRequestPartition,
)


# ---------------------------------------------------------------------------
# Fixture builders
# Each helper returns bytes encoding exactly what the corresponding parser
# expects. Tests wrap the result in BufferReader(buf) and call the parser.
# ---------------------------------------------------------------------------

def make_header(
    api_key: int = 18,
    api_version: int = 4,
    correlation_id: int = 1,
    client_id: str | None = None,
) -> bytes:
    """Encode bytes for parse_request_header (excludes the 4-byte length prefix)."""
    w = BufferWriter()
    w.write_int16(api_key)
    w.write_int16(api_version)
    w.write_int32(correlation_id)
    if client_id is not None:
        encoded = client_id.encode("utf-8")
        w.write_int16(len(encoded))
        w.write_bytes(encoded)
    else:
        w.write_int16(0)   # length 0 → no client_id
    w.write_tag_buffer()
    return w.get_bytes()


def make_describe_body(
    topic_names: list[str],
    partition_limit: int = 0,
    cursor: int = 0,
) -> bytes:
    """Encode bytes for parse_describe_topic_partitions_request."""
    w = BufferWriter()
    w.write_compact_array_length(len(topic_names))
    for name in topic_names:
        w.write_compact_string(name)   # varint(len+1) + utf-8
        w.write_tag_buffer()
    w.write_int32(partition_limit)
    w.write_int8(cursor)
    w.write_tag_buffer()
    return w.get_bytes()


def make_fetch_body(
    topics: list[tuple[bytes, list[tuple[int, int, int, int, int, int]]]],
    max_wait_ms: int = 500,
    min_bytes: int = 1,
    max_bytes: int = 1048576,
    isolation_level: int = 0,
    session_id: int = 0,
    session_epoch: int = -1,
) -> bytes:
    """
    Encode bytes for parse_fetch_request.

    topics is a list of (topic_id_bytes, partitions) where partitions is a list of
    (partition_index, current_leader_epoch, fetch_offset, last_fetched_epoch,
     log_start_offset, partition_max_bytes).
    """
    w = BufferWriter()
    w.write_int32(max_wait_ms)
    w.write_int32(min_bytes)
    w.write_int32(max_bytes)
    w.write_int8(isolation_level)
    w.write_int32(session_id)
    w.write_int32(session_epoch)
    w.write_compact_array_length(len(topics))
    for topic_id, partitions in topics:
        w.write_uuid(topic_id)
        w.write_compact_array_length(len(partitions))
        for (part_idx, leader_epoch, fetch_off, last_epoch, log_start, max_part_bytes) in partitions:
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
    transactional_id: str | None = None,
    acks: int = 1,
    timeout_ms: int = 5000,
) -> bytes:
    """
    Encode bytes for parse_produce_request.

    topics is a list of (topic_name, partitions) where partitions is a list of
    (partition_index, record_batch_bytes).

    Note: topic_name is encoded as compact_array_length + raw bytes (NOT compact_string),
    matching what parse_produce_request reads.
    """
    w = BufferWriter()
    if transactional_id is not None:
        encoded_tid = transactional_id.encode("utf-8")
        w.write_int8(len(encoded_tid))   # plain int8 length
        w.write_bytes(encoded_tid)
    else:
        w.write_int8(0)
    w.write_int16(acks)
    w.write_int32(timeout_ms)
    w.write_compact_array_length(len(topics))
    for topic_name, partitions in topics:
        topic_bytes = topic_name.encode("utf-8")
        w.write_compact_array_length(len(topic_bytes))   # varint(len+1)
        w.write_bytes(topic_bytes)                        # raw bytes (no extra prefix)
        w.write_compact_array_length(len(partitions))
        for part_idx, record_batch in partitions:
            w.write_int32(part_idx)
            w.write_compact_array_length(len(record_batch))
            w.write_bytes(record_batch)
            w.write_tag_buffer()
        w.write_tag_buffer()
    return w.get_bytes()


def reader_for(buf: bytes) -> BufferReader:
    return BufferReader(buf)


# ---------------------------------------------------------------------------
# parse_request_header
# ---------------------------------------------------------------------------

class TestParseRequestHeader:
    def test_with_client_id(self):
        buf = make_header(api_key=18, api_version=4, correlation_id=42, client_id="my-client")
        result = parse_request_header(reader_for(buf))
        assert isinstance(result, RequestHeader)
        assert result.api_key == 18
        assert result.api_version == 4
        assert result.correlation_id == 42
        assert result.client_id == "my-client"

    def test_without_client_id_zero_length(self):
        buf = make_header(api_key=1, api_version=0, correlation_id=99, client_id=None)
        result = parse_request_header(reader_for(buf))
        assert result.client_id is None

    def test_negative_client_id_length_gives_no_client_id(self):
        # Craft a header with int16(-1) as client_id_length
        w = BufferWriter()
        w.write_int16(18)   # api_key
        w.write_int16(4)    # api_version
        w.write_int32(7)    # correlation_id
        w.write_int16(-1)   # negative length → no client_id read
        w.write_tag_buffer()
        result = parse_request_header(reader_for(w.get_bytes()))
        assert result.client_id is None

    def test_api_key_produce(self):
        buf = make_header(api_key=0)
        assert parse_request_header(reader_for(buf)).api_key == 0

    def test_api_key_fetch(self):
        buf = make_header(api_key=1)
        assert parse_request_header(reader_for(buf)).api_key == 1

    def test_api_key_api_versions(self):
        buf = make_header(api_key=18)
        assert parse_request_header(reader_for(buf)).api_key == 18

    def test_api_key_describe_topic_partitions(self):
        buf = make_header(api_key=75)
        assert parse_request_header(reader_for(buf)).api_key == 75

    def test_api_version_zero(self):
        buf = make_header(api_version=0)
        assert parse_request_header(reader_for(buf)).api_version == 0

    def test_api_version_max_supported(self):
        buf = make_header(api_version=4)
        assert parse_request_header(reader_for(buf)).api_version == 4

    def test_correlation_id_preserved(self):
        buf = make_header(correlation_id=2_147_483_647)
        assert parse_request_header(reader_for(buf)).correlation_id == 2_147_483_647

    def test_reader_exhausted_after_parse(self):
        buf = make_header()
        r = reader_for(buf)
        parse_request_header(r)
        assert r.is_eof()

    def test_reader_exhausted_with_client_id(self):
        buf = make_header(client_id="kafka-console-producer")
        r = reader_for(buf)
        parse_request_header(r)
        assert r.is_eof()

    def test_unicode_client_id(self):
        buf = make_header(client_id="clíente")
        result = parse_request_header(reader_for(buf))
        assert result.client_id == "clíente"


# ---------------------------------------------------------------------------
# parse_describe_topic_partitions_request
# ---------------------------------------------------------------------------

class TestParseDescribeTopicPartitionsRequest:
    def test_zero_topics(self):
        buf = make_describe_body([])
        result = parse_describe_topic_partitions_request(reader_for(buf))
        assert isinstance(result, DescribeTopicPartitionsRequest)
        assert result.topics == []

    def test_one_topic(self):
        buf = make_describe_body(["my-topic"])
        result = parse_describe_topic_partitions_request(reader_for(buf))
        assert len(result.topics) == 1
        assert isinstance(result.topics[0], DescribeTopicPartitionsRequestTopic)
        assert result.topics[0].name == "my-topic"

    def test_multiple_topics(self):
        names = ["topic-a", "topic-b", "topic-c"]
        buf = make_describe_body(names)
        result = parse_describe_topic_partitions_request(reader_for(buf))
        assert len(result.topics) == 3
        assert [t.name for t in result.topics] == names

    def test_topic_order_preserved(self):
        # Parser does NOT sort; handler sorts. Parser must preserve input order.
        names = ["zzz", "aaa", "mmm"]
        buf = make_describe_body(names)
        result = parse_describe_topic_partitions_request(reader_for(buf))
        assert [t.name for t in result.topics] == names

    def test_unicode_topic_name(self):
        buf = make_describe_body(["tópico-español"])
        result = parse_describe_topic_partitions_request(reader_for(buf))
        assert result.topics[0].name == "tópico-español"

    def test_partition_limit_and_cursor_consumed(self):
        # partition_limit and cursor are read but not returned; reader should be at EOF
        buf = make_describe_body(["t"], partition_limit=100, cursor=1)
        r = reader_for(buf)
        parse_describe_topic_partitions_request(r)
        assert r.is_eof()

    def test_reader_exhausted_zero_topics(self):
        buf = make_describe_body([])
        r = reader_for(buf)
        parse_describe_topic_partitions_request(r)
        assert r.is_eof()

    def test_reader_exhausted_multiple_topics(self):
        buf = make_describe_body(["a", "b", "c"])
        r = reader_for(buf)
        parse_describe_topic_partitions_request(r)
        assert r.is_eof()


# ---------------------------------------------------------------------------
# parse_fetch_request
# ---------------------------------------------------------------------------

class TestParseFetchRequest:
    def _partition(
        self,
        partition_index=0,
        current_leader_epoch=0,
        fetch_offset=0,
        last_fetched_epoch=-1,
        log_start_offset=0,
        partition_max_bytes=1048576,
    ):
        return (partition_index, current_leader_epoch, fetch_offset,
                last_fetched_epoch, log_start_offset, partition_max_bytes)

    def test_zero_topics(self):
        buf = make_fetch_body([], max_wait_ms=500, min_bytes=1, max_bytes=10000,
                               isolation_level=0, session_id=0, session_epoch=-1)
        result = parse_fetch_request(reader_for(buf))
        assert isinstance(result, FetchRequest)
        assert result.topics == []
        assert result.max_wait_ms == 500
        assert result.min_bytes == 1
        assert result.max_bytes == 10000
        assert result.isolation_level == 0
        assert result.session_id == 0
        assert result.session_epoch == -1

    def test_all_scalar_fields_preserved(self):
        buf = make_fetch_body([], max_wait_ms=1000, min_bytes=512,
                               max_bytes=65536, isolation_level=1,
                               session_id=42, session_epoch=7)
        result = parse_fetch_request(reader_for(buf))
        assert result.max_wait_ms == 1000
        assert result.min_bytes == 512
        assert result.max_bytes == 65536
        assert result.isolation_level == 1
        assert result.session_id == 42
        assert result.session_epoch == 7

    def test_one_topic_one_partition(self):
        topic_id = bytes(range(16))
        partition = self._partition(partition_index=0, fetch_offset=100)
        buf = make_fetch_body([(topic_id, [partition])])
        result = parse_fetch_request(reader_for(buf))
        assert len(result.topics) == 1
        topic = result.topics[0]
        assert isinstance(topic, FetchRequestTopic)
        assert topic.topic_id == topic_id
        assert len(topic.partitions) == 1
        p = topic.partitions[0]
        assert isinstance(p, FetchRequestPartition)
        assert p.partition_index == 0
        assert p.fetch_offset == 100

    def test_partition_all_fields_preserved(self):
        topic_id = bytes(range(16))
        partition = self._partition(
            partition_index=3,
            current_leader_epoch=5,
            fetch_offset=9999,
            last_fetched_epoch=4,
            log_start_offset=0,
            partition_max_bytes=524288,
        )
        buf = make_fetch_body([(topic_id, [partition])])
        result = parse_fetch_request(reader_for(buf))
        p = result.topics[0].partitions[0]
        assert p.partition_index == 3
        assert p.current_leader_epoch == 5
        assert p.fetch_offset == 9999
        assert p.last_fetched_epoch == 4
        assert p.log_start_offset == 0
        assert p.partition_max_bytes == 524288

    def test_fetch_offset_is_int64_large_value(self):
        topic_id = b'\x00' * 16
        partition = self._partition(fetch_offset=9_000_000_000)
        buf = make_fetch_body([(topic_id, [partition])])
        result = parse_fetch_request(reader_for(buf))
        assert result.topics[0].partitions[0].fetch_offset == 9_000_000_000

    def test_one_topic_multiple_partitions(self):
        topic_id = bytes(range(16))
        partitions = [
            self._partition(partition_index=0, fetch_offset=0),
            self._partition(partition_index=1, fetch_offset=50),
            self._partition(partition_index=2, fetch_offset=200),
        ]
        buf = make_fetch_body([(topic_id, partitions)])
        result = parse_fetch_request(reader_for(buf))
        topic = result.topics[0]
        assert len(topic.partitions) == 3
        assert [p.partition_index for p in topic.partitions] == [0, 1, 2]
        assert [p.fetch_offset for p in topic.partitions] == [0, 50, 200]

    def test_multiple_topics(self):
        uuid1 = bytes(range(16))
        uuid2 = bytes(range(16, 32))
        buf = make_fetch_body([
            (uuid1, [self._partition(partition_index=0)]),
            (uuid2, [self._partition(partition_index=0), self._partition(partition_index=1)]),
        ])
        result = parse_fetch_request(reader_for(buf))
        assert len(result.topics) == 2
        assert result.topics[0].topic_id == uuid1
        assert result.topics[1].topic_id == uuid2
        assert len(result.topics[0].partitions) == 1
        assert len(result.topics[1].partitions) == 2

    def test_topic_id_is_raw_16_bytes(self):
        topic_id = bytes([0xDE, 0xAD, 0xBE, 0xEF] + [0x00] * 12)
        buf = make_fetch_body([(topic_id, [self._partition()])])
        result = parse_fetch_request(reader_for(buf))
        assert result.topics[0].topic_id == topic_id

    def test_reader_exhausted_zero_topics(self):
        buf = make_fetch_body([])
        r = reader_for(buf)
        parse_fetch_request(r)
        assert r.is_eof()

    def test_reader_exhausted_with_topics(self):
        topic_id = b'\xAB' * 16
        buf = make_fetch_body([(topic_id, [self._partition()])])
        r = reader_for(buf)
        parse_fetch_request(r)
        assert r.is_eof()


# ---------------------------------------------------------------------------
# parse_produce_request
# ---------------------------------------------------------------------------

class TestParseProduceRequest:
    def test_no_transactional_id(self):
        buf = make_produce_body([], transactional_id=None, acks=1, timeout_ms=5000)
        result = parse_produce_request(reader_for(buf))
        assert isinstance(result, ProduceRequest)
        assert result.transactional_id is None

    def test_with_transactional_id(self):
        buf = make_produce_body([], transactional_id="my-txn", acks=1, timeout_ms=5000)
        result = parse_produce_request(reader_for(buf))
        assert result.transactional_id == "my-txn"

    def test_negative_transactional_id_len_gives_none(self):
        # Craft a buffer with int8(-1) as transactional_id_len
        w = BufferWriter()
        w.write_int8(-1)    # negative → no transactional_id read
        w.write_int16(1)    # acks
        w.write_int32(5000) # timeout_ms
        w.write_compact_array_length(0)  # zero topics
        result = parse_produce_request(reader_for(w.get_bytes()))
        assert result.transactional_id is None

    def test_acks_and_timeout_preserved(self):
        buf = make_produce_body([], acks=-1, timeout_ms=30000)
        result = parse_produce_request(reader_for(buf))
        assert result.acks == -1
        assert result.timeout_ms == 30000

    def test_zero_topics(self):
        buf = make_produce_body([])
        result = parse_produce_request(reader_for(buf))
        assert result.topics == []

    def test_one_topic_one_partition(self):
        record_batch = b'\x00\x01\x02\x03'
        buf = make_produce_body([("test-topic", [(0, record_batch)])])
        result = parse_produce_request(reader_for(buf))
        assert len(result.topics) == 1
        topic = result.topics[0]
        assert isinstance(topic, ProduceRequestTopic)
        assert topic.name == "test-topic"
        assert len(topic.partitions) == 1
        p = topic.partitions[0]
        assert isinstance(p, ProduceRequestPartition)
        assert p.partition_index == 0
        assert p.record_batch == record_batch

    def test_empty_record_batch(self):
        buf = make_produce_body([("t", [(0, b'')])])
        result = parse_produce_request(reader_for(buf))
        assert result.topics[0].partitions[0].record_batch == b''

    def test_large_record_batch_preserved(self):
        record_batch = bytes(range(256)) * 2   # 512 bytes
        buf = make_produce_body([("t", [(0, record_batch)])])
        result = parse_produce_request(reader_for(buf))
        assert result.topics[0].partitions[0].record_batch == record_batch

    def test_multiple_partitions(self):
        batch0 = b'\xAA\xBB'
        batch1 = b'\xCC\xDD\xEE'
        buf = make_produce_body([("t", [(0, batch0), (1, batch1)])])
        result = parse_produce_request(reader_for(buf))
        topic = result.topics[0]
        assert len(topic.partitions) == 2
        assert topic.partitions[0].partition_index == 0
        assert topic.partitions[0].record_batch == batch0
        assert topic.partitions[1].partition_index == 1
        assert topic.partitions[1].record_batch == batch1

    def test_multiple_topics(self):
        buf = make_produce_body([
            ("topic-a", [(0, b'\x01')]),
            ("topic-b", [(0, b'\x02'), (1, b'\x03')]),
        ])
        result = parse_produce_request(reader_for(buf))
        assert len(result.topics) == 2
        assert result.topics[0].name == "topic-a"
        assert result.topics[1].name == "topic-b"
        assert len(result.topics[0].partitions) == 1
        assert len(result.topics[1].partitions) == 2

    def test_partition_index_preserved(self):
        buf = make_produce_body([("t", [(7, b'\xFF')])])
        result = parse_produce_request(reader_for(buf))
        assert result.topics[0].partitions[0].partition_index == 7

    def test_topic_order_preserved(self):
        topics = [("zzz", [(0, b'\x01')]), ("aaa", [(0, b'\x02')])]
        buf = make_produce_body(topics)
        result = parse_produce_request(reader_for(buf))
        assert result.topics[0].name == "zzz"
        assert result.topics[1].name == "aaa"

    def test_reader_exhausted_zero_topics(self):
        buf = make_produce_body([])
        r = reader_for(buf)
        parse_produce_request(r)
        assert r.is_eof()

    def test_reader_exhausted_with_content(self):
        buf = make_produce_body([("t", [(0, b'\x42\x43')])], transactional_id="txn")
        r = reader_for(buf)
        parse_produce_request(r)
        assert r.is_eof()


# ---------------------------------------------------------------------------
# Integration: full parse chain (header → body)
# ---------------------------------------------------------------------------

class TestFullParseChain:
    def test_api_versions_header_only(self):
        buf = make_header(api_key=18, api_version=4, correlation_id=5, client_id="cli")
        r = reader_for(buf)
        header = parse_request_header(r)
        assert header.api_key == 18
        assert header.api_version == 4
        assert header.correlation_id == 5
        assert header.client_id == "cli"

    def test_describe_topic_partitions_full_chain(self):
        header_buf = make_header(api_key=75, api_version=0, correlation_id=11)
        body_buf = make_describe_body(["events", "orders"])
        r = BufferReader(header_buf + body_buf)
        header = parse_request_header(r)
        request = parse_describe_topic_partitions_request(r)
        assert header.api_key == 75
        assert header.correlation_id == 11
        assert [t.name for t in request.topics] == ["events", "orders"]
        assert r.is_eof()

    def test_fetch_full_chain(self):
        topic_id = bytes(range(16))
        header_buf = make_header(api_key=1, api_version=16, correlation_id=99)
        body_buf = make_fetch_body(
            [(topic_id, [(0, 0, 500, -1, 0, 1048576)])],
            max_wait_ms=250,
            session_id=7,
        )
        r = BufferReader(header_buf + body_buf)
        header = parse_request_header(r)
        request = parse_fetch_request(r)
        assert header.api_key == 1
        assert request.max_wait_ms == 250
        assert request.session_id == 7
        assert request.topics[0].topic_id == topic_id
        assert request.topics[0].partitions[0].fetch_offset == 500
        assert r.is_eof()

    def test_produce_full_chain(self):
        record = b'\x00\x00\x00\x01\x02\x03\x04\x05'
        header_buf = make_header(api_key=0, api_version=11, correlation_id=3, client_id="producer-1")
        body_buf = make_produce_body(
            [("payments", [(0, record)])],
            acks=-1,
            timeout_ms=10000,
        )
        r = BufferReader(header_buf + body_buf)
        header = parse_request_header(r)
        request = parse_produce_request(r)
        assert header.api_key == 0
        assert header.client_id == "producer-1"
        assert request.acks == -1
        assert request.timeout_ms == 10000
        assert request.topics[0].name == "payments"
        assert request.topics[0].partitions[0].record_batch == record
        assert r.is_eof()

    def test_fetch_chain_multiple_topics_and_partitions(self):
        uuid1, uuid2 = bytes(range(16)), bytes(range(16, 32))
        header_buf = make_header(api_key=1, api_version=16, correlation_id=1)
        body_buf = make_fetch_body([
            (uuid1, [(0, 0, 0, -1, 0, 1048576), (1, 0, 100, -1, 0, 1048576)]),
            (uuid2, [(0, 0, 200, -1, 0, 1048576)]),
        ])
        r = BufferReader(header_buf + body_buf)
        parse_request_header(r)
        request = parse_fetch_request(r)
        assert len(request.topics) == 2
        assert len(request.topics[0].partitions) == 2
        assert request.topics[0].partitions[1].fetch_offset == 100
        assert request.topics[1].partitions[0].fetch_offset == 200
        assert r.is_eof()

    def test_produce_chain_no_client_id_with_transactional_id(self):
        header_buf = make_header(api_key=0, api_version=11, correlation_id=77, client_id=None)
        body_buf = make_produce_body(
            [("t", [(0, b'\xDE\xAD')])],
            transactional_id="my-transaction",
        )
        r = BufferReader(header_buf + body_buf)
        header = parse_request_header(r)
        request = parse_produce_request(r)
        assert header.client_id is None
        assert request.transactional_id == "my-transaction"
        assert r.is_eof()
