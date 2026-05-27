import uuid
from pathlib import Path
from unittest.mock import patch, AsyncMock
import pytest

from src.storage import Storage


LOG_FILE_NAME = "00000000000000000000.log"


def make_topic_record(name: str, topic_uuid: uuid.UUID) -> bytes:
    """Build a type-2 record in the format _process_record expects."""
    name_bytes = name.encode("utf-8")
    return (
        b'\x02'
        + b'\x00'
        + (len(name_bytes) + 1).to_bytes(1, 'big')
        + name_bytes
        + topic_uuid.bytes
    )


def make_partition_record(
    partition_index: int,
    topic_uuid: uuid.UUID,
    num_replicas: int = 1,
    num_isr: int = 1,
    leader_id: int = 1,
    leader_epoch: int = 0,
) -> bytes:
    """Build a type-3 record in the format _process_record expects."""
    record = bytearray(42)
    record[0] = 3
    record[1] = 0
    record[2:6] = partition_index.to_bytes(4, 'big')
    record[6:22] = topic_uuid.bytes
    record[22] = num_replicas
    record[27] = num_isr
    record[34:38] = leader_id.to_bytes(4, 'big')
    record[38:42] = leader_epoch.to_bytes(4, 'big')
    return bytes(record)


def wrap_record(record_bytes: bytes) -> bytes:
    """Wrap record_bytes into the batch-entry wire format (flag=0 path)."""
    n = len(record_bytes)
    encoded_size = (n + 6) * 2
    return encoded_size.to_bytes(1, 'big') + b'\x00' + b'\x00' * 5 + record_bytes


def make_record_batch(records: list[bytes]) -> bytes:
    """
    Build a full KRaft record batch.
    Layout:
      [0:8]   8 bytes padding
      [8:12]  body_size = 49 + len(record_array)
      [12:57] 45 bytes padding
      [57:61] num_records
      [61:]   record_array
    """
    record_array = b''.join(wrap_record(r) for r in records)
    body_size = 49 + len(record_array)
    return (
        b'\x00' * 8
        + body_size.to_bytes(4, 'big')
        + b'\x00' * 45
        + len(records).to_bytes(4, 'big')
        + record_array
    )


# ---------------------------------------------------------------------------
# _process_record (sync unit tests)
# ---------------------------------------------------------------------------

class TestProcessRecord:
    def test_topic_type_field(self):
        u = uuid.uuid4()
        assert Storage._process_record(make_topic_record("t", u))["type"] == "topic"

    def test_topic_name_extracted(self):
        u = uuid.uuid4()
        result = Storage._process_record(make_topic_record("payments", u))
        assert result["topic_name"] == "payments"

    def test_topic_uuid_extracted(self):
        u = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        result = Storage._process_record(make_topic_record("t", u))
        assert result["topic_uuid"] == u

    def test_topic_long_name(self):
        u = uuid.uuid4()
        name = "this-is-a-long-topic-name"
        result = Storage._process_record(make_topic_record(name, u))
        assert result["topic_name"] == name

    def test_partition_type_field(self):
        u = uuid.uuid4()
        assert Storage._process_record(make_partition_record(0, u))["type"] == "partition"

    def test_partition_all_fields(self):
        u = uuid.UUID("11111111-2222-3333-4444-555555555555")
        record = make_partition_record(
            partition_index=2,
            topic_uuid=u,
            num_replicas=3,
            num_isr=2,
            leader_id=1001,
            leader_epoch=5,
        )
        result = Storage._process_record(record)
        assert result["partition_index"] == 2
        assert result["topic_uuid"] == u
        assert result["num_replicas"] == 3
        assert result["num_isr"] == 2
        assert result["leader_id"] == 1001
        assert result["leader_epoch"] == 5

    def test_unknown_type_zero(self):
        record = b'\x00' + b'\x00' * 41
        assert Storage._process_record(record)["type"] == "unknown"

    def test_unknown_type_ff(self):
        record = b'\xFF' + b'\x00' * 41
        assert Storage._process_record(record)["type"] == "unknown"


# ---------------------------------------------------------------------------
# _process_record_batch (sync unit tests)
# ---------------------------------------------------------------------------

class TestProcessRecordBatch:
    def test_zero_records_returns_empty_list(self):
        batch = make_record_batch([])
        assert Storage._process_record_batch(batch) == []

    def test_one_record_flag_zero_extracted_correctly(self):
        u = uuid.uuid4()
        record = make_topic_record("events", u)
        batch = make_record_batch([record])
        result = Storage._process_record_batch(batch)
        assert len(result) == 1
        assert result[0] == record

    def test_one_record_flag_nonzero_uses_offset_nine(self):
        # flag byte != 0 → record = record_array[9:(record_size+2)]
        inner = b'\x02' + b'\x00' * 28   # 29 bytes of record data
        n = len(inner)
        record_size = n + 7              # record_size - 7 = n
        entry = (record_size * 2).to_bytes(1, 'big') + b'\x01' + b'\x00' * 7 + inner
        body_size = 49 + len(entry)
        batch = (
            b'\x00' * 8
            + body_size.to_bytes(4, 'big')
            + b'\x00' * 45
            + (1).to_bytes(4, 'big')
            + entry
        )
        result = Storage._process_record_batch(batch)
        assert len(result) == 1
        assert result[0] == inner

    def test_multiple_records_in_order(self):
        u1, u2, u3 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        r1 = make_topic_record("alpha", u1)
        r2 = make_topic_record("beta", u2)
        r3 = make_topic_record("gamma", u3)
        batch = make_record_batch([r1, r2, r3])
        result = Storage._process_record_batch(batch)
        assert len(result) == 3
        assert result[0] == r1
        assert result[1] == r2
        assert result[2] == r3

    def test_record_size_byte_is_data_len_plus_six_times_two(self):
        u = uuid.uuid4()
        record = make_topic_record("x", u)
        batch = make_record_batch([record])
        record_array_start = 61
        expected = (len(record) + 6) * 2
        assert batch[record_array_start] == expected


# ---------------------------------------------------------------------------
# _extract_record_batch (sync unit tests)
# ---------------------------------------------------------------------------

class TestExtractRecordBatch:
    def test_extracts_twelve_plus_body_size_bytes(self):
        batch = make_record_batch([])
        result = Storage._extract_record_batch(batch, 0)
        assert result == batch

    def test_offset_zero(self):
        u = uuid.uuid4()
        batch = make_record_batch([make_topic_record("t", u)])
        assert Storage._extract_record_batch(batch, 0) == batch

    def test_non_zero_offset(self):
        prefix = b'\xDE\xAD\xBE\xEF'
        batch = make_record_batch([])
        combined = prefix + batch
        assert Storage._extract_record_batch(combined, 4) == batch

    def test_two_sequential_batches(self):
        u1, u2 = uuid.uuid4(), uuid.uuid4()
        batch1 = make_record_batch([make_topic_record("first", u1)])
        batch2 = make_record_batch([make_topic_record("second", u2)])
        combined = batch1 + batch2
        assert Storage._extract_record_batch(combined, 0) == batch1
        assert Storage._extract_record_batch(combined, len(batch1)) == batch2


# ---------------------------------------------------------------------------
# _parse_cluster_metadata (sync unit tests)
# ---------------------------------------------------------------------------

class TestParseClusterMetadata:
    def _s(self) -> Storage:
        return Storage("/tmp", LOG_FILE_NAME)

    def test_empty_bytes_returns_empty_dict(self):
        assert self._s()._parse_cluster_metadata(b'') == {}

    def test_single_topic_no_partitions(self):
        u = uuid.uuid4()
        batch = make_record_batch([make_topic_record("orders", u)])
        result = self._s()._parse_cluster_metadata(batch)
        assert "orders" in result
        assert result["orders"]["topic_name"] == "orders"
        assert result["orders"]["topic_uuid"] == u
        assert result["orders"]["partitions"] == {}

    def test_topic_and_partition_in_same_batch(self):
        u = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000000")
        batch = make_record_batch([
            make_topic_record("payments", u),
            make_partition_record(0, u, leader_id=1, leader_epoch=0),
        ])
        result = self._s()._parse_cluster_metadata(batch)
        assert "payments" in result
        p = result["payments"]["partitions"][0]
        assert p["partition_index"] == 0
        assert p["leader_id"] == 1
        assert p["leader_epoch"] == 0

    def test_topic_and_partition_in_separate_batches(self):
        u = uuid.UUID("bbbbbbbb-0000-0000-0000-000000000000")
        batch1 = make_record_batch([make_topic_record("events", u)])
        batch2 = make_record_batch([make_partition_record(0, u)])
        result = self._s()._parse_cluster_metadata(batch1 + batch2)
        assert "events" in result
        assert 0 in result["events"]["partitions"]

    def test_multiple_topics_with_partitions(self):
        u1 = uuid.UUID("11111111-0000-0000-0000-000000000000")
        u2 = uuid.UUID("22222222-0000-0000-0000-000000000000")
        batch = make_record_batch([
            make_topic_record("alpha", u1),
            make_partition_record(0, u1),
            make_topic_record("beta", u2),
            make_partition_record(0, u2),
            make_partition_record(1, u2),
        ])
        result = self._s()._parse_cluster_metadata(batch)
        assert set(result.keys()) == {"alpha", "beta"}
        assert 0 in result["alpha"]["partitions"]
        assert 0 in result["beta"]["partitions"]
        assert 1 in result["beta"]["partitions"]

    def test_partition_without_matching_topic_raises(self):
        orphan = uuid.uuid4()
        batch = make_record_batch([make_partition_record(0, orphan)])
        with pytest.raises(Exception, match="no associated topic"):
            self._s()._parse_cluster_metadata(batch)


# ---------------------------------------------------------------------------
# write_partition_log (async integration tests)
# ---------------------------------------------------------------------------

class TestWritePartitionLog:
    @pytest.mark.asyncio
    async def test_write_populates_in_memory_buffer(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("topic", 0, b'\x01\x02\x03')
        assert ("topic", 0) in s.buffers
        assert bytes(s.buffers[("topic", 0)]) == b'\x01\x02\x03'

    @pytest.mark.asyncio
    async def test_write_does_not_create_file_on_disk(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("topic", 0, b'\xFF')
        assert not (tmp_path / "topic-0" / LOG_FILE_NAME).exists()

    @pytest.mark.asyncio
    async def test_multiple_writes_same_partition_accumulate_in_order(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("t", 0, b'\x01\x02')
        await s.write_partition_log("t", 0, b'\x03\x04')
        await s.write_partition_log("t", 0, b'\x05')
        assert bytes(s.buffers[("t", 0)]) == b'\x01\x02\x03\x04\x05'

    @pytest.mark.asyncio
    async def test_different_partitions_use_separate_buffer_keys(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("t", 0, b'\xAA')
        await s.write_partition_log("t", 1, b'\xBB')
        await s.write_partition_log("other", 0, b'\xCC')
        assert bytes(s.buffers[("t", 0)]) == b'\xAA'
        assert bytes(s.buffers[("t", 1)]) == b'\xBB'
        assert bytes(s.buffers[("other", 0)]) == b'\xCC'


# ---------------------------------------------------------------------------
# flush_buffers (async integration tests)
# ---------------------------------------------------------------------------

class TestFlushBuffers:
    @pytest.mark.asyncio
    async def test_empty_buffers_is_noop(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.flush_buffers()
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.asyncio
    async def test_single_partition_written_to_disk(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("topic", 0, b'\x01\x02\x03')
        await s.flush_buffers()
        log_path = tmp_path / "topic-0" / LOG_FILE_NAME
        assert log_path.exists()
        assert log_path.read_bytes() == b'\x01\x02\x03'

    @pytest.mark.asyncio
    async def test_multiple_partitions_written_to_separate_files(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("t", 0, b'\xAA')
        await s.write_partition_log("t", 1, b'\xBB')
        await s.flush_buffers()
        assert (tmp_path / "t-0" / LOG_FILE_NAME).read_bytes() == b'\xAA'
        assert (tmp_path / "t-1" / LOG_FILE_NAME).read_bytes() == b'\xBB'

    @pytest.mark.asyncio
    async def test_flush_clears_in_memory_buffers(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("t", 0, b'\x01')
        await s.flush_buffers()
        assert s.buffers == {}

    @pytest.mark.asyncio
    async def test_second_flush_appends_to_existing_file(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("t", 0, b'\x01\x02')
        await s.flush_buffers()
        await s.write_partition_log("t", 0, b'\x03\x04')
        await s.flush_buffers()
        assert (tmp_path / "t-0" / LOG_FILE_NAME).read_bytes() == b'\x01\x02\x03\x04'

    @pytest.mark.asyncio
    async def test_flush_failure_prepends_data_back_to_buffer(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        data = b'\x01\x02\x03'
        await s.write_partition_log("t", 0, data)

        with patch.object(Storage, '_append_file', new_callable=AsyncMock, side_effect=OSError("disk full")):
            await s.flush_buffers()

        assert ("t", 0) in s.buffers
        assert bytes(s.buffers[("t", 0)]) == data

    @pytest.mark.asyncio
    async def test_flush_failure_prepend_preserves_ordering_with_new_writes(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        first = b'\x01\x02'
        second = b'\x03\x04'
        await s.write_partition_log("t", 0, first)

        with patch.object(Storage, '_append_file', new_callable=AsyncMock, side_effect=OSError("disk full")):
            await s.flush_buffers()

        await s.write_partition_log("t", 0, second)
        # Failed data prepended → buffer is [first][second]
        assert bytes(s.buffers[("t", 0)]) == first + second


# ---------------------------------------------------------------------------
# read_partition_log (async integration tests)
# ---------------------------------------------------------------------------

class TestReadPartitionLog:
    @pytest.mark.asyncio
    async def test_no_file_no_buffer_returns_empty(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        result = await s.read_partition_log("t", 0, fetch_offset=0, max_bytes=1024)
        assert result == b''

    @pytest.mark.asyncio
    async def test_flushed_data_readable_from_disk(self, tmp_path):
        data = b'\x01\x02\x03\x04\x05'
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("t", 0, data)
        await s.flush_buffers()
        result = await s.read_partition_log("t", 0, fetch_offset=0, max_bytes=1024)
        assert result == data

    @pytest.mark.asyncio
    async def test_buffered_data_readable_before_flush(self, tmp_path):
        data = b'\xAA\xBB\xCC'
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("t", 0, data)
        result = await s.read_partition_log("t", 0, fetch_offset=0, max_bytes=1024)
        assert result == data

    @pytest.mark.asyncio
    async def test_disk_and_buffer_data_combined(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("t", 0, b'\x01\x02\x03')
        await s.flush_buffers()
        await s.write_partition_log("t", 0, b'\x04\x05')
        result = await s.read_partition_log("t", 0, fetch_offset=0, max_bytes=1024)
        assert result == b'\x01\x02\x03\x04\x05'

    @pytest.mark.asyncio
    async def test_fetch_offset_seeks_within_disk_data(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("t", 0, b'\x01\x02\x03\x04\x05')
        await s.flush_buffers()
        result = await s.read_partition_log("t", 0, fetch_offset=2, max_bytes=1024)
        assert result == b'\x03\x04\x05'

    @pytest.mark.asyncio
    async def test_fetch_offset_equals_file_size_reads_from_buffer(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("t", 0, b'\x01\x02\x03')
        await s.flush_buffers()
        await s.write_partition_log("t", 0, b'\x04\x05')
        # fetch_offset == file_size(3) → reads buffer from offset 0
        result = await s.read_partition_log("t", 0, fetch_offset=3, max_bytes=1024)
        assert result == b'\x04\x05'

    @pytest.mark.asyncio
    async def test_fetch_offset_beyond_file_size_adjusts_buffer_offset(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("t", 0, b'\x01\x02\x03')
        await s.flush_buffers()
        await s.write_partition_log("t", 0, b'\x04\x05\x06')
        # fetch_offset 4 = file_size(3) + 1 → buffer_offset = 1
        result = await s.read_partition_log("t", 0, fetch_offset=4, max_bytes=1024)
        assert result == b'\x05\x06'

    @pytest.mark.asyncio
    async def test_max_bytes_limits_disk_read(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("t", 0, b'\x01\x02\x03\x04\x05')
        await s.flush_buffers()
        result = await s.read_partition_log("t", 0, fetch_offset=0, max_bytes=3)
        assert result == b'\x01\x02\x03'

    @pytest.mark.asyncio
    async def test_max_bytes_satisfied_by_disk_no_buffer_appended(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        await s.write_partition_log("t", 0, b'\x01\x02\x03\x04\x05')
        await s.flush_buffers()
        await s.write_partition_log("t", 0, b'\x06\x07\x08')
        # max_bytes == file size → disk satisfies it entirely
        result = await s.read_partition_log("t", 0, fetch_offset=0, max_bytes=5)
        assert result == b'\x01\x02\x03\x04\x05'
        assert b'\x06' not in result


# ---------------------------------------------------------------------------
# load_metadata (async integration tests)
# ---------------------------------------------------------------------------

class TestLoadMetadata:
    def _write_metadata(self, tmp_path: Path, data: bytes) -> None:
        meta_dir = tmp_path / "__cluster_metadata-0"
        meta_dir.mkdir(parents=True)
        (meta_dir / LOG_FILE_NAME).write_bytes(data)

    @pytest.mark.asyncio
    async def test_no_metadata_file_returns_empty_dict(self, tmp_path):
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        assert await s.load_metadata() == {}

    @pytest.mark.asyncio
    async def test_valid_metadata_file_parsed(self, tmp_path):
        u = uuid.UUID("cccccccc-0000-0000-0000-000000000000")
        batch = make_record_batch([
            make_topic_record("invoices", u),
            make_partition_record(0, u, leader_id=1, leader_epoch=0),
        ])
        self._write_metadata(tmp_path, batch)
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        result = await s.load_metadata()
        assert "invoices" in result
        assert result["invoices"]["topic_uuid"] == u
        assert 0 in result["invoices"]["partitions"]
        assert result["invoices"]["partitions"][0]["leader_id"] == 1

    @pytest.mark.asyncio
    async def test_parse_error_returns_empty_dict_without_raising(self, tmp_path):
        # A partition record with no preceding topic triggers an exception in the parser.
        # load_metadata must catch it and return {}.
        orphan = uuid.uuid4()
        batch = make_record_batch([make_partition_record(0, orphan)])
        self._write_metadata(tmp_path, batch)
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        result = await s.load_metadata()
        assert result == {}

    @pytest.mark.asyncio
    async def test_metadata_path_derived_from_log_dir_and_file_name(self, tmp_path):
        u = uuid.uuid4()
        batch = make_record_batch([make_topic_record("path-check", u)])
        self._write_metadata(tmp_path, batch)
        s = Storage(str(tmp_path), LOG_FILE_NAME)
        result = await s.load_metadata()
        assert "path-check" in result
