import pytest
from src.protocol.writer import BufferWriter
from src.protocol.reader import BufferReader
from src.utils import encode_unsigned_varint


def varint(n: int) -> bytes:
    """Alias for encode_unsigned_varint used as a test fixture oracle."""
    return encode_unsigned_varint(n)


def make_writer(*ops) -> BufferWriter:
    """Create a BufferWriter and apply a sequence of (method_name, *args) operations."""
    w = BufferWriter()
    for method, *args in ops:
        getattr(w, method)(*args)
    return w


# ---------------------------------------------------------------------------
# Constructor / get_bytes
# ---------------------------------------------------------------------------

class TestConstructorAndGetBytes:
    def test_starts_empty(self):
        w = BufferWriter()
        assert w.get_bytes() == b''

    def test_get_bytes_returns_bytes_type(self):
        w = BufferWriter()
        assert isinstance(w.get_bytes(), bytes)

    def test_get_bytes_is_non_destructive(self):
        w = BufferWriter()
        w.write_int8(42)
        first = w.get_bytes()
        second = w.get_bytes()
        assert first == second

    def test_get_bytes_returns_accumulated_content(self):
        w = BufferWriter()
        w.write_bytes(b'\x01\x02')
        w.write_bytes(b'\x03')
        assert w.get_bytes() == b'\x01\x02\x03'


# ---------------------------------------------------------------------------
# write_bytes
# ---------------------------------------------------------------------------

class TestWriteBytes:
    def test_writes_raw_bytes_exactly(self):
        w = BufferWriter()
        w.write_bytes(b'\xAA\xBB\xCC')
        assert w.get_bytes() == b'\xAA\xBB\xCC'

    def test_empty_bytes_is_noop(self):
        w = BufferWriter()
        w.write_bytes(b'')
        assert w.get_bytes() == b''

    def test_sequential_calls_accumulate_in_order(self):
        w = BufferWriter()
        w.write_bytes(b'\x01\x02')
        w.write_bytes(b'\x03\x04')
        w.write_bytes(b'\x05')
        assert w.get_bytes() == b'\x01\x02\x03\x04\x05'

    def test_single_byte(self):
        w = BufferWriter()
        w.write_bytes(b'\xFF')
        assert w.get_bytes() == b'\xFF'

    def test_large_payload(self):
        data = bytes(range(256)) * 4
        w = BufferWriter()
        w.write_bytes(data)
        assert w.get_bytes() == data


# ---------------------------------------------------------------------------
# write_int8
# ---------------------------------------------------------------------------

class TestWriteInt8:
    def test_zero(self):
        w = BufferWriter()
        w.write_int8(0)
        assert w.get_bytes() == b'\x00'

    def test_one(self):
        w = BufferWriter()
        w.write_int8(1)
        assert w.get_bytes() == b'\x01'

    def test_positive_signed_max(self):
        w = BufferWriter()
        w.write_int8(127)
        assert w.get_bytes() == b'\x7F'

    def test_unsigned_max(self):
        # 255 takes the unsigned branch (value >= 0) → same bytes as -1
        w = BufferWriter()
        w.write_int8(255)
        assert w.get_bytes() == b'\xFF'

    def test_128_unsigned_path(self):
        # 128 takes unsigned branch; same byte pattern as -128
        w = BufferWriter()
        w.write_int8(128)
        assert w.get_bytes() == b'\x80'

    def test_minus_one(self):
        w = BufferWriter()
        w.write_int8(-1)
        assert w.get_bytes() == b'\xFF'

    def test_signed_min(self):
        w = BufferWriter()
        w.write_int8(-128)
        assert w.get_bytes() == b'\x80'

    def test_sequential_writes_accumulate(self):
        w = BufferWriter()
        w.write_int8(1)
        w.write_int8(-1)
        assert w.get_bytes() == b'\x01\xFF'

    def test_overflow_positive(self):
        w = BufferWriter()
        with pytest.raises(OverflowError):
            w.write_int8(256)

    def test_overflow_negative(self):
        w = BufferWriter()
        with pytest.raises(OverflowError):
            w.write_int8(-129)


# ---------------------------------------------------------------------------
# write_int16
# ---------------------------------------------------------------------------

class TestWriteInt16:
    def test_zero(self):
        w = BufferWriter()
        w.write_int16(0)
        assert w.get_bytes() == b'\x00\x00'

    def test_positive_signed_max(self):
        w = BufferWriter()
        w.write_int16(32767)
        assert w.get_bytes() == b'\x7F\xFF'

    def test_unsigned_max(self):
        w = BufferWriter()
        w.write_int16(65535)
        assert w.get_bytes() == b'\xFF\xFF'

    def test_minus_one(self):
        w = BufferWriter()
        w.write_int16(-1)
        assert w.get_bytes() == b'\xFF\xFF'

    def test_signed_min(self):
        w = BufferWriter()
        w.write_int16(-32768)
        assert w.get_bytes() == b'\x80\x00'

    def test_big_endian_byte_order(self):
        # 256 = 0x0100 in big-endian
        w = BufferWriter()
        w.write_int16(256)
        assert w.get_bytes() == b'\x01\x00'

    def test_writes_two_bytes(self):
        w = BufferWriter()
        w.write_int16(1)
        assert len(w.get_bytes()) == 2

    def test_overflow_positive(self):
        w = BufferWriter()
        with pytest.raises(OverflowError):
            w.write_int16(65536)

    def test_overflow_negative(self):
        w = BufferWriter()
        with pytest.raises(OverflowError):
            w.write_int16(-32769)


# ---------------------------------------------------------------------------
# write_int32
# ---------------------------------------------------------------------------

class TestWriteInt32:
    def test_zero(self):
        w = BufferWriter()
        w.write_int32(0)
        assert w.get_bytes() == b'\x00\x00\x00\x00'

    def test_positive_signed_max(self):
        w = BufferWriter()
        w.write_int32(2_147_483_647)
        assert w.get_bytes() == b'\x7F\xFF\xFF\xFF'

    def test_unsigned_max(self):
        w = BufferWriter()
        w.write_int32(4_294_967_295)
        assert w.get_bytes() == b'\xFF\xFF\xFF\xFF'

    def test_minus_one(self):
        w = BufferWriter()
        w.write_int32(-1)
        assert w.get_bytes() == b'\xFF\xFF\xFF\xFF'

    def test_signed_min(self):
        w = BufferWriter()
        w.write_int32(-2_147_483_648)
        assert w.get_bytes() == b'\x80\x00\x00\x00'

    def test_big_endian_byte_order(self):
        # 65536 = 0x00010000
        w = BufferWriter()
        w.write_int32(65536)
        assert w.get_bytes() == b'\x00\x01\x00\x00'

    def test_writes_four_bytes(self):
        w = BufferWriter()
        w.write_int32(0)
        assert len(w.get_bytes()) == 4

    def test_overflow_positive(self):
        w = BufferWriter()
        with pytest.raises(OverflowError):
            w.write_int32(4_294_967_296)

    def test_overflow_negative(self):
        w = BufferWriter()
        with pytest.raises(OverflowError):
            w.write_int32(-2_147_483_649)


# ---------------------------------------------------------------------------
# write_int64
# ---------------------------------------------------------------------------

class TestWriteInt64:
    def test_zero(self):
        w = BufferWriter()
        w.write_int64(0)
        assert w.get_bytes() == b'\x00' * 8

    def test_positive_signed_max(self):
        w = BufferWriter()
        w.write_int64(9_223_372_036_854_775_807)
        assert w.get_bytes() == b'\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF'

    def test_unsigned_max(self):
        w = BufferWriter()
        w.write_int64(18_446_744_073_709_551_615)
        assert w.get_bytes() == b'\xFF' * 8

    def test_minus_one(self):
        w = BufferWriter()
        w.write_int64(-1)
        assert w.get_bytes() == b'\xFF' * 8

    def test_signed_min(self):
        w = BufferWriter()
        w.write_int64(-9_223_372_036_854_775_808)
        assert w.get_bytes() == b'\x80' + b'\x00' * 7

    def test_known_value(self):
        # 1_000_000_000 = 0x3B9ACA00
        w = BufferWriter()
        w.write_int64(1_000_000_000)
        assert w.get_bytes() == b'\x00\x00\x00\x00\x3B\x9A\xCA\x00'

    def test_writes_eight_bytes(self):
        w = BufferWriter()
        w.write_int64(0)
        assert len(w.get_bytes()) == 8

    def test_overflow_positive(self):
        w = BufferWriter()
        with pytest.raises(OverflowError):
            w.write_int64(2 ** 64)

    def test_overflow_negative(self):
        w = BufferWriter()
        with pytest.raises(OverflowError):
            w.write_int64(-(2 ** 63) - 1)


# ---------------------------------------------------------------------------
# write_uuid
# ---------------------------------------------------------------------------

class TestWriteUuid:
    def test_valid_16_bytes_written_exactly(self):
        uuid = bytes(range(16))
        w = BufferWriter()
        w.write_uuid(uuid)
        assert w.get_bytes() == uuid

    def test_all_zeros(self):
        w = BufferWriter()
        w.write_uuid(b'\x00' * 16)
        assert w.get_bytes() == b'\x00' * 16

    def test_all_ff(self):
        w = BufferWriter()
        w.write_uuid(b'\xFF' * 16)
        assert w.get_bytes() == b'\xFF' * 16

    def test_writes_exactly_16_bytes(self):
        w = BufferWriter()
        w.write_uuid(b'\xAB' * 16)
        assert len(w.get_bytes()) == 16

    def test_empty_bytes_raises_value_error(self):
        w = BufferWriter()
        with pytest.raises(ValueError):
            w.write_uuid(b'')

    def test_15_bytes_raises_value_error(self):
        w = BufferWriter()
        with pytest.raises(ValueError):
            w.write_uuid(b'\x00' * 15)

    def test_17_bytes_raises_value_error(self):
        w = BufferWriter()
        with pytest.raises(ValueError):
            w.write_uuid(b'\x00' * 17)

    def test_sequential_uuid_writes(self):
        uuid1, uuid2 = bytes(range(16)), bytes(range(16, 32))
        w = BufferWriter()
        w.write_uuid(uuid1)
        w.write_uuid(uuid2)
        assert w.get_bytes() == uuid1 + uuid2


# ---------------------------------------------------------------------------
# write_unsigned_varint
# ---------------------------------------------------------------------------

class TestWriteUnsignedVarint:
    def test_zero(self):
        w = BufferWriter()
        w.write_unsigned_varint(0)
        assert w.get_bytes() == b'\x00'

    def test_one(self):
        w = BufferWriter()
        w.write_unsigned_varint(1)
        assert w.get_bytes() == b'\x01'

    def test_single_byte_max(self):
        w = BufferWriter()
        w.write_unsigned_varint(127)
        assert w.get_bytes() == b'\x7F'

    def test_two_byte_boundary(self):
        w = BufferWriter()
        w.write_unsigned_varint(128)
        assert w.get_bytes() == b'\x80\x01'

    def test_255(self):
        w = BufferWriter()
        w.write_unsigned_varint(255)
        assert w.get_bytes() == b'\xFF\x01'

    def test_two_byte_max(self):
        w = BufferWriter()
        w.write_unsigned_varint(16383)
        assert w.get_bytes() == b'\xFF\x7F'

    def test_three_byte_boundary(self):
        w = BufferWriter()
        w.write_unsigned_varint(16384)
        assert w.get_bytes() == b'\x80\x80\x01'

    def test_three_byte_max(self):
        w = BufferWriter()
        w.write_unsigned_varint(2_097_151)
        assert w.get_bytes() == b'\xFF\xFF\x7F'

    @pytest.mark.parametrize("value", [0, 1, 127, 128, 255, 1000, 16383, 16384, 2_097_151])
    def test_matches_encode_unsigned_varint_oracle(self, value):
        w = BufferWriter()
        w.write_unsigned_varint(value)
        assert w.get_bytes() == varint(value)

    @pytest.mark.parametrize("value", [0, 1, 63, 127, 128, 255, 1000, 16383, 16384, 100_000])
    def test_round_trip_with_buffer_reader(self, value):
        w = BufferWriter()
        w.write_unsigned_varint(value)
        reader = BufferReader(w.get_bytes())
        assert reader.read_unsigned_varint() == value

    def test_negative_value_raises_value_error(self):
        w = BufferWriter()
        with pytest.raises(ValueError):
            w.write_unsigned_varint(-1)

    def test_large_negative_raises_value_error(self):
        w = BufferWriter()
        with pytest.raises(ValueError):
            w.write_unsigned_varint(-100)

    def test_sequential_varints(self):
        w = BufferWriter()
        w.write_unsigned_varint(5)
        w.write_unsigned_varint(300)
        assert w.get_bytes() == varint(5) + varint(300)


# ---------------------------------------------------------------------------
# write_compact_string
# ---------------------------------------------------------------------------

class TestWriteCompactString:
    def test_empty_string(self):
        # "" → varint(1) + b'' = b'\x01'
        w = BufferWriter()
        w.write_compact_string("")
        assert w.get_bytes() == b'\x01'

    def test_simple_ascii(self):
        w = BufferWriter()
        w.write_compact_string("hello")
        assert w.get_bytes() == varint(6) + b'hello'

    def test_single_character(self):
        w = BufferWriter()
        w.write_compact_string("x")
        assert w.get_bytes() == b'\x02x'

    def test_unicode_multibyte_string(self):
        # "café": c(1) + a(1) + f(1) + é(2) = 5 UTF-8 bytes
        w = BufferWriter()
        w.write_compact_string("café")
        payload = "café".encode("utf-8")
        assert w.get_bytes() == varint(len(payload) + 1) + payload

    def test_emoji_string(self):
        w = BufferWriter()
        w.write_compact_string("hi 🎉")
        payload = "hi 🎉".encode("utf-8")
        assert w.get_bytes() == varint(len(payload) + 1) + payload

    def test_string_requiring_two_byte_varint(self):
        # 128 chars → varint(129) is two bytes
        s = "a" * 128
        w = BufferWriter()
        w.write_compact_string(s)
        assert w.get_bytes() == varint(129) + b'a' * 128

    def test_round_trip_ascii(self):
        w = BufferWriter()
        w.write_compact_string("kafka-topic")
        reader = BufferReader(w.get_bytes())
        assert reader.read_compact_string() == "kafka-topic"

    def test_round_trip_unicode(self):
        w = BufferWriter()
        w.write_compact_string("héllo wörld")
        reader = BufferReader(w.get_bytes())
        assert reader.read_compact_string() == "héllo wörld"

    def test_round_trip_empty_string(self):
        w = BufferWriter()
        w.write_compact_string("")
        reader = BufferReader(w.get_bytes())
        assert reader.read_compact_string() == ""

    def test_sequential_strings(self):
        w = BufferWriter()
        w.write_compact_string("foo")
        w.write_compact_string("bar")
        reader = BufferReader(w.get_bytes())
        assert reader.read_compact_string() == "foo"
        assert reader.read_compact_string() == "bar"


# ---------------------------------------------------------------------------
# write_compact_array_length
# ---------------------------------------------------------------------------

class TestWriteCompactArrayLength:
    def test_zero_length(self):
        # 0 → varint(1) = b'\x01'
        w = BufferWriter()
        w.write_compact_array_length(0)
        assert w.get_bytes() == b'\x01'

    def test_one(self):
        w = BufferWriter()
        w.write_compact_array_length(1)
        assert w.get_bytes() == b'\x02'

    def test_five(self):
        w = BufferWriter()
        w.write_compact_array_length(5)
        assert w.get_bytes() == b'\x06'

    def test_127(self):
        w = BufferWriter()
        w.write_compact_array_length(127)
        assert w.get_bytes() == varint(128)

    def test_128_requires_two_byte_varint(self):
        # 128 + 1 = 129 → varint(129) is two bytes
        w = BufferWriter()
        w.write_compact_array_length(128)
        assert w.get_bytes() == varint(129)
        assert len(w.get_bytes()) == 2

    def test_null_array_minus_one(self):
        # -1 → varint(0) = b'\x00' (null/empty array signal)
        w = BufferWriter()
        w.write_compact_array_length(-1)
        assert w.get_bytes() == b'\x00'

    @pytest.mark.parametrize("length", [0, 1, 5, 127, 128])
    def test_round_trip_with_reader(self, length):
        w = BufferWriter()
        w.write_compact_array_length(length)
        reader = BufferReader(w.get_bytes())
        assert reader.read_compact_array_length() == length

    def test_minus_two_raises_value_error(self):
        # -2 → varint(-1) → ValueError in encode_unsigned_varint
        w = BufferWriter()
        with pytest.raises(ValueError):
            w.write_compact_array_length(-2)

    def test_large_negative_raises_value_error(self):
        w = BufferWriter()
        with pytest.raises(ValueError):
            w.write_compact_array_length(-100)


# ---------------------------------------------------------------------------
# write_tag_buffer
# ---------------------------------------------------------------------------

class TestWriteTagBuffer:
    def test_writes_single_zero_byte(self):
        w = BufferWriter()
        w.write_tag_buffer()
        assert w.get_bytes() == b'\x00'

    def test_multiple_calls_each_append_zero(self):
        w = BufferWriter()
        w.write_tag_buffer()
        w.write_tag_buffer()
        w.write_tag_buffer()
        assert w.get_bytes() == b'\x00\x00\x00'

    def test_tag_buffer_between_other_writes(self):
        w = BufferWriter()
        w.write_int8(1)
        w.write_tag_buffer()
        w.write_int8(2)
        assert w.get_bytes() == b'\x01\x00\x02'

    def test_readable_by_buffer_reader(self):
        w = BufferWriter()
        w.write_tag_buffer()
        reader = BufferReader(w.get_bytes())
        reader.read_tag_buffer()   # should consume the b'\x00' without error
        assert reader.is_eof()


# ---------------------------------------------------------------------------
# Integration / Round-trip
# ---------------------------------------------------------------------------

class TestIntegration:
    @pytest.mark.parametrize("value", [0, 1, 127, -1, -128, 255])
    def test_round_trip_int8(self, value):
        w = BufferWriter()
        w.write_int8(value)
        reader = BufferReader(w.get_bytes())
        # read_int8 is signed, so 255 reads back as -1 — bytes are the same
        assert reader.read_bytes(1) == w.get_bytes()

    @pytest.mark.parametrize("value", [0, 32767, -1, -32768])
    def test_round_trip_int16(self, value):
        w = BufferWriter()
        w.write_int16(value)
        reader = BufferReader(w.get_bytes())
        assert reader.read_int16() == value

    @pytest.mark.parametrize("value", [0, 2_147_483_647, -1, -2_147_483_648])
    def test_round_trip_int32(self, value):
        w = BufferWriter()
        w.write_int32(value)
        reader = BufferReader(w.get_bytes())
        assert reader.read_int32() == value

    @pytest.mark.parametrize("value", [0, 9_223_372_036_854_775_807, -1, -9_223_372_036_854_775_808])
    def test_round_trip_int64(self, value):
        w = BufferWriter()
        w.write_int64(value)
        reader = BufferReader(w.get_bytes())
        assert reader.read_int64() == value

    def test_round_trip_uuid(self):
        uuid = bytes(range(16))
        w = BufferWriter()
        w.write_uuid(uuid)
        reader = BufferReader(w.get_bytes())
        assert reader.read_uuid() == uuid

    def test_mixed_types_sequential(self):
        w = BufferWriter()
        w.write_int8(1)
        w.write_int16(500)
        w.write_int32(70000)
        w.write_int64(1_000_000_000)

        reader = BufferReader(w.get_bytes())
        assert reader.read_int8() == 1
        assert reader.read_int16() == 500
        assert reader.read_int32() == 70000
        assert reader.read_int64() == 1_000_000_000
        assert reader.is_eof()

    def test_kafka_response_body_structure(self):
        # Simulates a DescribeTopicPartitions-style response fragment:
        # tag_buffer + throttle_time + compact_array_length(1) + compact_string("my-topic") + uuid
        topic_uuid = bytes(range(16))
        w = BufferWriter()
        w.write_tag_buffer()            # response header tag buffer
        w.write_int32(0)                # throttle_time
        w.write_compact_array_length(1) # 1 topic
        w.write_compact_string("my-topic")
        w.write_uuid(topic_uuid)
        w.write_tag_buffer()            # topic-level tag buffer

        reader = BufferReader(w.get_bytes())
        reader.read_tag_buffer()
        assert reader.read_int32() == 0
        assert reader.read_compact_array_length() == 1
        assert reader.read_compact_string() == "my-topic"
        assert reader.read_uuid() == topic_uuid
        reader.read_tag_buffer()
        assert reader.is_eof()

    def test_compact_array_of_int32_partition_indices(self):
        partition_indices = [0, 1, 2, 3]
        w = BufferWriter()
        w.write_compact_array_length(len(partition_indices))
        for idx in partition_indices:
            w.write_int32(idx)

        reader = BufferReader(w.get_bytes())
        count = reader.read_compact_array_length()
        assert count == 4
        for expected in partition_indices:
            assert reader.read_int32() == expected
        assert reader.is_eof()

    def test_write_negative_int64_used_as_sentinel(self):
        # -1 is used as a sentinel value in Fetch/Produce responses
        w = BufferWriter()
        w.write_int64(-1)  # log_append_time sentinel
        reader = BufferReader(w.get_bytes())
        assert reader.read_int64() == -1

    def test_get_bytes_idempotent_after_writes(self):
        w = BufferWriter()
        w.write_int32(42)
        w.write_compact_string("test")
        result1 = w.get_bytes()
        result2 = w.get_bytes()
        assert result1 == result2
