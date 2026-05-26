import pytest
from src.protocol.reader import BufferReader
from src.utils import encode_unsigned_varint


def varint(n: int) -> bytes:
    """Alias for encode_unsigned_varint used as a test fixture oracle."""
    return encode_unsigned_varint(n)


def compact_str(s: str) -> bytes:
    """Encode a string using the Kafka compact-string format (varint(len+1) + utf-8)."""
    payload = s.encode("utf-8")
    return varint(len(payload) + 1) + payload


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

class TestConstructor:
    def test_default_offset_is_zero(self):
        reader = BufferReader(b'\x01\x02\x03')
        assert reader.offset == 0

    def test_custom_offset_is_set(self):
        reader = BufferReader(b'\x01\x02\x03', offset=2)
        assert reader.offset == 2

    def test_buffer_is_stored(self):
        buf = b'\x01\x02\x03'
        reader = BufferReader(buf)
        assert reader.buffer is buf


# ---------------------------------------------------------------------------
# read_bytes
# ---------------------------------------------------------------------------

class TestReadBytes:
    def test_reads_exactly_n_bytes(self):
        reader = BufferReader(b'\x01\x02\x03\x04\x05')
        assert reader.read_bytes(3) == b'\x01\x02\x03'

    def test_advances_offset_by_n(self):
        reader = BufferReader(b'\x01\x02\x03\x04\x05')
        reader.read_bytes(3)
        assert reader.offset == 3

    def test_reads_zero_bytes_returns_empty(self):
        reader = BufferReader(b'\x01\x02\x03')
        assert reader.read_bytes(0) == b''
        assert reader.offset == 0

    def test_sequential_reads_advance_cursor(self):
        reader = BufferReader(b'\x01\x02\x03\x04')
        assert reader.read_bytes(2) == b'\x01\x02'
        assert reader.read_bytes(2) == b'\x03\x04'

    def test_reads_full_buffer(self):
        buf = b'\xAA\xBB\xCC'
        assert BufferReader(buf).read_bytes(3) == buf

    def test_past_end_returns_partial_no_error(self):
        # Python slice semantics: returns whatever remains, no exception
        reader = BufferReader(b'\x01\x02')
        assert reader.read_bytes(10) == b'\x01\x02'

    def test_empty_buffer_returns_empty_bytes(self):
        assert BufferReader(b'').read_bytes(5) == b''

    def test_reads_from_custom_starting_offset(self):
        reader = BufferReader(b'\xAA\xBB\xCC\xDD', offset=2)
        assert reader.read_bytes(2) == b'\xCC\xDD'


# ---------------------------------------------------------------------------
# read_int8
# ---------------------------------------------------------------------------

class TestReadInt8:
    def test_zero(self):
        assert BufferReader(b'\x00').read_int8() == 0

    def test_positive_max(self):
        assert BufferReader(b'\x7F').read_int8() == 127

    def test_minus_one(self):
        assert BufferReader(b'\xFF').read_int8() == -1

    def test_signed_min(self):
        assert BufferReader(b'\x80').read_int8() == -128

    def test_advances_offset_by_one(self):
        reader = BufferReader(b'\x01\x02')
        reader.read_int8()
        assert reader.offset == 1

    def test_sequential_reads(self):
        reader = BufferReader(b'\x7F\x80')
        assert reader.read_int8() == 127
        assert reader.read_int8() == -128


# ---------------------------------------------------------------------------
# read_int16
# ---------------------------------------------------------------------------

class TestReadInt16:
    def test_zero(self):
        assert BufferReader(b'\x00\x00').read_int16() == 0

    def test_positive_max(self):
        assert BufferReader(b'\x7F\xFF').read_int16() == 32767

    def test_minus_one(self):
        assert BufferReader(b'\xFF\xFF').read_int16() == -1

    def test_signed_min(self):
        assert BufferReader(b'\x80\x00').read_int16() == -32768

    def test_advances_offset_by_two(self):
        reader = BufferReader(b'\x00\x01\x00\x02')
        reader.read_int16()
        assert reader.offset == 2

    def test_sequential_reads(self):
        reader = BufferReader(b'\x00\x01\x7F\xFF')
        assert reader.read_int16() == 1
        assert reader.read_int16() == 32767

    def test_big_endian_byte_order(self):
        # 0x0100 big-endian = 256
        assert BufferReader(b'\x01\x00').read_int16() == 256


# ---------------------------------------------------------------------------
# read_int32
# ---------------------------------------------------------------------------

class TestReadInt32:
    def test_zero(self):
        assert BufferReader(b'\x00\x00\x00\x00').read_int32() == 0

    def test_positive_max(self):
        assert BufferReader(b'\x7F\xFF\xFF\xFF').read_int32() == 2_147_483_647

    def test_minus_one(self):
        assert BufferReader(b'\xFF\xFF\xFF\xFF').read_int32() == -1

    def test_signed_min(self):
        assert BufferReader(b'\x80\x00\x00\x00').read_int32() == -2_147_483_648

    def test_advances_offset_by_four(self):
        reader = BufferReader(b'\x00\x00\x00\x01\x00\x00\x00\x02')
        reader.read_int32()
        assert reader.offset == 4

    def test_big_endian_byte_order(self):
        # 0x00010000 big-endian = 65536
        assert BufferReader(b'\x00\x01\x00\x00').read_int32() == 65536

    def test_sequential_reads(self):
        reader = BufferReader(b'\x00\x00\x00\x05\xFF\xFF\xFF\xFF')
        assert reader.read_int32() == 5
        assert reader.read_int32() == -1


# ---------------------------------------------------------------------------
# read_int64
# ---------------------------------------------------------------------------

class TestReadInt64:
    def test_zero(self):
        assert BufferReader(b'\x00' * 8).read_int64() == 0

    def test_positive_max(self):
        assert BufferReader(b'\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF').read_int64() == 9_223_372_036_854_775_807

    def test_minus_one(self):
        assert BufferReader(b'\xFF' * 8).read_int64() == -1

    def test_signed_min(self):
        assert BufferReader(b'\x80' + b'\x00' * 7).read_int64() == -9_223_372_036_854_775_808

    def test_advances_offset_by_eight(self):
        reader = BufferReader(b'\x00' * 16)
        reader.read_int64()
        assert reader.offset == 8

    def test_known_value(self):
        # 1_000_000_000 = 0x3B9ACA00
        assert BufferReader(b'\x00\x00\x00\x00\x3B\x9A\xCA\x00').read_int64() == 1_000_000_000

    def test_sequential_reads(self):
        reader = BufferReader(b'\x00' * 8 + b'\xFF' * 8)
        assert reader.read_int64() == 0
        assert reader.read_int64() == -1


# ---------------------------------------------------------------------------
# read_uuid
# ---------------------------------------------------------------------------

class TestReadUuid:
    def test_reads_sixteen_bytes_unchanged(self):
        buf = bytes(range(16))
        assert BufferReader(buf).read_uuid() == buf

    def test_all_zeros(self):
        assert BufferReader(b'\x00' * 16).read_uuid() == b'\x00' * 16

    def test_all_ff(self):
        assert BufferReader(b'\xFF' * 16).read_uuid() == b'\xFF' * 16

    def test_advances_offset_by_sixteen(self):
        reader = BufferReader(b'\x00' * 20)
        reader.read_uuid()
        assert reader.offset == 16

    def test_sequential_uuid_reads(self):
        uuid1, uuid2 = bytes(range(16)), bytes(range(16, 32))
        reader = BufferReader(uuid1 + uuid2)
        assert reader.read_uuid() == uuid1
        assert reader.read_uuid() == uuid2

    def test_partial_buffer_returns_partial_no_error(self):
        # Fewer than 16 bytes: returns what's there, no exception
        reader = BufferReader(b'\x01\x02\x03')
        assert reader.read_uuid() == b'\x01\x02\x03'


# ---------------------------------------------------------------------------
# read_unsigned_varint
# ---------------------------------------------------------------------------

class TestReadUnsignedVarint:
    # --- single-byte values (high bit clear) ---

    def test_zero(self):
        assert BufferReader(b'\x00').read_unsigned_varint() == 0

    def test_one(self):
        assert BufferReader(b'\x01').read_unsigned_varint() == 1

    def test_single_byte_max(self):
        assert BufferReader(b'\x7F').read_unsigned_varint() == 127

    def test_single_byte_cursor_advance(self):
        reader = BufferReader(b'\x7F\xFF')
        reader.read_unsigned_varint()
        assert reader.offset == 1

    # --- two-byte values (first byte has high bit set) ---

    def test_two_byte_boundary(self):
        # 128 = b'\x80\x01'
        assert BufferReader(b'\x80\x01').read_unsigned_varint() == 128

    def test_255(self):
        assert BufferReader(b'\xFF\x01').read_unsigned_varint() == 255

    def test_two_byte_max(self):
        # 16383 = b'\xFF\x7F'
        assert BufferReader(b'\xFF\x7F').read_unsigned_varint() == 16383

    def test_two_byte_cursor_advance(self):
        reader = BufferReader(b'\x80\x01\x00')
        reader.read_unsigned_varint()
        assert reader.offset == 2

    # --- three-byte values ---

    def test_three_byte_boundary(self):
        # 16384 = b'\x80\x80\x01'
        assert BufferReader(b'\x80\x80\x01').read_unsigned_varint() == 16384

    def test_three_byte_max(self):
        # 2097151 = b'\xFF\xFF\x7F'
        assert BufferReader(b'\xFF\xFF\x7F').read_unsigned_varint() == 2_097_151

    def test_three_byte_cursor_advance(self):
        reader = BufferReader(b'\x80\x80\x01\x00')
        reader.read_unsigned_varint()
        assert reader.offset == 3

    # --- round-trip against encode_unsigned_varint oracle ---

    @pytest.mark.parametrize("value", [0, 1, 63, 64, 127, 128, 200, 255, 1000,
                                        16383, 16384, 100_000, 2_097_151])
    def test_round_trip(self, value):
        buf = varint(value)
        assert BufferReader(buf).read_unsigned_varint() == value

    def test_sequential_varints(self):
        buf = varint(5) + varint(300)
        reader = BufferReader(buf)
        assert reader.read_unsigned_varint() == 5
        assert reader.read_unsigned_varint() == 300

    # --- error states ---

    def test_empty_buffer_raises_index_error(self):
        with pytest.raises(IndexError):
            BufferReader(b'').read_unsigned_varint()

    def test_truncated_multi_byte_raises_index_error(self):
        # 0x80 has high bit set, signalling "more bytes", but buffer ends
        with pytest.raises(IndexError):
            BufferReader(b'\x80').read_unsigned_varint()

    def test_two_byte_truncated_raises_index_error(self):
        # Two continuation bytes expected but only one present
        with pytest.raises(IndexError):
            BufferReader(b'\x80\x80').read_unsigned_varint()


# ---------------------------------------------------------------------------
# read_compact_string
# ---------------------------------------------------------------------------

class TestReadCompactString:
    def test_null_encoding_varint_zero_returns_empty(self):
        # Varint 0 → length = -1 → implementation returns "" (not None)
        assert BufferReader(b'\x00').read_compact_string() == ""

    def test_empty_string_varint_one(self):
        # Varint 1 → length = 0 → reads 0 bytes → ""
        assert BufferReader(b'\x01').read_compact_string() == ""

    def test_simple_ascii_string(self):
        assert BufferReader(compact_str("hello")).read_compact_string() == "hello"

    def test_single_character(self):
        assert BufferReader(compact_str("x")).read_compact_string() == "x"

    def test_unicode_multibyte_string(self):
        # "café" has 5 UTF-8 bytes ('é' is 2 bytes)
        assert BufferReader(compact_str("café")).read_compact_string() == "café"

    def test_emoji_string(self):
        assert BufferReader(compact_str("hi 🎉")).read_compact_string() == "hi 🎉"

    def test_string_requiring_two_byte_varint_length(self):
        # 128-char string → varint(129) = 2 bytes
        s = "a" * 128
        assert BufferReader(compact_str(s)).read_compact_string() == s

    def test_cursor_advances_past_varint_and_payload(self):
        buf = compact_str("abc") + b'\xFF'  # sentinel after the string
        reader = BufferReader(buf)
        reader.read_compact_string()
        # 1 byte varint (4 encodes len=3) + 3 payload bytes = 4
        assert reader.offset == 4

    def test_cursor_advances_one_byte_for_null_encoding(self):
        reader = BufferReader(b'\x00\xFF')
        reader.read_compact_string()
        assert reader.offset == 1

    def test_sequential_compact_strings(self):
        buf = compact_str("foo") + compact_str("bar")
        reader = BufferReader(buf)
        assert reader.read_compact_string() == "foo"
        assert reader.read_compact_string() == "bar"

    def test_invalid_utf8_raises_unicode_decode_error(self):
        # Build a buffer with varint(3) and 2 invalid UTF-8 bytes
        invalid_payload = b'\xFF\xFE'
        buf = varint(len(invalid_payload) + 1) + invalid_payload
        with pytest.raises(UnicodeDecodeError):
            BufferReader(buf).read_compact_string()

    def test_empty_buffer_raises_index_error(self):
        # No varint to read
        with pytest.raises(IndexError):
            BufferReader(b'').read_compact_string()


# ---------------------------------------------------------------------------
# read_compact_array_length
# ---------------------------------------------------------------------------

class TestReadCompactArrayLength:
    def test_varint_zero_returns_minus_one(self):
        # Null array signal used by some Kafka protocol fields
        assert BufferReader(b'\x00').read_compact_array_length() == -1

    def test_varint_one_returns_zero(self):
        # Empty array
        assert BufferReader(b'\x01').read_compact_array_length() == 0

    def test_varint_two_returns_one(self):
        assert BufferReader(b'\x02').read_compact_array_length() == 1

    def test_varint_six_returns_five(self):
        assert BufferReader(b'\x06').read_compact_array_length() == 5

    def test_two_byte_varint(self):
        # varint(129) encodes 129; result = 129 - 1 = 128
        buf = varint(129)
        assert BufferReader(buf).read_compact_array_length() == 128

    def test_cursor_advances_one_byte_for_single_byte_varint(self):
        reader = BufferReader(b'\x05\xFF')
        reader.read_compact_array_length()
        assert reader.offset == 1

    def test_cursor_advances_two_bytes_for_two_byte_varint(self):
        buf = varint(129) + b'\xFF'
        reader = BufferReader(buf)
        reader.read_compact_array_length()
        assert reader.offset == 2

    def test_empty_buffer_raises_index_error(self):
        with pytest.raises(IndexError):
            BufferReader(b'').read_compact_array_length()

    def test_sequential_array_lengths(self):
        buf = varint(4) + varint(2)   # lengths 3 and 1
        reader = BufferReader(buf)
        assert reader.read_compact_array_length() == 3
        assert reader.read_compact_array_length() == 1


# ---------------------------------------------------------------------------
# skip
# ---------------------------------------------------------------------------

class TestSkip:
    def test_advances_offset_by_n(self):
        reader = BufferReader(b'\x01\x02\x03\x04\x05')
        reader.skip(3)
        assert reader.offset == 3

    def test_skip_zero_is_noop(self):
        reader = BufferReader(b'\x01\x02\x03')
        reader.skip(0)
        assert reader.offset == 0

    def test_subsequent_read_starts_after_skip(self):
        reader = BufferReader(b'\xAA\xBB\xCC\xDD')
        reader.skip(2)
        assert reader.read_bytes(2) == b'\xCC\xDD'

    def test_skip_past_end_does_not_raise(self):
        reader = BufferReader(b'\x01\x02')
        reader.skip(100)   # no exception
        assert reader.offset == 100

    def test_skip_sets_is_eof(self):
        reader = BufferReader(b'\x01\x02\x03')
        reader.skip(3)
        assert reader.is_eof()

    def test_cumulative_skips(self):
        reader = BufferReader(b'\x00' * 10)
        reader.skip(3)
        reader.skip(4)
        assert reader.offset == 7


# ---------------------------------------------------------------------------
# read_tag_buffer
# ---------------------------------------------------------------------------

class TestReadTagBuffer:
    def test_empty_tag_buffer_advances_one_byte(self):
        reader = BufferReader(b'\x00')
        reader.read_tag_buffer()
        assert reader.offset == 1

    def test_single_tag_with_payload(self):
        buf = (
            varint(1)        # num_tags = 1
            + varint(0)      # tag_id = 0
            + varint(3)      # tag_size = 3
            + b'\xAA\xBB\xCC'
        )
        reader = BufferReader(buf)
        reader.read_tag_buffer()
        assert reader.offset == len(buf)

    def test_tag_with_zero_size_payload(self):
        buf = (
            varint(1)        # num_tags = 1
            + varint(5)      # tag_id = 5
            + varint(0)      # tag_size = 0 (no payload bytes)
        )
        reader = BufferReader(buf)
        reader.read_tag_buffer()
        assert reader.offset == len(buf)

    def test_multiple_tags_all_consumed(self):
        buf = (
            varint(2)
            + varint(0) + varint(2) + b'\x01\x02'
            + varint(1) + varint(4) + b'\x03\x04\x05\x06'
        )
        reader = BufferReader(buf)
        reader.read_tag_buffer()
        assert reader.offset == len(buf)

    def test_data_after_tag_buffer_is_unaffected(self):
        sentinel = b'\xDE\xAD'
        buf = b'\x00' + sentinel   # empty tag buffer then sentinel bytes
        reader = BufferReader(buf)
        reader.read_tag_buffer()
        assert reader.read_bytes(2) == sentinel

    def test_tag_buffer_at_non_zero_offset(self):
        prefix = b'\xFF\xFF'
        tag_buf = b'\x00'   # empty tag buffer
        reader = BufferReader(prefix + tag_buf, offset=2)
        reader.read_tag_buffer()
        assert reader.offset == 3


# ---------------------------------------------------------------------------
# is_eof
# ---------------------------------------------------------------------------

class TestIsEof:
    def test_returns_false_at_start_with_data(self):
        assert not BufferReader(b'\x01\x02\x03').is_eof()

    def test_returns_true_after_reading_all_bytes(self):
        reader = BufferReader(b'\x01\x02')
        reader.read_bytes(2)
        assert reader.is_eof()

    def test_returns_true_for_empty_buffer(self):
        assert BufferReader(b'').is_eof()

    def test_returns_true_when_offset_equals_length(self):
        assert BufferReader(b'\x01\x02', offset=2).is_eof()

    def test_returns_true_when_offset_exceeds_length(self):
        assert BufferReader(b'\x01\x02', offset=10).is_eof()

    def test_returns_false_midway_through_buffer(self):
        reader = BufferReader(b'\x01\x02\x03\x04')
        reader.read_bytes(2)
        assert not reader.is_eof()

    def test_transitions_false_to_true(self):
        reader = BufferReader(b'\x42')
        assert not reader.is_eof()
        reader.read_int8()
        assert reader.is_eof()


# ---------------------------------------------------------------------------
# Integration / sequential
# ---------------------------------------------------------------------------

class TestIntegration:
    def test_mixed_types_sequential(self):
        buf = (
            b'\x01'                              # int8 = 1
            + b'\x00\x0A'                        # int16 = 10
            + b'\x00\x00\x01\x00'               # int32 = 256
            + b'\x00\x00\x00\x00\x00\x00\x02\x00'  # int64 = 512
        )
        reader = BufferReader(buf)
        assert reader.read_int8() == 1
        assert reader.read_int16() == 10
        assert reader.read_int32() == 256
        assert reader.read_int64() == 512
        assert reader.is_eof()

    def test_non_zero_initial_offset_skips_header(self):
        # First 2 bytes are a "header" we want to skip
        buf = b'\xFF\xFF' + b'\x00\x05'
        reader = BufferReader(buf, offset=2)
        assert reader.read_int16() == 5
        assert reader.is_eof()

    def test_kafka_style_length_prefix_framing(self):
        # 4-byte length prefix then payload: api_key (int16) + api_version (int16)
        api_key, api_version = 18, 4
        payload = api_key.to_bytes(2, 'big') + api_version.to_bytes(2, 'big')
        buf = len(payload).to_bytes(4, 'big') + payload

        reader = BufferReader(buf)
        assert reader.read_int32() == 4      # length prefix
        assert reader.read_int16() == 18     # api_key
        assert reader.read_int16() == 4      # api_version
        assert reader.is_eof()

    def test_compact_string_then_tag_buffer(self):
        buf = compact_str("test-topic") + b'\x00'   # string + empty tag buffer
        reader = BufferReader(buf)
        assert reader.read_compact_string() == "test-topic"
        reader.read_tag_buffer()
        assert reader.is_eof()

    def test_compact_array_of_uuids(self):
        uuid_bytes = bytes(range(16))
        count = 3
        buf = varint(count + 1) + uuid_bytes * count  # compact length then 3 UUIDs

        reader = BufferReader(buf)
        assert reader.read_compact_array_length() == count
        for _ in range(count):
            assert reader.read_uuid() == uuid_bytes
        assert reader.is_eof()

    def test_nested_structure_topics_and_partitions(self):
        # Simulates the inner loop of parse_fetch_request:
        # [num_topics varint] [uuid(16)] [num_partitions varint]
        #   [partition_index int32] [fetch_offset int64] [tag_buffer]
        topic_id = bytes(range(16))
        partition_index = 0
        fetch_offset = 100

        buf = (
            varint(2)                               # 1 topic (compact: length + 1)
            + topic_id                              # topic UUID
            + varint(2)                             # 1 partition (compact: length + 1)
            + partition_index.to_bytes(4, 'big')    # partition_index int32
            + fetch_offset.to_bytes(8, 'big')       # fetch_offset int64
            + b'\x00'                               # empty tag buffer
            + b'\x00'                               # outer topic tag buffer
        )

        reader = BufferReader(buf)
        assert reader.read_compact_array_length() == 1        # 1 topic
        assert reader.read_uuid() == topic_id
        assert reader.read_compact_array_length() == 1        # 1 partition
        assert reader.read_int32() == partition_index
        assert reader.read_int64() == fetch_offset
        reader.read_tag_buffer()   # partition tag buffer
        reader.read_tag_buffer()   # topic tag buffer
        assert reader.is_eof()

    def test_skip_used_to_jump_over_unknown_fields(self):
        # skip() is used inside read_tag_buffer to bypass tag payload bytes
        buf = b'\x00' * 4 + b'\x42'   # 4 padding bytes + 0x42
        reader = BufferReader(buf)
        reader.skip(4)
        assert reader.read_int8() == 0x42
        assert reader.is_eof()
