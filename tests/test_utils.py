import pytest
from src.utils import encode_unsigned_varint, varint_encoding_size


class TestEncodeUnsignedVarint:
    @pytest.mark.parametrize("value, expected", [
        (0,         b'\x00'),
        (1,         b'\x01'),
        (127,       b'\x7f'),
        (128,       b'\x80\x01'),
        (255,       b'\xff\x01'),
        (16383,     b'\xff\x7f'),
        (16384,     b'\x80\x80\x01'),
        (2097151,   b'\xff\xff\x7f'),
        (2097152,   b'\x80\x80\x80\x01'),
        (268435455, b'\xff\xff\xff\x7f'),
        (268435456, b'\x80\x80\x80\x80\x01'),
    ])
    def test_known_encodings(self, value: int, expected: bytes) -> None:
        assert encode_unsigned_varint(value) == expected

    def test_returns_bytes(self) -> None:
        assert isinstance(encode_unsigned_varint(0), bytes)

    def test_single_byte_range_has_no_high_bit(self) -> None:
        for v in range(128):
            encoded = encode_unsigned_varint(v)
            assert len(encoded) == 1
            assert encoded[0] & 0x80 == 0

    def test_continuation_bytes_have_high_bit_set(self) -> None:
        encoded = encode_unsigned_varint(128)
        assert encoded[0] & 0x80 != 0, "continuation byte should have high bit set"
        assert encoded[-1] & 0x80 == 0, "final byte must not have high bit set"

    def test_large_value_u32_max(self) -> None:
        encoded = encode_unsigned_varint(2**32 - 1)
        assert len(encoded) == 5
        assert encoded == b'\xff\xff\xff\xff\x0f'

    def test_large_value_u64_boundary(self) -> None:
        encoded = encode_unsigned_varint(2**35)
        assert len(encoded) == 6

    @pytest.mark.parametrize("value", [-1, -128, -(2**31)])
    def test_negative_raises_value_error(self, value: int) -> None:
        with pytest.raises(ValueError):
            encode_unsigned_varint(value)


class TestVarintEncodingSize:
    @pytest.mark.parametrize("data, index, expected", [
        (b'\x00',                   0, 1),
        (b'\x7f',                   0, 1),
        (b'\x80\x01',               0, 2),
        (b'\xff\x7f',               0, 2),
        (b'\x80\x80\x01',           0, 3),
        (b'\xff\xff\x7f',           0, 3),
        (b'\x80\x80\x80\x80\x01',  0, 5),
        # mid-buffer: skip a leading 0x00 byte, read a two-byte varint
        (b'\x00\x80\x01\x00',       1, 2),
        # adjacent varints: first is single-byte, second starts at index 1
        (b'\x01\x80\x01',           1, 2),
        # single-byte varint at the last position of a larger buffer
        (b'\x00\x00\x7f',           2, 1),
    ])
    def test_known_sizes(self, data: bytes, index: int, expected: int) -> None:
        assert varint_encoding_size(data, index) == expected

    def test_single_byte_standalone(self) -> None:
        assert varint_encoding_size(b'\x42', 0) == 1

    def test_returns_int(self) -> None:
        assert isinstance(varint_encoding_size(b'\x01', 0), int)

    def test_index_out_of_bounds_raises(self) -> None:
        with pytest.raises(IndexError):
            varint_encoding_size(b'\x01', 5)

    def test_truncated_varint_raises(self) -> None:
        # All bytes have the continuation bit set — the sequence never terminates
        with pytest.raises(IndexError):
            varint_encoding_size(b'\x80\x80\x80', 0)


class TestRoundTrip:
    @pytest.mark.parametrize("value", [
        0, 1, 127, 128, 16383, 16384, 2097151, 2097152,
        2**28, 2**32 - 1,
    ])
    def test_size_matches_encoded_length(self, value: int) -> None:
        encoded = encode_unsigned_varint(value)
        assert varint_encoding_size(encoded, 0) == len(encoded)

    @pytest.mark.parametrize("value", [0, 127, 128, 16384, 2097152])
    def test_size_matches_at_nonzero_offset(self, value: int) -> None:
        prefix = b'\x00\xff'
        encoded = encode_unsigned_varint(value)
        buffer = prefix + encoded + b'\x00'
        assert varint_encoding_size(buffer, len(prefix)) == len(encoded)
