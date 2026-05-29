import pytest
from src.utils import encode_unsigned_varint


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

