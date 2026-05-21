from src.utils import encode_unsigned_varint


class BufferWriter:
    """
    A utility class for writing Kafka protocol primitives to a byte sequence.
    """

    def __init__(self):
        self.buffer = bytearray()

    def write_bytes(self, data: bytes) -> None:
        self.buffer.extend(data)

    def write_int8(self, value: int) -> None:
        if value >= 0:
            self.buffer.extend(value.to_bytes(1, byteorder="big"))
        else:
            self.buffer.extend(value.to_bytes(1, byteorder="big", signed=True))

    def write_int16(self, value: int) -> None:
        if value >= 0:
            self.buffer.extend(value.to_bytes(2, byteorder="big"))
        else:
            self.buffer.extend(value.to_bytes(2, byteorder="big", signed=True))

    def write_int32(self, value: int) -> None:
        if value >= 0:
            self.buffer.extend(value.to_bytes(4, byteorder="big"))
        else:
            self.buffer.extend(value.to_bytes(4, byteorder="big", signed=True))

    def write_int64(self, value: int) -> None:
        if value >= 0:
            self.buffer.extend(value.to_bytes(8, byteorder="big"))
        else:
            self.buffer.extend(value.to_bytes(8, byteorder="big", signed=True))

    def write_uuid(self, value: bytes) -> None:
        if len(value) != 16:
            raise ValueError("UUID must be exactly 16 bytes")
        self.write_bytes(value)

    def write_compact_string(self, value: str) -> None:
        encoded = value.encode("utf-8")
        self.write_unsigned_varint(len(encoded) + 1)
        self.write_bytes(encoded)

    def write_compact_array_length(self, length: int) -> None:
        self.write_unsigned_varint(length + 1)

    def write_unsigned_varint(self, value: int) -> None:
        self.write_bytes(encode_unsigned_varint(value))

    def write_tag_buffer(self) -> None:
        self.write_unsigned_varint(0)

    def get_bytes(self) -> bytes:
        return bytes(self.buffer)
