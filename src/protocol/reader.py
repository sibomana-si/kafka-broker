class BufferReader:
    """
    A utility class for reading Kafka protocol primitives from a byte sequence.
    Maintains an internal cursor position to allow sequential reading.
    """

    def __init__(self, buffer: bytes, offset: int = 0):
        self.buffer = buffer
        self.offset = offset

    def read_bytes(self, num_bytes: int) -> bytes:
        data = self.buffer[self.offset : self.offset + num_bytes]
        self.offset += num_bytes
        return data

    def read_int8(self) -> int:
        data = self.read_bytes(1)
        return int.from_bytes(data, byteorder="big", signed=True)

    def read_int16(self) -> int:
        data = self.read_bytes(2)
        return int.from_bytes(data, byteorder="big", signed=True)

    def read_int32(self) -> int:
        data = self.read_bytes(4)
        return int.from_bytes(data, byteorder="big", signed=True)

    def read_int64(self) -> int:
        data = self.read_bytes(8)
        return int.from_bytes(data, byteorder="big", signed=True)

    def read_uuid(self) -> bytes:
        return self.read_bytes(16)

    def read_compact_string(self) -> str:
        # Compact strings start with an unsigned varint representing length + 1
        # Length of 0 means null.
        length = self.read_unsigned_varint() - 1
        if length < 0:
            return ""
        data = self.read_bytes(length)
        return data.decode("utf-8")

    def read_compact_array_length(self) -> int:
        # Compact arrays start with an unsigned varint representing length + 1
        return self.read_unsigned_varint() - 1

    def read_unsigned_varint(self) -> int:
        value = 0
        shift = 0
        while True:
            b = self.read_bytes(1)[0]
            value |= (b & 0x7F) << shift
            if not (b & 0x80):
                break
            shift += 7
        return value

    def skip(self, num_bytes: int) -> None:
        self.offset += num_bytes

    def read_tag_buffer(self) -> None:
        num_tags = self.read_unsigned_varint()
        for _ in range(num_tags):
            self.read_unsigned_varint()
            tag_size = self.read_unsigned_varint()
            self.skip(tag_size)

    def is_eof(self) -> bool:
        return self.offset >= len(self.buffer)
