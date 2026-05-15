def encode_unsigned_varint(value: int) -> bytes:
    """
    Encodes an unsigned integer into a variable-length byte representation.

    :param value: The unsigned integer to encode. The value must be non-negative.
    :return: A bytes object containing the varint encoding of the input value.
    :raises ValueError: If the input value is negative, as unsigned varint does not
        support negative values.
    """

    if value < 0:
        raise ValueError("cannot encode negative values")

    res = []
    while value >= 0x80:
        res.append((value & 0x7f) | 0x80)
        value >>= 7
    res.append(value)
    return bytes(res)

def varint_encoding_size(request: bytes, index: int) -> int:
    """
    Calculate the size, in bytes, of a variable-length integer encoded within a byte sequence.

    This function determines the number of bytes consumed by a varint encoding, starting
    from a specified index in the byte sequence. A varint is a compact, variable-length
    encoding for integers commonly used in serialization formats.

    :param request: A sequence of bytes containing the varint encoding.
    :param index: The starting index in the byte sequence to read the varint from.
    :return: The number of bytes used to encode the varint.
    """

    num_bytes = 1
    while request[index] & 0x80:
        num_bytes += 1
        index += 1
    return num_bytes
