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
