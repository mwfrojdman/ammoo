from io import BytesIO
from struct import unpack, error as struct_error, pack

from .exceptions import IncompleteFrame, InvalidFloat, InvalidDouble


def parse_octet_uint(bio: BytesIO) -> int:
    try:
        return unpack('!B', bio.read(1))[0]
    except struct_error as exc:
        raise IncompleteFrame() from exc


def pack_octet_uint(value: int) -> bytes:
    return pack('!B', value)


def build_octet_uint(bio: BytesIO, value: int) -> None:
    bio.write(pack('!B', value))


def parse_octet_int(bio: BytesIO) -> int:
    try:
        return unpack('!b', bio.read(1))[0]
    except struct_error as exc:
        raise IncompleteFrame() from exc


def pack_octet_int(value: int) -> bytes:
    return pack('!b', value)


def parse_short_uint(bio: BytesIO) -> int:
    try:
        return unpack('!H', bio.read(2))[0]
    except struct_error as exc:
        raise IncompleteFrame() from exc


def pack_short_uint(value: int) -> bytes:
    return pack('!H', value)


RESERVED_SHORT_UINT_DATA = pack_short_uint(0)
ZERO_SHORT_UINT_DATA = pack_short_uint(0)


def parse_short_int(bio: BytesIO) -> int:
    try:
        return unpack('!h', bio.read(2))[0]
    except struct_error as exc:
        raise IncompleteFrame() from exc


def pack_short_int(value: int) -> bytes:
    return pack('!h', value)


def parse_long_uint(bio: BytesIO) -> int:
    try:
        return unpack('!I', bio.read(4))[0]
    except struct_error as exc:
        raise IncompleteFrame() from exc


def pack_long_uint(value: int) -> bytes:
    return pack('!I', value)


def parse_long_int(bio: BytesIO) -> int:
    try:
        return unpack('!i', bio.read(4))[0]
    except struct_error as exc:
        raise IncompleteFrame() from exc



def pack_long_int(value: int) -> bytes:
    return pack('!i', value)


def parse_long_long_uint(bio: BytesIO) -> int:
    try:
        return unpack('!Q', bio.read(8))[0]
    except struct_error as exc:
        raise IncompleteFrame() from exc


def pack_long_long_uint(value: int) -> bytes:
    return pack('!Q', value)


def parse_long_long_int(bio: BytesIO) -> int:
    try:
        return unpack('!q', bio.read(8))[0]
    except struct_error as exc:
        raise IncompleteFrame() from exc


def pack_long_long_int(value: int) -> bytes:
    return pack('!q', value)


def parse_float(bio: BytesIO) -> float:
    data = bio.read(4)
    try:
        return unpack('!f', data)[0]
    except struct_error as exc:
        if len(data) != 4:
            raise IncompleteFrame() from exc
        else:
            raise InvalidFloat(data) from exc


def parse_double(bio: BytesIO) -> float:
    data = bio.read(8)
    try:
        return unpack('!d', data)[0]
    except struct_error as exc:
        if len(data) != 8:
            raise IncompleteFrame() from exc
        else:
            raise InvalidDouble(data) from exc


def pack_double(value: float) -> bytes:
    return pack('!d', value)
