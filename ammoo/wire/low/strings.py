from io import BytesIO
from typing import List

from .exceptions import IncompleteFrame, ShortstrDecodingError, LongstrDecodingError
from .structs import parse_octet_uint, parse_long_uint, pack_octet_uint, pack_long_uint


def parse_shortstr(bio: BytesIO, encoding: str) -> str:
    size = parse_octet_uint(bio)
    data = bio.read(size)
    if len(data) != size:
        raise IncompleteFrame()
    try:
        return data.decode(encoding)
    except UnicodeDecodeError as exc:
        raise ShortstrDecodingError(data, encoding) from exc


def parse_split_shortstr(bio: BytesIO, encoding: str) -> List[str]:
    return parse_shortstr(bio, encoding).split(' ')


def pack_shortstr(text: str, encoding: str) -> bytes:
    data = text.encode(encoding)
    return pack_octet_uint(len(data)) + data


def build_shortstr(bio: BytesIO, text: str, encoding: str) -> None:
    data = text.encode(encoding)
    bio.write(pack_octet_uint(len(data)))
    bio.write(data)


def build_split_shortstr(bio: BytesIO, split_text: List[str], encoding: str) -> None:
    build_shortstr(bio, ' '.join(split_text), encoding)


EMPTY_SHORTSTR_DATA = pack_octet_uint(0)  # empty string constant


def parse_binary_shortstr(bio: BytesIO) -> bytes:
    size = parse_octet_uint(bio)
    data = bio.read(size)
    if len(data) != size:
        raise IncompleteFrame()
    return data


def build_binary_shortstr(bio: BytesIO, data: bytes) -> None:
    bio.write(pack_octet_uint(len(data)))
    bio.write(data)


def parse_longstr(bio: BytesIO, encoding: str) -> str:
    size = parse_long_uint(bio)
    data = bio.read(size)
    if len(data) != size:
        raise IncompleteFrame()
    try:
        return data.decode(encoding)
    except UnicodeDecodeError as exc:
        raise LongstrDecodingError(data, encoding) from exc


def parse_split_longstr(bio: BytesIO, encoding: str) -> List[str]:
    return parse_longstr(bio, encoding).split(' ')


def parse_binary_longstr(bio: BytesIO) -> bytes:
    size = parse_long_uint(bio)
    data = bio.read(size)
    if len(data) != size:
        raise IncompleteFrame()
    return data


def pack_binary_longstr(data: bytes) -> bytes:
    return pack_long_uint(len(data)) + data


def build_binary_longstr(bio: BytesIO, data: bytes) -> None:
    bio.write(pack_long_uint(len(data)))
    bio.write(data)


def build_longstr(bio: BytesIO, text: str, encoding: str) -> None:
    data = text.encode(encoding)
    bio.write(pack_long_uint(len(data)))
    bio.write(data)
