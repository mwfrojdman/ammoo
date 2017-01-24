import calendar
from datetime import datetime, timezone
from decimal import Decimal
from io import BytesIO
from typing import Sequence

from ammoo.wire.typing import DeliveryMode
from .exceptions import InvalidBoolean, InvalidDeliveryMode
from .structs import parse_octet_uint, parse_long_int, parse_long_long_uint, pack_octet_uint, pack_long_long_uint


def parse_boolean(bio: BytesIO) -> bool:
    value = parse_octet_uint(bio)
    if value == 1:
        return True
    if value == 0:
        return False
    raise InvalidBoolean(value)


TRUE_DATA = pack_octet_uint(1)
BOOLEAN_FALSE_DATA = pack_octet_uint(0)


def pack_boolean(value: bool) -> bytes:
    if value:
        return TRUE_DATA
    return BOOLEAN_FALSE_DATA


_DECIMAL_10 = Decimal(10)


def parse_decimal(bio: BytesIO) -> Decimal:
    scale = parse_octet_uint(bio)
    integer = parse_long_int(bio)
    value = Decimal(integer)
    if scale > 0:
        value *= _DECIMAL_10 ** -scale
    return value


def parse_timestamp(bio: BytesIO) -> datetime:
    return datetime.utcfromtimestamp(parse_long_long_uint(bio)).replace(tzinfo=timezone.utc)


def pack_timestamp(timestamp: datetime) -> bytes:
    return pack_long_long_uint(calendar.timegm(timestamp.utctimetuple()))


def build_timestamp(bio: BytesIO, timestamp: datetime) -> None:
    bio.write(pack_timestamp(timestamp))


def parse_no_field(bio: BytesIO) -> None:
    return None


DELIVERY_MODE_NON_PERSISTENT = 1
DELIVERY_MODE_PERSISTENT = 2


def parse_delivery_mode(bio: BytesIO) -> DeliveryMode:
    delivery_mode = parse_octet_uint(bio)
    if delivery_mode != DELIVERY_MODE_NON_PERSISTENT and delivery_mode != DELIVERY_MODE_PERSISTENT:
        raise InvalidDeliveryMode(delivery_mode)
    return delivery_mode


def parse_booleans(bio: BytesIO, n: int) -> Sequence[bool]:
    if n > 8:
        raise NotImplementedError()
    byte = parse_octet_uint(bio)
    return tuple(bool(byte & (1 << i)) for i in range(n))


def pack_booleans(*booleans: Sequence[bool]) -> bytes:
    if len(booleans) > 8:
        raise ValueError('Max one byte of booleans is supported')
    byte = 0
    for i, boolean in enumerate(booleans):
        if boolean:
            byte |= 1 << i
    return pack_octet_uint(byte)
