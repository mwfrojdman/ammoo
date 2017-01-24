from asyncio import StreamReader
from collections import namedtuple
from datetime import datetime
from io import BytesIO
from struct import unpack, pack
from typing import NamedTuple, Optional

from ammoo.wire.classes import CLASS_BASIC
from ammoo.wire.low.containers import parse_field_table, STRING_ARGS, FIELD_ARGS, NO_ARGS, build_field_table, FieldTable
from ammoo.wire.low.exceptions import InvalidWeight, ExcessiveFramePayload, UnsupportedHeaderFrameClass, \
    TooManyHeaderProperties
from ammoo.wire.low.misc import parse_timestamp, parse_delivery_mode, build_timestamp
from ammoo.wire.low.strings import parse_shortstr, parse_binary_shortstr, build_shortstr, build_binary_shortstr
from ammoo.wire.low.structs import parse_short_uint, parse_octet_uint, pack_long_long_uint, pack_short_uint, \
    build_octet_uint, ZERO_SHORT_UINT_DATA
from ammoo.wire.typing import DeliveryMode


_BasicHeaderProperty = namedtuple('_BasicHeaderProperty', ['bit', 'name', 'parse', 'build', 'args_type'])

_BASIC_HEADER_PROPERTIES = [
    _BasicHeaderProperty(1 << 15, 'content_type', parse_shortstr, build_shortstr, STRING_ARGS),
    _BasicHeaderProperty(1 << 14, 'content_encoding', parse_shortstr, build_shortstr, STRING_ARGS),
    _BasicHeaderProperty(1 << 13, 'headers', parse_field_table, build_field_table, FIELD_ARGS),
    _BasicHeaderProperty(1 << 12, 'delivery_mode', parse_delivery_mode, build_octet_uint, NO_ARGS),
    _BasicHeaderProperty(1 << 11, 'priority', parse_octet_uint, build_octet_uint, NO_ARGS),
    _BasicHeaderProperty(1 << 10, 'correlation_id', parse_shortstr, build_shortstr, STRING_ARGS),
    _BasicHeaderProperty(1 << 9,  'reply_to', parse_shortstr, build_shortstr, STRING_ARGS),
    _BasicHeaderProperty(1 << 8,  'expiration', parse_shortstr, build_shortstr, STRING_ARGS),
    _BasicHeaderProperty(1 << 7,  'message_id', parse_shortstr, build_shortstr, STRING_ARGS),
    _BasicHeaderProperty(1 << 6,  'timestamp', parse_timestamp, build_timestamp, NO_ARGS),
    _BasicHeaderProperty(1 << 5,  'type_', parse_shortstr, build_shortstr, STRING_ARGS),
    _BasicHeaderProperty(1 << 4,  'user_id', parse_shortstr, build_shortstr, STRING_ARGS),
    _BasicHeaderProperty(1 << 3,  'app_id', parse_shortstr, build_shortstr, STRING_ARGS),
    # this is a rabbitmq extension, vanilla AMQP leaves it reserved
    # XXX: should this be non-binary?
    _BasicHeaderProperty(1 << 2,  'cluster_id', parse_binary_shortstr, build_binary_shortstr, NO_ARGS),
]


BasicHeaderProperties = NamedTuple('BasicHeaderProperties', [
    ('content_type', Optional[str]),
    ('content_encoding', Optional[str]),
    ('headers', Optional[FieldTable]),
    ('delivery_mode', Optional[DeliveryMode]),
    ('priority', Optional[int]),
    ('correlation_id', Optional[str]),
    ('reply_to', Optional[str]),  # RoutingKey?
    ('expiration', Optional[str]),
    ('message_id', Optional[str]),
    ('timestamp', Optional[datetime]),
    ('type_', Optional[str]),
    ('user_id', Optional[str]),
    ('app_id', Optional[str]),
    ('cluster_id', Optional[str]),
])


def parse_basic_header_properties(bio: BytesIO, encoding: str, rabbitmq: bool) -> BasicHeaderProperties:
    # The basic class only has 13 (+1 unused) properties, which means there will never be a second 16-bit value set
    # If there would, then this single loop would not be enough
    values = []

    flags = parse_short_uint(bio)
    if flags & 1 != 0:
        # Basic only has 13(+1) properties
        raise TooManyHeaderProperties(CLASS_BASIC)

    for property in _BASIC_HEADER_PROPERTIES:
        if flags & property.bit:
            if property.args_type == NO_ARGS:
                value = property.parse(bio)
            elif property.args_type == STRING_ARGS:
                value = property.parse(bio, encoding)
            else:
                value = property.parse(bio, encoding, rabbitmq)
            values.append(value)
        else:
            values.append(None)

    return BasicHeaderProperties(*values)


def build_basic_header_properties(
        bio: BytesIO, properties: BasicHeaderProperties, encoding: str, rabbitmq: bool) -> None:
    flags = 0
    properties_bio = BytesIO()
    for property, value in zip(_BASIC_HEADER_PROPERTIES, properties):
        if value is not None:
            flags |= property.bit
            if property.args_type == NO_ARGS:
                property.build(properties_bio, value)
            elif property.args_type == STRING_ARGS:
                property.build(properties_bio, value, encoding)
            else:
                property.build(properties_bio, value, encoding, rabbitmq)
    bio.write(pack_short_uint(flags))
    bio.write(properties_bio.getvalue())


def build_empty_header_properties(bio: BytesIO) -> None:
    """When all properties are None, this just writes a flags int with all false"""
    bio.write(ZERO_SHORT_UINT_DATA)


FRAME_TYPE_HEADER = 2


class BasicHeaderFrame:
    __slots__ = 'channel_id', 'body_size', 'properties'
    frame_type = FRAME_TYPE_HEADER

    def __init__(self, channel_id, body_size, properties):
        self.channel_id = channel_id
        self.body_size = body_size
        self.properties = properties

    def __str__(self):
        return 'Header frame for class basic, body size {}, properties {}'.format(self.body_size, self.properties)


async def parse_header_frame(
        reader: StreamReader, payload_size: int, channel_id: int, encoding: str, rabbitmq: bool) -> BasicHeaderFrame:
    class_id, weight, body_size = unpack('!HHQ', await reader.readexactly(12))
    if class_id != CLASS_BASIC:
        raise UnsupportedHeaderFrameClass(class_id)
    if weight != 0:
        raise InvalidWeight(weight)
    bio = BytesIO(await reader.readexactly(payload_size - 12))
    properties = parse_basic_header_properties(bio, encoding, rabbitmq)
    payload_offset = bio.tell() + 12
    if payload_offset != payload_size:
        raise ExcessiveFramePayload(payload_size, payload_offset)
    return BasicHeaderFrame(channel_id, body_size, properties)


BASIC_HEADER_PAYLOAD_PREFIX = pack('!HH', CLASS_BASIC, 0)  # class and weight


def pack_basic_header_frame_payload(
        body_size: int, properties: Optional[BasicHeaderProperties], encoding: str, rabbitmq: bool) -> bytes:
    bio = BytesIO()
    bio.write(BASIC_HEADER_PAYLOAD_PREFIX)
    bio.write(pack_long_long_uint(body_size))
    if properties is None:
        build_empty_header_properties(bio)
    else:
        build_basic_header_properties(bio, properties, encoding, rabbitmq)
    return bio.getvalue()
