from io import BytesIO
from struct import pack
from typing import NamedTuple, List

from ammoo.wire import methods as m
from ammoo.wire.low.misc import parse_boolean, pack_boolean
from ammoo.wire.low.peer_properties import parse_peer_properties, PeerProperties, build_peer_properties
from ammoo.wire.low.strings import parse_binary_longstr, parse_shortstr, parse_binary_shortstr, parse_split_longstr, \
    build_shortstr, build_binary_longstr, parse_split_shortstr, build_split_shortstr
from ammoo.wire.low.structs import parse_octet_uint, parse_short_uint, parse_long_uint
from ammoo.wire.typing import VirtualHost, ReplyCode


ConnectionStartParameters = NamedTuple('ConnectionStartParameters', [
    ('version_major', int),
    ('version_minor', int),
    ('server_properties', PeerProperties),
    ('mechanisms', List[str]),
    ('locales', List[str]),
])
ConnectionStartOkParameters = NamedTuple('ConnectionStartOkParameters', [
    ('client_properties', PeerProperties),
    ('mechanism', str),
    ('response', bytes),
    ('locale', str),
])
# Also used for connection.tune-ok
ConnectionTuneParameters = NamedTuple('ConnectionTuneParameters', [
    ('channel_max', int),
    ('frame_max', int),
    ('heartbeat', int),
])
ConnectionOpenParameters = NamedTuple('ConnectionOpenParameters', [
    ('virtualhost', VirtualHost),
    ('capabilities', List[str]),
    ('insist', bool),
])
ConnectionCloseParameters = NamedTuple('ConnectionCloseParameters', [
    ('reply_code', ReplyCode),
    ('reply_text', str),
    ('class_id', int),
    ('method_id', int),
])


def parse_connection_start_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> ConnectionStartParameters:
    return ConnectionStartParameters(
        parse_octet_uint(bio),
        parse_octet_uint(bio),
        parse_peer_properties(bio, encoding, rabbitmq),
        parse_split_longstr(bio, encoding),
        parse_split_longstr(bio, encoding)
    )


def parse_connection_start_ok_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> ConnectionStartOkParameters:
    return ConnectionStartOkParameters(
        parse_peer_properties(bio, encoding, rabbitmq),
        parse_shortstr(bio, encoding),
        parse_binary_longstr(bio),
        parse_shortstr(bio, encoding)
    )


def pack_connection_start_ok_parameters(
        client_properties: PeerProperties, mechanism: str, response: bytes, locale: str, encoding: str) -> bytes:
    bio = BytesIO()
    build_peer_properties(bio, client_properties, encoding)
    build_shortstr(bio, mechanism, encoding),
    build_binary_longstr(bio, response),
    build_shortstr(bio, locale, encoding)
    return bio.getvalue()


# used for connection.secure-ok too
def parse_connection_secure_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> bytes:
    return parse_binary_longstr(bio)  # value is the response parameter


def parse_connection_tune_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> ConnectionTuneParameters:
    return ConnectionTuneParameters(
        parse_short_uint(bio),
        parse_long_uint(bio),  # check somewhere this is at least 4k
        parse_short_uint(bio)
    )


def pack_connection_tune_ok_parameters(channel_max: int, frame_max: int, heartbeat: int) -> bytes:
    return pack('!HIH', channel_max, frame_max, heartbeat)


def parse_connection_open_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> ConnectionOpenParameters:
    return ConnectionOpenParameters(
        parse_shortstr(bio, encoding),
        parse_split_shortstr(bio, encoding),
        parse_boolean(bio)
    )


def pack_connection_open_parameters(
        virtualhost: VirtualHost, capabilities: List[str], insist: bool, encoding: str) -> bytes:
    bio = BytesIO()
    build_shortstr(bio, virtualhost, encoding)
    build_split_shortstr(bio, capabilities, encoding),
    bio.write(pack_boolean(insist))
    return bio.getvalue()


def parse_connection_open_ok_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> None:
    parse_binary_shortstr(bio)  # reserved value, throw away
    return None


def parse_connection_close_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> ConnectionCloseParameters:
    return ConnectionCloseParameters(
        parse_short_uint(bio),
        parse_shortstr(bio, encoding),
        parse_short_uint(bio),
        parse_short_uint(bio)
    )


CONNECTION_METHOD_PARAMETER_PARSERS = {
    m.METHOD_CONNECTION_START: parse_connection_start_parameters,
    m.METHOD_CONNECTION_START_OK: parse_connection_start_ok_parameters,
    m.METHOD_CONNECTION_SECURE: parse_connection_secure_parameters,  # value is the challenge parameter
    m.METHOD_CONNECTION_SECURE_OK: parse_connection_secure_parameters,
    m.METHOD_CONNECTION_TUNE: parse_connection_tune_parameters,
    m.METHOD_CONNECTION_TUNE_OK: parse_connection_tune_parameters,
    m.METHOD_CONNECTION_OPEN: parse_connection_open_parameters,
    m.METHOD_CONNECTION_OPEN_OK: parse_connection_open_ok_parameters,
    m.METHOD_CONNECTION_CLOSE: parse_connection_close_parameters,
    m.METHOD_CONNECTION_CLOSE_OK: None,
}
CLEAN_CONNECTION_CLOSE_PARAMETERS = pack('!HBHH', 0, 0, 0, 0)
