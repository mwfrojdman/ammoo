from io import BytesIO
from struct import pack
from typing import NamedTuple

from ammoo.wire import methods as m
from ammoo.wire.low.misc import parse_boolean
from ammoo.wire.low.strings import parse_shortstr, parse_binary_shortstr, parse_binary_longstr
from ammoo.wire.low.structs import parse_short_uint
from ammoo.wire.typing import ReplyCode, FlowActive


ChannelCloseParameters = NamedTuple('ChannelCloseParameters', [
    ('reply_code', ReplyCode),
    ('reply_text', str),
    ('class_id', int),
    ('method_id', int),
])


CLEAN_CHANNEL_CLOSE_PARAMETERS_DATA = pack('!HBHH', 0, 0, 0, 0)


def parse_channel_open_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> None:
    parse_binary_shortstr(bio)
    return None


def parse_channel_open_ok_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> None:
    parse_binary_longstr(bio)
    return None


# used for channel.flow-ok too
def parse_channel_flow_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> FlowActive:
    return parse_boolean(bio)


def parse_channel_close_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> ChannelCloseParameters:
    return ChannelCloseParameters(
        parse_short_uint(bio),
        parse_shortstr(bio, encoding),
        parse_short_uint(bio),
        parse_short_uint(bio),
    )


CHANNEL_METHOD_PARAMETER_PARSERS = {
    m.METHOD_CHANNEL_OPEN: parse_channel_open_parameters,
    m.METHOD_CHANNEL_OPEN_OK: parse_channel_open_ok_parameters,
    m.METHOD_CHANNEL_FLOW: parse_channel_flow_parameters,
    m.METHOD_CHANNEL_FLOW_OK: parse_channel_flow_parameters,
    m.METHOD_CHANNEL_CLOSE: parse_channel_close_parameters,
    m.METHOD_CHANNEL_CLOSE_OK: None,
}
