from asyncio import StreamReader
from io import BytesIO

from ammoo.wire.class_and_method import parse_class_and_method, pretty_format_cam, cam_long_uint
from ammoo.wire import classes as c
from ammoo.wire.low.exceptions import ExcessiveFramePayload, UnsupportedMethod
from .basic import BASIC_METHOD_PARAMETER_PARSERS
from .channel import CHANNEL_METHOD_PARAMETER_PARSERS
from .confirm import CONFIRM_METHOD_PARAMETER_PARSERS
from .exchange import EXCHANGE_METHOD_PARAMETER_PARSERS
from .queue import QUEUE_METHOD_PARAMETER_PARSERS
from .connection import CONNECTION_METHOD_PARAMETER_PARSERS


METHOD_PARAMETER_PARSERS = {
    c.CLASS_CONNECTION: CONNECTION_METHOD_PARAMETER_PARSERS,
    c.CLASS_CHANNEL: CHANNEL_METHOD_PARAMETER_PARSERS,
    c.CLASS_EXCHANGE: EXCHANGE_METHOD_PARAMETER_PARSERS,
    c.CLASS_QUEUE: QUEUE_METHOD_PARAMETER_PARSERS,
    c.CLASS_BASIC: BASIC_METHOD_PARAMETER_PARSERS,
    c.CLASS_CONFIRM: CONFIRM_METHOD_PARAMETER_PARSERS,
    # Not used by client: c.CLASS_TX: TX_METHOD_PARAMETER_PARSERS,
}
CAM_PARAMETER_PARSERS = {}
for class_id, method_parsers in METHOD_PARAMETER_PARSERS.items():
    for method_id, parse_parameters in method_parsers.items():
        CAM_PARAMETER_PARSERS[cam_long_uint(class_id, method_id)] = parse_parameters


FRAME_TYPE_METHOD = 1


class MethodFrame:
    __slots__ = 'channel_id', 'cam', 'parameters'
    frame_type = FRAME_TYPE_METHOD

    def __init__(self, channel_id: int, cam: int, parameters):  # XXX: typing missing for parameters
        self.channel_id = channel_id
        self.cam = cam
        self.parameters = parameters

    def __str__(self):
        return 'Method {} frame channel {}, parameters: {}'.format(
            pretty_format_cam(self.cam), self.channel_id, self.parameters
        )


async def parse_method_frame(
        reader: StreamReader, payload_size: int, channel_id: int, encoding: str, rabbitmq: bool) -> MethodFrame:
    bio = BytesIO(await reader.readexactly(payload_size))
    cam = parse_class_and_method(bio)
    try:
        parse_parameters = CAM_PARAMETER_PARSERS[cam]
    except KeyError:
        raise UnsupportedMethod(cam)
    if parse_parameters is None:
        parameters = None
    else:
        parameters = parse_parameters(bio, encoding, rabbitmq)
    if bio.tell() != payload_size:
        raise ExcessiveFramePayload(payload_size, bio.tell())
    return MethodFrame(channel_id, cam, parameters)
