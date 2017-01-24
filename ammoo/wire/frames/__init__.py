from asyncio import StreamReader, StreamWriter
from struct import unpack, pack
from typing import Union

from ammoo.wire.constants import FRAME_END
from ammoo.wire.frames.body import BodyFrame, FRAME_TYPE_BODY
from ammoo.wire.frames.header import parse_header_frame, BasicHeaderFrame, FRAME_TYPE_HEADER
from ammoo.wire.frames.heartbeat import HeartbeatFrame, HEARTBEAT_FRAME, FRAME_TYPE_HEARTBEAT
from ammoo.wire.frames.method import parse_method_frame, FRAME_TYPE_METHOD, MethodFrame
from ammoo.wire.low.exceptions import InvalidFrameEnd, InvalidFrameType

Frame = Union[MethodFrame, BasicHeaderFrame, BodyFrame, HeartbeatFrame]


async def parse_frame(reader: StreamReader, encoding: str, rabbitmq: bool) -> Frame:
    frame_type, channel_id, payload_size = unpack('!BHI', await reader.readexactly(7))

    if frame_type == FRAME_TYPE_METHOD:
        frame = await parse_method_frame(reader, payload_size, channel_id, encoding, rabbitmq)
    elif frame_type == FRAME_TYPE_BODY:
        frame = BodyFrame(channel_id, payload_size, await reader.readexactly(payload_size))
    elif frame_type == FRAME_TYPE_HEADER:
        frame = await parse_header_frame(reader, payload_size, channel_id, encoding, rabbitmq)
    elif frame_type == FRAME_TYPE_HEARTBEAT:
        frame = HEARTBEAT_FRAME
    else:
        raise InvalidFrameType(frame_type)

    frame_end = await reader.readexactly(1)
    if frame_end != FRAME_END:
        raise InvalidFrameEnd(frame_end)

    return frame


def build_frame(writer: StreamWriter, frame_type: int, channel_id: int, payload):
    payload_size = len(payload)
    writer.write(pack('!BHI', frame_type, channel_id, payload_size))
    writer.write(payload)
    writer.write(FRAME_END)
