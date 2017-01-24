import pytest

from ammoo.connect import connect
from ammoo.connection import Connection
from ammoo.wire.frames import FRAME_TYPE_BODY
from tests.conftest import pytestmark, check_clean_channel_close, check_clean_connection_close, setup_channel


class FrameMaxConnection(Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.received_body_frames = []
        self.sent_body_payloads = []

    def _dispatch_frame(self, frame):
        if frame.frame_type == FRAME_TYPE_BODY:
            self.received_body_frames.append(frame)
        super()._dispatch_frame(frame)

    def _send_frame(self, frame_type, channel_id, payload):
        if frame_type == FRAME_TYPE_BODY:
            self.sent_body_payloads.append(payload)
        super()._send_frame(frame_type, channel_id, payload)


@pytest.mark.timeout(10)
@pytestmark
async def test_frame_max(event_loop, rabbitmq_host):
    frame_max = 10 * 1024  # 10kB
    async with await connect(
            host=rabbitmq_host, loop=event_loop, frame_max=frame_max, connection_factory=FrameMaxConnection
    ) as connection:
        assert connection.frame_max == frame_max
        assert isinstance(connection, FrameMaxConnection)
        async with connection.channel() as channel:
            setup = await setup_channel(event_loop, channel)
            body = b''.join(
                bytes([i]) * (frame_max - 8)
                for i in range(7)
            ) + bytes([8]) * (7 * 8)
            assert len(body) == frame_max * 7
            await channel.publish(setup.exchange_name, setup.routing_key, body)

            # check published message was sent in body frames with size == frame-max except the last one
            assert len(connection.sent_body_payloads) == 8
            assert b''.join(connection.sent_body_payloads) == body
            for i, payload in enumerate(connection.sent_body_payloads[:-1]):
                assert payload == bytes([i]) * (10 * 1024 - 8)  # 7 bytes for frame header, 1 for frame end
            # 7 * 8 = num of frames with max_frame payload * leftover/frame
            assert connection.sent_body_payloads[-1] == bytes([8]) * (7 * 8)

            # get a message, check that it's split into body frames with size == frame-max except the last one
            message = await channel.get(setup.queue_name)

            assert message.body == body
            assert len(connection.received_body_frames) == 8
            for i, body_frame in enumerate(connection.received_body_frames[:-1]):
                assert body_frame.payload_size == 10 * 1024 - 8
                assert body_frame.payload == bytes([i]) * (10 * 1024 - 8)
            assert connection.received_body_frames[-1].payload_size == 7 * 8
            assert connection.received_body_frames[-1].payload == bytes([8]) * (7 * 8)
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)
