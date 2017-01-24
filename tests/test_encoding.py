from pytest import raises

from ammoo.wire.class_and_method import BASIC_GET_OK_CAM
from ammoo.message import PartialMessage, Message
from ammoo.channel import _ChannelToolbox
from ammoo.wire.frames import BasicHeaderFrame
from ammoo.wire.frames import BodyFrame
from ammoo.wire.frames import MethodFrame
from ammoo.wire.frames.header import BasicHeaderProperties
from ammoo.wire.frames.method.basic import BasicGetOkParameters


def test_content_encoding():
    body = 'pööböö'.encode('iso-8859-1')
    assert len(body) == 6

    partial_message = PartialMessage(
        # could use some mock for this
        channel_toolbox=_ChannelToolbox(
            server_has_capability=None,
            ack=None,
            reject=None,
            publish=None,
            default_encoding='utf-8'
        )
    )

    partial_message.feed_method_frame(MethodFrame(
        channel_id=1,
        cam=BASIC_GET_OK_CAM,
        parameters=BasicGetOkParameters(
            delivery_tag=0,
            redelivered=False,
            exchange_name='',
            routing_key='rk',
            message_count=1
        )
    ))
    assert not partial_message.feed_header_frame(BasicHeaderFrame(
        channel_id=1,
        body_size=len(body),
        properties=BasicHeaderProperties(
            content_type=None,
            content_encoding='iso-8859-1',
            headers=None,
            delivery_mode=None,
            priority=None,
            correlation_id=None,
            reply_to=None,
            expiration=None,
            message_id=None,
            timestamp=None,
            type_=None,
            user_id=None,
            app_id=None,
            cluster_id=None
        )
    ))
    message = partial_message.feed_body_frame(BodyFrame(
        channel_id=1,
        payload_size=len(body),
        payload=body
    ))
    assert isinstance(message, Message)
    assert message.decode() == 'pööböö'
    assert message.decode('iso-8859-1') == 'pööböö'
    with raises(UnicodeDecodeError) as ctx:
        assert message.decode('utf-8')
    assert str(ctx.value) == "'utf-8' codec can't decode byte 0xf6 in position 1: invalid start byte"
