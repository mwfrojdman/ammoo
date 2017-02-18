import pytest

from ammoo.wire.classes import CLASS_BASIC
from ammoo.wire.frames.body import FRAME_TYPE_BODY
from pytest import raises

from ammoo.exceptions.channel import ServerClosedChannel
from ammoo.exceptions.connection import ServerClosedConnection
from ammoo.wire.methods import METHOD_BASIC_PUBLISH
from ammoo_pytest_helpers import pytestmark, check_clean_connection_close


def check_exchange_not_found(excinfo):
    exc = excinfo.value
    assert isinstance(exc, ServerClosedChannel)
    assert exc.reply_code == 404
    assert exc.reply_text == "NOT_FOUND - no exchange 'testcase_exchange' in vhost '/'"
    assert exc.class_id == CLASS_BASIC
    assert exc.method_id == METHOD_BASIC_PUBLISH


@pytest.mark.timeout(7)
@pytestmark
async def test_caught_channel_exception(connect_to_broker):
    """A Channel closed exception raised inside async with channel() should not escape the context manager
    when it's caught inside the context manager"""
    exchange_name = 'testcase_exchange'
    routing_key = 'testcase_routing_key'
    async with await connect_to_broker() as connection:
        async with connection.channel() as channel:
            # without this channel.publish won't raise an exception, instead it occurs in channel.__aexit__
            await channel.select_confirm()
            await channel.delete_exchange(exchange_name)
            with raises(ServerClosedChannel) as excinfo:
                await channel.publish(exchange_name, routing_key, 'test body 1')
            check_exchange_not_found(excinfo)
        assert channel._closing_exc is excinfo.value
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_uncaught_channel_exception(event_loop, connect_to_broker):
    """A Channel closed exception raised inside async with channel() should escape the context manager
    when it's not caught inside it"""
    exchange_name = 'testcase_exchange'
    routing_key = 'testcase_routing_key'
    async with await connect_to_broker() as connection:
        delete_succeeded = False
        publish_succeeded = False
        with raises(ServerClosedChannel) as excinfo:
            async with connection.channel() as channel:
                # without this channel.publish won't raise an exception, instead it occurs in channel.__aexit__
                await channel.select_confirm()
                await channel.delete_exchange(exchange_name)
                delete_succeeded = True
                await channel.publish(exchange_name, routing_key, 'test body 1')  # this ought to raise an exception
                publish_succeeded = True
        assert delete_succeeded
        assert not publish_succeeded
        check_exchange_not_found(excinfo)
    check_clean_connection_close(connection)


def _assert_unexpcted_frame(exc):
    assert isinstance(exc, ServerClosedConnection)
    assert exc.reply_code == 505
    assert exc.reply_text == "UNEXPECTED_FRAME - expected method frame, got non method frame instead"
    assert exc.class_id == 0
    assert exc.method_id == 0


@pytest.mark.timeout(7)
@pytestmark
async def test_caught_connection_exception_in_channel(connect_to_broker):
    """A Connection closed exception raised inside async with channel() should not escape the context manager
    when it's caught inside the context manager"""
    exchange_name = 'testcase_exchange'
    routing_key = 'testcase_routing_key'
    async with await connect_to_broker() as connection:
        async with connection.channel() as channel:
            # without this channel.publish won't raise an exception, instead it occurs in channel.__aexit__
            await channel.select_confirm()
            await channel.delete_exchange(exchange_name)
            # send a frame which causes the server to close the connection
            connection._send_frame(
                frame_type=FRAME_TYPE_BODY,
                channel_id=channel._channel_id + 1,
                payload=b'xyz'
            )
            with raises(ServerClosedConnection) as excinfo:
                await channel.publish(exchange_name, routing_key, 'test body 1')
            _assert_unexpcted_frame(excinfo.value)
        assert channel._closing_exc is excinfo.value
    assert connection._closing_exc is excinfo.value
