import asyncio

import pytest
from pytest import raises

from ammoo.exceptions.channel import EmptyQueue, ServerClosedChannel
from ammoo.exceptions.connection import ServerClosedConnection
from ammoo.wire.classes import CLASS_EXCHANGE
from ammoo.wire.methods import METHOD_EXCHANGE_DECLARE

from ammoo_pytest_helpers import pytestmark, check_clean_connection_close, check_clean_channel_close


@pytest.mark.timeout(7)
@pytestmark
async def test_declare_invalid_exchange_type(connect_to_broker):
    exchange_name = 'testcase_exchange'
    async with await connect_to_broker() as connection:
        async with connection.channel() as channel:
            await channel.delete_exchange(exchange_name)
            with raises(ServerClosedConnection) as excinfo:
                await channel.declare_exchange(exchange_name, 'badtype')
            assert excinfo.value.reply_code == 503
            assert excinfo.value.reply_text == "COMMAND_INVALID - invalid exchange type 'badtype'"
            assert excinfo.value.class_id == CLASS_EXCHANGE
            assert excinfo.value.method_id == METHOD_EXCHANGE_DECLARE
        assert channel._closing_exc is excinfo.value
    assert connection._closing_exc is excinfo.value


@pytest.mark.timeout(7)
@pytestmark
async def test_exchange_to_exchange_bind(event_loop, connect_to_broker):
    queue_name = 'test_queue'
    src_exchange_name = 'test_source_exchange'
    dest_exchange_name = 'test_dest_exchange'
    routing_key = 'test_routing_key'
    async with await connect_to_broker() as connection:
        async with connection.channel() as channel:
            await asyncio.gather(
                channel.select_confirm(),
                channel.delete_exchange(src_exchange_name),
                channel.delete_exchange(dest_exchange_name),
                channel.delete_queue(queue_name),
                loop=event_loop)
            await asyncio.gather(
                channel.declare_exchange(src_exchange_name, 'direct'),
                channel.declare_exchange(dest_exchange_name, 'direct'),
                channel.declare_queue(queue_name),
                loop=event_loop)
            await channel.bind_queue(queue_name, dest_exchange_name, routing_key)

            # source exchange not yet bound to destination exchange, message does not end up in queue
            await channel.publish(src_exchange_name, routing_key, 'message 1')
            with raises(EmptyQueue):
                message = await channel.get(queue_name)
                assert message.body == b'message 1'

            await channel.bind_exchange(dest_exchange_name, src_exchange_name, routing_key)
            await channel.publish(src_exchange_name, routing_key, 'message 2')
            message = await channel.get(queue_name)
            assert message.body.decode() == 'message 2'
            assert message.routing_key == routing_key
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_alternate_exchange(event_loop, connect_to_broker):
    async with await connect_to_broker() as connection:
        orig_exchange_name = 'test_orig_ex'
        alternate_exchange_name = 'test_alt_ex'
        queue_name = 'test_q'
        async with connection.channel() as channel:
            await asyncio.gather(
                channel.select_confirm(),
                channel.delete_exchange(orig_exchange_name),
                channel.delete_exchange(alternate_exchange_name),
                channel.delete_queue(queue_name),
                loop=event_loop
            )
            await asyncio.gather(
                channel.declare_exchange(alternate_exchange_name, 'fanout'),
                channel.declare_queue(queue_name),
                loop=event_loop
            )
            await asyncio.gather(
                channel.bind_queue(queue_name, alternate_exchange_name, ''),
                channel.declare_exchange(orig_exchange_name, 'direct', alternate_exchange_name=alternate_exchange_name),
                loop=event_loop
            )

            await channel.publish(orig_exchange_name, 'test_rk', 'alternate exchange test message')
            message = await channel.get(queue_name)
            assert message.body.decode() == 'alternate exchange test message'
            assert message.routing_key == 'test_rk'


@pytest.mark.timeout(7)
@pytestmark
async def test_exchange_exists(connect_to_broker):
    async with await connect_to_broker() as connection:
        async with connection.channel() as channel:
            exchange_name = 'testcase_exchange'
            await channel.delete_exchange(exchange_name)
            # durable, to see that the exchange.declare(passive=True) from exchange_exists doesn't mind
            await channel.declare_exchange(exchange_name, 'topic', durable=True)
            await channel.assert_exchange_exists(exchange_name)
            await channel.delete_exchange(exchange_name)
            with raises(ServerClosedChannel) as excinfo:
                await channel.assert_exchange_exists(exchange_name)
            assert excinfo.value.reply_code == 404
            assert excinfo.value.reply_text == "NOT_FOUND - no exchange 'testcase_exchange' in vhost '/'"
            assert excinfo.value.class_id == CLASS_EXCHANGE
            assert excinfo.value.method_id == METHOD_EXCHANGE_DECLARE
        assert channel._closing_exc is excinfo.value
    check_clean_connection_close(connection)
