import asyncio

import pytest
from pytest import raises

from ammoo.channel import Channel
from ammoo.connect import connect
from ammoo.exceptions.channel import EmptyQueue
from ammoo.message import Message, GetMessage
from ammoo_pytest_helpers import pytestmark, check_clean_connection_close, check_clean_channel_close


@pytest.mark.timeout(7)
@pytestmark
async def test_open_channel(connect_to_broker):
    async with await connect_to_broker() as connection:
        async with connection.channel() as channel:
            assert isinstance(channel, Channel)
            assert channel.channel_id == 1
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_exchange_declare(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.delete_exchange('testcase_exchange')
            await channel.declare_exchange('testcase_exchange', 'fanout')
            await channel.delete_exchange('testcase_exchange')
            await channel.delete_exchange('testcase_exchange')
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_queue_declare(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.delete_queue('testcase_queue')
            await channel.declare_queue('testcase_queue')
            assert await channel.purge_queue('testcase_queue') == 0
            assert await channel.delete_queue('testcase_queue') == 0
            assert await channel.delete_queue('testcase_queue') == 0
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_queue_bind(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.declare_queue('testcase_queue')
            await channel.declare_exchange('testcase_exchange', 'fanout')
            await channel.bind_queue('testcase_queue', 'testcase_exchange', 'test_routing_key')
            await channel.unbind_queue('testcase_queue', 'testcase_exchange', 'test_routing_key')
            await channel.unbind_queue('testcase_queue', 'testcase_exchange', 'test_routing_key')
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_publish_bytes(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.declare_exchange('testcase_exchange', 'fanout')
            await channel.select_confirm()  # so server has a chance to nack/close
            await channel.publish('testcase_exchange', 'test_routing_key', b'abc')  # bytes
            await channel.publish('testcase_exchange', 'test_routing_key', 'böö')  # str
            await channel.delete_exchange('testcase_exchange')
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_publish_json(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.declare_exchange('testcase_exchange', 'fanout')
            await channel.select_confirm()
            await channel.publish('testcase_exchange', 'test_routing_key', json=['string', {'dict': 123}])
            await channel.delete_exchange('testcase_exchange')
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_get_empty(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.delete_queue('testcase_queue')
            await channel.declare_queue('testcase_queue')
            with raises(EmptyQueue):
                await channel.get('testcase_queue')
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_get_ok(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.select_confirm()
            await channel.delete_queue('testcase_queue')
            await channel.declare_queue('testcase_queue')
            await channel.delete_exchange('testcase_exchange')
            await channel.declare_exchange('testcase_exchange', 'direct')
            await channel.bind_queue('testcase_queue', 'testcase_exchange', 'testcase_routing_key')
            with raises(EmptyQueue):
                await channel.get('testcase_queue')
            await channel.publish('testcase_exchange', 'testcase_routing_key', b'testcase_body')
            message = await channel.get('testcase_queue')
            assert isinstance(message, Message)
            assert message.body == b'testcase_body'
            await message.ack()
            await asyncio.sleep(0.05, loop=event_loop)
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_empty_message(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.select_confirm()
            await channel.delete_queue('testcase_queue')
            await channel.declare_queue('testcase_queue')
            await channel.delete_exchange('testcase_exchange')
            await channel.declare_exchange('testcase_exchange', 'direct')
            await channel.bind_queue('testcase_queue', 'testcase_exchange', 'testcase_routing_key')
            await channel.publish('testcase_exchange', 'testcase_routing_key', b'')
            message = await channel.get('testcase_queue')
            assert isinstance(message, GetMessage)
            assert message.body == b''
            await message.ack()
            await asyncio.sleep(0.05, loop=event_loop)
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_message_reject(event_loop, rabbitmq_host):
    queue_name = 'testcase_queue'
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.select_confirm()
            await channel.delete_queue(queue_name)
            await channel.declare_queue(queue_name)

            # requeue false
            await channel.publish('', queue_name, b'test message 1')
            message = await channel.get(queue_name)
            assert message.body == b'test message 1'
            await message.reject(requeue=False)
            await asyncio.sleep(0.1, loop=event_loop)
            with raises(EmptyQueue):
                await channel.get(queue_name)

            # requeue true
            await channel.publish('', queue_name, b'test message 2')
            message = await channel.get(queue_name)
            assert message.body == b'test message 2'
            await message.reject(requeue=True)
            message = await channel.get(queue_name)
            assert message.body == b'test message 2'
            await message.ack()
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_multiple_select_confirms(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.select_confirm()
            await channel.select_confirm()
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)
