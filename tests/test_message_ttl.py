import asyncio

import pytest
from pytest import raises

from ammoo.connect import connect
from ammoo.exceptions.channel import EmptyQueue
from ammoo_pytest_helpers import pytestmark, check_clean_connection_close, check_clean_channel_close


@pytest.mark.timeout(7)
@pytestmark
async def test_queue_ttl(event_loop, rabbitmq_host):
    queue_name = 'testcase_queue'
    exchange_name = 'testcase_exchange'
    routing_key = 'testcase_routing_key'
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.select_confirm()
            await channel.delete_queue(queue_name)
            await channel.declare_queue(queue_name, ttl=1000)
            await channel.delete_exchange(exchange_name)
            await channel.declare_exchange(exchange_name, 'direct')
            await channel.bind_queue(queue_name, exchange_name, routing_key)

            await channel.publish(exchange_name, routing_key, 'test body 1')
            message = await channel.get(queue_name)  # if we run slow and this takes over 1 secs, test may fail
            assert message.decode() == 'test body 1'
            await message.ack()

            await channel.publish(exchange_name, routing_key, 'test body 2')
            await asyncio.sleep(1.5, loop=event_loop)  # test may fail if rabbitmq hasn't deleted message in 0.5 secs
            with raises(EmptyQueue):
                await channel.get(queue_name)

            # this should work again
            await channel.publish(exchange_name, routing_key, 'test body 3')
            message = await channel.get(queue_name)  # if we run slow and this takes over 1 secs, test may fail
            assert message.decode() == 'test body 3'
            await message.ack()
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_message_ttl(event_loop, rabbitmq_host):
    queue_name = 'testcase_queue'
    exchange_name = 'testcase_exchange'
    routing_key = 'testcase_routing_key'
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.select_confirm()
            await channel.delete_queue(queue_name)
            await channel.declare_queue(queue_name)
            await channel.delete_exchange(exchange_name)
            await channel.declare_exchange(exchange_name, 'direct')
            await channel.bind_queue(queue_name, exchange_name, routing_key)

            # this one we fetch in time
            await channel.publish(exchange_name, routing_key, 'test body 1', expiration=1000)
            message = await channel.get(queue_name)  # if we run slow and this takes over 1 secs, test may fail
            assert message.decode() == 'test body 1'
            await message.ack()

            # and this one we don't
            await channel.publish(exchange_name, routing_key, 'test body 1', expiration='100')
            await asyncio.sleep(0.3, loop=event_loop)
            with raises(EmptyQueue):
                await channel.get(queue_name)  # if rabbitmq is slow this might succeed
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)
