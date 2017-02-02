import asyncio
from datetime import datetime

import pytest
from pytest import raises

from ammoo.connect import connect
from ammoo.exceptions.channel import ServerClosedChannel, EmptyQueue
from ammoo.wire.classes import CLASS_BASIC, CLASS_QUEUE
from ammoo.wire.methods import METHOD_BASIC_GET, METHOD_QUEUE_PURGE, METHOD_QUEUE_DECLARE
from ammoo_pytest_helpers import pytestmark, check_clean_connection_close, check_clean_channel_close, aenumerate, azip


@pytest.mark.timeout(7)
@pytestmark
async def test_queue_expires(event_loop, rabbitmq_host):
    queue_name = 'testcase_queue'
    exchange_name = 'testcase_exchange'
    routing_key = 'testcase_routing_key'
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.select_confirm()
            await channel.delete_exchange(exchange_name)
            await channel.declare_exchange(exchange_name, 'direct')
            await channel.delete_queue(queue_name)
            await channel.declare_queue(queue_name, expires=1000)
            await channel.bind_queue(queue_name, exchange_name, routing_key)

            # quick, let's publish and get a message before it dies
            await channel.publish(exchange_name, routing_key, 'test body 1')
            await channel.publish(exchange_name, routing_key, 'test body 2')
            message = await channel.get(queue_name)  # if we run slow and this takes over 1 secs, test may fail
            assert message.decode() == 'test body 1'
            await message.ack()

            await asyncio.sleep(1.5, loop=event_loop)
            with raises(ServerClosedChannel) as excinfo:
                await channel.get(queue_name)
            assert excinfo.value.reply_code == 404
            assert excinfo.value.reply_text == "NOT_FOUND - no queue 'testcase_queue' in vhost '/'"
            assert excinfo.value.class_id == CLASS_BASIC
            assert excinfo.value.method_id == METHOD_BASIC_GET
        assert channel._closing_exc is excinfo.value
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_queue_max_length(event_loop, rabbitmq_host):
    queue_name = 'testcase_queue'
    exchange_name = 'testcase_exchange'
    routing_key = 'testcase_routing_key'
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.select_confirm()
            await channel.delete_exchange(exchange_name)
            await channel.declare_exchange(exchange_name, 'direct')
            await channel.delete_queue(queue_name)
            await channel.declare_queue(queue_name, max_length=3)
            await channel.bind_queue(queue_name, exchange_name, routing_key)

            for i in [1, 2, 3, 4]:
                await channel.publish(exchange_name, routing_key, 'test body {}'.format(i))

            # oldest message is thrown out first
            for i in [2, 3, 4]:
                message = await channel.get(queue_name)
                assert message.body.decode() == 'test body {}'.format(i)

            with raises(EmptyQueue):
                await channel.get(queue_name)
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_queue_max_length_bytes(event_loop, rabbitmq_host):
    queue_name = 'testcase_queue'
    exchange_name = 'testcase_exchange'
    routing_key = 'testcase_routing_key'
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.select_confirm()
            await channel.delete_exchange(exchange_name)
            await channel.declare_exchange(exchange_name, 'direct')
            await channel.delete_queue(queue_name)
            await channel.declare_queue(queue_name, max_length_bytes=3*500*1024)
            await channel.bind_queue(queue_name, exchange_name, routing_key)

            for i in [1, 2, 3, 4]:
                # a bit less than the total limit/message count to make room for message/frame overhead
                body = str(i).encode('ascii') * (470 * 1024)
                await channel.publish(exchange_name, routing_key, body)

            # oldest message is thrown out first
            for i in [2, 3, 4]:
                message = await channel.get(queue_name)
                assert message.body == str(i).encode('ascii') * (470 * 1024)

            with raises(EmptyQueue):
                await channel.get(queue_name)
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_dead_letter_exchange(event_loop, rabbitmq_host):
    def check_message_1(message):
        assert message.body == b'message 1'
        assert message.routing_key == 'orig_rk1'
        x_death_headers = message.properties.headers['x-death']
        assert len(x_death_headers) == 1
        x_death_header, = x_death_headers
        assert x_death_header['count'] == 1
        assert x_death_header['reason'] == 'expired'
        assert x_death_header['queue'] == orig_queue_name_1
        assert isinstance(x_death_header['time'], datetime)  # TODO: convert this from UTC in datetime_pb
        assert x_death_header['exchange'] == orig_exchange_name
        assert x_death_header['routing-keys'] == ['orig_rk1']
    def check_message_2(message):
        assert message.body == b'message 2'
        # routing should have changed because of dead_letter_routing_key
        assert message.routing_key == 'dead_rk'
        x_death_headers = message.properties.headers['x-death']
        assert len(x_death_headers) == 1
        x_death_header, = x_death_headers
        assert x_death_header['count'] == 1
        assert x_death_header['reason'] == 'expired'
        assert x_death_header['queue'] == orig_queue_name_2
        assert isinstance(x_death_header['time'], datetime)
        assert x_death_header['exchange'] == orig_exchange_name
        assert x_death_header['routing-keys'] == ['orig_rk2']
    orig_queue_name_1 = 'testcase_queue_1'
    orig_queue_name_2 = 'testcase_queue_2'
    dead_queue_name = 'testcase_dead_queue'
    orig_exchange_name = 'testcase_orig_exchange'
    dead_exchange_name = 'testcase_dead_exchange'
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.select_confirm()
            await asyncio.gather(
                channel.delete_queue(orig_queue_name_1),
                channel.delete_queue(orig_queue_name_2),
                channel.delete_queue(dead_queue_name),
                channel.delete_exchange(dead_exchange_name),
                channel.delete_exchange(orig_exchange_name),
                loop=event_loop)
            # this is where the dead lettering puts rejected/expired messages
            await asyncio.gather(
                channel.declare_exchange(dead_exchange_name, 'direct'),
                channel.declare_queue(dead_queue_name),
                loop=event_loop)
            # queue 1 with dead lettering, keeps original routing key
            # queue 2 with dead lettering, modifies routing key
            await asyncio.gather(
                channel.declare_queue(orig_queue_name_1, dead_letter_exchange=dead_exchange_name, ttl=0),
                channel.declare_queue(
                    orig_queue_name_2, dead_letter_exchange=dead_exchange_name, dead_letter_routing_key='dead_rk',
                    ttl=0),
                channel.bind_queue(dead_queue_name, dead_exchange_name, 'orig_rk1'),
                channel.bind_queue(dead_queue_name, dead_exchange_name, 'dead_rk'),
                channel.declare_exchange(orig_exchange_name, 'direct'),
                loop=event_loop)

            await asyncio.gather(
                channel.bind_queue(orig_queue_name_1, orig_exchange_name, 'orig_rk1'),
                channel.bind_queue(orig_queue_name_2, orig_exchange_name, 'orig_rk2'),
                loop=event_loop)

            await channel.publish(orig_exchange_name, 'orig_rk1', 'message 1')
            await asyncio.sleep(0.1, loop=event_loop)
            for queue_name in orig_queue_name_1, orig_queue_name_2:
                with raises(EmptyQueue):
                    await channel.get(queue_name)
            message = await channel.get(dead_queue_name)
            check_message_1(message)

            await channel.publish(orig_exchange_name, 'orig_rk2', 'message 2')
            await asyncio.sleep(0.1, loop=event_loop)
            for queue_name in orig_queue_name_1, orig_queue_name_2:
                with raises(EmptyQueue):
                    await channel.get(queue_name)
            message = await channel.get(dead_queue_name)
            check_message_2(message)
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_priority(event_loop, rabbitmq_host):
    queue_name = 'testcase_queue'
    exchange_name = 'testcase_exchange'
    routing_key = 'testcase_routing_key'
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel(prefetch_count=2) as channel:
            await channel.select_confirm()
            await asyncio.gather(
                channel.delete_queue(queue_name),
                channel.delete_exchange(exchange_name),
                loop=event_loop)
            await asyncio.gather(
                channel.declare_exchange(exchange_name, 'direct'),
                channel.declare_queue(queue_name, max_priority=7),
                loop=event_loop)
            await channel.bind_queue(queue_name, exchange_name, routing_key)
            message_priorities = [0, 3, 1, 6, 7, 9, 4, 2, 8, 5]
            # list items are (message_priorities index, priority). max_priority 7 -> 7 sorts before 9
            expected_order = [(4, 7), (5, 9), (8, 8), (3, 6), (9, 5), (6, 4), (1, 3), (7, 2), (2, 1), (0, 0)]
            for i, message_priority in enumerate(message_priorities):
                await channel.publish(
                    exchange_name, routing_key, 'message {} priority {}'.format(i, message_priority),
                    priority=message_priority)
            async with channel.consume(queue_name) as consumer:
                async for i, (message, (send_i, expected_priority)) in aenumerate(azip(consumer, expected_order), 1):
                    assert message.body.decode() == 'message {} priority {}'.format(send_i, expected_priority)
                    assert message.properties.priority == expected_priority
                    await message.ack()
                    if i == len(message_priorities):
                        break
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_purge_no_queue_name(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        # first channel gets closed because we purge a nonexisting queue
        async with connection.channel() as channel:
            with raises(ServerClosedChannel) as excinfo:
                await channel.purge_queue('')
            assert excinfo.value.reply_code == 404
            assert excinfo.value.reply_text == "NOT_FOUND - no previously declared queue"
            assert excinfo.value.class_id == CLASS_QUEUE
            assert excinfo.value.method_id == METHOD_QUEUE_PURGE
        assert channel._closing_exc is excinfo.value

        # second purge ought to work ok
        async with connection.channel() as channel:
            await channel.select_confirm()
            queue_name_1 = (await channel.declare_queue('')).queue_name
            queue_name_2 = (await channel.declare_queue('')).queue_name
            # put different amount of messages to queues to know which one was purged based on purge-ok message count
            await channel.publish('', queue_name_1, 'q1 msg 1')
            await channel.publish('', queue_name_2, 'q2 msg 1')
            await channel.publish('', queue_name_2, 'q2 msg 2')
            assert queue_name_1 != queue_name_2

            assert await channel.purge_queue('') == 2  # queue 2 was purged
            assert await channel.purge_queue('') == 0  # queue 2 was purged again
            assert await channel.purge_queue(queue_name_1) == 1  # queue 1 purge with queue name
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_queue_exists(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            queue_name = 'testcase_queue'
            await channel.delete_queue(queue_name)
            # durable, to see that the queue.declare(passive=True) from queue_exists doesn't mind
            await channel.declare_queue(queue_name, durable=True)
            await channel.assert_queue_exists(queue_name)
            await channel.delete_queue(queue_name)
            with raises(ServerClosedChannel) as excinfo:
                await channel.assert_queue_exists(queue_name)
            assert excinfo.value.reply_code == 404
            assert excinfo.value.reply_text == "NOT_FOUND - no queue 'testcase_queue' in vhost '/'"
            assert excinfo.value.class_id == CLASS_QUEUE
            assert excinfo.value.method_id == METHOD_QUEUE_DECLARE
        assert channel._closing_exc is excinfo.value
    check_clean_connection_close(connection)
