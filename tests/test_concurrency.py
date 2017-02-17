from collections import namedtuple

import asyncio

import logging
import pytest
from pytest import raises

from ammoo.exceptions.channel import EmptyQueue
from ammoo.channel import Channel

from ammoo_pytest_helpers import pytestmark, check_clean_connection_close, check_clean_channel_close

Names = namedtuple('Names', 'exchange_name get_queue_name get_routing_key consume_queue_name consume_routing_key')


async def setup_concurrent_queues(
        channel,
        exchange_name='test_concurrent_exchange',
        get_queue_name='test_get_queue',
        get_routing_key='get_rk',
        consume_queue_name='test_consume_queue',
        consume_routing_key='consume_rk',
):
    await channel.delete_exchange(exchange_name)
    await channel.declare_exchange(exchange_name, 'direct')

    await channel.delete_queue(get_queue_name)
    await channel.declare_queue(get_queue_name)
    await channel.bind_queue(get_queue_name, exchange_name, get_routing_key)

    await channel.delete_queue(consume_queue_name)
    await channel.declare_queue(consume_queue_name)
    await channel.bind_queue(consume_queue_name, exchange_name, consume_routing_key)

    return Names(exchange_name, get_queue_name, get_routing_key, consume_queue_name, consume_routing_key)


async def publish_messages(channel, iterations, names: Names, batch_size, loop):
    logger = logging.getLogger('publisher')
    for iteration in range(iterations):
        logger.info('Starting iteration %s', iteration)
        coros = []
        for i in range(iteration * batch_size, (iteration + 1) * batch_size):
            logger.info('Publishing message %s', i)
            if i % 4 == 0:
                routing_key = names.get_routing_key
            else:
                routing_key = names.consume_routing_key
            coro = channel.publish(names.exchange_name, routing_key, json=i)
            coros.append(coro)
        tasks = [loop.create_task(coro) for coro in coros]
        logger.info('Created tasks')
        await asyncio.gather(*tasks, loop=loop)
        logger.info('Gathered tasks')
        logger.info('Iteration %s done', iteration)
    logger.info('Publisher done')


async def consume_messages(channel: Channel, names: Names, do_ack: bool, loop, batch_size, consumed_ids: set):
    logger = logging.getLogger('consumer {}'.format('acks' if do_ack else 'noacks'))
    tasks = []
    async with channel.consume(names.consume_queue_name, no_ack=not do_ack) as consumer:
        i = 0
        async for message in consumer:
            message_id = message.json()
            logger.info('Received message %s with id %s', i, message_id)
            consumed_ids.add(message_id)
            i += 1
            if do_ack:
                tasks.append(loop.create_task(message.ack()))
                if i % batch_size == 0:
                    logger.info('Waiting for acks to complete')
                    await asyncio.gather(*tasks, loop=loop)
                    tasks = []


@pytest.mark.timeout(60)
@pytestmark
async def test_concurrency(connect_to_broker, event_loop):
    logger = logging.getLogger('testcase')
    iterations = 10
    publish_batch_size = 300
    async with await connect_to_broker() as connection:
        async with connection.channel() as channel_one, connection.channel() as channel_two:
            names = await setup_concurrent_queues(channel_one)
            consumed_ids = set()
            ack_consumer_task = event_loop.create_task(consume_messages(channel_one, names, True, event_loop, 100, consumed_ids))
            noack_consumer_one_task = event_loop.create_task(consume_messages(channel_two, names, True, event_loop, 500, consumed_ids))
            publish_task = event_loop.create_task(publish_messages(
                channel_one, iterations, names, publish_batch_size, event_loop))
            get_message_count = (iterations * publish_batch_size) // 4
            get_ids = set()
            while len(get_ids) < get_message_count:
                logger.info('Got %s/%s messages', len(get_ids), get_message_count)
                try:
                    message = await channel_two.get(names.get_queue_name, no_ack=False)
                except EmptyQueue:
                    logger.info('No message in queue, sleeping a bit')
                    await asyncio.sleep(0.1, loop=event_loop)
                else:
                    get_ids.add(message.json())
                    await message.ack()
            await publish_task
            consume_message_count = (3 * iterations * publish_batch_size) // 4
            while len(consumed_ids) < consume_message_count:
                logger.info('%s/%s messages consumed', len(consumed_ids), consume_message_count)
                await asyncio.sleep(0.1, loop=event_loop)
            for consumer_task in ack_consumer_task, noack_consumer_one_task:
                assert not consumer_task.done()
                consumer_task.cancel()
                with raises(asyncio.CancelledError):
                    await consumer_task
            assert not channel_one._consumers
            assert not channel_two._consumers
            assert get_ids | consumed_ids == set(range(iterations * publish_batch_size))
        check_clean_channel_close(channel_one)
        check_clean_channel_close(channel_two)
    check_clean_connection_close(connection)
