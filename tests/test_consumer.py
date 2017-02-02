import asyncio
import logging

import pytest
from ammoo.wire.methods import METHOD_BASIC_CONSUME

from ammoo.wire.classes import CLASS_BASIC
from pytest import raises

from ammoo.connect import connect
from ammoo.consumer import Consumer
from ammoo.exceptions.channel import ServerClosedChannel
from ammoo.exceptions.consumer import ServerCancelledConsumer
from ammoo.message import DeliverMessage
from ammoo_pytest_helpers import pytestmark, check_clean_connection_close, check_clean_channel_close, \
    check_clean_consumer_cancel, aiter, anext, aenumerate, setup_channel


@pytest.mark.timeout(7)
@pytestmark
async def test_consume_and_cancel(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.select_confirm()
            await channel.delete_queue('testcase_queue')
            await channel.declare_queue('testcase_queue')
            await channel.delete_exchange('testcase_exchange')
            await channel.declare_exchange('testcase_exchange', 'direct')
            await channel.bind_queue('testcase_queue', 'testcase_exchange', 'testcase_routing_key')
            await channel.publish('testcase_exchange', 'testcase_routing_key', b'message one')
            async with channel.consume('testcase_queue') as consumer:
                assert isinstance(consumer, Consumer)
                await channel.publish('testcase_exchange', 'testcase_routing_key', b'message two')
                assert consumer._message_queue.qsize() == 2, consumer._message_queue.qsize()
                async for i, message in aenumerate(consumer, 1):
                    assert isinstance(message, DeliverMessage)
                    await message.ack()
                    if i == 2:
                        break
            check_clean_consumer_cancel(consumer)
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_server_cancel(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.delete_queue('testcase_queue')
            await channel.declare_queue('testcase_queue')
            async with channel.consume('testcase_queue') as consumer:
                assert isinstance(consumer, Consumer)
                async def go():
                    async for message in consumer:
                        raise ValueError("Didn't expect to get a message: {!r}".format(message))
                    raise ValueError('consumer iteration done')
                task = event_loop.create_task(go())
                await channel.delete_queue('testcase_queue')
                with raises(ServerCancelledConsumer) as excinfo:
                    await task
            assert consumer._closing_exc is excinfo.value
            await channel.delete_queue('testcase_queue')  # just testing operations on channel still work
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_exclusive(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.delete_queue('testcase_queue')
            await channel.declare_queue('testcase_queue')
            consuming_event = asyncio.Event(loop=event_loop)  # go() sets this when it's consuming
            cancel_event = asyncio.Event(loop=event_loop)  # go() will cancel the consumer when it's set
            async def go():
                async with channel.consume('testcase_queue') as consumer:
                    consuming_event.set()
                    await cancel_event.wait()
                    with raises(ServerClosedChannel) as excinfo:
                        async for message in consumer:
                            raise ValueError('Consumer got message: {}'.format(message))
                        raise ValueError()
                return excinfo.value
            task = event_loop.create_task(go())
            await consuming_event.wait()
            with raises(ServerClosedChannel) as excinfo:
                async with channel.consume('testcase_queue', exclusive=True):
                    pass
            assert excinfo.value.reply_code == 403
            assert excinfo.value.reply_text == "ACCESS_REFUSED - queue 'testcase_queue' in vhost '/' in exclusive use"
            assert excinfo.value.class_id == CLASS_BASIC
            assert excinfo.value.method_id == METHOD_BASIC_CONSUME
            cancel_event.set()
            go_exc = await task
            assert go_exc is excinfo.value
        assert channel._closing_exc is excinfo.value
    check_clean_connection_close(connection)


@pytest.mark.timeout(15)
@pytestmark
async def test_qos(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        prefetch_count = 23
        message_count = prefetch_count + 10
        async with connection.channel(prefetch_count=prefetch_count) as channel:
            setup = await setup_channel(event_loop, channel)
            for i in range(1, message_count+1):
                await channel.publish(setup.exchange_name, setup.routing_key, 'message {}'.format(i))
            # no_ack must be false, otherwise broker will deliver as many messages as possible
            async with channel.consume(setup.queue_name, no_ack=False) as consumer:
                # gice broker a chance to deliver prefetch_count messages to us
                await asyncio.sleep(1.0, loop=event_loop)
                async for i, message in aenumerate(consumer, 1):
                    assert message.body.decode() == 'message {}'.format(i)
                    messages_left = message_count - i
                    if prefetch_count <= messages_left:
                        # queue object on Consumer() has one message less, this one
                        assert consumer._message_queue.qsize() == prefetch_count - 1, i
                    else:
                        assert consumer._message_queue.qsize() == messages_left, i
                    await message.ack()
                    await asyncio.sleep(0.1, loop=event_loop)
                    # and now it should be back up to prefetch count
                    if prefetch_count <= messages_left:
                        assert consumer._message_queue.qsize() == prefetch_count, i
                    else:
                        assert consumer._message_queue.qsize() == messages_left, i
                    if i == message_count:
                        assert consumer._message_queue.qsize() == 0
                        break
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.rabbitmq36
@pytest.mark.timeout(15)
@pytestmark
async def test_consumer_priority(event_loop, rabbitmq_host):
    """
    - Set qos prefetch count to 2
    - Publishes 10 messages to a queue
    - Start two consumers, one with priority 5 and one with 0
    - Coordinate message acks in consumers so that broker will deliver messages to the high priority one when it can
      and to the low one when the high one is blocked
    - Check both consumers get the messages coordination from previous step should result in
    """
    low_to_high = asyncio.Queue(loop=event_loop)  # low signals to high that high can continue
    high_to_low = asyncio.Queue(loop=event_loop)  # and vice versa

    async def consume_high(consumer):
        """This one takes messages 0, 1 and 4"""
        message_iter = aiter(consumer)
        message0 = await anext(message_iter)
        assert message0.body == b'message 0'
        message1 = await anext(message_iter)
        assert message1.body == b'message 1'
        assert consumer._message_queue.qsize() == 0  # now messages go to the low consumer too until we ack
        await message0.ack()  # our queue gets another one
        assert (await low_to_high.get()) == 'l1'
        message4 = await anext(message_iter)
        assert message4.body == b'message 4'
        assert consumer._message_queue.qsize() == 0  # now messages go to the low consumer too until we ack
        high_to_low.put_nowait('h1')  # signal that low can take rest of messages
        assert (await low_to_high.get()) == 'l2'
        await message1.ack()
        await message4.ack()
        assert consumer._message_queue.qsize() == 0
        with raises(asyncio.TimeoutError):
            await asyncio.wait_for(anext(message_iter), 1.0, loop=event_loop)
        assert consumer._message_queue.qsize() == 0

    async def consume_low(consumer):
        """This one takes messages 2, 3 and 5-9"""
        message_iter = aiter(consumer)
        message2 = await anext(message_iter)
        assert message2.body == b'message 2'
        message3 = await anext(message_iter)
        assert message3.body == b'message 3'
        assert consumer._message_queue.qsize() == 0
        low_to_high.put_nowait('l1')  # signal that high can ack a message
        assert (await high_to_low.get()) == 'h1'
        await message2.ack()
        await message3.ack()
        async for i, message in aenumerate(message_iter, 5):
            assert message.decode() == 'message {}'.format(i)
            await message.ack()
            if i == 9:
                break
        assert consumer._message_queue.qsize() == 0
        low_to_high.put_nowait('l2')  # go on then, nothing left for high

    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel(prefetch_count=2) as channel:
            setup = await setup_channel(event_loop, channel)
            for i in range(10):
                await channel.publish(setup.exchange_name, setup.routing_key, 'message {}'.format(i))
            async with channel.consume(setup.queue_name, priority=5) as consumer_high:
                async with channel.consume(setup.queue_name) as consumer_low:  # implicit priority 0
                    high_task = event_loop.create_task(consume_high(consumer_high))
                    low_task = event_loop.create_task(consume_low(consumer_low))
                    await asyncio.gather(high_task, low_task, loop=event_loop)
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(15)
@pytestmark
async def test_recover(event_loop, rabbitmq_host):
    logger = logging.getLogger('test_recover')
    message_n = 10
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel(prefetch_count=50) as channel:
            setup = await setup_channel(event_loop, channel)
            # TODO: test this, it causes the server to send basic.ack with multiple=True which we don't support yet
            await asyncio.gather(*(
                event_loop.create_task(
                    channel.publish(setup.exchange_name, setup.routing_key, 'message {}'.format(i))
                ) for i in range(10)
            ), loop=event_loop)
            #for i in range(10):
            #    await channel.publish(setup.exchange_name, setup.routing_key, 'message {}'.format(i))
            async with channel.consume(setup.queue_name) as consumer:
                async for i, message in aenumerate(consumer):
                    logger.info(
                        'Got message number %s: redelivered = %s body = %r',
                        i, message.redelivered, message.body)
                    if i < 10:
                        assert message.decode() == 'message {}'.format(i)
                        assert message.redelivered is False
                    else:
                        # 10 -> 1, 11 -> 3, 12 -> 5 etc
                        assert message.decode() == 'message {}'.format((i - 10) * 2 +1 )
                        assert message.redelivered is True
                    if i % 2 == 0 or i >= 10:
                        await message.ack()  # messages 0, 2, 4, 6, 8 and 10+
                    elif i == 9:
                        assert consumer._message_queue.qsize() == 0
                        await channel.recover(requeue=True)
                        await asyncio.sleep(0.2, loop=event_loop)
                        assert consumer._message_queue.qsize() == 5  # 10 - 5 = 5
                    if i == 14:
                        assert consumer._message_queue.qsize() == 0
                        break
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)
