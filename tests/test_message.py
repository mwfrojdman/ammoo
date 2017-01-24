import asyncio
import pytest
from pytest import raises

from ammoo.connect import connect
from ammoo.exceptions.channel import EmptyQueue
from ammoo.message import GetMessage
from tests.conftest import pytestmark, setup_channel, check_clean_channel_close, check_clean_connection_close


@pytest.mark.timeout(7)
@pytestmark
async def test_reply(event_loop, rabbitmq_host):
    """Publish a message with reply_to and correlation_id, get it from queue, use Message.reply(), get the reply message
    from the server"""
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.select_confirm()
            await channel.delete_queue('testcase_queue')
            await channel.declare_queue('testcase_queue')
            await channel.delete_exchange('testcase_exchange')
            await channel.declare_exchange('testcase_exchange', 'direct')
            await channel.bind_queue('testcase_queue', 'testcase_exchange', 'testcase_routing_key')

            reply_queue_name = (await channel.declare_queue('', exclusive=True, durable=False)).queue_name
            await channel.publish(
                'testcase_exchange', 'testcase_routing_key', 'original body', correlation_id='test correlation id',
                reply_to=reply_queue_name)
            orig_message = await channel.get('testcase_queue')
            assert orig_message.properties.correlation_id == 'test correlation id'
            assert orig_message.decode() == 'original body'
            await orig_message.reply(b'reply body')
            await orig_message.ack()

            reply_message = await channel.get(reply_queue_name, no_ack=True)
            assert reply_message.properties.correlation_id == 'test correlation id'
            assert reply_message.body == b'reply body'
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)
