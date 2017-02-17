import pytest
from pytest import raises

from ammoo.exceptions.channel import EmptyQueue
from ammoo_pytest_helpers import pytestmark


# marking as rabbitmq36, because the non-string values do not seem to work on 3.3
@pytest.mark.rabbitmq36
@pytest.mark.timeout(7)
@pytestmark
async def test_headers_bind(connect_to_broker):
    """Set up to queues and bind them to an exchange, one with x-match all and one with x-match any"""
    async with await connect_to_broker() as connection:
        async with connection.channel() as channel:
            await channel.select_confirm()
            all_queues = 'test_match_all_queue', 'test_match_any_queue'
            for queue_name in all_queues:
                await channel.delete_queue(queue_name)
                await channel.declare_queue(queue_name)
            await channel.delete_exchange('testcase_exchange')
            await channel.declare_exchange('testcase_exchange', 'headers')
            # XXX: Having an arguments table value of type shortstr crashes rabbitmq: {'a': (123, b'I'), 'b': ('hello world', b's')}
            # library uses b'I' as default type for ints and longstr for strings and bytes
            await channel.bind_queue(
                'test_match_all_queue', 'testcase_exchange',
                {'a': 123, 'b': b'hello world'}  # if x-match is not set, rabbitmq defaults to all
            )
            await channel.bind_queue(
                'test_match_any_queue', 'testcase_exchange',
                {'a': 123, 'b': 'hello world', 'x-match': b'any'}
            )

            # this message uses a routing key, not header table -> not routed to queue
            await channel.publish('testcase_exchange', 'testcase_routing_key', 'test body 1')
            for queue_name in all_queues:
                with raises(EmptyQueue):
                    await channel.get(queue_name)

            # this message should be routed ok to both queues
            await channel.publish('testcase_exchange', {'a': 123, 'b': 'hello world'}, 'test body 2')
            for queue_name in all_queues:
                message = await channel.get(queue_name)
                assert message.decode() == 'test body 2'
                await message.ack()

            # key b not set -> only any gets it
            await channel.publish('testcase_exchange', {'a': 123}, 'test body 3')
            with raises(EmptyQueue):
                await channel.get('test_match_all_queue')
            message = await channel.get('test_match_any_queue')
            assert message.decode() == 'test body 3'
            await message.ack()

            # b != hello world -> only any gets it
            await channel.publish('testcase_exchange', {'a': 123, 'b': 'hello kitty'}, 'test body 4')
            with raises(EmptyQueue):
                await channel.get('test_match_all_queue')
            message = await channel.get('test_match_any_queue')
            assert message.decode() == 'test body 4'
            await message.ack()

            # neither key matches -> neither queue gets it
            await channel.publish(
                'testcase_exchange',
                {
                    'a': 567,
                    'b': b'hello kitty',
                },
                'test body 5')
            for queue_name in all_queues:
                with raises(EmptyQueue):
                    await channel.get(queue_name)
