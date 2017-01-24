import asyncio
from datetime import datetime, timezone, timedelta

import pytest
from pytest import raises

from ammoo.wire.methods import METHOD_BASIC_PUBLISH
from ammoo.wire.classes import CLASS_BASIC
from ammoo.connect import connect
from ammoo.exceptions.channel import EmptyQueue, ServerClosedChannel
from ammoo.exceptions.connection import ServerClosedConnection
from ammoo.wire.frames.method.channel import ChannelCloseParameters
from ammoo.wire.frames.method.connection import ConnectionCloseParameters
from ammoo.wire.low.misc import DELIVERY_MODE_NON_PERSISTENT, DELIVERY_MODE_PERSISTENT
from tests.conftest import pytestmark, check_clean_channel_close, check_clean_connection_close, setup_channel


@pytest.mark.timeout(7)
@pytestmark
async def test_cc(event_loop, rabbitmq_host):
    vanilla_queue_name = 'testcase_queue'
    cc_queue_name = 'testcase_cc_queue'
    bcc_queue_name = 'testcase_bcc_queue'
    exchange_name = 'testcase_exchange'
    routing_key = 'testcase_routing_key'
    cc_routing_key = 'testcase_cc_routing_key'
    bcc_routing_key = 'testcase_bcc_routing_key'
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await channel.select_confirm()
            await asyncio.gather(
                channel.delete_queue(vanilla_queue_name),
                channel.delete_queue(cc_queue_name),
                channel.delete_queue(bcc_queue_name),
                channel.delete_exchange(exchange_name),
                loop=event_loop)
            await asyncio.gather(
                channel.declare_queue(vanilla_queue_name),
                channel.declare_queue(cc_queue_name),
                channel.declare_queue(bcc_queue_name),
                channel.declare_exchange(exchange_name, 'direct'),
                loop=event_loop)
            await asyncio.gather(
                channel.bind_queue(vanilla_queue_name, exchange_name, routing_key),
                channel.bind_queue(cc_queue_name, exchange_name, cc_routing_key),
                channel.bind_queue(bcc_queue_name, exchange_name, bcc_routing_key),
                loop=event_loop)

            def check_message(message):
                assert message.body == b'message body'
                assert message.routing_key == routing_key
                assert message.properties.headers['CC'] == [cc_routing_key]
                assert 'BCC' not in message.properties.headers

            await channel.publish(exchange_name, routing_key, 'message body', cc=cc_routing_key, bcc=[bcc_routing_key])
            await asyncio.sleep(0.05, loop=event_loop)
            for queue_name in vanilla_queue_name, cc_queue_name, bcc_queue_name:
                message = await channel.get(queue_name)
                check_message(message)
                with raises(EmptyQueue):
                    await channel.get(queue_name)

        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_delivery_mode(event_loop, rabbitmq_host):
    exchange_name = 'test_ex'
    routing_key = 'test_rk'
    queue_name = 'test_q'
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            await asyncio.gather(
                channel.select_confirm(),
                channel.delete_exchange(exchange_name),
                channel.delete_queue(queue_name),
                loop=event_loop)
            await asyncio.gather(
                channel.declare_exchange(exchange_name, 'direct'),
                channel.declare_queue(queue_name),
                loop=event_loop)
            await channel.bind_queue(queue_name, exchange_name, routing_key)

            await channel.publish(
                exchange_name, routing_key, 'non-persistent message 1', delivery_mode=DELIVERY_MODE_NON_PERSISTENT)
            await channel.publish(exchange_name, routing_key, 'persistent message 2', delivery_mode=2)
            # XXX: test how messages survive RabbitMQ restart?
            message = await channel.get(queue_name)
            assert message.decode() == 'non-persistent message 1'
            assert message.properties.delivery_mode == DELIVERY_MODE_NON_PERSISTENT
            message = await channel.get(queue_name)
            assert message.decode() == 'persistent message 2'
            assert message.properties.delivery_mode == DELIVERY_MODE_PERSISTENT
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_timestamp(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            setup = await setup_channel(event_loop, channel)
            timestamp = datetime.now(tz=timezone.utc)
            await channel.publish(setup.exchange_name, setup.routing_key, 'test body', timestamp=timestamp)
            timestamp = timestamp.replace(microsecond=0)  # resolution is 1 sec
            message = await channel.get(setup.queue_name)
            assert message.body == b'test body'
            assert isinstance(message.properties.timestamp, datetime)
            assert message.properties.timestamp == timestamp
            assert message.properties.timestamp.utcoffset() == timedelta(0)
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_content_encoding(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            setup = await setup_channel(event_loop, channel)

            # defaults
            await channel.publish(
                setup.exchange_name, setup.routing_key, 'lë böö 0')
            message = await channel.get(setup.queue_name)
            assert len(message.body) == 11  # utf-8
            assert message.properties.content_encoding is None
            assert message.decode() == 'lë böö 0'
            assert message.decode('utf-8') == 'lë böö 0'

            # boolean with encoding
            await channel.publish(
                setup.exchange_name, setup.routing_key, 'lë böö 1', encoding='iso-8859-1', content_encoding=True)
            message = await channel.get(setup.queue_name)
            assert len(message.body) == 8  # iso-8859 chars are always 1 byte
            assert message.properties.content_encoding == 'iso-8859-1'
            assert message.decode() == 'lë böö 1'

            # boolean without encoding, fallback to channel default
            await channel.publish(
                setup.exchange_name, setup.routing_key, 'lë böö 2', content_encoding=True)
            message = await channel.get(setup.queue_name)
            assert len(message.body) == 11  # utf-8 non-ascii chars 2 bytes each in this case
            assert message.properties.content_encoding == 'utf-8'
            assert message.properties.content_encoding == channel.body_encoding
            assert message.decode() == 'lë böö 2'

            # string, no encoding
            await channel.publish(
                setup.exchange_name, setup.routing_key, 'lë böö 3', content_encoding='iso-8859-1')
            message = await channel.get(setup.queue_name)
            assert len(message.body) == 8  # iso-8859 chars are always 1 byte
            assert message.properties.content_encoding == 'iso-8859-1'
            assert message.decode() == 'lë böö 3'

            # string, conflicting encoding
            await channel.publish(
                setup.exchange_name, setup.routing_key, 'lë böö 4', encoding='cp850', content_encoding='iso-8859-1')
            message = await channel.get(setup.queue_name)
            assert len(message.body) == 8  # cp850 chars are 1 byte
            assert message.properties.content_encoding == 'iso-8859-1'
            # cp850 data deocding as iso-8859 actually succeeds, it's just garbage. Otherwise we'd expect an
            # UnicodeDecodeError
            assert message.decode() != 'lë böö 4'
            assert message.decode('cp850') == 'lë böö 4'

            # binary body
            await channel.publish(
                setup.exchange_name, setup.routing_key, 'lë böö 5'.encode('iso-8859-1'), content_encoding='iso-8859-1')
            message = await channel.get(setup.queue_name)
            assert len(message.body) == 8  # iso-8859 chars are always 1 byte
            assert message.properties.content_encoding == 'iso-8859-1'
            assert message.decode() == 'lë böö 5'
            assert message.decode('iso-8859-1') == 'lë böö 5'

            # binary body, just encoding without content_encoding should fail
            await channel.publish(
                setup.exchange_name, setup.routing_key, 'lë böö 6'.encode('iso-8859-1'), encoding='iso-8859-1')
            message = await channel.get(setup.queue_name)
            assert len(message.body) == 8  # iso-8859 chars are always 1 byte
            assert message.properties.content_encoding is None
            with raises(UnicodeDecodeError):
                assert message.decode() == 'lë böö 6'  # decoding this iso-8859-1 with the channel default (utf-8)
            assert message.decode('iso-8859-1') == 'lë böö 6'

        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_json_null(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            setup = await setup_channel(event_loop, channel)
            await channel.publish(setup.exchange_name, setup.routing_key, json=None)
            message = await channel.get(setup.queue_name)
            assert message.body == b'null'
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_json_empty_string(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            setup = await setup_channel(event_loop, channel)
            await channel.publish(setup.exchange_name, setup.routing_key, json='')
            message = await channel.get(setup.queue_name)
            assert message.body == b'""'
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_neither_body_nor_json(event_loop, rabbitmq_host):
    """This should raise a TypeError"""
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            setup = await setup_channel(event_loop, channel)
            with raises(TypeError) as excinfo:
                await channel.publish(setup.exchange_name, setup.routing_key)
            assert str(excinfo.value) == 'Either body or json must be given'
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_content_type(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            setup = await setup_channel(event_loop, channel)

            # no content_type
            await channel.publish(setup.exchange_name, setup.routing_key, 'test body 1')
            message = await channel.get(setup.queue_name)
            assert message.body == b'test body 1'
            assert message.properties.content_type is None

            # with content_type
            await channel.publish(
                setup.exchange_name, setup.routing_key, b'test body 2', content_type='application/foo')
            message = await channel.get(setup.queue_name)
            assert message.body == b'test body 2'
            assert message.properties.content_type == 'application/foo'

            # bytes body, content_type=True
            await channel.publish(
                setup.exchange_name, setup.routing_key, b'test body 2.1', content_type=True)
            message = await channel.get(setup.queue_name)
            assert message.body == b'test body 2.1'
            assert message.properties.content_type == 'application/octet-stream'

            # str body, content_type=True
            await channel.publish(
                setup.exchange_name, setup.routing_key, 'test body 2.3', content_type=True)
            message = await channel.get(setup.queue_name)
            assert message.body == b'test body 2.3'
            assert message.properties.content_type == 'text/plain'

            # json + no content_type, default
            await channel.publish(setup.exchange_name, setup.routing_key, json=['test', {'body': 3}])
            message = await channel.get(setup.queue_name)
            assert message.json() == ['test', {'body': 3}]
            assert message.properties.content_type is None

            # json + content_type=True
            await channel.publish(
                setup.exchange_name, setup.routing_key, json=['test', {'body': 4}], content_type=True)
            message = await channel.get(setup.queue_name)
            assert message.json() == ['test', {'body': 4}]
            assert message.properties.content_type == 'application/json'

            # json + content_type=False
            await channel.publish(
                setup.exchange_name, setup.routing_key, json=['test', {'body': 5}], content_type=False)
            message = await channel.get(setup.queue_name)
            assert message.json() == ['test', {'body': 5}]
            assert message.properties.content_type is None

        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_message_id(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            setup = await setup_channel(event_loop, channel)

            # default, no message-id
            await channel.publish(setup.exchange_name, setup.routing_key, 'test body 1')
            message = await channel.get(setup.queue_name)
            assert message.body == b'test body 1'
            assert message.properties.message_id is None

            # with message-id
            await channel.publish(setup.exchange_name, setup.routing_key, 'test body 2', message_id='test msg id')
            message = await channel.get(setup.queue_name)
            assert message.body == b'test body 2'
            assert message.properties.message_id == 'test msg id'

        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_user_id(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            setup = await setup_channel(event_loop, channel)

            # default, no user-id
            await channel.publish(setup.exchange_name, setup.routing_key, 'test body 1')
            message = await channel.get(setup.queue_name)
            assert message.body == b'test body 1'
            assert message.properties.user_id is None

            # with user-id matching what connect() used
            await channel.publish(setup.exchange_name, setup.routing_key, 'test body 2', user_id='guest')
            message = await channel.get(setup.queue_name)
            assert message.body == b'test body 2'
            assert message.properties.user_id == 'guest'

            # RabbitMQ closes channel if we pass user_id != auth username
            with raises(ServerClosedChannel) as excinfo:
                await channel.publish(setup.exchange_name, setup.routing_key, 'test body 2', user_id='testing user id')
            assert excinfo.value.reply_code == 406
            assert excinfo.value.reply_text == \
                   "PRECONDITION_FAILED - user_id property set to 'testing user id' but authenticated user was 'guest'"
            assert excinfo.value.class_id == CLASS_BASIC
            assert excinfo.value.method_id == METHOD_BASIC_PUBLISH
        assert channel._closing_exc is excinfo.value
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_app_id(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            setup = await setup_channel(event_loop, channel)

            # default, no app-id
            await channel.publish(setup.exchange_name, setup.routing_key, 'test body 1')
            message = await channel.get(setup.queue_name)
            assert message.body == b'test body 1'
            assert message.properties.app_id is None

            # with app-id
            await channel.publish(setup.exchange_name, setup.routing_key, 'test body 2', app_id='test application')
            message = await channel.get(setup.queue_name)
            assert message.body == b'test body 2'
            assert message.properties.app_id == 'test application'
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_type(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            setup = await setup_channel(event_loop, channel)

            # default, no type
            await channel.publish(setup.exchange_name, setup.routing_key, 'test body 1')
            message = await channel.get(setup.queue_name)
            assert message.body == b'test body 1'
            assert message.properties.type_ is None

            # with app-id
            await channel.publish(setup.exchange_name, setup.routing_key, 'test body 2', type_='test type')
            message = await channel.get(setup.queue_name)
            assert message.body == b'test body 2'
            assert message.properties.type_ == 'test type'
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(10)
@pytestmark
async def test_return(event_loop, rabbitmq_host):
    async with await connect(host=rabbitmq_host, loop=event_loop) as connection:
        async with connection.channel() as channel:
            setup = await setup_channel(event_loop, channel, routing_key=None)
            # mandatory and no matching routing key for exchange -> message is returned
            await channel.publish(setup.exchange_name, 'unbound_rk', 'test body 1', mandatory=True)
            message = await channel.get_next_returned_message()
            assert message.reply_code == 312
            assert message.reply_text == 'NO_ROUTE'
            assert message.body == b'test body 1'
            assert message.routing_key == 'unbound_rk'
            await channel.bind_queue(setup.queue_name, setup.exchange_name, 'test_rk')

            # this will never finish - we test that the None put to the queue by channel cleanup raises an exception
            get_returned_task = event_loop.create_task(channel.get_next_returned_message())

            # now it's routed to a queue, mandatory ought to work
            await channel.publish(setup.exchange_name, 'test_rk', 'test body 2', mandatory=True)
            message = await channel.get(setup.queue_name)
            assert message.decode() == 'test body 2'

            assert not get_returned_task.done()

            # rabbitmq does not support immediate
            with raises(ServerClosedConnection) as excinfo:
                await channel.publish(setup.exchange_name, 'unbound_rk', 'test body 3', immediate=True)
            assert excinfo.value.reply_code == 540
            assert excinfo.value.reply_text == "NOT_IMPLEMENTED - immediate=true"
            assert excinfo.value.class_id == CLASS_BASIC
            assert excinfo.value.method_id == METHOD_BASIC_PUBLISH

            # this should raise the identical exception as the previous one
            with raises(ServerClosedConnection) as excinfo2:
                await get_returned_task
            assert excinfo2.value is excinfo.value
        assert channel._closing_exc is excinfo.value
    assert connection._closing_exc is excinfo.value
