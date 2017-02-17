import asyncio

import pytest
from pytest import raises

from ammoo.connect import connect
from ammoo.connection import Connection
from ammoo_pytest_helpers import pytestmark


@pytestmark
async def test_connect_no_server(event_loop):
    with raises(OSError):
        await connect(port=10000, loop=event_loop)


@pytest.mark.timeout(7)
@pytestmark
async def test_connect(connect_to_broker):
    async with await connect_to_broker() as connection:
        assert isinstance(connection, Connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_connect_cancel(event_loop, connect_to_broker):
    task = event_loop.create_task(connect_to_broker(connect_to_broker))
    task.cancel()
    with raises(asyncio.CancelledError):
        await task


@pytest.mark.timeout(7)
@pytestmark
async def test_connect_aenter_cancel(event_loop, connect_to_broker):
    connection = await connect_to_broker()
    task = event_loop.create_task(connection.__aenter__())
    async def once():
        pass
    once_task = event_loop.create_task(once())
    await once_task
    task.cancel()
    with raises(asyncio.CancelledError):
        await task


@pytest.mark.timeout(7)
@pytestmark
async def test_opened_connection_cancel(event_loop, rabbitmq_host):
    enter_event = asyncio.Event(loop=event_loop)
    async def go():
        never_set = asyncio.Event(loop=event_loop)
        async with await connect(host=rabbitmq_host, loop=event_loop):
            enter_event.set()
            await never_set.wait()
    task = event_loop.create_task(go())
    await enter_event.wait()
    await asyncio.sleep(0.01, loop=event_loop)
    assert not task.done()
    task.cancel()
    with raises(asyncio.CancelledError):
        await task
