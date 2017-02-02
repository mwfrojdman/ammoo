import asyncio
import collections
import itertools
from collections import namedtuple

import pytest

from ammoo import Connection, Channel, Consumer
from ammoo.exceptions.channel import ClientClosedChannelOK
from ammoo.exceptions.connection import ClientClosedConnectionOK
from ammoo.exceptions.consumer import ClientCancelledConsumerOK

pytestmark = pytest.mark.asyncio(forbid_global_loop=True)


def check_clean_connection_close(connection):
    """Checks client closed connection without errors"""
    assert isinstance(connection, Connection)
    assert isinstance(connection._closing_exc, ClientClosedConnectionOK)


def check_clean_channel_close(channel):
    assert isinstance(channel, Channel)
    assert isinstance(channel._closing_exc, ClientClosedChannelOK)


def check_clean_consumer_cancel(consumer: Consumer):
    assert isinstance(consumer, Consumer)
    assert isinstance(consumer._closing_exc, ClientCancelledConsumerOK)


def aiter(aiterable):
    return aiterable.__aiter__()


async def anext(aiterator):
    return await type(aiterator).__anext__(aiterator)


class Aenumerate:
    def __init__(self, aiterable, start=0):
        self._aiterable = aiterable
        self.counter = itertools.count(start)

    def __aiter__(self):
        self._aiter = aiter(self._aiterable)
        return self

    async def __anext__(self):
        n = next(self.counter)
        value = await anext(self._aiter)
        return n, value


collections.AsyncIterable.register(Aenumerate)
collections.AsyncIterator.register(Aenumerate)


aenumerate = Aenumerate


class _Azip:
    def __init__(self, *aiterables):
        self._aiters = []
        for aiterable in aiterables:
            if isinstance(aiterable, collections.AsyncIterable):
                aiterator = aiter(aiterable)
                is_async = True
            elif isinstance(aiterable, collections.Iterable):
                aiterator = iter(aiterable)
                is_async = False
            else:
                raise TypeError(aiterable)
            self._aiters.append((aiterator, is_async))

    def __aiter__(self):
        return self

    async def __anext__(self):
        items = []
        for aiterator, is_async in self._aiters:
            item = await anext(aiterator) if is_async else next(aiterator)
            items.append(item)
        return tuple(items)


collections.AsyncIterable.register(_Azip)
collections.AsyncIterator.register(_Azip)


azip = _Azip
ChannelSetup = namedtuple('Setup', 'queue_name exchange_name routing_key')


async def setup_channel(
        event_loop, channel, queue_name='test_q', exchange_name='test_ex', exchange_type='direct',
        routing_key='test_rk', confirm=True) -> ChannelSetup:
    coros = []
    if confirm:
        coros.append(channel.select_confirm())
    await asyncio.gather(
        channel.delete_queue(queue_name),
        channel.delete_exchange(exchange_name),
        *coros,
        loop=event_loop)
    await asyncio.gather(
        channel.declare_queue(queue_name),
        channel.declare_exchange(exchange_name, exchange_type),
        loop=event_loop
    )
    if routing_key is not None:
        await channel.bind_queue(queue_name, exchange_name, routing_key)
    return ChannelSetup(queue_name, exchange_name, routing_key)
