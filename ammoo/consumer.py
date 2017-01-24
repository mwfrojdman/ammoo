import asyncio
import logging
from collections import deque
from traceback import format_exc
import collections
from typing import Callable, Optional, Union
import sys
from typing import Dict

from ammoo.closeable_queue import CloseableQueue, QueueClosed
from ammoo.exceptions.channel import ChannelClosed

from ammoo.exceptions.connection import UnexpectedFrame, ConnectionClosed
from ammoo.exceptions.consumer import ConsumerCancelled
from ammoo.message import Message, DeliverMessage
from ammoo.wire.class_and_method import BASIC_CONSUME_CAM, BASIC_CONSUME_OK_CAM, BASIC_CANCEL_CAM, BASIC_CANCEL_OK_CAM
from ammoo.exceptions import consumer as consumer_exceptions
from ammoo.wire.frames import MethodFrame
from ammoo.wire.frames.method.basic import pack_basic_cancel_parameters, pack_basic_consume_parameters
from ammoo.wire.low.strings import pack_shortstr
from ammoo.wire.typing import ConsumerTag, QueueName

__all__ = 'ConsumerIterator', 'Consumer'

logger = logging.getLogger(__name__)


class ConsumerIterator:
    def __init__(
            self,
            message_queue: CloseableQueue,
            get_closing_exc: Callable[[], Union[None, ConsumerCancelled, ChannelClosed, ConnectionClosed]]
    ):
        self._message_queue = message_queue
        self._get_closing_exc = get_closing_exc

    if sys.version_info < (3, 5, 2):
        async def __aiter__(self) -> 'ConsumerIterator':
            return self
    else:
        def __aiter__(self) -> 'ConsumerIterator':
            return self

    async def __anext__(self) -> DeliverMessage:
        try:
            return await self._message_queue.get()
        except QueueClosed:
            raise self._get_closing_exc()


collections.AsyncIterator.register(ConsumerIterator)
collections.AsyncIterable.register(ConsumerIterator)


class Consumer:
    def __init__(
            self,
            consumer_tag: ConsumerTag,
            queue_name: QueueName,
            send_method_frame: Callable[[int, int, bytes], None],
            channel_id: int,
            loop: asyncio.AbstractEventLoop,
            on_exit: Callable[[], None],
            no_ack: bool,
            no_local: bool,
            exclusive: bool,
            priority: Optional[int],
            amqp_encoding: str,
            rabbitmq: bool
    ):
        self._consumer_tag = consumer_tag
        self._queue_name = queue_name
        self._send_method_frame = send_method_frame
        self._channel_id = channel_id
        self._entered = False  # set to true in __aenter__
        self._closing_exc = None  # type: Union[None, ConsumerCancelled, ChannelClosed, ConnectionClosed]
        self._on_exit = on_exit  # callback from channel to call when this consumer quits one way or another
        self._message_queue = CloseableQueue(loop=loop)
        self._loop = loop
        self._no_ack = no_ack
        self._no_local = no_local
        self._exclusive = exclusive
        self._priority = priority
        # type: Dict[int, deque[asyncio.Future]]
        self._cam_waiters = {cam: deque() for cam in {BASIC_CONSUME_OK_CAM, BASIC_CANCEL_OK_CAM}}
        self.amqp_encoding = amqp_encoding
        self.rabbitmq = rabbitmq

    @property
    def entered(self) -> bool:
        return self._entered

    @property
    def closing(self) -> bool:
        return self._closing_exc is not None

    def _check_open(self) -> None:
        if not self.entered:
            raise ValueError('Consumer not entered')
        if self.closing:
            raise self._closing_exc

    def release(self, exc: Union[ConsumerCancelled, ChannelClosed, ConnectionClosed]) -> None:
        """Called from channel"""
        if not self.closing:
            self._closing_exc = exc
            self._message_queue.close()
        for futures in self._cam_waiters.values():
            for future in futures:
                if not future.done():
                    future.set_exception(self._closing_exc)
            futures.clear()
        self._on_exit()

    def dispatch_frame(self, frame_or_msg: Union[MethodFrame, DeliverMessage]) -> None:
        if isinstance(frame_or_msg, Message):
            self._message_queue.put_nowait(frame_or_msg)
        else:  # method frame
            cam = frame_or_msg.cam
            if cam == BASIC_CANCEL_CAM:
                self._server_cancel_subscriber(frame_or_msg)
            elif cam in self._cam_waiters:
                try:
                    future = self._cam_waiters[cam].popleft()
                except IndexError as exc:
                    raise UnexpectedFrame(frame_or_msg) from exc
                if not future.done():
                    future.set_result(frame_or_msg)
            else:
                raise UnexpectedFrame(frame_or_msg)

    def _server_cancel_subscriber(self, frame: MethodFrame) -> None:
        """Server is cancelling consumer"""
        if not self.closing:
            if not frame.parameters.no_wait:
                self._send_method_frame(
                    self._channel_id, BASIC_CANCEL_OK_CAM, pack_shortstr(self._consumer_tag, self.amqp_encoding))
            self.release(consumer_exceptions.ServerCancelledConsumer())

    async def __aenter__(self) -> 'Consumer':
        try:
            if self.entered:
                raise ValueError('Consumer already entered')
            if self.closing:
                raise self._closing_exc
            arguments = {}
            if self._priority is not None:
                arguments['x-priority'] = self._priority
            self._send_method_frame(self._channel_id, BASIC_CONSUME_CAM, pack_basic_consume_parameters(
                queue_name=self._queue_name,
                consumer_tag=self._consumer_tag,
                no_local=self._no_local,
                no_ack=self._no_ack,
                exclusive=self._exclusive,
                no_wait=False,
                arguments=arguments,
                encoding=self.amqp_encoding,
                rabbitmq=self.rabbitmq
            ))
            consume_ok_frame = await self._wait_for_cam_frame(BASIC_CONSUME_OK_CAM)
            if consume_ok_frame.parameters != self._consumer_tag:
                raise ValueError('{} != {}'.format(consume_ok_frame.parameters, self._consumer_tag))
            self._entered = True
            return self
        except Exception as exc:
            logger.error('Unhandled exception in Consumer.__aenter__: %s', format_exc())
            self.release(exc=consumer_exceptions.ClientCancelledConsumerError(exc))
            raise

    async def _wait_for_cam_frame(self, cam: int) -> MethodFrame:
        future = self._loop.create_future()
        self._cam_waiters[cam].append(future)
        return await future

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        logger.debug('Consumer.__aexit__: exc_type = %r', exc_type)
        if not self.entered:
            raise ValueError('Consumer not entered')
        try:
            if not self.closing:
                self._check_open()
                self._send_method_frame(self._channel_id, BASIC_CANCEL_CAM, pack_basic_cancel_parameters(
                    consumer_tag=self._consumer_tag,
                    no_wait=False,
                    encoding=self.amqp_encoding
                ))
                await self._wait_for_cam_frame(BASIC_CANCEL_OK_CAM)
                if not self.closing:
                    self._closing_exc = consumer_exceptions.ClientCancelledConsumerOK()
                    self._message_queue.close()
        except Exception as exc:
            if not self.closing:
                self._closing_exc = consumer_exceptions.ClientCancelledConsumerError(exc)
                self._message_queue.close()
            raise
        finally:
            self.release(exc=self._closing_exc)
        return

    if sys.version_info < (3, 5, 2):
        async def __aiter__(self) -> ConsumerIterator:
            self._check_open()
            return ConsumerIterator(self._message_queue, lambda: self._closing_exc)
    else:
        def __aiter__(self) -> ConsumerIterator:
            self._check_open()
            return ConsumerIterator(self._message_queue, lambda: self._closing_exc)


collections.AsyncIterable.register(Consumer)
