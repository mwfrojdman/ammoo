import asyncio
import collections
import itertools
import json as json_module
import logging
import typing
from asyncio import QueueFull
from collections import Mapping, OrderedDict, deque
from datetime import datetime
from functools import partial
from typing import Dict, Tuple, Iterator, Union, Optional

from ammoo._utils import add_int_arg, add_routing_keys_header
from ammoo.consumer import Consumer
from ammoo.exceptions import channel as channel_exceptions
from ammoo.exceptions.channel import ChannelClosed, PublishNack, ChannelInactive
from ammoo.exceptions.connection import UnexpectedFrame, ConnectionClosed
from ammoo.message import PartialMessage, AbstractMessage, GetMessage, ReturnMessage
from ammoo.validatation import validate_bool, validate_delivery_tag, validate_exchange_name, validate_shortstr, \
    validate_long_uint, \
    validate_short_uint, validate_priority, validate_timestamp, validate_delivery_mode, validate_queue_name, \
    validate_routing_key
from ammoo.wire.class_and_method import CHANNEL_OPEN_CAM, CHANNEL_OPEN_OK_CAM, CHANNEL_CLOSE_CAM, \
    CHANNEL_CLOSE_OK_CAM, EXCHANGE_DECLARE_CAM, EXCHANGE_DECLARE_OK_CAM, EXCHANGE_DELETE_OK_CAM, \
    EXCHANGE_DELETE_CAM, QUEUE_DECLARE_CAM, QUEUE_DECLARE_OK_CAM, QUEUE_DELETE_CAM, QUEUE_DELETE_OK_CAM, \
    QUEUE_PURGE_CAM, QUEUE_PURGE_OK_CAM, QUEUE_BIND_CAM, QUEUE_BIND_OK_CAM, QUEUE_UNBIND_CAM, QUEUE_UNBIND_OK_CAM, \
    BASIC_PUBLISH_CAM, BASIC_ACK_CAM, CONFIRM_SELECT_CAM, CONFIRM_SELECT_OK_CAM, BASIC_GET_CAM, BASIC_GET_EMPTY_CAM, \
    BASIC_GET_OK_CAM, BASIC_CONSUME_OK_CAM, BASIC_CANCEL_OK_CAM, BASIC_DELIVER_CAM, BASIC_CANCEL_CAM, \
    EXCHANGE_BIND_CAM, EXCHANGE_BIND_OK_CAM, BASIC_REJECT_CAM, BASIC_QOS_CAM, BASIC_NACK_CAM, BASIC_QOS_OK_CAM, \
    BASIC_RETURN_CAM, BASIC_RECOVER_CAM, BASIC_RECOVER_OK_CAM, CHANNEL_FLOW_CAM, CHANNEL_FLOW_OK_CAM
from ammoo.wire.frames import FRAME_TYPE_BODY, FRAME_TYPE_HEADER, FRAME_TYPE_METHOD, MethodFrame
from ammoo.wire.frames.header import pack_basic_header_frame_payload, BasicHeaderProperties
from ammoo.wire.frames.method.basic import pack_basic_publish_parameters, pack_basic_get_parameters, \
    pack_basic_ack_parameters, pack_basic_nack_parameters, pack_basic_reject_parameters, pack_basic_qos_parameters
from ammoo.wire.frames.method.channel import CLEAN_CHANNEL_CLOSE_PARAMETERS_DATA
from ammoo.wire.frames.method.exchange import pack_exchange_declare_parameters, pack_exchange_delete_parameters, \
    pack_exchange_bind_parameters
from ammoo.wire.frames.method.queue import QueueDeclareOkParameters, pack_queue_declare_parameters, \
    pack_queue_delete_parameters, pack_queue_purge_parameters, pack_queue_bind_parameters, pack_queue_unbind_parameters
from ammoo.wire.low.containers import FieldValue
from ammoo.wire.low.misc import BOOLEAN_FALSE_DATA, pack_boolean
from ammoo.wire.low.strings import EMPTY_SHORTSTR_DATA
from ammoo.wire.typing import MessageCount, ConsumerTag, ExchangeName, ExchangeType, RoutingKey, QueueName, \
    DeliveryMode, DeliveryTag, EncodeJson, OrderedDictT, Deque

__all__ = 'Channel',

logger = logging.getLogger(__name__)


_JSON_COMPACT_SEPARATORS = (',', ':')

class _DefaultArgumentValue:
    """For typing. Using object() singleton default argument values makes any value acceptable."""


_NO_JSON = _DefaultArgumentValue()  # can't use None for default argument value, as that's null for json


def _publish_encoding(
        body: Union[str, bytes, bytearray, None],
        json: Union[EncodeJson, _DefaultArgumentValue],
        content_encoding: Union[None, str, bool],
        content_type: Union[None, str, bool],
        encoding: Optional[str],
        default_encoding: str
) -> Tuple[Union[bytes, bytearray], Optional[str], Optional[str]]:
    """Helper method for Channel.publish(). Encodes body/json, and sets content_encoding and content_type based
    on (in this order) the encoding argument, the content_encoding argument or the channel's default"""
    # validate body/json and set flags whether we need to do encode str -> bytes later
    if body is None:
        if json is _NO_JSON:
            raise TypeError('Either body or json must be given')
        encode_text = True
        json_set = True
    else:
        if isinstance(body, str):
            encode_text = True
        elif isinstance(body, (bytes, bytearray)):
            encode_text = False
        else:
            raise TypeError('body must be an str, bytes or bytearray instance, not: {!r}'.format(body))
        json_set = False

    # handle content_encoding
    if content_encoding is None:
        pass
    elif isinstance(content_encoding, bool):
        if not encode_text:
            raise TypeError('boolean value for content_encoding cannot be used when body is bytes/bytearray')
        if content_encoding:
            content_encoding = default_encoding if encoding is None else encoding
        else:
            content_encoding = None
    else:
        validate_shortstr(content_encoding, 'content_encoding')
        if encoding is None:
            encoding = content_encoding
        elif encoding != content_encoding:
            logger.debug(
                'Connection.publish argument encoding = %r is different from content_encoding = %r',
                encoding, content_encoding
            )
    if encode_text and encoding is None:
        encoding = default_encoding if content_encoding is None else content_encoding

    # handle content_type
    if content_type is not None:
        if isinstance(content_type, bool):
            if content_type:
                if json_set:
                    content_type = 'application/json'
                elif encode_text:
                    content_type = 'text/plain'
                else:
                    content_type = 'application/octet-stream'
            else:
                content_type = None
        else:
            validate_shortstr(content_type, 'content_type')

    if json_set:
        body = json_module.dumps(json, separators=_JSON_COMPACT_SEPARATORS)
    if encode_text:
        body = body.encode(encoding)

    return body, content_encoding, content_type


# Passed to Message, to avoid it having a direct reference to the channel
_ChannelToolbox = collections.namedtuple('_ChannelToolbox', 'server_has_capability ack reject publish default_encoding')


class Channel:
    def __init__(
            self, channel_id, send_frame, send_method_frame, loop, on_exit, frame_max, server_has_capability,
            drain_writer, prefetch_size, prefetch_count, body_encoding, amqp_encoding, rabbitmq):
        self._channel_id = channel_id
        # set to true in __aenter__
        self._entered = False  # type: bool
        self._closing_exc = None  # type: Union[None, ChannelClosed, ConnectionClosed]
        self._send_frame = send_frame
        self._send_method_frame = send_method_frame
        self._loop = loop
        self._on_exit = on_exit # callback from connection to call when this channel quits one way or another
        self._frame_max = frame_max
        self._publish_confirm_mode = False  # type: bool
        self._delivery_tag_iter = None  # type: Optional[Iterator[DeliveryTag]]
        self._consumer_tag_iter = map(lambda n: '{:x}'.format(n), itertools.count(0))  # type: Iterator[ConsumerTag]
        self._server_has_capability = server_has_capability
        self._consumers = {}  # consumer_tag -> Consumer()
        self._drain_writer = drain_writer
        self._prefetch_size = prefetch_size
        self._prefetch_count = prefetch_count
        self._returned_messages_queue = asyncio.Queue(maxsize=20, loop=self._loop)
        self.body_encoding = body_encoding
        self.amqp_encoding = amqp_encoding
        self.rabbitmq = rabbitmq
        # set to False on receiving a channel.flow from server
        self._active = True  # type: bool
        # passed to Message(), to avoid giving them a direct reference to self, or to pass n+1 args every time
        self._toolbox = _ChannelToolbox(
            self._server_has_capability, self.ack, self.reject, self.publish, self.body_encoding)
        self._partial_message = PartialMessage(self._toolbox)

        self._cam_waiters = {
            cam: deque() for cam in {
                CHANNEL_OPEN_OK_CAM, CHANNEL_CLOSE_OK_CAM, EXCHANGE_DECLARE_OK_CAM, EXCHANGE_DELETE_OK_CAM,
                EXCHANGE_BIND_OK_CAM, QUEUE_DECLARE_OK_CAM, QUEUE_DELETE_OK_CAM, QUEUE_PURGE_OK_CAM,
                QUEUE_BIND_OK_CAM, QUEUE_UNBIND_OK_CAM, CONFIRM_SELECT_OK_CAM, BASIC_QOS_OK_CAM, BASIC_RECOVER_OK_CAM
            }
        }  # type: Dict[int, Deque[asyncio.Future]]
        get_ok_or_empty_deque = deque()
        for cam in BASIC_GET_OK_CAM, BASIC_GET_EMPTY_CAM:
            self._cam_waiters[cam] = get_ok_or_empty_deque
        self._pending_acks = OrderedDict()  # type: OrderedDictT[DeliveryTag, asyncio.Future]

    async def get_next_returned_message(self) -> ReturnMessage:
        """Returns the next basic.return message we have"""
        self._check_open()
        message = await self._returned_messages_queue.get()
        if message is None:
            raise self._closing_exc
        return message

    @property
    def entered(self):
        return self._entered

    @property
    def closing(self):
        return self._closing_exc is not None

    @property
    def channel_id(self):
        return self._channel_id

    def dispatch_frame(self, frame):
        assert frame.channel_id == self._channel_id
        frame_type = frame.frame_type
        if frame_type == FRAME_TYPE_METHOD:
            cam = frame.cam
            if cam in (BASIC_GET_OK_CAM, BASIC_DELIVER_CAM, BASIC_RETURN_CAM):
                self._partial_message.feed_method_frame(frame)
                # XXX: for all other method frames than these, clear the current partial message
            elif cam == BASIC_ACK_CAM or cam == BASIC_NACK_CAM:
                if not self._publish_confirm_mode:
                    logger.warning('Broker sent basic.ack, but we\'re not in publish confirm mode')
                    raise UnexpectedFrame(frame)
                parameters = frame.parameters
                ack_delivery_tag = parameters.delivery_tag
                multiple = parameters.multiple
                # basic.nack has parameter requeue too, but spec tells to ignore it
                if multiple:
                    if cam == BASIC_ACK_CAM:
                        def finish_future(future: asyncio.Future) -> None:
                            if not future.done():
                                future.set_result(None)
                    else:
                        nack_exc = PublishNack()
                        def finish_future(future):
                            if not future.done():
                                future.set_exception(nack_exc)
                    logger.debug('Got multiple ack for delivery tag %s', ack_delivery_tag)
                    while True:
                        try:
                            pending_delivery_tag, future = self._pending_acks.popitem(False)  # False ~ popleft
                        except KeyError as exc:
                            logger.warning(
                                'Broker sent basic.ack for delivery tag %s, but none are pending', ack_delivery_tag)
                            raise UnexpectedFrame(frame) from exc
                        if ack_delivery_tag < pending_delivery_tag:
                            logger.warning(
                                'Broker sent basic.ack for delivery-tag %s with multiple, but that tag if not pending',
                                ack_delivery_tag
                            )
                            raise UnexpectedFrame(frame)
                        logger.debug('Multiple ack -> resolving %s', pending_delivery_tag)
                        finish_future(future)
                        if ack_delivery_tag == pending_delivery_tag:
                            break
                else:
                    logger.debug('Got single ack for delivery tag %s', ack_delivery_tag)
                    try:
                        future = self._pending_acks[ack_delivery_tag]
                    except KeyError as exc:
                        raise UnexpectedFrame(frame) from exc
                    if not future.done():
                        if cam == BASIC_ACK_CAM:
                            future.set_result(None)
                        else:
                            future.set_exception(PublishNack())
            elif cam == CHANNEL_CLOSE_CAM:
                self._send_method_frame(self._channel_id, CHANNEL_CLOSE_OK_CAM, b'')
                if not self.closing:
                    self.release(channel_exceptions.ServerClosedChannel(frame.parameters))
            elif cam == CHANNEL_FLOW_CAM:
                active = frame.parameters
                self._active = active
                logger.debug('Channel active state changed to %s', active)
                self._send_method_frame(self._channel_id, CHANNEL_FLOW_OK_CAM, pack_boolean(active))
            elif cam == BASIC_CANCEL_CAM:
                consumer_tag = frame.parameters.consumer_tag
                self._dispatch_to_consumer(frame, consumer_tag)
            elif cam == BASIC_CANCEL_OK_CAM or cam == BASIC_CONSUME_OK_CAM:
                consumer_tag = frame.parameters
                self._dispatch_to_consumer(frame, consumer_tag)
            else:
                if cam in self._cam_waiters:
                    try:
                        future = self._cam_waiters[cam].popleft()
                    except IndexError as exc:
                        raise UnexpectedFrame(frame) from exc
                    if not future.done():
                        future.set_result(frame)
                else:
                    raise UnexpectedFrame(frame)
        elif frame_type == FRAME_TYPE_HEADER:
            message = self._partial_message.feed_header_frame(frame)
            if message is not None:
                self._dispatch_message(message)
        elif frame_type == FRAME_TYPE_BODY:
            message = self._partial_message.feed_body_frame(frame)
            if message is not None:
                self._dispatch_message(message)
        else:
            raise UnexpectedFrame(frame)

    def _dispatch_message(self, message: AbstractMessage):
        cam = message.cam
        if cam == BASIC_GET_OK_CAM:
            try:
                future = self._cam_waiters[cam].popleft()
            except IndexError:
                raise UnexpectedFrame(message)  # XXX: exception expects a frame, not a message?
            if not future.done():
                future.set_result(message)
        elif cam == BASIC_DELIVER_CAM:
            consumer_tag = message.consumer_tag
            logger.debug('Dispatching basic.deliver message to consumer with tag %s', consumer_tag)
            self._dispatch_to_consumer(message, consumer_tag)
        elif message.cam == BASIC_RETURN_CAM:
            if self._returned_messages_queue is not None:
                try:
                    self._returned_messages_queue.put_nowait(message)
                except QueueFull:
                    logger.warning('Received a basic.return message, but could not put it to full queue')
        else:
            raise ValueError(message)

    def release(self, exc: Union[ChannelClosed, ConnectionClosed]) -> None:
        """Called from __aenter__/__aexit__ + Connection._close"""
        if self._closing_exc is None:
            self._closing_exc = exc
        for futures in self._cam_waiters.values():
            for future in futures:
                if not future.done():
                    future.set_exception(self._closing_exc)
            futures.clear()
        for future in self._pending_acks.values():
            if not future.done():
                future.set_exception(self._closing_exc)
        self._pending_acks.clear()
        for consumer in list(self._consumers.values()):
            consumer.release(self._closing_exc)
        self._returned_messages_queue.put_nowait(None)  # signals we're done here
        assert not self._consumers
        self._on_exit()

    def _dispatch_to_consumer(
            self, frame_or_message: Union[AbstractMessage, MethodFrame], consumer_tag: ConsumerTag) -> None:
        try:
            consumer = self._consumers[consumer_tag]
        except KeyError:
            logger.info('Got frame/message for consumer tag %s, but it is not open', consumer_tag)
            raise UnexpectedFrame(frame_or_message)  # XXX: connection's dispatcher expects a frame, not a message
        else:
            consumer.dispatch_frame(frame_or_message)

    async def __aenter__(self):
        try:
            if self.entered:
                raise ValueError('Channel already entered')
            if self.closing:
                raise self._closing_exc
            self._send_method_frame(self._channel_id, CHANNEL_OPEN_CAM, EMPTY_SHORTSTR_DATA)
            await self._wait_for_cam_frame(CHANNEL_OPEN_OK_CAM)
            self._entered = True
            if self._prefetch_size is not None or self._prefetch_count is not None:
                prefetch_size = 0 if self._prefetch_size is None else self._prefetch_size
                prefetch_count = 0 if self._prefetch_count is None else self._prefetch_count
                # global = false, because when qos'ing on RabbitMQ before any consumers exist on channel, the effect
                # is the same as for global = true, and this way the result is the same as on vanilla AMQP
                # (on which global = true means the qos applies on all channels)
                await self.qos(prefetch_size, prefetch_count, global_=False)
            return self
        except Exception as exc:
            self.release(exc=channel_exceptions.ClientClosedChannelError(exc))
            raise

    async def _wait_for_cam_frame(self, cam: int) -> Union[MethodFrame, AbstractMessage]:
        future = self._loop.create_future()
        self._cam_waiters[cam].append(future)
        return await future

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self.entered:
            raise ValueError('Channel not entered')
        try:
            if not self.closing:
                self._send_method_frame(self._channel_id, CHANNEL_CLOSE_CAM, CLEAN_CHANNEL_CLOSE_PARAMETERS_DATA)
                self._closing_exc = channel_exceptions.ClientClosedChannelOK()
                await self._wait_for_cam_frame(CHANNEL_CLOSE_OK_CAM)
        finally:
            self.release(exc=self._closing_exc)

    def _check_open(self) -> None:
        """Utility method to check channel is __aentered__ and not closing/-ed. Must be called before sending frames
        to broker, or waiting for receiving them (with eg self._wait_for_cam_frame)"""
        if not self.entered:
            raise ValueError('Channel not entered')
        if self.closing:
            raise self._closing_exc

    async def assert_exchange_exists(self, exchange_name: ExchangeName) -> None:
        """
        Asserts *exchange_name* exists*. Channel will be closed if it does not!

        :param exchange_name: Exchange name
        """
        validate_exchange_name(exchange_name)
        self._check_open()
        self._send_method_frame(
            self._channel_id, EXCHANGE_DECLARE_CAM,
            pack_exchange_declare_parameters(
                exchange_name=exchange_name,
                type_='',
                passive=True,
                durable=False,
                auto_delete=False,
                internal=False,
                no_wait=False,
                arguments={},
                encoding=self.amqp_encoding,
                rabbitmq=self.rabbitmq
            )
        )
        await self._wait_for_cam_frame(EXCHANGE_DECLARE_OK_CAM)  # no parameters returned
        return None

    async def declare_exchange(
            self,
            exchange_name: ExchangeName,
            exchange_type: ExchangeType, *,
            durable: bool=False,
            auto_delete: bool=False,
            alternate_exchange_name: Optional[ExchangeName]=None
    ) -> None:
        """
        Declare exchange.

        :param str exchange_name: Exchange name
        :param str exchange_type: Exchange type: direct, fanout, topic, or headers.
        :param bool durable: Optional: If True, exchange will survive server restart.
        :param bool auto_delete: Optional: If True, the exchange is deleted (after a delay) when the last queue is unbound from it.
        :param alternate_exchange_name: Optional: Alternate exchange name. Messages that can't be routed to any queue are instead published on the alternate exchange (Sets alternate-exchange on the exchange).
        """
        validate_exchange_name(exchange_name)
        validate_bool(durable, 'durable')
        validate_bool(auto_delete, 'auto_delete')
        arguments = {}
        if alternate_exchange_name is not None:
            validate_exchange_name(alternate_exchange_name, 'alternate_exchange_name')
            arguments['alternate-exchange'] = alternate_exchange_name
        self._check_open()
        self._send_method_frame(self._channel_id, EXCHANGE_DECLARE_CAM, pack_exchange_declare_parameters(
            exchange_name=exchange_name,
            type_=exchange_type,
            passive=False,
            durable=durable,
            auto_delete=auto_delete,
            internal=False,
            no_wait=False,
            arguments=arguments,
            encoding=self.amqp_encoding,
            rabbitmq=self.rabbitmq
        ))
        await self._wait_for_cam_frame(EXCHANGE_DECLARE_OK_CAM)  # no parameters returned
        return

    async def delete_exchange(self, exchange_name: ExchangeName, *, if_unused: bool=False) -> None:
        """
        Delete exchange *exchange_name*.

        :param str exchange_name: Exchange name
        :param bool if_unused: Optional: Only delete exchange if it is unused.
        """
        validate_exchange_name(exchange_name)
        validate_bool(if_unused, 'if_unused')
        self._check_open()
        self._send_method_frame(self._channel_id, EXCHANGE_DELETE_CAM, pack_exchange_delete_parameters(
            exchange_name=exchange_name,
            if_unused=if_unused,
            no_wait=False,
            encoding=self.amqp_encoding
        ))
        await self._wait_for_cam_frame(EXCHANGE_DELETE_OK_CAM)  # no parameters returned
        return

    async def bind_exchange(self, destination: ExchangeName, source: ExchangeName, routing_key: RoutingKey) -> None:
        """
        Bind *source* exchange to *destination* exchange for *routing_key*.

        :param str destination: Destination exchange name
        :param str source: Source exchange name
        :param str routing_key: Routing key

        .. note:: RabbitMQ extension. Not supported by standard AMQP.
        """
        validate_exchange_name(destination, 'destination')
        validate_exchange_name(source, 'source')
        validate_routing_key(routing_key)
        if not self._server_has_capability('exchange_exchange_bindings'):
            raise ValueError('Server does not support capability exchange_exchange_bindings')
        self._check_open()
        self._send_method_frame(self._channel_id, EXCHANGE_BIND_CAM, pack_exchange_bind_parameters(
            destination=destination,
            source=source,
            routing_key=routing_key,
            no_wait=False,
            arguments={},
            encoding=self.amqp_encoding,
            rabbitmq=self.rabbitmq
        ))
        await self._wait_for_cam_frame(EXCHANGE_BIND_OK_CAM)  # no parameters returned
        return

    async def assert_queue_exists(self, queue_name: QueueName) -> QueueDeclareOkParameters:
        """
        Asserts *queue_name* exists*. Channel is closed by the server if it does not!

        :param queue_name: Queue name
        :rtype: ~ammoo.wire.frames.method.queue.QueueDeclareOkParameters
        """
        """If queue exists, return its parameters. If not, the channel is closed and ServerClosedChannel raised"""
        validate_queue_name(queue_name)
        self._check_open()
        self._send_method_frame(self._channel_id, QUEUE_DECLARE_CAM, pack_queue_declare_parameters(
            queue_name=queue_name,
            passive=True,
            durable=False,
            exclusive=False,
            auto_delete=False,
            no_wait=False,
            arguments={},
            encoding=self.amqp_encoding,
            rabbitmq=self.rabbitmq
        ))
        frame = await self._wait_for_cam_frame(QUEUE_DECLARE_OK_CAM)
        return frame.parameters

    async def declare_queue(
            self,
            queue_name: QueueName, *,
            exclusive: bool=False, durable: bool=False, auto_delete: bool=False,
            ttl: Optional[int]=None, expires: Optional[int]=None,
            max_length: Optional[int]=None, max_length_bytes: Optional[int]=None,
            dead_letter_exchange: Optional[ExchangeName]=None, dead_letter_routing_key: Optional[RoutingKey]=None,
            max_priority: Optional[int]=None
    ) -> QueueDeclareOkParameters:
        """
        Declare a queue named *queue_name*.

        :param str queue_name: Queue name
        :param bool exclusive: Optional: If True, the queue can be accessed only by this connection and is deleted \
        when connection is closed.
        :param bool durable: Optional: If True, the queue will be marked as durable, surviving server restarts.
        :param bool auto_delete: Optional: If True, the queue is deleted when all consumers are finished using it.
        :param int ttl: Optional: Messages die after this number of milliseconds, if no one has consumed them first \
        (Sets x-message-ttl on the queue).
        :param int expires: Optional: Milliseconds queue is unused before it is deleted (Sets x-expires on the queue).
        :param int max_length: Optional: Maximum number of messages in the queue before the oldest will die (Sets \
        x-max-length on the queue).
        :param int max_length_bytes: Optional: Maximum number of message bytes in the queue before the oldest will \
        die (Sets x-max-length on the queue).
        :param str dead_letter_exchange: Optional: Name of dead letter exchange. The queue's dead messages are routed \
        here (Sets x-dead-letter-exchange on the queue).
        :param dead_letter_routing_key: Optional: Override dead messages' routing key when routing them to \
        *dead_letter_exchange* (Sets x-dead-letter-routing-key on the queue).
        :param int max_priority: Optional: Queue's maximum priority (Sets x-max-priority).
        :rtype: ~ammoo.wire.frames.method.queue.QueueDeclareOkParameters
        """
        validate_queue_name(queue_name)
        validate_bool(exclusive, 'exclusive')
        validate_bool(durable, 'durable')
        validate_bool(auto_delete, 'auto_delete')
        arguments = {}
        if ttl is not None:
            add_int_arg('ttl', ttl, 'x-message-ttl', arguments, True)
        if expires is not None:
            add_int_arg('expires', expires, 'x-expires', arguments, False)
        if max_length is not None:
            add_int_arg('max_length', max_length, 'x-max-length', arguments, True)
        if max_length_bytes is not None:
            add_int_arg('max_length_bytes', max_length_bytes, 'x-max-length-bytes', arguments, True)
        if dead_letter_exchange is not None:
            validate_exchange_name(dead_letter_exchange, 'dead_letter_exchange')
            arguments['x-dead-letter-exchange'] = dead_letter_exchange
        if dead_letter_routing_key is not None:
            if dead_letter_exchange is None:
                raise TypeError('dead_letter_routing_key can\'t be used without dead_letter_exchange')
            validate_routing_key(dead_letter_routing_key, 'dead_letter_routing_key')
            arguments['x-dead-letter-routing-key'] = dead_letter_routing_key
        if max_priority is not None:
            validate_priority(max_priority, 'max_priority')
            arguments['x-max-priority'] = max_priority
        self._check_open()
        self._send_method_frame(self._channel_id, QUEUE_DECLARE_CAM, pack_queue_declare_parameters(
            queue_name=queue_name,
            passive=False,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            no_wait=False,
            arguments=arguments,
            encoding=self.amqp_encoding,
            rabbitmq=self.rabbitmq
        ))
        frame = await self._wait_for_cam_frame(QUEUE_DECLARE_OK_CAM)
        return frame.parameters

    async def delete_queue(self, queue_name: QueueName, *, if_unused: bool=False, if_empty: bool=False) -> MessageCount:
        """
        Delete a queue named *queue_name*. If the queue does not exist, the method merely asserts it is not there.

        :param queue_name: Queue name
        :param bool if_unused: Optional: Only delete queue if it has no consumers.
        :param bool if_empty: Optional: Only delete queue if it has no messages.
        :return: Number of messages in queue before it was deleted
        """
        validate_queue_name(queue_name)
        validate_bool(if_unused, 'if_unused')
        validate_bool(if_empty, 'if_empty')
        self._check_open()
        self._send_method_frame(self._channel_id, QUEUE_DELETE_CAM, pack_queue_delete_parameters(
            queue_name=queue_name,
            if_unused=if_unused,
            if_empty=if_empty,
            no_wait=False,
            encoding=self.amqp_encoding
        ))
        frame = await self._wait_for_cam_frame(QUEUE_DELETE_OK_CAM)
        return frame.parameters

    async def purge_queue(self, queue_name: QueueName) -> MessageCount:
        """
        Purges a queue of messages, emptying it.

        :param str queue_name: Queue name
        :return: Number of messages in queue before it was purged
        """
        validate_queue_name(queue_name)
        self._check_open()
        self._send_method_frame(self._channel_id, QUEUE_PURGE_CAM, pack_queue_purge_parameters(
            queue_name=queue_name,
            no_wait=False,
            encoding=self.amqp_encoding
        ))
        frame = await self._wait_for_cam_frame(QUEUE_PURGE_OK_CAM)
        return frame.parameters

    async def bind_queue(self, queue_name: QueueName, exchange_name: ExchangeName, routing_key: RoutingKey):
        """
        Bind *queue_name* to *exchange_name* for *routing_key*.

        :param str queue_name: Queue name
        :param str exchange_name: Exchange name
        :param routing_key: A :class:`str` is used as a literal routing key, and a :class:`~collections.abc.Mapping` for the headers \
        exchange type.
        """
        validate_queue_name(queue_name)
        validate_exchange_name(exchange_name)
        if isinstance(routing_key, str):
            validate_routing_key(routing_key)
            arguments = {}
        elif isinstance(routing_key, Mapping):
            # NB: https://www.rabbitmq.com/amqp-0-9-1-errata.html 3. Field types. So much for standards.
            arguments = {}
            for argument_name, value in routing_key.items():
                arguments[argument_name] = value
            routing_key = ''
        else:
            raise TypeError('routing_key is not str or Mapping: {!r}'.format(routing_key))
        self._check_open()
        self._send_method_frame(self._channel_id, QUEUE_BIND_CAM, pack_queue_bind_parameters(
            queue_name=queue_name,
            exchange_name=exchange_name,
            routing_key=routing_key,
            no_wait=False,
            arguments=arguments,
            encoding=self.amqp_encoding,
            rabbitmq=self.rabbitmq
        ))
        await self._wait_for_cam_frame(QUEUE_BIND_OK_CAM)  # no parameters returned

    async def unbind_queue(self, queue_name: QueueName, exchange_name: ExchangeName, routing_key: RoutingKey) -> None:
        """
        Unbind *queue_name* from *exchange_name* for *routing_key*. Undoes :meth:`bind_queue`.

        :param str queue_name: Queue name
        :param str exchange_name: Exchange name
        :param routing_key: Same as for :meth:`bind_queue`.
        """
        # TODO: support mapping like bind does
        validate_queue_name(queue_name)
        validate_exchange_name(exchange_name)
        self._check_open()
        self._send_method_frame(self._channel_id, QUEUE_UNBIND_CAM, pack_queue_unbind_parameters(
            queue_name=queue_name,
            exchange_name=exchange_name,
            routing_key=routing_key,
            arguments={},
            encoding=self.amqp_encoding,
            rabbitmq=self.rabbitmq
        ))
        await self._wait_for_cam_frame(QUEUE_UNBIND_OK_CAM)  # no parameters returned
        return

    async def _publish(
            self,
            exchange_name: ExchangeName,
            routing_key: RoutingKey,
            body: Union[bytes, bytearray],
            mandatory: bool,
            immediate: bool,
            properties: Optional[BasicHeaderProperties]
    ) -> None:
        """Utility method for the high level API publish method. Does NOT check the arguments"""
        body_size = len(body)

        # drain writer before sending frames so we don't flood the buffer
        self._check_open()
        await self._drain_writer()
        self._check_open()  # all bets are off after await
        if not self._active:
            # RabbitMQ does not send channel.flow since version 2.0 so this is a bit theoretical. We could also have
            # a mode that awaits until channel is active again
            raise ChannelInactive()

        if self._publish_confirm_mode:
            delivery_tag = next(self._delivery_tag_iter)
        else:
            delivery_tag = None

        # NOTE: if refactored to use writer.drain() (or some other await) between frames, use a mutex so another task
        # on the same channel cannot send frames in between which would be interpreted as cancelling the publishing of
        # this message. Other channels may interleave frames.
        self._send_method_frame(self._channel_id, BASIC_PUBLISH_CAM, pack_basic_publish_parameters(
            exchange_name=exchange_name,
            routing_key=routing_key,
            mandatory=mandatory,
            immediate=immediate,
            encoding=self.amqp_encoding
        ))
        self._send_frame(FRAME_TYPE_HEADER, self.channel_id, pack_basic_header_frame_payload(
            body_size=body_size,
            properties=properties,
            encoding=self.amqp_encoding, rabbitmq=self.rabbitmq
        ))

        offset = 0
        # max payload bytes that can fit into a body frame. 7 is the frame header size and +1 for frame end
        max_payload_size = self._frame_max - 8
        while offset < body_size:
            next_offset = offset + max_payload_size
            if next_offset > body_size:
                next_offset = body_size
            self._send_frame(FRAME_TYPE_BODY, self._channel_id, body[offset:next_offset])
            offset = next_offset

        if self._publish_confirm_mode:
            future = self._loop.create_future()
            self._pending_acks[delivery_tag] = future
            # if server basic.nacks us, this will raise PublishNack
            await future  # no parameters returned
        return

    async def publish(
            self,
            exchange_name: ExchangeName,
            route: Union[RoutingKey, typing.Mapping[str, FieldValue]],
            body: Union[None, str, bytes, bytearray]=None, *,
            json: Union[EncodeJson, _DefaultArgumentValue]=_NO_JSON,
            mandatory: bool=False,
            immediate: bool=False,
            encoding: Optional[str]=None,
            correlation_id: Optional[str]=None,
            reply_to: Optional[str]=None,
            expiration: Union[str, int]=None,
            cc: Optional[RoutingKey]=None,
            bcc: Optional[RoutingKey]=None,
            priority: Optional[int]=None,
            delivery_mode: Optional[DeliveryMode]=None,
            timestamp: Optional[datetime]=None,
            content_encoding: Optional[str]=None,
            content_type: Optional[str]=None,
            message_id: Optional[str]=None,
            type_: Optional[str]=None,
            user_id: Optional[str]=None,
            app_id: Optional[str]=None
    ) -> None:
        """
        Publish a message *body* to *exchange_name* with *route*. *body* and *json* are mutually exclusive, but
        one of them has to be used.

        Publish a binary body with :class:`bytes` or :class:`bytearray`::

            await channel.publish(exchange_name, routing_key, b'binary bytes')
            await channel.publish(exchange_name, routing_key, bytearray(b'binary bytarray'))

        Publish a :class:`str`::

            # encoded to bytes with channel's default encoding
            await channel.publish(exchange_name, routing_key, 'text string')
            # use non-default encoding
            await channel.publish(exchange_name, routing_key, 'text string', encoding='iso-8859-1')

        Serialize JSON into body::

            await channel.publish(exchange_name, routing_key, json={'key': 123})

        Set the content-encoding property::

            # body will also be encoded with iso-8859-1 instead of channel's default encoding
            await channel.publish(exchange_name, routing_key, 'some text', content_encoding='iso-8859-1'})
            # content-encoding property is set to channel's default encoding
            await channel.publish(exchange_name, routing_key, 'some text', content_encoding=True})
            # can be used for json too
            await channel.publish(exchange_name, routing_key, json={'key': 123}, content_encoding='iso-8859-1'})

        Set the content-type property::

            await channel.publish(exchange_name, routing_key, b'binary data', content_type=True})  # bytes body -> content-type is set to application/octet-stream
            await channel.publish(exchange_name, routing_key, 'some text', content_type=True})  # str body -> content-type is set to text/plain
            await channel.publish(exchange_name, routing_key, json={'key': 123}, content_type=True})  # json -> content-type is set to application/json
            await channel.publish(exchange_name, routing_key, body, content_type='application/acme-2000'})  # or a regular str

        :param str exchange_name: Exchange name. Use an empty string for the default exchange.
        :param route: A :class:`str` is used as a literal routing key, while a :class:`~collections.abc.Mapping` is used for the headers exchange type.
        :param body: Message body. :class:`str`, :class:`bytes` or :class:`bytearray`. If the *json* keyword argument is used, body may be omitted.
        :param json: Optional: Object to serialize as JSON into body. Cannot be used at the same time as the body argument.
        :param bool mandatory: Optional: When True, messages the cannot be routed to a queue are returned back to the client.
        :param bool immediate: Optional: When True, messages that are not routed to a consumer immediately are returned back to the client.
        :param str encoding: Optional: Encode :class:`str` body to bytes with this encoding.
        :param str correlation_id: Optional: Correlation-id property.
        :param str reply_to: Optional: Reply-to property.
        :param expiration: Optional: Message expiration property, usually in milliseconds. Messages die if they are \
        not consumed from queue within this TTL. :class:`int` or :class:`str`.
        :param cc: Optional: Additional routing keys to use when routing message to queues.
        :param bcc: Optional: Like cc, but bcc will be removed from message before delivery.
        :param int priority: Optional: priority property.
        :param int delivery_mode: Optional: delivery-mode property 1 for non-persistent or 2 for persistent.
        :param datetime timestamp: Optional: Message timestamp property. If time zone is not set, UTC is assumed.
        :param content_encoding: Optional: content-encoding property. A :class:`str`, or if a :class:`bool` True, the \
        value of the *encoding* argument or the channel's default.
        :param content_type: Optional: content-type property. A :class:`str`, or if a :class:`bool` True, set \
        `application/octet-stream` if body is :class:`bytes` or :class:`bytearray`, `text/plain` if it's :class:`str`, \
        and `application/json` if the *json* argument was used.
        :param str message_id: Optional: message-id property.
        :param str type_: Optional: type property.
        :param str user_id: Optional: user-id property.
        :param str app_id: Optional: app-id property.

        .. note:: cc and bcc are RabbitMQ extensions. Not supported by standard AMQP.
        """
        body, content_encoding, content_type = _publish_encoding(
            body, json, content_encoding, content_type, encoding, self.body_encoding)
        validate_exchange_name(exchange_name)

        headers = {}
        # NB: if route is a str, it's interpreted as a regular routing key. If it's a dict, routing key will be
        # set to the empty string and the dictionary used for the headers property
        if isinstance(route, str):
            validate_routing_key(route)
        elif isinstance(route, Mapping):
            for name, value in route.items():
                headers[name] = value
            route = ''
        else:
            raise TypeError('route is not a str or a Mapping: {!r}'.format(route))
        validate_bool(immediate, 'immediate')
        if expiration is not None:
            if isinstance(expiration, int):
                if expiration < 0:
                    raise ValueError('expiration is negative: {}'.format(expiration))
                expiration = str(expiration)
            elif not isinstance(expiration, str):
                raise TypeError('expiration is not int or str: {!r}'.format(expiration))
            validate_shortstr(expiration, 'expiration')
        if cc is not None:
            add_routing_keys_header(cc, 'cc', headers, 'CC')
        if bcc is not None:
            add_routing_keys_header(bcc, 'bcc', headers, 'BCC')
        if priority is not None:
            validate_priority(priority)
        if delivery_mode is not None:
            validate_delivery_mode(delivery_mode)
        if timestamp is not None:
            validate_timestamp(timestamp)
        if message_id is not None:
            validate_shortstr(message_id, 'message_id')
        if type_ is not None:
            validate_shortstr(type_, 'type_')
        if user_id is not None:
            validate_shortstr(user_id, 'user_id')
        if app_id is not None:
            validate_shortstr(app_id, 'app_id')
        if (mandatory or immediate) and not self._publish_confirm_mode:
            # without publish confirms we don't get acks, and then we don't know if the return was for us.
            raise ValueError(
                'To use mandatory or immediate, channel has to be set into publisher confirm mode with ' +
                'select_confirm() first')
        if not headers:
            headers = None

        properties = BasicHeaderProperties(
            content_type, content_encoding, headers, delivery_mode, priority, correlation_id, reply_to,
            expiration, message_id, timestamp, type_, user_id, app_id, None
        )

        return await self._publish(exchange_name, route, body, mandatory, immediate, properties)

    async def select_confirm(self) -> None:
        """
        Turns publisher confirms on for the channel.

        .. note:: RabbitMQ extension. Not supported by standard AMQP.
        """
        if self._publish_confirm_mode:
            return  # no need to do this twice, though RabbitMQ allows sending many confirm.selects on same channel
        if not self._server_has_capability('publisher_confirms'):
            raise ValueError('Server does not support capability publisher_confirms')
        self._check_open()
        self._send_method_frame(self._channel_id, CONFIRM_SELECT_CAM, BOOLEAN_FALSE_DATA)
        await self._wait_for_cam_frame(CONFIRM_SELECT_OK_CAM)
        self._delivery_tag_iter = itertools.count(1)
        self._publish_confirm_mode = True

    async def get(self, queue_name: QueueName, *, no_ack: bool=False) -> GetMessage:
        """
        Get a message from queue.

        :param str queue_name: Queue name
        :param bool no_ack: Optional: If True, server does not expect message to be acknowledged or rejected.
        :raises EmptyQueue: If there are no messages in queue, EmptyQueue is raised
        :rtype: GetMessage
        """
        validate_queue_name(queue_name)
        validate_bool(no_ack, 'no_ack')
        self._check_open()
        self._send_method_frame(self._channel_id, BASIC_GET_CAM, pack_basic_get_parameters(
            queue_name=queue_name,
            no_ack=no_ack,
            encoding=self.amqp_encoding
        ))
        value = await self._wait_for_cam_frame(BASIC_GET_OK_CAM)  # also resolved by basic.empty
        if isinstance(value, GetMessage):
            # it's basic.get-ok, a message
            return value
        else:
            raise channel_exceptions.EmptyQueue()  # got a basic.empty

    async def ack(self, delivery_tag: DeliveryTag) -> None:
        """
        Acknowledge a message.

        :param int delivery_tag: Delivery tag of message to acknowledge (:attr:`ExpectedMessage.delivery_tag`)

        .. seealso:: :meth:`ExpectedMessage.ack`
        """
        validate_delivery_tag(delivery_tag)
        self._check_open()
        self._send_method_frame(self._channel_id, BASIC_ACK_CAM, pack_basic_ack_parameters(
            delivery_tag=delivery_tag,
            multiple=False
        ))
        return

    async def _nack(self, delivery_tag: DeliveryTag, requeue: bool, multiple: bool) -> None:
        # Unused for now. Does the same as reject when multiple is False.
        validate_delivery_tag(delivery_tag)
        validate_bool(requeue, 'requeue')
        validate_bool(multiple, 'multiple')
        self._check_open()
        self._send_method_frame(self._channel_id, BASIC_NACK_CAM, pack_basic_nack_parameters(
            delivery_tag=delivery_tag,
            multiple=multiple,
            requeue=requeue
        ))
        return

    async def reject(self, delivery_tag: DeliveryTag, requeue: bool) -> None:
        """
        Reject a message. Opposite of :meth:`ack`.

        :param int delivery_tag: Delivery tag of message to reject (:attr:`ExpectedMessage.delivery_tag`)
        :param bool requeue: If True, the server will try to requeue the message. False means the message is discarded or dead-lettered.

        .. seealso:: :meth:`ExpectedMessage.reject`
        """
        validate_delivery_tag(delivery_tag)
        validate_bool(requeue, 'requeue')
        self._check_open()
        self._send_method_frame(self._channel_id, BASIC_REJECT_CAM, pack_basic_reject_parameters(
            delivery_tag=delivery_tag,
            requeue=requeue
        ))
        return

    def _on_consumer_exit(self, consumer_tag: ConsumerTag) -> None:
        """Callback for Consumer to call on its release"""
        if consumer_tag in self._consumers:
            del self._consumers[consumer_tag]

    def consume(
            self,
            queue_name: QueueName, *,
            no_ack: bool=False, no_local: bool=False, exclusive: bool=False,
            priority: Optional[int]=None
    ) -> Consumer:
        """
        Start a new consumer on a queue.

        :param str queue_name: Queue name
        :param bool no_ack: Optional: If True, server does not expect messages delivered to consumer to be \
        acknowledged or rejected.
        :param bool no_local: Optional: If True, the server will not deliver messages to the connection that published them.
        :param bool exclusive: Optional: If True, only this consumer can access the queue.
        :param int priority: Optional: Set consumer priority. Lower priority consumers will receive messages only when higher priority ones are busy (Sets x-priority on the consumer).
        :rtype: Consumer

        .. note:: priority is a RabbitMQ extension
        """
        validate_queue_name(queue_name)
        validate_bool(no_ack, 'no_ack')
        validate_bool(no_local, 'no_local')
        validate_bool(exclusive, 'exclusive')
        if priority is not None:
            validate_priority(priority)
            if not self._server_has_capability('consumer_priorities'):
                raise ValueError('Server does not support capability consumer_priorities')
        self._check_open()
        consumer_tag = next(self._consumer_tag_iter)
        if len(consumer_tag) > 255:
            # Not very likely..
            raise ValueError('Out of consumer tags: {} is too long for shortstr'.format(consumer_tag))

        consumer = Consumer(
            consumer_tag, queue_name, self._send_method_frame, self._channel_id, self._loop,
            partial(self._on_consumer_exit, consumer_tag), no_ack=no_ack,
            no_local=no_local, exclusive=exclusive, priority=priority, amqp_encoding=self.amqp_encoding,
            rabbitmq=self.rabbitmq
        )
        self._consumers[consumer_tag] = consumer
        return consumer

    async def qos(self, prefetch_size: int, prefetch_count: int, global_: bool) -> None:
        """
        Limit how many unacknowledged messages (or message data) will be delivered to consumers. Without `qos`, the
        server will deliver all of the queue's messages to the consumer, possibly causing the consumer to run out of
        memory or starving other consumers of messages.

        :param int prefetch_size: Maximum number of unacknowledged messages server will deliver to a consumer. Zero turns the limit off.
        :param int prefetch_count: Maximum combined size of unacknowledged messages server will deliver to a consumer. Zero turns the limit off.
        :param bool global_: For standard AMQP: True applies the qos to the whole connection, and False to the channel \
        only. For RabbitMQ: True applies the setting to both the channel's current consumers and future ones, while \
        False only applies to the latter.
        """
        validate_long_uint(prefetch_size, 'prefetch_size')
        validate_short_uint(prefetch_count, 'prefetch_count')
        validate_bool(global_, 'global_')
        self._check_open()
        self._send_method_frame(self._channel_id, BASIC_QOS_CAM, pack_basic_qos_parameters(
            prefetch_size=prefetch_size,
            prefetch_count=prefetch_count,
            global_=global_
        ))

        await self._wait_for_cam_frame(BASIC_QOS_OK_CAM)  # no parameters returned
        self._prefetch_size = prefetch_size
        self._prefetch_count = prefetch_count
        return

    async def recover(self, requeue: bool) -> None:
        """
        Ask server to redeliver unacknowledged messages

        :param bool requeue: If False, redeliver messages to the original recipient. If True, the message may be \
        delivered to another recipient.
        """
        validate_bool(requeue, 'requeue')
        self._check_open()
        self._send_method_frame(self._channel_id, BASIC_RECOVER_CAM, pack_boolean(requeue))
        await self._wait_for_cam_frame(BASIC_RECOVER_OK_CAM)  # no parameters returned
        return
