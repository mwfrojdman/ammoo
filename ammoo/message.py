import json
from typing import Optional, Union
import logging

from ammoo.wire.class_and_method import BASIC_RETURN_CAM, BASIC_DELIVER_CAM, BASIC_GET_OK_CAM
from ammoo.exceptions.connection import UnexpectedFrame
from ammoo.wire.frames import BodyFrame
from ammoo.wire.frames import MethodFrame
from ammoo.wire.frames.header import BasicHeaderProperties, BasicHeaderFrame
from ammoo.wire.frames.method.basic import BasicDeliverParameters
from ammoo.wire.frames.method.basic import BasicGetOkParameters
from ammoo.wire.frames.method.basic import BasicReturnParameters
from ammoo.wire.typing import DecodeJson, RoutingKey, ExchangeName, DeliveryTag, ConsumerTag

__all__ = 'Message', 'ReturnMessage', 'ExpectedMessage', 'DeliverMessage', 'GetMessage', 'AbstractMessage'

logger = logging.getLogger(__name__)


class Message:
    """
    Base class for messages. Not instantiated directly::

        Message: body, decode(), json(), exchange_name, routing_key, properties
        -> ReturnMessage: reply_code, reply_text
        -> ExpectedMessage: ack(), reject(), reply(), delivery_tag, redelivered
           -> DeliverMessage: consumer_tag
           -> GetMessage: message_count

    .. attribute:: body

        Message's raw body as a :class:`bytearray` instance

            >>> message.body
            bytearray(b'binary data')

    .. attribute:: exchange_name

        Exchange message was published to

    .. attribute:: routing_key

        Routing key message was published with

    .. attribute:: properties

        Message's :class:`~ammoo.wire.frames.header.BasicHeaderProperties`
    """

    def __init__(
            self, exchange_name: ExchangeName, routing_key: RoutingKey, properties: BasicHeaderProperties,
            body: bytearray, channel_toolbox
    ):
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.properties = properties
        self.body = body
        # this is a subset of Channel's attributes. We don't want to give direct access to its internals.
        self._channel_toolbox = channel_toolbox  # a _ChannelToolbox instance

    def decode(self, encoding: Optional[str]=None) -> str:
        """
        Decode body into a :class:`str`. When the encoding argument is not passed, the encoding defaults to the
        message's content-encoding property (if defined), or the channel's default encoding.

        >>> message.decode()
        'text body'
        >>> message.decode('iso-8859-1')
        'sí'

        :param str encoding: Optional: Encoding to use to decode body instead of content-encoding property/channel's \
        default encoding
        :rtype: str
        """
        if encoding is None:
            content_encoding = self.properties.content_encoding
            if content_encoding is None:
                encoding = self._channel_toolbox.default_encoding
            else:
                encoding = content_encoding
        return self.body.decode(encoding)

    def json(self, encoding: Optional[str]=None) -> DecodeJson:
        """
        Decode body as JSON.

        :param str encoding: Optional: Encoding to use to decode body instead of content-encoding property/channel's \
        default encoding
        :rtype: str, bool, int, float, None, dict, list
        """
        return json.loads(self.decode(encoding))


class ReturnMessage(Message):
    """
    Message returned by server as a consequence of using the `mandatory` or `immediate` flags of
    :meth:`Channel.publish`. Subclass of :class:`Message`.

    .. attribute:: reply_code

        :class:`int` code for why message could not be routed to queue/consumed.

    .. attribute:: reply_text

        :class:`str` description of why message was returned.
    """

    __slots__ = 'exchange_name', 'routing_key', 'properties', 'body', '_channel_toolbox', 'reply_code', 'reply_text'
    cam = BASIC_RETURN_CAM

    def __init__(
            self, exchange_name: ExchangeName, routing_key: RoutingKey, properties: BasicHeaderProperties,
            body: bytearray, channel_toolbox, reply_code: int, reply_text: str):
        super().__init__(exchange_name, routing_key, properties, body, channel_toolbox)
        self.reply_code = reply_code
        self.reply_text = reply_text


class ExpectedMessage(Message):
    """
    Base class for :class:`DeliverMessage` and :class:`GetMessage`; not instantiated directly. Subclass of
    :class:`Message`.

    .. attribute:: delivery_tag

        Message's delivery tag, used for acknowledging or rejecting message to server

        .. note:: Using the :meth:`ack` or :meth:`reject` methods of this class instead of :class:`Channel`'s avoids \
        needing to pass the delivery tag explicitly.

    .. attribute: redelivered

        :class:`bool` that is true when the message has been redelivered to queue because it was rejected or \
        dead-lettered before.

    """
    def __init__(
            self, exchange_name: ExchangeName, routing_key: RoutingKey, properties: BasicHeaderProperties,
            body: bytearray, channel_toolbox, delivery_tag: DeliveryTag, redelivered: bool):
        super().__init__(exchange_name, routing_key, properties, body, channel_toolbox)
        self.delivery_tag = delivery_tag
        self.redelivered = redelivered

    async def ack(self) -> None:
        """
        Acknowledge message to server. Calls :meth:`Channel.ack` with the message's delivery tag.
        """
        return await self._channel_toolbox.ack(self.delivery_tag)

    async def reject(self, requeue: bool) -> None:
        """
        Reject message to server. Calls :meth:`Channel.reject` with the message's delivery tag.
        """
        return await self._channel_toolbox.reject(self.delivery_tag, requeue=requeue)

    async def reply(self, body=None, *, correlation_id=None, **kwargs) -> None:
        """
        Publish a reply to a message that has the reply-to property set. If the message has the correlation-id
        property, it's also set on the published message.

        The method accepts the same keyword arguments as :meth:`Channel.publish`.

        .. note:: Direct reply to is a RabbitMQ extension
        """
        # NB: reply_to is the routing key the server should reply to, not the one the client will send
        if not self._channel_toolbox.server_has_capability('direct_reply_to'):
            logger.warn('Server does not support capability direct_reply_to')
        reply_to = self.properties.reply_to
        if reply_to is None:
            raise ValueError('Message has no reply_to property')
        if correlation_id is None:
            correlation_id = self.properties.correlation_id
        return await self._channel_toolbox.publish('', reply_to, body, correlation_id=correlation_id, **kwargs)


class DeliverMessage(ExpectedMessage):
    """
    Message delivered to a :class:`Consumer`. Subclass of :class:`ExpectedMessage`.

    .. attribute:: consumer_tag

        :class:`str` consumer tag parameter of delivered message.
    """

    __slots__ = (
        'exchange_name', 'routing_key', 'properties', 'body', '_channel_toolbox', 'delivery_tag', 'redelivered',
        'consumer_tag'
    )
    cam = BASIC_DELIVER_CAM

    def __init__(
            self, exchange_name: ExchangeName, routing_key: RoutingKey, properties: BasicHeaderProperties,
            body: bytearray, channel_toolbox, delivery_tag: DeliveryTag, redelivered: bool, consumer_tag: ConsumerTag):
        super().__init__(exchange_name, routing_key, properties, body, channel_toolbox, delivery_tag, redelivered)
        self.consumer_tag = consumer_tag


class GetMessage(ExpectedMessage):
    """
    A message from queue returned by calling :meth:`Channel.get`. Subclass of :class:`ExpectedMessage`.

    .. attribute:: message_count

        Number of messages still in queue after getting this message.
    """

    cam = BASIC_GET_OK_CAM
    __slots__ = (
        'exchange_name', 'routing_key', 'properties', 'body', '_channel_toolbox', 'delivery_tag', 'redelivered',
        'message_count'
    )

    def __init__(
            self, exchange_name: ExchangeName, routing_key: RoutingKey, properties: BasicHeaderProperties,
            body: bytearray, channel_toolbox, delivery_tag: DeliveryTag, redelivered: bool, message_count: int):
        super().__init__(exchange_name, routing_key, properties, body, channel_toolbox, delivery_tag, redelivered)
        self.message_count = message_count


# typing to exclude superclasses
AbstractMessage = Union[DeliverMessage, GetMessage, ReturnMessage]


class PartialMessage:
    def __init__(self, channel_toolbox):
        # parameters are basic_get_ok_parameters_pb or basic_deliver_parameters_pb or...
        # basic_return_pb, in which case it doesn't have delivery-tag or some other things
        self._cam = None  # type:  Optional[int]
        self.parameters = None  # type: Union[BasicDeliverParameters, BasicGetOkParameters, BasicReturnParameters]
        self.properties = None  # type: Optional[BasicHeaderProperties]
        self.body = None  # type:  Optional[bytes]
        self._body_offset = None  # type:  Optional[int]
        self._body_size = None  # type:  Optional[int]
        self._channel_toolbox = channel_toolbox

    def _assemble_message(self) -> AbstractMessage:
        """Assembles the method, header and body frames into a message, then resets self ready for the next message
        to be received.
        """
        parameters = self.parameters
        if self._cam == BASIC_DELIVER_CAM:
            message = DeliverMessage(
                parameters.exchange_name, parameters.routing_key, self.properties, self.body, self._channel_toolbox,
                parameters.delivery_tag, parameters.redelivered, parameters.consumer_tag
            )
        elif self._cam == BASIC_GET_OK_CAM:
            message = GetMessage(
                parameters.exchange_name, parameters.routing_key, self.properties, self.body, self._channel_toolbox,
                parameters.delivery_tag, parameters.redelivered, parameters.message_count
            )
        elif self._cam == BASIC_RETURN_CAM:
            message = ReturnMessage(
                parameters.exchange_name, parameters.routing_key, self.properties, self.body, self._channel_toolbox,
                parameters.reply_code, parameters.reply_text
            )
        else:
            raise ValueError(self._cam)
        self._cam = None
        self.parameters = None
        self.properties = None
        self.body = None
        self._body_offset = None
        self._body_size = None
        return message

    def feed_method_frame(self, method_frame: MethodFrame) -> None:
        self._cam = method_frame.cam
        self.parameters = method_frame.parameters
        self.properties = None
        self.body = None
        self._body_offset = None
        self._body_size = None

    def feed_header_frame(self, header_frame: BasicHeaderFrame) -> Optional[AbstractMessage]:
        if self.parameters is None:
            raise UnexpectedFrame(header_frame)
        # XXX: header_frame.class_ not there anymore to check
        #if payload.class_ is not self._cam.class_:
        #    raise UnexpectedFrame(header_frame)
        self.properties = header_frame.properties
        body_size = header_frame.body_size
        self._body_size = body_size
        self._body_offset = 0
        if body_size == 0:
            self.body = bytearray()
            return self._assemble_message()
        return None

    def feed_body_frame(self, body_frame: BodyFrame) -> Optional[AbstractMessage]:
        if self.properties is None:
            raise UnexpectedFrame(body_frame)
        payload = body_frame.payload
        payload_size = body_frame.payload_size
        body_offset = self._body_offset
        body_size = self._body_size
        next_body_offset = body_offset + payload_size
        if next_body_offset > body_size:
            logger.warning('Server sent more body data than header frame specified')
            raise UnexpectedFrame(body_frame)
        if body_offset == 0:
            if payload_size == body_size:
                # first body frame contains all of the body data
                self.body = payload
                return self._assemble_message()
            else:
                self.body = bytearray(body_size)
        self.body[body_offset:next_body_offset] = payload
        if next_body_offset == body_size:
            return self._assemble_message()
        self._body_offset = next_body_offset
        return None
