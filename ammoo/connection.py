import asyncio
import logging
import platform
import time
from collections import deque
from functools import partial
from struct import pack
from traceback import format_exc
from typing import Dict, Union
from typing import MutableSequence
from typing import Optional

from ammoo.auth.abc import Authentication, AuthenticationMechanism
from ammoo.channel import Channel
from ammoo.channel_id_manager import ChannelIdManager, AlreadyFree
from ammoo.exceptions import connection as connection_exceptions
from ammoo.exceptions.connection import UnexpectedFrame, ConnectionClosed, ServerAuthMechanismsNotSupportedError, \
    ServerLocalesNotSupportedError, ClientClosedConnectionError
from ammoo.validatation import validate_frame_max, validate_shortstr
from ammoo.version import __version__
from ammoo.wire import constants
from ammoo.wire.class_and_method import CONNECTION_CLOSE_OK_CAM, CONNECTION_CLOSE_CAM, CONNECTION_START_CAM, \
    CONNECTION_SECURE_CAM, CONNECTION_TUNE_CAM, CONNECTION_OPEN_OK_CAM, CONNECTION_START_OK_CAM, pretty_format_cam, \
    CONNECTION_OPEN_CAM, CONNECTION_SECURE_OK_CAM, CONNECTION_TUNE_OK_CAM
from ammoo.wire.frames import FRAME_TYPE_HEARTBEAT
from ammoo.wire.frames import FRAME_TYPE_METHOD
from ammoo.wire.frames import build_frame
from ammoo.wire.frames import parse_frame
from ammoo.wire.frames.method import MethodFrame
from ammoo.wire.frames.method.connection import pack_connection_start_ok_parameters, pack_connection_open_parameters, \
    pack_connection_tune_ok_parameters, CLEAN_CONNECTION_CLOSE_PARAMETERS
from ammoo.wire.low.exceptions import InvalidParse
from ammoo.wire.low.peer_properties import PeerProperties
from ammoo.wire.low.strings import pack_binary_longstr
from ammoo.wire.typing import VirtualHost

__all__ = 'Connection',


logger = logging.getLogger(__name__)


class Connection(asyncio.StreamReaderProtocol):
    amqp_encoding = 'utf-8'

    client_properties = PeerProperties(
        host=None,
        product='ammoo',
        version=__version__,
        # use just platform.system() and not platform.platform() to not reveal exact OS details to peer
        platform='Python {} on {}'.format(platform.python_version(), platform.system()),
        copyright='Mathias Frojdman 2016',
        information=None,  # TODO: add URL here
        capabilities={
            'authentication_failure_close': True,  # https://www.rabbitmq.com/auth-notification.html
            'consumer_cancel_notify': True,  # https://www.rabbitmq.com/consumer-cancel.html,
        },
        others={}
    )

    def __init__(
            self,
            virtualhost: VirtualHost,
            loop: asyncio.AbstractEventLoop,
            heartbeat_interval: Optional[int],
            auth: Union[Authentication, AuthenticationMechanism],
            frame_max: Optional[int],
            body_encoding: str='utf-8'
    ):
        validate_shortstr(virtualhost, 'virtualhost')
        if not isinstance(loop, asyncio.AbstractEventLoop):
            raise TypeError('loop is not an asyncio event loop: {!r}'.format(loop))
        if heartbeat_interval is not None:
            if not isinstance(heartbeat_interval, int):
                raise TypeError()
            if heartbeat_interval < 0:
                raise ValueError('heartbeat_interval must be >= 0: Not {}'.format(heartbeat_interval))
        if not isinstance(auth, (Authentication, AuthenticationMechanism)):
            raise TypeError('auth is not a Authentication or AuthenticationMechanism instance: {!r}'.format(auth))
        if frame_max is not None:
            validate_frame_max(frame_max)
        self._reader = None  # type: Optional[asyncio.StreamReader] # set in _client_connected()
        self._writer = None  # type: Optional[asyncio.StreamWriter] # set in _client_connected()
        self._transport = None  # type: Optional[asyncio.Transport]  # set in connection_made()
        self._dispatcher = None  # type: Optional[Task]  # set in __aenter__()
        self._entered = False  #  type: bool  # set to true in __aenter__()
        self._virtualhost = virtualhost  #  type: VirtualHost
        # the heartbeat interval suggested by the user of this library
        self._client_heartbeat_interval = heartbeat_interval  # type: Optional[int]
        # heartbeat interval negotiated with the server. May be different from self._client_heartbeat_interval
        # set in _tune()
        self._heartbeat_interval = None  #  type: Optional[int]
        # timestamp of last sent frame. Used to determine when the next heartbeat needs to be sent. Set in _send_frame
        self._last_send_timestamp = None  # type: Optional[float]
        # timestamp of last received frame. Used to determine when heartbeat timeout fires
        self._last_receive_timestamp = None  # set in _dispatch_frames()
        # task sending heartbeats to broker, set in tune()
        self._heartbeat_sender = None  # type: Optional[asyncio.Task]
        # task using _last_receive_timestamp to check broker sends frames regularly, set in tune()
        self._heartbeat_monitor = None  # type: Optional[asyncio.Task]
        # frame max suggested by the user of this library
        self._client_frame_max = frame_max  # type: Optional[int]
        # frame max negotiated with broker, set in _tune()
        self._frame_max = None  # type: Optional[int]
        self._loop = loop
        # Authentication used for login with connection.start(-ok) and connection.secure(-ok) methods
        self.auth = auth
        # Keeps track of unused channel ids
        self._channel_id_manager = None # set in _tune()
        # Exception is set when connection is closing/closed.
        self._closing_exc = None
        self._channels = {}  # channel id -> Channel
        self._server_properties = None  # set in _enter_start()
        # Queues for method frame waiters. Library sends a class.method frame, adds a new future to the queue and waits
        # until it is resolved by the dispatcher receiving a class.method-ok frame and setting it as a result.
        # Having a queue of futures means it's possible to wait for multiple method frames of the same type without
        # locks. They're resolved in the same order they were sent out to the broker. Not really used in Connection,
        # but Channel uses plenty of it (send many basic.get methods before receiving basic.get-ok/basic.empty,
        # declare multiple queues etc).
        secure_or_tune_deque = deque()  # type: Dict[int, MutableSequence[asyncio.Future]]
        self._cam_waiters = {
            CONNECTION_CLOSE_OK_CAM: deque(),
            CONNECTION_START_CAM: deque(),
            CONNECTION_SECURE_CAM: secure_or_tune_deque,
            CONNECTION_TUNE_CAM: secure_or_tune_deque,
            CONNECTION_OPEN_OK_CAM: deque(),
        }
        # RabbitMQ and Qpid has their own spec, different from vanilla AMQP 0.9.1 for table/array field types.
        # Only some field types are supported in RabbitMQ/Qpid, and some have a different type byte.
        self.rabbitmq = None  # type: Optional[bool]
        # Used by _drain_writer. Not in the shower.
        self._drain_lock = asyncio.Lock(loop=loop)
        self.body_encoding = body_encoding
        asyncio.StreamReaderProtocol.__init__(
            self, asyncio.StreamReader(loop=self._loop), self._client_connected, loop=self._loop)

    @property
    def heartbeat_interval(self) -> int:
        heartbeat_interval = self._heartbeat_interval
        if heartbeat_interval is None:
            raise ValueError('heartbeat_interval not yet set for this connection')
        return heartbeat_interval

    @property
    def frame_max(self) -> int:
        frame_max = self._frame_max
        if frame_max is None:
            raise ValueError('frame_max not yet set for this connection')
        return frame_max

    def _client_connected(self, reader, writer):
        self._reader = reader
        self._writer = writer

    def connection_made(self, transport):
        self._transport = transport
        super().connection_made(transport)

    def connection_lost(self, exc):
        logger.debug('Connection lost: %r', exc)
        if self._dispatcher is not None:
            self._dispatcher.cancel()
            # it will be set to None in exit()
        # exc is None when client closes the connection -> do not call _close again
        if exc is not None:
            self._close(connection_exceptions.ConnectionLost(exc))
        super().connection_lost(exc)

    @property
    def entered(self) -> bool:
        return self._entered

    @property
    def closing(self) -> bool:
        return self._closing_exc is not None

    def _close(self, close_exc: ConnectionClosed):
        """Only call on errors when connection needs to be abruptly closed without notifying the server"""
        logger.debug('_close because %s', close_exc)
        if self.closing:
            logger.debug('Not setting self._closing_exc because it\'s already %s', self._closing_exc)
        else:
            logger.debug('self._closing_exc is None, setting it to %s', close_exc)
            self._closing_exc = close_exc
        if self._transport is not None:
            logger.debug('Closing transport in _close')
            self._transport.close()
            self._transport = None
        logger.debug('Cancelling subscribers because of close exception %s', close_exc)
        for futures in self._cam_waiters.values():
            for future in futures:
                future.set_exception(self._closing_exc)
            futures.clear()
        for channel in list(self._channels.values()):
            channel.release(self._closing_exc)
        assert not self._channels

    def _dispatch_frame(self, frame):
        logger.debug('Received frame: %s', frame)
        self._last_receive_timestamp = time.monotonic()
        if self.closing:
            if frame.frame_type != FRAME_TYPE_METHOD:
                logger.debug('Closing so ignoring frame of type %s', frame.frame_type)
                return
            elif frame.cam not in (CONNECTION_CLOSE_CAM, CONNECTION_CLOSE_OK_CAM):
                logger.debug('Closing so ignoring method %s frame', pretty_format_cam(frame.cam))
                return
        try:
            frame_type = frame.frame_type
            if frame_type == FRAME_TYPE_HEARTBEAT:
                if self._heartbeat_interval is None or self._heartbeat_interval == 0:
                    logger.warning('Server sent heartbeat frame though heartbeats are turned off')
                    raise UnexpectedFrame(frame)
            elif frame.channel_id > 0:
                channel_id = frame.channel_id
                try:
                    channel = self._channels[channel_id]
                except KeyError as exc:
                    logger.warning('Got frame for channel id %s, but it is not open', channel_id)
                    raise UnexpectedFrame(frame) from exc
                else:
                    channel.dispatch_frame(frame)
            elif frame_type == FRAME_TYPE_METHOD:
                cam = frame.cam
                if cam == CONNECTION_CLOSE_CAM:
                    self._on_connection_close(frame)
                elif cam in self._cam_waiters:
                    try:
                        future = self._cam_waiters[cam].popleft()
                    except IndexError:
                        raise UnexpectedFrame(frame)
                    if not future.done():
                        future.set_result(frame)
                else:
                    raise UnexpectedFrame(frame)
            else:
                raise UnexpectedFrame(frame)
        except UnexpectedFrame:
            # TODO: send a 503 or whatnot connection.close to server? Need to wait for connection.close-ok then?
            raise

    async def _dispatch_frames(self):
        """Reads frames from self._reader and dispatches them to handlers"""
        while True:
            try:
                frame = await parse_frame(self._reader, self.amqp_encoding, self.rabbitmq)
            except asyncio.CancelledError:
                raise
            except InvalidParse as exc:
                logger.error('Could not parse server data: %s', exc)
                return self._close(connection_exceptions.InvalidServerFrame(exc))
            except Exception as exc:
                logger.error('Unhandled exception occurred in parsing frame: %s', format_exc())
                return self._close(connection_exceptions.ClientClosedConnectionError(exc))
            try:
                self._dispatch_frame(frame)
            except asyncio.CancelledError:
                raise
            except UnexpectedFrame as exc:
                return self._close(exc)
            except Exception as exc:
                logger.error('Unhandled exception occurred in frame dispatch: %s', format_exc())
                return self._close(connection_exceptions.ClientClosedConnectionError(exc))

    def _on_connection_close(self, frame: MethodFrame) -> None:
        logger.debug('in _on_connection_close')
        if not self.closing:
            logger.debug('Setting _closing_reason to ConnectionClosed by server')
            self._closing_exc = connection_exceptions.ServerClosedConnection(frame.parameters)
        else:
            logger.debug('self.closing is already true, not setting _closing_exc')
        self._send_method_frame(0, CONNECTION_CLOSE_OK_CAM, b'')

    async def __aenter__(self) -> 'Connection':
        if self.entered:
            raise ValueError('Connection already entered')
        self._entered = True
        if self.closing:
            raise self._closing_exc
        self._dispatcher = self._loop.create_task(self._dispatch_frames())
        # run remainder of function in try and do cleanup similar to __aexit__ if it fails
        try:
            self._writer.write(constants.PROTOCOL_HEADER)
            tune_frame = await self._enter_start()
            self._tune(tune_frame)
            await self._connection_open()
            self._entered = True
            logger.debug('enter done!')
        except ConnectionClosed as exc:
            logger.error(format_exc())
            if not self.closing:
                self._closing_exc = exc
            await self._cleanup()
            raise
        except Exception as exc:
            logger.error(format_exc())
            if not self.closing:
                self._closing_exc = connection_exceptions.ClientClosedConnectionError(exc)
            await self._cleanup()
            raise
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self.entered:
            raise ValueError('Connection not entered')
        try:
            if not self.closing:
                self._send_method_frame(0, CONNECTION_CLOSE_CAM, CLEAN_CONNECTION_CLOSE_PARAMETERS)
                self._closing_exc = connection_exceptions.ClientClosedConnectionOK()
                await self._wait_for_cam_frame(CONNECTION_CLOSE_OK_CAM)
        finally:
            await self._cleanup()

    async def _wait_for_cam_frame(self, cam: int) -> MethodFrame:
        future = self._loop.create_future()
        self._cam_waiters[cam].append(future)
        log_debug = logger.isEnabledFor(logging.DEBUG)
        if log_debug:
            logger.debug('Waiting for %s', pretty_format_cam(cam))
        method_frame = await future
        if log_debug:
            logger.debug('Received %s (same key as for %s)', pretty_format_cam(method_frame.cam), pretty_format_cam(cam))
        return method_frame

    async def _cleanup(self) -> None:
        """Called always from exit(). Called from enter() when an exception occured"""
        cancellees = set()
        dispatcher = self._dispatcher
        if dispatcher is not None:
            logger.debug('Cancelling dispatcher')
            if not dispatcher.done():
                dispatcher.cancel()
            self._dispatcher = None
            cancellees.add(dispatcher)
        heartbeat_sender = self._heartbeat_sender
        if heartbeat_sender is not None:
            logger.debug('Cancelling heartbeat sender')
            if not heartbeat_sender.done():
                heartbeat_sender.cancel()
            self._heartbeat_sender = None
            cancellees.add(heartbeat_sender)
        heartbeat_monitor = self._heartbeat_monitor
        if heartbeat_monitor is not None:
            logger.debug('Cancelling heartbeat monitor')
            if not heartbeat_monitor.done():
                heartbeat_monitor.cancel()
            self._heartbeat_monitor = None
            cancellees.add(heartbeat_sender)
        for cancellee in cancellees:
            try:
                await cancellee
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.error('Not a CancelledError: %r', exc)
                raise
        if self._transport is not None:
            logger.debug('Closing transport in _cleanup')
            self._transport.close()
            self._transport = None
        return

    async def _enter_start(self) -> MethodFrame:
        """Waits for connection.start, saves server properties to self, sends uses self.auth to authenticate with
        connection.start-ok. It then replies to connection.secure methods with connection.secure-ok until the broker
        sends a connection.tune methods indicating authentication is done.
        :return: The connection.tune method frame sent by the broker
        """
        start_method_frame = await self._wait_for_cam_frame(CONNECTION_START_CAM)
        server_properties = start_method_frame.parameters.server_properties
        self._server_properties = server_properties
        self.rabbitmq = server_properties.product == 'RabbitMQ'  # TODO: Figure out what product string QPid uses
        if 'en_US' not in start_method_frame.parameters.locales:
            exc = ServerLocalesNotSupportedError(start_method_frame.parameters.locales)
            self._close(exc)
            raise exc
        if isinstance(self.auth, Authentication):
            try:
                auth_mech = self.auth.select_mechanism(start_method_frame.parameters.mechanisms)
            except ServerAuthMechanismsNotSupportedError as exc:
                self._close(exc)
                raise
        else:
            auth_mech = self.auth
            if auth_mech.name() not in start_method_frame.parameters.mechanisms:
                exc = ServerAuthMechanismsNotSupportedError()
                self._close(exc)
                raise exc
        start_ok_response = auth_mech.build_start_ok_response(self.amqp_encoding, self.rabbitmq)  # type: bytes
        start_ok_params = pack_connection_start_ok_parameters(
            self.client_properties,
            auth_mech.name(),
            start_ok_response,
            'en_US',
            self.amqp_encoding
        )
        self._send_method_frame(0, CONNECTION_START_OK_CAM, start_ok_params)

        while True:
            # if our auth in connection.start-ok's response parameter wasn't any good, this is where the server will
            # close the connection
            frame = await self._wait_for_cam_frame(CONNECTION_TUNE_CAM)  # same deque() also for connection.secure
            if frame.cam == CONNECTION_SECURE_CAM:
                try:
                    start_ok_response = await auth_mech.challenge(frame.parameters)
                except ConnectionClosed as exc:
                    # most likely an AuthenticationError
                    self._close(ClientClosedConnectionError(exc))
                    raise
                except Exception as exc:
                    self._close(ClientClosedConnectionError(exc))
                    raise
                if not isinstance(start_ok_response, bytes):
                    exc = ValueError('{!r}.challenge did not return bytes: {!r}'.format(
                        auth_mech.__class__, start_ok_response
                    ))
                    self._close(ClientClosedConnectionError(exc))
                    raise exc
                self._send_method_frame(0, CONNECTION_SECURE_OK_CAM, pack_binary_longstr(start_ok_response))
            else:  # connection.tune
                return frame

    def _tune(self, tune_frame: MethodFrame) -> None:
        """Takes the connection settings suggested by the broker and replies with a connection.tune-ok method.
        Starts tasks for sending heartbeats and monitoring for received ones, if heartbeat_interval > 0.
        """
        tune_params = tune_frame.parameters
        if tune_params.channel_max == 0:
            channel_max = None  # zero means no max
        else:
            channel_max = tune_params.channel_max
        self._channel_id_manager = ChannelIdManager(channel_max)

        if tune_params.frame_max < self._client_frame_max:
            self._frame_max = tune_params.frame_max
        else:
            self._frame_max = self._client_frame_max

        if self._client_heartbeat_interval is None:
            self._heartbeat_interval = tune_params.heartbeat
        elif tune_params.heartbeat == 0:
            self._heartbeat_interval = self._client_heartbeat_interval
        else:
            self._heartbeat_interval = min(self._client_heartbeat_interval, tune_params.heartbeat)
        logger.debug('Setting heartbeat interval to %r', self._heartbeat_interval)

        tune_ok_params = pack_connection_tune_ok_parameters(
            tune_params.channel_max, self._frame_max, self._heartbeat_interval)
        self._send_method_frame(0, CONNECTION_TUNE_OK_CAM, tune_ok_params)
        if self._heartbeat_interval > 0:
            self._heartbeat_sender = self._loop.create_task(self._send_heartbeats())
            self._heartbeat_monitor = self._loop.create_task(self._monitor_heartbeats(self._heartbeat_interval))

    async def _connection_open(self) -> None:
        """Sends a connection.open method, then waits for connection.open-ok"""
        connection_open_params = pack_connection_open_parameters(self._virtualhost, [], False, self.amqp_encoding)
        self._send_method_frame(0, CONNECTION_OPEN_CAM, connection_open_params)
        await self._wait_for_cam_frame(CONNECTION_OPEN_OK_CAM)
        return

    async def _monitor_heartbeats(self, heartbeat_interval: int, *, heartbeat_window: int=2) -> None:
        """
        Closes the connection if broker doesn't send frames (heartbeat or not) in heartbeat_interval *
        heartbeat_multiplier seconds.

        :param heartbeat_interval: Interval in seconds client expects broker to send heartbeats
        :param heartbeat_window: Number of heartbeats allowed to be missed before closing the connection
        """
        heartbeat_timeout = heartbeat_interval * heartbeat_window
        while True:
            delay = self._last_receive_timestamp + heartbeat_timeout - time.monotonic()
            if delay > 0.0:
                await asyncio.sleep(delay, loop=self._loop)
            else:
                break
        if self.closing:
            return
        self._close(connection_exceptions.HeartbeatTimeout(heartbeat_timeout))

    async def _send_heartbeats(self) -> None:
        """
        Coroutine to be run as task started when the heartbeat interval has been negotiated with the broker with
        connection.tune and connection.tune-ok. Sends heartbeat frames every heartbeat_interval seconds, if no other
        frames have been sent in the meanwhile. It trusts they set self._last_send_timestamp, so it can skip sending
        a heartbeat in vain.
        """
        # NB: must not be called before first frame is sent
        while True:
            # this loop takes care of not sending heartbeat frames if other frames have been sent
            while True:
                delay = self._last_send_timestamp + self._heartbeat_interval - time.monotonic()
                if delay > 0.0:
                    await asyncio.sleep(delay, loop=self._loop)
                else:
                    break
            if self.closing:
                break
            else:
                logger.debug('Sending heartbeat')
                self._send_frame(FRAME_TYPE_HEARTBEAT, 0, b'')
        return

    def _send_method_frame(self, channel_id: int, cam: int, parameters: bytes) -> None:
        if self.closing and cam != CONNECTION_CLOSE_OK_CAM:
            raise ValueError('Refusing to send frame when closing')
        payload = pack('!I', cam) + parameters
        build_frame(self._writer, FRAME_TYPE_METHOD, channel_id, payload)
        self._last_send_timestamp = time.monotonic()

    def _send_frame(self, frame_type: int, channel_id: int, payload: bytes) -> None:
        if self.closing:
            raise ValueError('Refusing to send frame when closing')
        build_frame(self._writer, frame_type, channel_id, payload)
        self._last_send_timestamp = time.monotonic()

    async def _drain_writer(self) -> None:
        # need to wrap StreamWriter.drain in a lock to avoid AssertionError in FlowControlMixin._drain_helper
        # https://groups.google.com/forum/#!topic/python-tulip/JA0-FC_pliA
        async with self._drain_lock:
            await self._writer.drain()
        return

    def server_has_capability(self, capability: str) -> bool:
        return self._server_properties.capabilities.get(capability, False)

    def _on_channel_exit(self, channel_id: int) -> None:
        # _on_channel_exit will be called twice from Channel.release for open channels when connection is closed:
        # once by Channel.__aexit__ and once by Connection._close. That's why we need to check if channel_id is still
        # in self._channels and the channel id manager
        if channel_id in self._channels:
            del self._channels[channel_id]
        try:
            self._channel_id_manager.release(channel_id)
        except AlreadyFree:
            pass

    def channel(self, *, prefetch_size: Optional[int]=None, prefetch_count: Optional[int]=None) -> Channel:
        if not self.entered:
            raise ValueError()
        if self.closing:
            raise self._closing_exc
        channel_id = self._channel_id_manager.acquire()
        channel = Channel(
            channel_id,
            self._send_frame,
            self._send_method_frame,
            self._loop,
            partial(self._on_channel_exit, channel_id),
            self._frame_max,
            self.server_has_capability,
            self._drain_writer,
            prefetch_size=prefetch_size,
            prefetch_count=prefetch_count,
            body_encoding=self.body_encoding,
            amqp_encoding=self.amqp_encoding,
            rabbitmq=self.rabbitmq
        )
        self._channels[channel_id] = channel
        return channel
