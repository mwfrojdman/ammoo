import asyncio
import socket
import ssl as ssl_module
from functools import partial
from typing import Optional
from urllib.parse import urlparse, parse_qs

from ammoo.auth.abc import Authentication
from ammoo.wire.typing import VirtualHost

from ammoo.auth.password import PasswordAuthentication
from ammoo.connection import Connection


DEFAULT_FRAME_MAX = 128 * 1024
DEFAULT_HEARTBEAT = None
DEFAULT_LOGIN = 'guest'
DEFAULT_PASSWORD = 'guest'


async def connect(
        url: Optional[str]=None, *,
        host: Optional[str]=None,
        port: Optional[int]=None,
        virtualhost: Optional[VirtualHost]= '/',
        ssl: bool=False,
        ssl_context: Optional[ssl_module.SSLContext]=None,
        heartbeat_interval: Optional[int]=None,
        auth: Optional[Authentication]=None,
        login: Optional[str]=None,
        password: Optional[str]=None,
        frame_max: Optional[int]=None,
        loop: Optional[asyncio.AbstractEventLoop]=None,
        connection_factory=Connection
) -> Connection:
    """
    :param url: URL to connect to. Has the format "amqp[s]://login:password@host:port/virtualhost?parameters". All of \
    the URL components are optional. Any other arguments used override the values in the URL.
    :param host: Server address or hostname. Defaults to "localhost".
    :param port: Server port. Defaults to 5672 for AMQP and 5671 or AMQPS.
    :param virtualhost: Virtual host to open. Defaults to "/".
    :param ssl: Use AMQPS instead of unencrypted AMQP.
    :param ssl_context:
    :param heartbeat_interval: Heartbeat interval in seconds to send to and receive heartbeats from server. By \
    default the value suggested by the server is used. 0 turns heartbeats off.
    :param auth: Authentication/AuthenticationMechanism instance to use instead of default PasswordAuthentication with
    the login and password arguments.
    :param login: Username to use for plain password authentication. Defaults to "guest".
    :param password: Password to go along with login. Defaults to "guest".
    :param frame_max: Maximum frame size. Default is 128k, or lower if the server demands it.
    :param loop: Optional asyncio event loop to use
    :param connection_factory: Optional connection class returning function to use instead of Connection
    :return: Connection instance

    """
    if url is not None:
        parsed_url = urlparse(url)
        if not parsed_url.scheme or parsed_url.scheme == 'amqp':
            if ssl is None:
                ssl = False
        elif parsed_url.scheme == 'amqps':
            if ssl is None:
                ssl = True
        else:
            raise ValueError('URL schema must be amqp, amqps or empty')
        if parsed_url.query:
            parsed_query = {key: value[0] for key, value in parse_qs(parsed_url.query, strict_parsing=True).items()}
        else:
            parsed_query = {}
    else:
        parsed_url = None
        parsed_query = None

    if host is None:
        if parsed_url is None or parsed_url.hostname is None:
            host = 'localhost'
        else:
            host = parsed_url.hostname

    if frame_max is None:
        if parsed_query is None or 'frame_max' not in parsed_query:
            frame_max = DEFAULT_FRAME_MAX
        else:
            frame_max = int(parsed_query['frame_max'])

    if ssl:
        if ssl_context is None:
            ssl_context = ssl_module.create_default_context()
    elif ssl_context is not None:
        ssl = True

    if heartbeat_interval is None:
        if parsed_query is None or 'heartbeat_interval' not in parsed_query:
            heartbeat_interval = DEFAULT_HEARTBEAT
        else:
            heartbeat_interval = int(parsed_query['heartbeat_interval'])

    if port is None:
        if parsed_url is None or parsed_url.port is None:
            port = 5671 if ssl else 5672
        else:
            port = parsed_url.port

    if auth is None:
        if login is None:
            if parsed_url is None or parsed_url.username is None:
                login = DEFAULT_LOGIN
            else:
                login = parsed_url.username
        if password is None:
            if parsed_url is None or parsed_url.password is None:
                password = DEFAULT_PASSWORD
            else:
                password = parsed_url.password
        auth = PasswordAuthentication(login, password)

    if loop is None:
        loop = asyncio.get_event_loop()

    protocol_factory = partial(
        connection_factory, virtualhost=virtualhost, loop=loop, heartbeat_interval=heartbeat_interval, auth=auth,
        frame_max=frame_max
    )

    transport, protocol = await loop.create_connection(protocol_factory, host, port, ssl=ssl_context)

    # Without TCP_NODELAY the RPC benchmark gets ~ 80x slower.
    # Not setting TCP_CORK, as that would have a 200ms delay and would probably just slow down the many small packets
    # we send (basically anything except basic.publish + its header and body frames, which we can handle by replacing
    # the 2+#body_frames StreamWriter.write calls with fewer ones if needed)
    transport_socket = transport.get_extra_info('socket')
    if (transport_socket is not None and
            transport_socket.family in {socket.AF_INET, socket.AF_INET6} and
            transport_socket.type == socket.SOCK_STREAM and
            transport_socket.proto == socket.IPPROTO_TCP):
        transport_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return protocol
