from typing import Union, Set

from ammoo.wire.frames import Frame
from ammoo.wire.low.exceptions import InvalidParse
from ammoo.wire.class_and_method import pretty_format_cam, cam_long_uint
from ammoo.wire.typing import ReplyCode
from ammoo.wire.frames.method.connection import ConnectionCloseParameters


class ConnectionClosed(Exception):
    """Superclass for connection lost + server and client closed exceptions. Not instantiated directly."""

    def __init__(self):
        raise Exception('Do not call me')


class ConnectionLost(ConnectionClosed):
    """This means the broker closed the connection without a connection.close method frame, or we just lost network
    connectivity."""

    def __init__(self, exc: Exception):
        self.exc = exc  # type: Exception

    def __str__(self):
        return 'Connection to server was lost: {}'.format(self.exc)


class ServerClosedConnection(ConnectionClosed):
    """Server sent a connection.close method frame"""

    def __init__(self, parameters: ConnectionCloseParameters):
        self.reply_code = parameters.reply_code  # type: ReplyCode
        self.reply_text = parameters.reply_text  # type: str
        self.class_id = parameters.class_id  # type: int
        self.method_id = parameters.method_id  # type: int

    def __str__(self):
        if self.class_id != 0 and self.method_id != 0:
            cam_text = ' because of method {}'.format(pretty_format_cam(cam_long_uint(self.class_id, self.method_id)))
        else:
            cam_text = ''
        return 'Server closed connection with code {}{}: {}'.format(self.reply_code, cam_text, self.reply_text)


class ClientClosedConnection(ConnectionClosed):
    pass


class ClientClosedConnectionOK(ClientClosedConnection):
    def __init__(self):
        pass

    def __str__(self):
        return 'Client closed connection without errors'


class ClientClosedConnectionError(ClientClosedConnection):
    """Use this class only to wrap exceptions we did not expect to happen. Ie. we do not know why self.exc was raised"""

    def __init__(self, exc: Exception):
        self.exc = exc  # type: Exception

    def __str__(self):
        return 'Client closed connection because of an unhandled exception: {!r}'.format(self.exc)


class ServerAuthMechanismsNotSupportedError(ClientClosedConnection):
    def __init__(self):
        pass

    def __str__(self):
        return 'Client closed connection because none of the server\'s auth mechanisms are supported'


class AuthenticationError(ClientClosedConnection):
    """Raised by authentication mechanism classes in build_start_ok_response() or challenge() when they're not able
    to authenticate with the server. May be subclassed to provide a more specific exception"""

    def __init__(self, mechanism: str):
        self.mechanism = mechanism

    def __str__(self):
        return 'Client closed connection because it could not authenticate with the server using the {} mechanism'.format(
            self.mechanism
        )


class NoChallengesAuthenticationError(AuthenticationError):
    def __str__(self):
        return (
            'Client closed connection because the server sent a connection.secure method with a challenge, which '
            'authentication mechanism {} does not support'
        ).format(self.mechanism)


class ServerLocalesNotSupportedError(ClientClosedConnection):
    def __init__(self, server_locales: Set[str]):
        self.server_locales = server_locales

    def __str__(self) -> str:
        return 'Client closed connection because none of the server\'s locales are supported: {}'.format(
            ', '.join(self.server_locales)
        )


class HeartbeatTimeout(ClientClosedConnection):
    def __init__(self, timeout: Union[int, float]):
        self.timeout = timeout  # type: Union[int, float]

    def __str__(self):
        return 'Client closed connection because server did not send any frames in {} seconds'.format(self.timeout)


class InvalidServerFrame(ClientClosedConnection):
    def __init__(self, parse_exc: InvalidParse):
        self.parse_exc = parse_exc  # type: InvalidParse

    def __str__(self):
        return 'Client closed connection because server sent invalid frame data: {}'.format(self.parse_exc)


class UnexpectedFrame(ClientClosedConnection):
    def __init__(self, frame: Frame):
        self.frame = frame  # type: Frame

    def __str__(self):
        return 'Client closed connection because server sent an unexpected frame: {}'.format(self.frame)
