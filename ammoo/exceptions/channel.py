from ammoo.wire.class_and_method import cam_long_uint, pretty_format_cam
from ammoo.wire.typing import ReplyCode
from ammoo.wire.frames.method.channel import ChannelCloseParameters

__all__ = (
    'ChannelClosed', 'ServerClosedChannel', 'ClientClosedChannel', 'ClientClosedChannelOK', 'ClientClosedChannelError',
    'EmptyQueue', 'PublishNack', 'ChannelInactive'
)


class ChannelClosed(Exception):
    def __init__(self):
        raise Exception('Do not instantiate me directly')


class ServerClosedChannel(ChannelClosed):
    def __init__(self, parameters: ChannelCloseParameters):
        self.reply_code = parameters.reply_code  # type: ReplyCode
        self.reply_text = parameters.reply_text  # type: str
        self.class_id = parameters.class_id  # type: int
        self.method_id = parameters.method_id  # type: int

    def __str__(self) -> str:
        if self.class_id != 0 and self.method_id != 0:
            cam_text = ' because of method {}'.format(pretty_format_cam(cam_long_uint(self.class_id, self.method_id)))
        else:
            cam_text = ''
        return 'Server closed channel with code {}{}: {}'.format(self.reply_code, cam_text, self.reply_text)


class ClientClosedChannel(ChannelClosed):
    pass


class ClientClosedChannelOK(ClientClosedChannel):
    def __init__(self):
        pass

    def __str__(self):
        return 'Client closed channel without errors'


class ClientClosedChannelError(ClientClosedChannel):
    def __init__(self, exc: Exception):
        self.exc = exc  # type: Exception

    def __str__(self):
        return 'Client closed channel because of an unhandled exception: {!r}'.format(self.exc)


class EmptyQueue(Exception):
    def __init__(self):
        pass

    def __str__(self):
        return 'Queue is empty'


class PublishNack(Exception):
    def __init__(self):
        pass

    def __str__(self):
        return 'Publishing message failed because server replied with a negative acknowledgement'


class ChannelInactive(Exception):
    def __init__(self):
        pass

    def __str__(self):
        return 'Publishing message failed because the channel is not active'
