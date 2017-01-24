class ConsumerCancelled(Exception):
    def __init__(self):
        raise Exception('Do not instantiate me directly')


class ServerCancelledConsumer(ConsumerCancelled):
    __slots__ = ()

    def __init__(self):
        pass

    def __str__(self):
        return 'Server cancelled consumer'


class ClientCancelledConsumer(ConsumerCancelled):
    pass


class ClientCancelledConsumerOK(ClientCancelledConsumer):
    __slots__ = ()

    def __init__(self):
        pass

    def __str__(self):
        return 'Client cancelled consumer without errors'


class ClientCancelledConsumerError(ClientCancelledConsumer):
    __slots__ = 'exc',

    def __init__(self, exc: Exception):
        self.exc = exc

    def __str__(self):
        return 'Client cancelled consumer because of an unhandled exception: {!r}'.format(self.exc)
