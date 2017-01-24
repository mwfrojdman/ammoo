.. _api:

API documentation
=================

.. module:: ammoo

.. default-domain:: py


Ammoo leverages Python 3.5's new syntax to make working with AMQP easier::

    import ammoo

    async with await ammoo.connect('amqp://broker/') as connection:
        async with connection.channel() as channel:
            await channel.publish('my_exchange', 'routing_key', 'text body')

            async with channel.consume('my_queue') as consumer:
                async for message in consumer:
                    print('Received message: {}'.format(message.body)
                    await message.ack()


Get a message from queue, decode it's body as text and reply with some JSON::

    message = await channel.get('my_queue', no_ack=True)
    await message.reply(json={'original message': message.decode()})


Connect
-------

.. cofunction:: connect(url=None, *, host, port, virtualhost)

    :param str url: URL for the connection. Any component in the URL can be overridden with a keyword argument, or omitted enitrely.
    :param str host: Server hostname or IP address. Defaults to localhost.
    :param int port: Server port. Defaults to 5671 for unencrypted connections and 5672 for SSL.
    :param str virtualhost: AMQP virtualhost to open on connection initialization. Defaults to "/".
    :param bool ssl: Force encryption on/off. By default the connection is unencrypted. Using the amqps schema in the URL turns encryption on.
    :param ssl.SSLContext ssl_context: Explicit SSL context object. Use this argument for eg. client certificates or validate the server certificate against a particular certificate chain.
    :param int heartbeat_interval: Expected number of seconds between frames to consider the connection alive. Both the server and client will send a heartbeat frame with this interval if no other frames have been sent. Defaults to whatever the server suggests. 0 turns heartbeats off.
    :param auth: Authentication mechanism/chooser to use. Defaults to password authentication with the AMQPLAIN or PLAIN mechanism.
    :param str login: Username to use for default ``auth``. Defaults to "guest".
    :param str password: Password to use for default ``auth``. Defaults to "guest".
    :param int frame_max: Maximum AMQP frame size. Must be at least 4096 bytes. Defaults to 128kB, until the server demands a smaller one.
    :param asyncio.AbstractEventLoop loop: Event loop. Defaults to :func:`asyncio.get_event_loop`
    :param connection_factory: Class (or class returning function) to use instead of default :class:`Connection`.

    Connects to an AMQP server and returns a :class:`Connection` instance::

        await ammoo.connect('amqps://myserver/myvhost')
        await ammoo.connect(host='myserver', virtualhost='myvhost', ssl=True)

    Connecting to unencrypted AMQP on `localhost` to virtualhost `/` is simply::

        await ammoo.connect()



Connection
----------

.. class:: Connection()

    An AMQP connection. It's an asynchronous context manager, which does the server handshake while entering, and closes the connection in exit. Most of the class's methods are unusable until the connection has been entered, so use :func:`connect` to get one, and use it within `async for`::

        async with await connect() as connection:
            # correct!

        async with connect() as connection:
            # Fails: connect() is a coroutine that needs to be awaited

        connection = await connect()  # works, but...
        connection.channel()  # raises an exception because connection isn't entered

    .. method:: channel(*, prefetch_size=None, prefetch_count=None)

        :param int prefetch_size: Passed to :meth:`Channel.qos` if used.
        :param int prefetch_count: Passed to :meth:`Channel.qos` if used.
        :rtype: Channel

        Open a new channel. Should be used in a context manager::

            async with connection.channel() as channel:
                ...

        If prefetch_size or prefetch_count are given, :meth:`Channel.qos` is called after opening the channel. The two async with blocks achieve the same::

            async with connection.channel(prefetch_count=5) as channel:
                ...

            async with connection.channel() as channel:
                await channel.qos(prefetch_count=5, prefetch_size=0)
                ...

Channel
-------

.. class:: Channel()

    An AMQP channel.

    .. automethod:: consume(queue_name, *, ...)
    .. autocomethod:: get(queue_name, *, ...)
    .. autocomethod:: ack(delivery_tag)
    .. autocomethod:: reject(delivery_tag, requeue)
    .. autocomethod:: qos(prefetch_size, prefetch_count, global_)
    .. autocomethod:: recover(requeue)

    .. autocomethod:: publish(exchange_name, route[, body], *, ...)
    .. autocomethod:: select_confirm()

    .. autocomethod:: declare_exchange(exchange_name, exchange_type, *, ...)
    .. autocomethod:: delete_exchange(exchange_name, *, ...)
    .. autocomethod:: assert_exchange_exists(exchange_name)
    .. autocomethod:: bind_exchange(destination, source, routing_key)

    .. autocomethod:: declare_queue(queue_name, *, ...)
    .. autocomethod:: delete_queue(queue_name)
    .. autocomethod:: purge_queue(queue_name)
    .. autocomethod:: bind_queue(queue_name, exchange_name, routing_key)
    .. autocomethod:: unbind_queue(queue_name, exchange_name, routing_key)
    .. autocomethod:: assert_queue_exists(queue_name)


Consumer
--------

.. class:: Consumer()

    An AMQP consumer. Asynchronous iterable that returns :class:`DeliverMessage` instances. Must be used in an
    `async for` block::

        async with channel.consume('my_queue') as consumer:
            async for message in consumer:
                ...


Message
-------

.. autoclass:: Message()

    .. automethod:: decode(encoding=None)
    .. automethod:: json(encoding=None)

.. autoclass:: ExpectedMessage()

    .. autocomethod:: ack()
    .. autocomethod:: reject(requeue)
    .. autocomethod:: reply([body], *, ...)

.. autoclass:: DeliverMessage()

.. autoclass:: GetMessage()

.. autoclass:: ReturnMessage()


Message properties
~~~~~~~~~~~~~~~~~~

.. autoclass:: ammoo.wire.frames.header.BasicHeaderProperties
    :members:


Parameters
~~~~~~~~~~

.. autoclass:: ammoo.wire.frames.method.queue.QueueDeclareOkParameters
    :members:
