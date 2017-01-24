.. _get:

Get messages
============

.. default-domain:: py

.. module:: ammoo

Direct access to a queue
------------------------

Messages can be retrieved from a queue synchronously with the :meth:`Channel.get` method::

    message = await channel.get('my_queue')

If the queue is empty, an :exc:`EmptyQueue` exception is raised. Otherwise `get` returns a :class:`GetMessage` object.
It has a body attribute, which is a `bytearray`::

    message.body
    # bytearray(b'message body')

To get a `str` instead, call `message.decode`::

    message.decode()
    # 'message body'

While using message.body.decode() is possible, message.decode is shorter and uses the message's content-encoding
property if set.

If the message body is JSON, use the `json` method to decode it::

    message.json()
    # ['body', 'is', {'json': True}]


Finally, acknowledge the message (unless ``no_ack=True`` was passed to `get`)::

    await message.ack()

or reject it::

    await message.reject(requeue=False)

Subscribe to messages from queue with a consumer
------------------------------------------------

Creating a consumer on queue will make the server send messages to the client when they arrive in the queue, without
the need to retrieve each one separately. Calling :meth:`Channel.consume` and entering the returned :class:`Consumer`
context with `async for` subscribes to messages from a queue. To access the messages delivered to the consumer, use
`async for`::

    async with channel.consume('my_queue') as consumer:
        async for message in consumer:
            print(message.body)
            await message.ack()

