.. _connect:

Connect to AMQP server
======================

.. default-domain:: py

.. module:: ammoo

Use the :func:`connect` function to connect to an AMQP server, which returns a :class:`Connection` object. The
connection to the server is opened when entering the `async for` block, and likewise closed when it's exited.

Most of the library's functionality is in the :class:`Channel` class. Call :meth:`Connection.channel` to open one. It's
a context manager too: The channel is closed when `async for` exits::

    import ammoo

    async with await ammoo.connect('amqp://localhost/') as connection:
        async with connection.channel() as channel:
            pass  # channel is open

A connection may have several channels open at the same time::

    async with await ammoo.connect('amqp://localhost/') as connection:
        async with connection.channel() as channel_1, connection.channel() as channel_2:
            print('two channels opened')
            async with connection.channel() as channel_3:
                print('yet another one opened - 3 in total')
            print('channel_3 is closed now')

Next: :doc:`declare`
