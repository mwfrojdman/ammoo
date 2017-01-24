.. _declare:

Declare a queue and exchange
============================

.. default-domain:: py

.. module:: ammoo


Declare a queue
---------------

Declare a queue with :meth:`Channel.declare_queue`::

    await channel.declare_queue('my_queue')  # declares queue with explicit name

`declare_queue` returns some parameters, which might come handy if allowing the server to generate a queue name::

    declaration = await channel.declare_queue('', exclusive=True)
    print(declaration.queue_name)
    # something like amq.gen-3Wb1ZY42ejtq31P5LmKVkw on RabbitMQ


Declare an exchange
-------------------

Declare an exchange with :meth:`Channel.declare_exchange`::

    await channel.declare_exchange('my_exchange', 'fanout')

Bind a queue to an exchange
---------------------------

Use :meth:`Channel.bind_queue`::

    await channel.bind_queue('my_queue', 'my_exchange', 'my_routing_key')


Next: :doc:`publish`
