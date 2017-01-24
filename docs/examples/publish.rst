.. _publish:

Publish
=======

.. default-domain:: py

.. module:: ammoo


Publish bytes, str and json bodies
----------------------------------

Publishing messages happens with :meth:`Channel.publish`, which has a rather huge amount of arguments. Only three are
mandatory though: *exchange_name*, *route* and *body*/*json*. Publish some bytes::

    await channel.publish('my_exchange', 'my_routing_key', b'message body')

A :class:`str` can be used for body. It will be encoded to bytes with the `Channel`'s encoding (utf-8 by default), or
with the *encoding* argument if used::

    await channel.publish('my_exchange', 'my_routing_key', 'text body')
    await channel.publish('my_exchange', 'my_routing_key', 'text body', encoding='iso-8859-1')

The body argument can be replaced with the json keyword argument::

    await channel.publish('my_exchange', 'my_routing_key', json=['a', 'list', 'of', {'json: 123}])


Publish to a headers exchange
-----------------------------

While the other exchange types use routing keys (direct and fanout) or patterns for routing keys (topic) that are
strings, the headers exchange type works with keys and values. When publishing a message to a headers exchange, pass a
`dict` as route::

    await channel.publish('my_exchange', {'key': 'value', 'another_key': 123}, b'message body')

Next: :doc:`get`
