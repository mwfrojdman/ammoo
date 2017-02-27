# ammoo
Pythonic AMQP library for asyncio

## Documentation

https://ammoo.readthedocs.io/en/latest/

## Builds

https://pypi.python.org/pypi/ammoo

## Remote procedure call example

### RPC Server

```python
import asyncio

from ammoo import connect


async def server():
    async with await connect('amqp://rabbitmq-server/') as connection:
        async with connection.channel(prefetch_count=3) as channel:
            await channel.declare_queue('request_queue')
            await channel.declare_exchange('my_exchange', 'direct')
            await channel.bind_queue('request_queue', 'my_exchange', 'request')

            n = 0
            async with channel.consume('request_queue') as consumer:
                async for message in consumer:
                    body = message.decode()
                    print('Received message {}: {} (as bytes: {!r})'.format(n, body, message.body))
                    if message.properties.reply_to is not None:
                        print('Replying to RPC request')
                        await message.reply(json={'n': n, 'body': body})
                    await message.ack()
                    n += 1


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(server())
```

```
# python rpc_server.py
Received message 0: first (as bytes: b'first')
Replying to RPC request
Received message 1: second (as bytes: b'second')
Replying to RPC request
Received message 2: third (as bytes: b'third')
Replying to RPC request
```

### RPC Client

```python
import sys
import asyncio

from ammoo import connect


async def client(body):
    async with await connect('amqp://rabbitmq-server/') as connection:
        async with connection.channel() as channel:
            reply_queue_name = (await channel.declare_queue('', exclusive=True)).queue_name
            print('Expecting replies to queue {}'.format(reply_queue_name))
            async with channel.consume(reply_queue_name, no_ack=True) as consumer:
                await channel.publish('my_exchange', 'request', body, reply_to=reply_queue_name)
                async for message in consumer:
                    reply_json = message.json()
                    assert reply_json['body'] == body
                    print('My message was number {} received by server'.format(reply_json['n']))
                    break


if __name__ == '__main__':
    body = sys.argv[1]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(client(body))
```

```
# python rpc_client.py first
Expecting replies to queue amq.gen-mHXRv6P4WyfG_QAEIsa4wQ
My message was number 0 received by server
# python rpc_client.py second
Expecting replies to queue amq.gen-RrZlLiHZSp8We4Ee0nkg4A
My message was number 1 received by server
# python rpc_client.py third
Expecting replies to queue amq.gen-Op0XhCr7HQRR7tY8sCknVQ
My message was number 2 received by server
```

## Installation

```bash
pip install ammoo
```

You'll need Python ≥ 3.5.

## To contribute
* Create an issue if there isn't already one for your change on https://github.com/mwfrojdman/ammoo/issues
* Fork the repository and base your changes on the master branch
* Create a pull request
