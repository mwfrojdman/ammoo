# ammoo
Pythonic AMQP library for asyncio

Quickstart:

```python
import asyncio

from ammoo import connect


async def example():
    async with await connect('amqp://rabbitmq-server/') as connection:
        async with connection.channel(prefetch_count=3) as channel:
            await channel.declare_queue('my_queue')
            await channel.declare_exchange('my_exchange', 'direct')
            await channel.declare_exchange('broadcast', 'fanout')
            await channel.bind_queue('my_queue', 'my_exchange', 'my_routing_key')

            recv_count = 0

            async with channel.consume('my_queue') as consumer:
                async for message in consumer:
                    recv_count += 1
                    print('Got message {}: {!r}'.format(recv_count, message.body))

                    await channel.publish(
                        'broadcast',
                        'another_routing_key',
                        json={'orig_body': message.decode(), 'count': recv_count}
                    )
                    await message.ack()

                    if recv_count == 5:
                        print("That's enough, I'm done")
                        break


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(example())
```
