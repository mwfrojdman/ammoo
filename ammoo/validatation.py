from datetime import datetime

from ammoo.wire.constants import FRAME_MIN_SIZE
from ammoo.wire.typing import ExchangeName, DeliveryMode, DeliveryTag, QueueName, RoutingKey


def validate_bool(value: bool, name: str) -> None:
    if not isinstance(value, bool):
        raise TypeError('{} is not a bool: {!r}'.format(name, value))


def validate_str(value: str, name: str) -> None:
    if not isinstance(value, str):
        raise TypeError('{} is not a str: {!r}'.format(name, value))


def validate_int(value: int, name: str) -> None:
    if not isinstance(value, int):
        raise TypeError('{} is not an int: {!r}'.format(name, value))


UINT_64_MAX = 2**64 - 1
UINT_32_MAX = 2**32 - 1
UINT_16_MAX = 2**16 - 1
UINT_8_MAX = 2**8 - 1


def validate_short_short_uint(value: int, name) -> None:
    validate_int(value, name)
    if value < 0 or value > UINT_8_MAX:
        raise ValueError('{} not in range for an 8-bit unsigned integer: {}'.format(name, value))


def validate_short_uint(value: int, name: str) -> None:
    validate_int(value, name)
    if value < 0 or value > UINT_16_MAX:
        raise ValueError('{} not in range for a 16-bit unsigned integer: {}'.format(name, value))


def validate_long_uint(value: int, name: str) -> None:
    validate_int(value, name)
    if value < 0 or value > UINT_32_MAX:
        raise ValueError('{} not in range for a 32-bit unsigned integer: {}'.format(name, value))


def validate_long_long_uint(value: int, name: str) -> None:
    validate_int(value, name)
    if value < 0 or value > UINT_64_MAX:
        raise ValueError('{} not in range for a 64-bit unsigned integer: {}'.format(name, value))


def validate_delivery_tag(value: DeliveryTag, name: str='delivery_tag') -> None:
    validate_long_long_uint(value, name)


def validate_priority(value: int, name: str='priority') -> None:
    # Note: AMQP 0.9.1 defines this as 0-9, but RabbitMQ allows 0-255
    validate_short_short_uint(value, name)


def validate_delivery_mode(value: DeliveryMode, name: str='delivery_mode') -> None:
    validate_int(value, name)
    if value != 1 and value != 2:
        raise ValueError('{} is not a valid delivery mode (1 or 2)')


def validate_frame_max(value: int, name='frame_max') -> None:
    validate_long_uint(value, name)
    if value < FRAME_MIN_SIZE:
        raise ValueError('{} must be at least {}'.format(name, FRAME_MIN_SIZE))


def validate_timestamp(value: datetime, name: str='timestamp') -> None:
    if not isinstance(value, datetime):
        raise TypeError('{} is not datetime: {!r}'.format(name, value))


def validate_shortstr(value: str, name: str):
    validate_str(value, name)
    if len(value) > 255:
        raise ValueError('{} is over 255 characters: {}'.format(name, value))


def validate_exchange_name(value: ExchangeName, name: str='exchange_name') -> None:
    validate_shortstr(value, name)


def validate_queue_name(value: QueueName, name: str='queue_name') -> None:
    validate_shortstr(value, name)


def validate_routing_key(value: RoutingKey, name: str='routing_key') -> None:
    validate_shortstr(value, name)
