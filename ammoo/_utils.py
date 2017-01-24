from ammoo.wire.low.containers import FieldTable
from ammoo.validatation import validate_int, validate_routing_key


def add_int_arg(name: str, value: int, argument_name: str, arguments: FieldTable, zero_allowed: bool) -> None:
    """Used for adding the x-message-ttl and x-expires arguments to queue.declare"""
    validate_int(value, name)
    if zero_allowed:
        if value < 0:
            raise ValueError('{} is negative: {}'.format(name, value))
    elif value <= 0:
        raise ValueError('{} is non-positive: {}'.format(name, value))
    arguments[argument_name] = value


def add_routing_keys_header(items, arg_name, headers, header_name):
    """Utility function for publish()"""
    if isinstance(items, str):
        items = [items]
    elif not isinstance(items, list):
        raise TypeError('{} is not str or list: {!r}'.format(arg_name, items))
    for item in items:
        validate_routing_key(item, '{} item'.format(arg_name))
    headers[header_name] = items
