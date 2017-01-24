from io import BytesIO
from typing import NamedTuple

from ammoo.wire import methods as m
from ammoo.wire.low.containers import parse_field_table, FieldTable, build_field_table
from ammoo.wire.low.misc import parse_booleans, parse_boolean, pack_booleans, pack_boolean
from ammoo.wire.low.strings import parse_binary_shortstr, parse_shortstr, build_shortstr
from ammoo.wire.low.structs import parse_long_uint, RESERVED_SHORT_UINT_DATA
from ammoo.wire.typing import MessageCount, QueueName, ConsumerCount, ExchangeName, RoutingKey


QueueDeclareParameters = NamedTuple('QueueDeclareParameters', [
    ('queue_name', QueueName),
    ('passive', bool),
    ('durable', bool),
    ('exclusive', bool),
    ('auto_delete', bool),
    ('no_wait', bool),
    ('arguments', FieldTable),
])
QueueDeclareOkParameters = NamedTuple('QueueDeclareOkParameters', [
    ('queue_name', QueueName),
    ('message_count', MessageCount),
    ('consumer_count', ConsumerCount),
])
QueueBindParameters = NamedTuple('QueueBindParameters', [
    ('queue_name', QueueName),
    ('exchange_name', ExchangeName),
    ('routing_key', RoutingKey),
    ('no_wait', bool),
    ('arguments', FieldTable),
])
QueuePurgeParameters = NamedTuple('QueuePurgeParameters', [
    ('queue_name', QueueName),
    ('no_wait', bool),
])
QueueDeleteParameters = NamedTuple('QueueDeleteParameters', [
    ('queue_name', QueueName),
    ('if_unused', bool),
    ('if_empty', bool),
    ('no_wait', bool),
])
QueueUnbindParameters = NamedTuple('QueueUnbindParameters', [
    ('queue_name', QueueName),
    ('exchange_name', ExchangeName),
    ('routing_key', RoutingKey),
    ('arguments', FieldTable),
])


def parse_queue_declare_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> QueueDeclareParameters:
    parse_binary_shortstr(bio)
    return QueueDeclareParameters(
        parse_shortstr(bio, encoding),
        parse_booleans(bio, 5),
        parse_field_table(bio, encoding, rabbitmq)
    )


def pack_queue_declare_parameters(
        queue_name: QueueName, passive: bool, durable: bool, exclusive: bool, auto_delete: bool, no_wait: bool,
        arguments: FieldTable, encoding: str, rabbitmq: bool) -> bytes:
    bio = BytesIO()
    bio.write(RESERVED_SHORT_UINT_DATA)
    build_shortstr(bio, queue_name, encoding)
    bio.write(pack_booleans(passive, durable, exclusive, auto_delete, no_wait))
    build_field_table(bio, arguments, encoding, rabbitmq)
    return bio.getvalue()


def parse_queue_declare_ok_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> QueueDeclareOkParameters:
    return QueueDeclareOkParameters(
        parse_shortstr(bio, encoding),
        parse_long_uint(bio),
        parse_long_uint(bio)
    )


def parse_queue_bind_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> QueueBindParameters:
    parse_binary_shortstr(bio)
    return QueueBindParameters(
        parse_shortstr(bio, encoding),
        parse_shortstr(bio, encoding),
        parse_shortstr(bio, encoding),
        parse_boolean(bio),
        parse_field_table(bio, encoding, rabbitmq)
    )


def pack_queue_bind_parameters(
        queue_name: QueueName, exchange_name: ExchangeName, routing_key: RoutingKey, no_wait: bool,
        arguments: FieldTable, encoding: str, rabbitmq: bool) -> bytes:
    bio = BytesIO()
    bio.write(RESERVED_SHORT_UINT_DATA)
    build_shortstr(bio, queue_name, encoding)
    build_shortstr(bio, exchange_name, encoding)
    build_shortstr(bio, routing_key, encoding)
    bio.write(pack_boolean(no_wait))
    build_field_table(bio, arguments, encoding, rabbitmq)
    return bio.getvalue()


def parse_queue_purge_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> QueuePurgeParameters:
    parse_binary_shortstr(bio)
    return QueuePurgeParameters(
        parse_shortstr(bio, encoding),
        parse_boolean(bio)
    )


def pack_queue_purge_parameters(queue_name: QueueName, no_wait: bool, encoding: str) -> bytes:
    bio = BytesIO()
    bio.write(RESERVED_SHORT_UINT_DATA)
    build_shortstr(bio, queue_name, encoding)
    bio.write(pack_boolean(no_wait))
    return bio.getvalue()


def parse_queue_purge_ok_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> MessageCount:
    return parse_long_uint(bio)


def parse_queue_delete_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> QueueDeleteParameters:
    parse_binary_shortstr(bio)
    return QueueDeleteParameters(
        parse_shortstr(bio, encoding),
        *parse_booleans(bio, 2),
        parse_field_table(bio, encoding, rabbitmq)
    )


def pack_queue_delete_parameters(
        queue_name: QueueName, if_unused: bool, if_empty: bool, no_wait: bool, encoding: str) -> bytes:
    bio = BytesIO()
    bio.write(RESERVED_SHORT_UINT_DATA)
    build_shortstr(bio, queue_name, encoding)
    bio.write(pack_booleans(if_unused, if_empty, no_wait))
    return bio.getvalue()


def parse_queue_delete_ok_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> MessageCount:
    return parse_long_uint(bio)


def parse_queue_unbind_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> QueueUnbindParameters:
    parse_binary_shortstr(bio)
    return QueueUnbindParameters(
        parse_shortstr(bio, encoding),
        parse_shortstr(bio, encoding),
        parse_shortstr(bio, encoding),
        parse_field_table(bio, encoding, rabbitmq)
    )


def pack_queue_unbind_parameters(
        queue_name: QueueName, exchange_name: ExchangeName, routing_key: RoutingKey, arguments: FieldTable,
        encoding: str, rabbitmq: bool) -> bytes:
    bio = BytesIO()
    bio.write(RESERVED_SHORT_UINT_DATA)
    build_shortstr(bio, queue_name, encoding)
    build_shortstr(bio, exchange_name, encoding)
    build_shortstr(bio, routing_key, encoding)
    build_field_table(bio, arguments, encoding, rabbitmq)
    return bio.getvalue()


QUEUE_METHOD_PARAMETER_PARSERS = {
    m.METHOD_QUEUE_DECLARE: parse_queue_declare_parameters,
    m.METHOD_QUEUE_DECLARE_OK: parse_queue_declare_ok_parameters,
    m.METHOD_QUEUE_BIND: parse_queue_bind_parameters,
    m.METHOD_QUEUE_BIND_OK: None,
    m.METHOD_QUEUE_PURGE: parse_queue_purge_parameters,
    m.METHOD_QUEUE_PURGE_OK: parse_queue_purge_ok_parameters,
    m.METHOD_QUEUE_DELETE: parse_queue_delete_parameters,
    m.METHOD_QUEUE_DELETE_OK: parse_queue_delete_ok_parameters,
    m.METHOD_QUEUE_UNBIND: parse_queue_unbind_parameters,
    m.METHOD_QUEUE_UNBIND_OK: None,
}
