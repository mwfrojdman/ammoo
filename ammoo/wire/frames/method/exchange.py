from io import BytesIO
from typing import NamedTuple

from ammoo.wire import methods as m
from ammoo.wire.low.containers import parse_field_table, FieldTable, build_field_table
from ammoo.wire.low.misc import parse_booleans, parse_boolean, pack_booleans, pack_boolean
from ammoo.wire.low.strings import parse_binary_shortstr, parse_shortstr, build_shortstr
from ammoo.wire.low.structs import RESERVED_SHORT_UINT_DATA
from ammoo.wire.typing import ExchangeName, ExchangeType, RoutingKey

ExchangeDeclareParameters = NamedTuple('ExchangeDeclareParameters', [
    ('exchange_name', ExchangeName),
    ('type_', ExchangeType),
    ('passive', bool),
    ('durable', bool),
    ('auto_delete', bool),
    ('internal', bool),
    ('no_wait', bool),
    ('arguments', FieldTable),
])
ExchangeDeleteParameters = NamedTuple('ExchangeDeleteParameters', [
    ('exchange_name', ExchangeName),
    ('if_unused', bool),
    ('no_wait', bool),
])
ExchangeBindParameters = NamedTuple('ExchangeBindParameters', [
    ('destination', ExchangeName),
    ('source', ExchangeName),
    ('routing_key', RoutingKey),
    ('no_wait', bool),
    ('arguments', FieldTable),
])


def parse_exchange_declare_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> ExchangeDeclareParameters:
    parse_binary_shortstr(bio)  # reserved
    return ExchangeDeclareParameters(
        parse_shortstr(bio, encoding),
        parse_shortstr(bio, encoding),
        *parse_booleans(bio, 5),
        parse_field_table(bio, encoding, rabbitmq)
    )


def pack_exchange_declare_parameters(
        exchange_name: ExchangeName, type_: ExchangeType, passive: bool, durable: bool, auto_delete: bool,
        internal: bool, no_wait: bool, arguments: FieldTable, encoding: str, rabbitmq: bool) -> bytes:
    bio = BytesIO()
    bio.write(RESERVED_SHORT_UINT_DATA)
    build_shortstr(bio, exchange_name, encoding)
    build_shortstr(bio, type_, encoding)
    bio.write(pack_booleans(passive, durable, auto_delete, internal, no_wait))
    build_field_table(bio, arguments, encoding, rabbitmq)
    return bio.getvalue()


def parse_exchange_delete_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> ExchangeDeleteParameters:
    parse_binary_shortstr(bio)  # reserved
    return ExchangeDeleteParameters(
        parse_shortstr(bio, encoding),
        *parse_booleans(bio, 2)
    )


def pack_exchange_delete_parameters(
        exchange_name: ExchangeName, if_unused: bool, no_wait: bool, encoding: str) -> bytes:
    bio = BytesIO()
    bio.write(RESERVED_SHORT_UINT_DATA)
    build_shortstr(bio, exchange_name, encoding)
    bio.write(pack_booleans(if_unused, no_wait))
    return bio.getvalue()


def parse_exchange_bind_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> ExchangeBindParameters:
    parse_binary_shortstr(bio)  # reserved
    return ExchangeBindParameters(
        parse_shortstr(bio, encoding),
        parse_shortstr(bio, encoding),
        parse_shortstr(bio, encoding),
        parse_boolean(bio),
        parse_field_table(bio, encoding, rabbitmq)
    )


def pack_exchange_bind_parameters(
        destination: ExchangeName, source: ExchangeName, routing_key: RoutingKey, no_wait: bool,
        arguments: FieldTable, encoding: str, rabbitmq: bool) -> bytes:
    bio = BytesIO()
    bio.write(RESERVED_SHORT_UINT_DATA)
    build_shortstr(bio, destination, encoding)
    build_shortstr(bio, source, encoding)
    build_shortstr(bio, routing_key, encoding)
    bio.write(pack_boolean(no_wait))
    build_field_table(bio, arguments, encoding, rabbitmq)
    return bio.getvalue()


EXCHANGE_METHOD_PARAMETER_PARSERS = {
    m.METHOD_EXCHANGE_DECLARE: parse_exchange_declare_parameters,
    m.METHOD_EXCHANGE_DECLARE_OK: None,
    m.METHOD_EXCHANGE_DELETE: parse_exchange_delete_parameters,
    m.METHOD_EXCHANGE_DELETE_OK: None,
    m.METHOD_EXCHANGE_BIND: parse_exchange_bind_parameters,
    m.METHOD_EXCHANGE_BIND_OK: None,
}
