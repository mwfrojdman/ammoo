from collections import OrderedDict
from io import BytesIO
from typing import NamedTuple, Dict

from .containers import parse_field_anystring, parse_boolean_table_field, STRING_ARGS, FIELD_ARGS, parse_field_value, \
    FieldValue, build_boolean_table_field
from .strings import parse_shortstr, build_shortstr, build_longstr
from .structs import parse_long_uint, pack_long_uint


PEER_PROPERTIES_FIELDS = [
    ('host', (parse_field_anystring, FIELD_ARGS)),
    ('product', (parse_field_anystring, FIELD_ARGS)),
    ('version', (parse_field_anystring, FIELD_ARGS)),
    ('platform', (parse_field_anystring, FIELD_ARGS)),
    ('copyright', (parse_field_anystring, FIELD_ARGS)),
    ('information', (parse_field_anystring, FIELD_ARGS)),
    ('capabilities', (parse_boolean_table_field, STRING_ARGS))
]
PEER_PROPERTIES_MAPPING = dict(PEER_PROPERTIES_FIELDS)
PeerProperties = NamedTuple('PeerProperties', [
    ('host', str),
    ('product', str),
    ('version', str),
    ('platform', str),
    ('copyright', str),
    ('information', str),
    ('capabilities', Dict[str, bool]),
    ('others', Dict[str, FieldValue])
])


def parse_peer_properties(bio: BytesIO, encoding: str, rabbitmq: bool) -> PeerProperties:
    length = parse_long_uint(bio)
    others = {}
    table = OrderedDict()
    for key, _ in PEER_PROPERTIES_FIELDS[:-1]:
        table[key] = None
    table['capabilities'] = {}
    end_offset = bio.tell() + length
    while bio.tell() < end_offset:
        name = parse_shortstr(bio, encoding)
        if name in PEER_PROPERTIES_MAPPING:
            parse_field, args_type = PEER_PROPERTIES_MAPPING[name]
            if args_type == FIELD_ARGS:
                value = parse_field(bio, encoding, rabbitmq)
            else:  # STRING_ARGS
                value = parse_field(bio, encoding)
            table[name] = value
        else:
            value = parse_field_value(bio, encoding, rabbitmq)
            others[name] = value
    return PeerProperties(*table.values(), others)


STRING_PEER_PROPERTIES = [
    'host',
    'product',
    'version',
    'platform',
    'copyright',
    'information',
]


def build_peer_properties(bio: BytesIO, peer_properties: PeerProperties, encoding: str) -> None:
    table_bio = BytesIO()
    for name in STRING_PEER_PROPERTIES:
        value = getattr(peer_properties, name)
        if value is not None:
            build_shortstr(table_bio, name, encoding)
            table_bio.write(b'S')  # always use longstring, both RabbitMQ and vanilla AMQP accepts this
            build_longstr(table_bio, value, encoding)
    build_shortstr(table_bio, 'capabilities', encoding)
    build_boolean_table_field(table_bio, peer_properties.capabilities, encoding)
    # XXX: peer_properties.others is currently not used for client properties, but implement it here if it would

    bio.write(pack_long_uint(table_bio.tell()))
    bio.write(table_bio.getvalue())
