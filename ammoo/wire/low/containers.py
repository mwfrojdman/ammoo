from datetime import datetime
from io import BytesIO
from typing import Dict, Union, List
from collections.abc import Mapping as MappingABC

from .exceptions import IncompleteFrame, InvalidFieldValueType, InvalidBooleanTable
from .misc import parse_boolean, parse_decimal, parse_timestamp, parse_no_field, pack_boolean, pack_timestamp
from .strings import parse_shortstr, parse_longstr, build_shortstr, build_longstr, build_binary_longstr, \
    parse_binary_longstr
from .structs import parse_long_uint, parse_octet_uint, parse_short_uint, parse_long_long_uint, parse_octet_int, \
    parse_short_int, parse_long_int, parse_long_long_int, parse_float, parse_double, pack_long_uint, pack_octet_uint, \
    pack_short_uint, pack_long_long_uint, pack_long_long_int, pack_octet_int, pack_short_int, pack_long_int, pack_double


FieldValue = Union[int, float, str, datetime, bool, 'FieldTable', 'FieldTable', None]
FieldTable = Dict[str, FieldValue]
FieldArray = List[FieldValue]


def parse_field_value(bio: BytesIO, encoding: str, rabbitmq: bool) -> FieldValue:
    value_type = bio.read(1)
    if rabbitmq:
        field_value_types = RABBITMQ_FIELD_VALUE_TYPES
    else:
        field_value_types = FIELD_VALUE_TYPES
    try:
        parse_value, args_type = field_value_types[value_type]
    except KeyError:
        if len(value_type) == 0:
            raise IncompleteFrame()
        raise InvalidFieldValueType(value_type)
    if args_type == NO_ARGS:
        return parse_value(bio)
    elif args_type == STRING_ARGS:
        return parse_value(bio, encoding)
    else:
        return parse_value(bio, encoding, rabbitmq)


def parse_field_anystring(bio: BytesIO, encoding: str, rabbitmq: bool) -> str:
    value_type = bio.read(1)
    if rabbitmq:
        field_value_types = RABBITMQ_STRING_FIELD_VALUE_TYPES
    else:
        field_value_types = STRING_FIELD_VALUE_TYPES
    try:
        parse_value = field_value_types[value_type]
    except KeyError:
        if len(value_type) == 0:
            raise IncompleteFrame()
        raise InvalidFieldValueType(value_type)
    return parse_value(bio, encoding)


def parse_field_table(bio: BytesIO, encoding: str, rabbitmq: bool) -> FieldTable:
    length = parse_long_uint(bio)
    table = {}  # could be OrderedDict, but we don't really care about the order
    end_offset = bio.tell() + length
    while bio.tell() < end_offset:
        name = parse_shortstr(bio, encoding)
        value = parse_field_value(bio, encoding, rabbitmq)
        table[name] = value
    return table


def build_field_table(bio: BytesIO, table: FieldTable, encoding: str, rabbitmq: bool) -> None:
    table_bio = BytesIO()
    for name, value in table.items():
        build_shortstr(table_bio, name, encoding)
        pack_field_value(table_bio, value, encoding, rabbitmq)
    bio.write(pack_long_uint(table_bio.tell()))
    bio.write(table_bio.getvalue())


def build_lengthless_field_table(bio: BytesIO, table: FieldTable, encoding: str, rabbitmq: bool) -> None:
    for name, value in table.items():
        build_shortstr(bio, name, encoding)
        pack_field_value(bio, value, encoding, rabbitmq)


def parse_boolean_table_field(bio: BytesIO, encoding: str) -> Dict[str, bool]:
    value_type = bio.read(1)
    if value_type != b'F':
        if len(value_type) == 0:
            raise IncompleteFrame()
        raise InvalidFieldValueType(value_type)
    length = parse_long_uint(bio)
    table = {}  # could be OrderedDict, but we don't really care about the order
    end_offset = bio.tell() + length
    while bio.tell() < end_offset:
        name = parse_shortstr(bio, encoding)
        field_value_type = bio.read(1)
        if field_value_type != b't':
            if field_value_type == b'':
                raise IncompleteFrame()
            else:
                raise InvalidBooleanTable(field_value_type)
        table[name] = parse_boolean(bio)
    return table


def build_boolean_table_field(bio: BytesIO, table: Dict[str, bool], encoding: str) -> None:
    bio.write(b'F')
    table_bio = BytesIO()
    for name, value in table.items():
        build_shortstr(table_bio, name, encoding)
        table_bio.write(b't')
        table_bio.write(pack_boolean(value))
    bio.write(pack_long_uint(table_bio.tell()))
    bio.write(table_bio.getvalue())


def parse_field_array(bio: BytesIO, encoding: str, rabbitmq: bool) -> FieldArray:
    length = parse_long_uint(bio)
    array = []
    end_offset = bio.tell() + length
    while bio.tell() < end_offset:
        value = parse_field_value(bio, encoding, rabbitmq)
        array.append(value)
    return array


def build_field_array(bio: BytesIO, items: FieldArray, encoding: str, rabbitmq: bool) -> None:
    array_bio = BytesIO()
    for item in items:
        pack_field_value(array_bio, item, encoding, rabbitmq)
    bio.write(pack_long_uint(array_bio.tell()))
    bio.write(array_bio.getvalue())


NO_ARGS = 1
FIELD_ARGS = 2
STRING_ARGS = 3

STRING_FIELD_VALUE_TYPES = {
    b's': parse_shortstr,  # rabbitmq: N/A
    b'S': parse_longstr,
}


FIELD_VALUE_TYPES = {
    b't': (parse_boolean, NO_ARGS),
    b'b': (parse_octet_int, NO_ARGS),
    b'B': (parse_octet_uint, NO_ARGS),
    b'U': (parse_short_int, NO_ARGS),  # rabbitmq: (s
    b'u': (parse_short_uint, NO_ARGS),
    b'I': (parse_long_int, NO_ARGS),
    b'i': (parse_long_uint, NO_ARGS),
    b'L': (parse_long_long_int, NO_ARGS),  # rabbitmq: (l
    b'l': (parse_long_long_uint, NO_ARGS),  # rabbitmq: (N/A
    b'f': (parse_float, NO_ARGS),
    b'd': (parse_double, NO_ARGS),
    b'D': (parse_decimal, NO_ARGS),
    b'T': (parse_timestamp, NO_ARGS),
    b'V': (parse_no_field, NO_ARGS),
    b's': (parse_shortstr, STRING_ARGS),  # rabbitmq: N/A
    b'S': (parse_longstr, STRING_ARGS),
    b'A': (parse_field_array, FIELD_ARGS),
    b'F': (parse_field_table, FIELD_ARGS),
}


RABBITMQ_STRING_FIELD_VALUE_TYPES = {
    b'S': parse_longstr,
}


#  https://www.rabbitmq.com/amqp-0-9-1-errata.html / 3 Field types
RABBITMQ_FIELD_VALUE_TYPES = {
    b't': (parse_boolean, NO_ARGS),
    b'b': (parse_octet_int, NO_ARGS),
    b'B': (parse_octet_uint, NO_ARGS),
    b's': (parse_short_int, NO_ARGS),  # differs
    b'u': (parse_short_uint, NO_ARGS),
    b'I': (parse_long_int, NO_ARGS),
    b'i': (parse_long_uint, NO_ARGS),
    b'l': (parse_long_long_int, NO_ARGS),  # differs
    # no Unsigned 64-bit
    b'f': (parse_float, NO_ARGS),
    b'd': (parse_double, NO_ARGS),
    b'D': (parse_decimal, NO_ARGS),
    b'T': (parse_timestamp, NO_ARGS),
    b'V': (parse_no_field, NO_ARGS),
    # no short string
    b'S': (parse_longstr, STRING_ARGS),
    b'A': (parse_field_array, FIELD_ARGS),
    b'F': (parse_field_table, FIELD_ARGS),
    b'x': (parse_binary_longstr, NO_ARGS)  # rabbitmq extension
}


MIN_8BIT = -2 ** 7
MIN_16BIT = -2 ** 15
MIN_32BIT = -2 ** 31
TWO_POW_16 = 2 ** 16
TWO_POW_32 = 2 ** 32


#FieldValue = Union[int, float, str, datetime, bool, 'FieldTable', 'FieldTable']
def pack_field_value(bio: BytesIO, value: FieldValue, encoding: str, rabbitmq: bool):
    if isinstance(value, str):
        # on non-rabbitmq we could use shortstr if binary length < 256 bytes
        bio.write(b'S')
        build_longstr(bio, value, encoding)
    elif isinstance(value, bytes):
        # on non-rabbitmq we could use shortstr if binary length < 256 bytes
        # and on rabbitmq use type byte 'x'
        bio.write(b'S')
        build_binary_longstr(bio, value)
    elif isinstance(value, bool):
        bio.write(b't')
        bio.write(pack_boolean(value))
    elif isinstance(value, int):
        if value >= 0:
            if value < 256:
                bio.write(b'B')
                bio.write(pack_octet_uint(value))
            elif value < TWO_POW_16:
                bio.write(b'u')
                bio.write(pack_short_uint(value))
            elif value < TWO_POW_32:
                bio.write(b'i')
                bio.write(pack_long_uint(value))
            else:  # 64-bit
                bio.write(b'l')
                if rabbitmq:
                    bio.write(pack_long_long_int(value))
                else:
                    bio.write(pack_long_long_uint(value))
        else:
            if value >= MIN_8BIT:
                bio.write(b'b')
                bio.write(pack_octet_int(value))
            elif value >= MIN_16BIT:
                if rabbitmq:
                    bio.write(b's')
                else:
                    bio.write(b'U')
                bio.write(pack_short_int(value))
            elif value >= MIN_32BIT:
                bio.write(b'I')
                bio.write(pack_long_int(value))
            else:  # 64-bit
                if rabbitmq:
                    bio.write(b'l')
                else:
                    bio.write(b'L')
                bio.write(pack_long_long_int(value))
    elif isinstance(value, float):
        bio.write(b'd')
        bio.write(pack_double(value))
    elif isinstance(value, MappingABC):
        bio.write(b'F')
        build_field_table(bio, value, encoding, rabbitmq)
    elif isinstance(value, list):
        bio.write(b'A')
        build_field_array(bio, value, encoding, rabbitmq)
    elif isinstance(value, datetime):
        bio.write(b'T')
        bio.write(pack_timestamp(value))
    elif value is None:
        bio.write(b'V')
    else:
        raise TypeError(value)
