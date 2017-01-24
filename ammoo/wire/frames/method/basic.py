from io import BytesIO
from typing import NamedTuple

from ammoo.wire import methods as m
from ammoo.wire.low.containers import parse_field_table, FieldTable, build_field_table
from ammoo.wire.low.misc import parse_boolean, parse_booleans, pack_boolean, pack_booleans
from ammoo.wire.low.strings import parse_shortstr, parse_binary_shortstr, build_shortstr, EMPTY_SHORTSTR_DATA
from ammoo.wire.low.structs import parse_short_uint, parse_long_uint, parse_long_long_uint, pack_long_long_uint, \
    pack_long_uint, pack_short_uint, RESERVED_SHORT_UINT_DATA
from ammoo.wire.typing import QueueName, ConsumerTag, ExchangeName, RoutingKey, ReplyCode, DeliveryTag, MessageCount, \
    Requeue

BasicQosParameters = NamedTuple('BasicQosParameters', [
    ('prefetch_size', int),
    ('prefetch_count', int),
    ('global_', bool),
])
BasicConsumeParameters = NamedTuple('BasicConsumeParameters', [
    ('queue_name', QueueName),
    ('consumer_tag', ConsumerTag),
    ('no_local', bool),
    ('no_ack', bool),
    ('exclusive', bool),
    ('no_wait', bool),
    ('arguments', FieldTable),
])
BasicCancelParameters = NamedTuple('BasicCancelParameters', [
    ('consumer_tag', ConsumerTag),
    ('no_wait', bool),
])
BasicPublishParameters = NamedTuple('BasicPublishParameters', [
    ('exchange_name', ExchangeName),
    ('routing_key', RoutingKey),
    ('mandatory', bool),
    ('immediate', bool)
])
BasicReturnParameters = NamedTuple('BasicReturnParameters', [
    ('reply_code', ReplyCode),
    ('reply_text', str),
    ('exchange_name', ExchangeName),
    ('routing_key', RoutingKey)
])
BasicDeliverParameters = NamedTuple('BasicDeliverParameters', [
    ('consumer_tag', ConsumerTag),
    ('delivery_tag', DeliveryTag),
    ('redelivered', bool),
    ('exchange_name', ExchangeName),
    ('routing_key', RoutingKey)
])
BasicGetParameters = NamedTuple('BasicGetParameters', [
    ('queue_name', QueueName),
    ('no_ack', bool)
])
BasicGetOkParameters = NamedTuple('BasicGetOkParameters', [
    ('delivery_tag', DeliveryTag),
    ('redelivered', bool),
    ('exchange_name', ExchangeName),
    ('routing_key', RoutingKey),
    ('message_count', MessageCount)
])
BasicAckParameters = NamedTuple('BasicAckParameters', [
    ('delivery_tag', DeliveryTag),
    ('multiple', bool),
])
BasicRejectParameters = NamedTuple('BasicRejectParameters', [
    ('delivery_tag', DeliveryTag),
    ('requeue', bool),
])
BasicNackParameters = NamedTuple('BasicNackParameters', [
    ('delivery_tag', DeliveryTag),
    ('multiple', bool),
    ('requeue', bool),
])


def parse_basic_qos_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> BasicQosParameters:
    return BasicQosParameters(
        parse_long_uint(bio),
        parse_short_uint(bio),
        parse_boolean(bio)
    )


def pack_basic_qos_parameters(prefetch_size: int, prefetch_count: int, global_: bool) -> bytes:
    return pack_long_uint(prefetch_size) + pack_short_uint(prefetch_count) + pack_boolean(global_)


def parse_basic_consume_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> BasicConsumeParameters:
    parse_binary_shortstr(bio)
    return BasicConsumeParameters(
        parse_shortstr(bio, encoding),
        parse_shortstr(bio, encoding),
        *parse_booleans(bio, 4),
        parse_field_table(bio, encoding, rabbitmq)
    )


def pack_basic_consume_parameters(
        queue_name: QueueName, consumer_tag: ConsumerTag, no_local: bool, no_ack: bool, exclusive: bool, no_wait: bool,
        arguments: FieldTable, encoding: str, rabbitmq: bool) -> bytes:
    bio = BytesIO()
    bio.write(RESERVED_SHORT_UINT_DATA)
    build_shortstr(bio, queue_name, encoding)
    build_shortstr(bio, consumer_tag, encoding)
    bio.write(pack_booleans(no_local, no_ack, exclusive, no_wait))
    build_field_table(bio, arguments, encoding, rabbitmq)
    return bio.getvalue()


def parse_basic_consume_ok_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> ConsumerTag:
    return parse_shortstr(bio, encoding)


def parse_basic_cancel_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> BasicCancelParameters:
    return BasicCancelParameters(
        parse_shortstr(bio, encoding),
        parse_boolean(bio)
    )


def pack_basic_cancel_parameters(consumer_tag: ConsumerTag, no_wait: bool, encoding: str) -> bytes:
    bio = BytesIO()
    build_shortstr(bio, consumer_tag, encoding)
    bio.write(pack_boolean(no_wait))
    return bio.getvalue()


def parse_basic_cancel_ok_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> ConsumerTag:
    return parse_shortstr(bio, encoding)


def parse_basic_publish_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> BasicPublishParameters:
    parse_binary_shortstr(bio)
    return BasicPublishParameters(
        parse_shortstr(bio, encoding),
        parse_shortstr(bio, encoding),
        *parse_booleans(bio, 2)
    )


def pack_basic_publish_parameters(
        exchange_name: ExchangeName, routing_key: RoutingKey, mandatory: bool, immediate: bool, encoding: str) -> bytes:
    bio = BytesIO()
    bio.write(RESERVED_SHORT_UINT_DATA)
    build_shortstr(bio, exchange_name, encoding)
    build_shortstr(bio, routing_key, encoding)
    bio.write(pack_booleans(mandatory, immediate))
    return bio.getvalue()


def parse_basic_return_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> BasicReturnParameters:
    return BasicReturnParameters(
        parse_short_uint(bio),
        parse_shortstr(bio, encoding),
        parse_shortstr(bio, encoding),
        parse_shortstr(bio, encoding)
    )


def parse_basic_deliver_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> BasicDeliverParameters:
    return BasicDeliverParameters(
        parse_shortstr(bio, encoding),
        parse_long_long_uint(bio),
        parse_boolean(bio),
        parse_shortstr(bio, encoding),
        parse_shortstr(bio, encoding)
    )


def parse_basic_get_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> BasicGetParameters:
    parse_binary_shortstr(bio)
    return BasicGetParameters(
        parse_shortstr(bio, encoding),
        parse_boolean(bio)
    )


def pack_basic_get_parameters(queue_name: QueueName, no_ack: bool, encoding: str) -> bytes:
    bio = BytesIO()
    bio.write(RESERVED_SHORT_UINT_DATA)
    build_shortstr(bio, queue_name, encoding)
    bio.write(pack_boolean(no_ack))
    return bio.getvalue()


def parse_basic_get_ok_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> BasicGetOkParameters:
    return BasicGetOkParameters(
        parse_long_long_uint(bio),
        parse_boolean(bio),
        parse_shortstr(bio, encoding),
        parse_shortstr(bio, encoding),
        parse_long_uint(bio)
    )


def parse_basic_get_empty_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> None:
    parse_binary_shortstr(bio)
    return None


def parse_basic_ack_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> BasicAckParameters:
    return BasicAckParameters(
        parse_long_long_uint(bio),
        parse_boolean(bio)
    )


def pack_basic_ack_parameters(delivery_tag: DeliveryTag, multiple: bool) -> bytes:
    return pack_long_long_uint(delivery_tag) + pack_boolean(multiple)


def parse_basic_reject_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> BasicRejectParameters:
    return BasicRejectParameters(
        parse_long_long_uint(bio),
        parse_boolean(bio)
    )


def pack_basic_reject_parameters(delivery_tag: DeliveryTag, requeue: bool) -> bytes:
    return pack_long_long_uint(delivery_tag) + pack_boolean(requeue)


# used for basic.recover-async too
def parse_basic_recover_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> Requeue:
    return parse_boolean(bio)


def parse_basic_nack_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> BasicNackParameters:
    return BasicNackParameters(
        parse_long_long_uint(bio),
        *parse_booleans(bio, 2)
    )


def pack_basic_nack_parameters(delivery_tag: DeliveryTag, multiple: bool, requeue: bool) -> bytes:
    return pack_long_long_uint(delivery_tag) + pack_booleans(multiple, requeue)


BASIC_METHOD_PARAMETER_PARSERS = {
    m.METHOD_BASIC_QOS: parse_basic_qos_parameters,
    m.METHOD_BASIC_QOS_OK: None,
    m.METHOD_BASIC_CONSUME: parse_basic_consume_parameters,
    m.METHOD_BASIC_CONSUME_OK: parse_basic_consume_ok_parameters,
    m.METHOD_BASIC_CANCEL: parse_basic_cancel_parameters,
    m.METHOD_BASIC_CANCEL_OK: parse_basic_cancel_ok_parameters,
    m.METHOD_BASIC_PUBLISH: parse_basic_publish_parameters,
    m.METHOD_BASIC_RETURN_: parse_basic_return_parameters,
    m.METHOD_BASIC_DELIVER: parse_basic_deliver_parameters,
    m.METHOD_BASIC_GET: parse_basic_get_parameters,
    m.METHOD_BASIC_GET_OK: parse_basic_get_ok_parameters,
    m.METHOD_BASIC_GET_EMPTY: parse_basic_get_empty_parameters,
    m.METHOD_BASIC_ACK: parse_basic_ack_parameters,
    m.METHOD_BASIC_REJECT: parse_basic_reject_parameters,
    m.METHOD_BASIC_RECOVER_ASYNC: parse_basic_recover_parameters,
    m.METHOD_BASIC_RECOVER: parse_basic_recover_parameters,
    m.METHOD_BASIC_RECOVER_OK: None,
    m.METHOD_BASIC_NACK: parse_basic_nack_parameters,
}
