from typing import NewType

from ammoo.wire import methods as m
from ammoo.wire.low.structs import parse_long_uint
from . import classes as c


CAM = NewType('CAM', int)

parse_class_and_method = parse_long_uint

TWO_POW16 = 2 ** 16


def cam_long_uint(class_id: int, method_id: int ) -> CAM:
    """Combines a 16-bit class id and a 16-bit method id into a 32-bit integer. This allows to parse the class and
    method directly into a 32-bit int from the reader, and use it as a dictionary key instead of a 2-tuple."""
    return class_id * TWO_POW16 + method_id


CONNECTION_START_CAM = cam_long_uint(c.CLASS_CONNECTION, m.METHOD_CONNECTION_START)
CONNECTION_START_OK_CAM = cam_long_uint(c.CLASS_CONNECTION, m.METHOD_CONNECTION_START_OK)
CONNECTION_SECURE_CAM = cam_long_uint(c.CLASS_CONNECTION, m.METHOD_CONNECTION_SECURE)
CONNECTION_SECURE_OK_CAM = cam_long_uint(c.CLASS_CONNECTION, m.METHOD_CONNECTION_SECURE_OK)
CONNECTION_TUNE_CAM = cam_long_uint(c.CLASS_CONNECTION, m.METHOD_CONNECTION_TUNE)
CONNECTION_TUNE_OK_CAM = cam_long_uint(c.CLASS_CONNECTION, m.METHOD_CONNECTION_TUNE_OK)
CONNECTION_OPEN_CAM = cam_long_uint(c.CLASS_CONNECTION, m.METHOD_CONNECTION_OPEN)
CONNECTION_OPEN_OK_CAM = cam_long_uint(c.CLASS_CONNECTION, m.METHOD_CONNECTION_OPEN_OK)
CONNECTION_CLOSE_CAM = cam_long_uint(c.CLASS_CONNECTION, m.METHOD_CONNECTION_CLOSE)
CONNECTION_CLOSE_OK_CAM = cam_long_uint(c.CLASS_CONNECTION, m.METHOD_CONNECTION_CLOSE_OK)

CHANNEL_OPEN_CAM = cam_long_uint(c.CLASS_CHANNEL, m.METHOD_CHANNEL_OPEN)
CHANNEL_OPEN_OK_CAM = cam_long_uint(c.CLASS_CHANNEL, m.METHOD_CHANNEL_OPEN_OK)
CHANNEL_FLOW_CAM = cam_long_uint(c.CLASS_CHANNEL, m.METHOD_CHANNEL_FLOW)
CHANNEL_FLOW_OK_CAM = cam_long_uint(c.CLASS_CHANNEL, m.METHOD_CHANNEL_FLOW_OK)
CHANNEL_CLOSE_CAM = cam_long_uint(c.CLASS_CHANNEL, m.METHOD_CHANNEL_CLOSE)
CHANNEL_CLOSE_OK_CAM = cam_long_uint(c.CLASS_CHANNEL, m.METHOD_CHANNEL_CLOSE_OK)

EXCHANGE_DECLARE_CAM = cam_long_uint(c.CLASS_EXCHANGE, m.METHOD_EXCHANGE_DECLARE)
EXCHANGE_DECLARE_OK_CAM = cam_long_uint(c.CLASS_EXCHANGE, m.METHOD_EXCHANGE_DECLARE_OK)
EXCHANGE_DELETE_CAM = cam_long_uint(c.CLASS_EXCHANGE, m.METHOD_EXCHANGE_DELETE)
EXCHANGE_DELETE_OK_CAM = cam_long_uint(c.CLASS_EXCHANGE, m.METHOD_EXCHANGE_DELETE_OK)
EXCHANGE_BIND_CAM = cam_long_uint(c.CLASS_EXCHANGE, m.METHOD_EXCHANGE_BIND)
EXCHANGE_BIND_OK_CAM = cam_long_uint(c.CLASS_EXCHANGE, m.METHOD_EXCHANGE_BIND_OK)

QUEUE_DECLARE_CAM = cam_long_uint(c.CLASS_QUEUE, m.METHOD_QUEUE_DECLARE)
QUEUE_DECLARE_OK_CAM = cam_long_uint(c.CLASS_QUEUE, m.METHOD_QUEUE_DECLARE_OK)
QUEUE_BIND_CAM = cam_long_uint(c.CLASS_QUEUE, m.METHOD_QUEUE_BIND)
QUEUE_BIND_OK_CAM = cam_long_uint(c.CLASS_QUEUE, m.METHOD_QUEUE_BIND_OK)
QUEUE_PURGE_CAM = cam_long_uint(c.CLASS_QUEUE, m.METHOD_QUEUE_PURGE)
QUEUE_PURGE_OK_CAM = cam_long_uint(c.CLASS_QUEUE, m.METHOD_QUEUE_PURGE_OK)
QUEUE_DELETE_CAM = cam_long_uint(c.CLASS_QUEUE, m.METHOD_QUEUE_DELETE)
QUEUE_DELETE_OK_CAM = cam_long_uint(c.CLASS_QUEUE, m.METHOD_QUEUE_DELETE_OK)
QUEUE_UNBIND_CAM = cam_long_uint(c.CLASS_QUEUE, m.METHOD_QUEUE_UNBIND)
QUEUE_UNBIND_OK_CAM = cam_long_uint(c.CLASS_QUEUE, m.METHOD_QUEUE_UNBIND_OK)

BASIC_QOS_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_QOS)
BASIC_QOS_OK_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_QOS_OK)
BASIC_CONSUME_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_CONSUME)
BASIC_CONSUME_OK_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_CONSUME_OK)
BASIC_CANCEL_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_CANCEL)
BASIC_CANCEL_OK_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_CANCEL_OK)
BASIC_PUBLISH_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_PUBLISH)
BASIC_RETURN_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_RETURN_)
BASIC_DELIVER_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_DELIVER)
BASIC_GET_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_GET)
BASIC_GET_OK_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_GET_OK)
BASIC_GET_EMPTY_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_GET_EMPTY)
BASIC_ACK_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_ACK)
BASIC_REJECT_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_REJECT)
BASIC_RECOVER_ASYNC_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_RECOVER_ASYNC)
BASIC_RECOVER_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_RECOVER)
BASIC_RECOVER_OK_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_RECOVER_OK)
BASIC_NACK_CAM = cam_long_uint(c.CLASS_BASIC, m.METHOD_BASIC_NACK)

CONFIRM_SELECT_CAM = cam_long_uint(c.CLASS_CONFIRM, m.METHOD_CONFIRM_SELECT)
CONFIRM_SELECT_OK_CAM = cam_long_uint(c.CLASS_CONFIRM, m.METHOD_CONFIRM_SELECT_OK)


_CLASS_NAMES = {
    c.CLASS_CONNECTION: 'connection',
    c.CLASS_CHANNEL: 'channel',
    c.CLASS_EXCHANGE: 'exchange',
    c.CLASS_QUEUE: 'queue',
    c.CLASS_BASIC: 'basic',
    c.CLASS_CONFIRM: 'confirm',
    c.CLASS_TX: 'tx',
}

_METHOD_NAMES = {
    c.CLASS_CONNECTION: {
        m.METHOD_CONNECTION_START: 'start',
        m.METHOD_CONNECTION_START_OK: 'start-ok',
        m.METHOD_CONNECTION_SECURE: 'secure',
        m.METHOD_CONNECTION_SECURE_OK: 'secure-ok',
        m.METHOD_CONNECTION_TUNE: 'tune',
        m.METHOD_CONNECTION_TUNE_OK: 'tune-ok',
        m.METHOD_CONNECTION_OPEN: 'open',
        m.METHOD_CONNECTION_OPEN_OK: 'open-ok',
        m.METHOD_CONNECTION_CLOSE: 'close',
        m.METHOD_CONNECTION_CLOSE_OK: 'close-ok',
    },
    c.CLASS_CHANNEL: {
        m.METHOD_CHANNEL_OPEN: 'open',
        m.METHOD_CHANNEL_OPEN_OK: 'open-ok',
        m.METHOD_CHANNEL_FLOW: 'flow',
        m.METHOD_CHANNEL_FLOW_OK: 'flow-ok',
        m.METHOD_CHANNEL_CLOSE: 'close',
        m.METHOD_CHANNEL_CLOSE_OK: 'close-ok',
    },
    c.CLASS_EXCHANGE: {
        m.METHOD_EXCHANGE_DECLARE: 'declare',
        m.METHOD_EXCHANGE_DECLARE_OK: 'declare-ok',
        m.METHOD_EXCHANGE_DELETE: 'delete',
        m.METHOD_EXCHANGE_DELETE_OK: 'delete-ok',
        m.METHOD_EXCHANGE_BIND: 'bind',
        m.METHOD_EXCHANGE_BIND_OK: 'bind-ok',
    },
    c.CLASS_QUEUE: {
        m.METHOD_QUEUE_DECLARE: 'declare',
        m.METHOD_QUEUE_DECLARE_OK: 'declare-ok',
        m.METHOD_QUEUE_BIND: 'bind',
        m.METHOD_QUEUE_BIND_OK: 'bind-ok',
        m.METHOD_QUEUE_PURGE: 'purge',
        m.METHOD_QUEUE_PURGE_OK: 'purge-ok',
        m.METHOD_QUEUE_DELETE: 'delete',
        m.METHOD_QUEUE_DELETE_OK: 'delete-ok',
        m.METHOD_QUEUE_UNBIND: 'unbind',
        m.METHOD_QUEUE_UNBIND_OK: 'unbind-ok',
    },
    c.CLASS_BASIC: {
        m.METHOD_BASIC_QOS: 'qos',
        m.METHOD_BASIC_QOS_OK: 'qos-ok',
        m.METHOD_BASIC_CONSUME: 'consume',
        m.METHOD_BASIC_CONSUME_OK: 'consume-ok',
        m.METHOD_BASIC_CANCEL: 'cancel',
        m.METHOD_BASIC_CANCEL_OK: 'cancel-ok',
        m.METHOD_BASIC_PUBLISH: 'publish',
        m.METHOD_BASIC_RETURN_: 'return',
        m.METHOD_BASIC_DELIVER: 'deliver',
        m.METHOD_BASIC_GET: 'get',
        m.METHOD_BASIC_GET_OK: 'get-ok',
        m.METHOD_BASIC_GET_EMPTY: 'get-empty',
        m.METHOD_BASIC_ACK: 'ack',
        m.METHOD_BASIC_REJECT: 'reject',
        m.METHOD_BASIC_RECOVER_ASYNC: 'recover-async',
        m.METHOD_BASIC_RECOVER: 'recover',
        m.METHOD_BASIC_RECOVER_OK: 'recover-ok',
        m.METHOD_BASIC_NACK: 'nack',
    },
    c.CLASS_CONFIRM: {
        m.METHOD_CONFIRM_SELECT: 'select',
        m.METHOD_CONFIRM_SELECT_OK: 'select-ok',
    }
}


def pretty_format_cam(cam):
    class_id, method_id = divmod(cam, TWO_POW16)
    return '{}.{}'.format(_CLASS_NAMES[class_id], _METHOD_NAMES[class_id][method_id])
