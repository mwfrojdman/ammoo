from io import BytesIO

from ammoo.wire import methods as m
from ammoo.wire.low.misc import parse_boolean
from ammoo.wire.typing import NoWait


def parse_confirm_select_parameters(bio: BytesIO, encoding: str, rabbitmq: bool) -> NoWait:
    return parse_boolean(bio)


CONFIRM_METHOD_PARAMETER_PARSERS = {
    m.METHOD_CONFIRM_SELECT: parse_confirm_select_parameters,
    m.METHOD_CONFIRM_SELECT_OK: None,
}
