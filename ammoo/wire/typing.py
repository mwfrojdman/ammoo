from typing import NewType, TypeVar, List, Dict, Union

VirtualHost = NewType('VirtualHost', str)
ConsumerTag = NewType('ConsumerTag', str)
QueueName = NewType('QueueName', str)
ExchangeName = NewType('ExchangeName', str)
ExchangeType = NewType('ExchangeType', str)
RoutingKey = NewType('RoutingKey', str)

ReplyCode = NewType('ReplyCode', int)
DeliveryTag = NewType('DeliveryTag', int)
MessageCount = NewType('MessageCount', int)
ConsumerCount = NewType('ConsumerCount', int)
DeliveryMode = NewType('DeliveryMode', int)

Requeue = NewType('Requeue', bool)
FlowActive = NewType('Active', bool)
NoWait = NewType('NoWait', bool)

# could not use Union because of recursive typing with List and Dict.
# when decoding json, the dict key is always an str, but for encoding, the type can be any json value except list/dict
DecodeJson = TypeVar('DecodeJson', None, int, float, str, List['DecodeJson'], Dict[str, 'DecodeJson'])
JsonKey = Union[None, int, float, str]
EncodeJson = TypeVar('EncodeJson', None, int, float, str, List['DecodeJson'], Dict[JsonKey, 'DecodeJson'])

T = TypeVar('T')  # Any type.
KT = TypeVar('KT')  # Key type.
VT = TypeVar('VT')  # Value type.
