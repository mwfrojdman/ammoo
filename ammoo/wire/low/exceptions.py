from ammoo.wire.constants import FRAME_END


class InvalidParse(Exception):
    """Superclass for all parsing errors"""


class IncompleteFrame(InvalidParse):
    def __init__(self):
        pass


class InvalidFloat(InvalidParse):
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return 'Could not parse a float from {!r}'.format(self.data)


class InvalidDouble(InvalidParse):
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return 'Could not parse a double from {!r}'.format(self.data)


class ShortstrDecodingError(InvalidParse):
    def __init__(self, data, encoding):
        self.data = data
        self.encoding = encoding

    def __str__(self):
        return 'Could not decode shortstr data as {}: {!r}'.format(self.encoding, self.data)


class LongstrDecodingError(InvalidParse):
    def __init__(self, data, encoding):
        self.data = data
        self.encoding = encoding

    def __str__(self):
        return 'Could not decode longstr data as {}: {!r}'.format(self.encoding, self.data)


class InvalidFieldValueType(InvalidParse):
    """Field tables and arrays have a one byte character to specify what type the subsequent value has. Except there
    was an value we don't support."""

    def __init__(self, type_byte):
        self.type_byte = type_byte

    def __str__(self):
        return 'Invalid field value type byte {!r}'.format(self.type_byte)


class InvalidBoolean(InvalidParse):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return 'Could not parse boolean (0 or 1) from value {}'.format(self.value)


class InvalidBooleanTable(InvalidParse):
    def __init__(self, field_type_byte):
        self.field_type_byte = field_type_byte

    def __str__(self):
        return 'Expected a field table with boolean values, not type byte {!r}'.format(self.field_type_byte)


class InvalidDeliveryMode(InvalidParse):
    """Allowed delivery modes are 1 for non-persistent and 2 for persistent, nothing else"""

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return 'Delivery mode was not 1 or 2: {}'.format(self.value)


class InvalidWeight(InvalidParse):
    """Spec says weight is always zero. Except in this case."""

    def __init__(self, weight):
        self.weight = weight

    def __str__(self):
        return 'Weight is not zero: {}'.format(self.weight)


class InvalidFrameEnd(InvalidParse):
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return 'Could not parse frame end {!r} from data {!r}'.format(FRAME_END, self.data)


class ExcessiveFramePayload(InvalidParse):
    def __init__(self, payload_size: int, read_size: int):
        self.payload_size = payload_size
        self.read_size = read_size

    def __str__(self) -> str:
        return 'Frame has {} bytes of payload, but only {} of it was read'.format(self.payload_size, self.read_size)


class InvalidFrameType(InvalidParse):
    """Raised when the frame type byte in the frame header is unknown"""

    def __init__(self, frame_type: int):
        self.frame_type = frame_type

    def __str__(self):
        return 'Invalid frame type {}'.format(self.frame_type)


class UnsupportedHeaderFrameClass(InvalidParse):
    """AMQP 0.9.1 only specifies Header frames for the basic class. This means we got one for another class."""

    def __init__(self, class_id: int):
        self.class_id = class_id

    def __str__(self) -> str:
        return 'Header frames are only supported for class basic, not {}'.format(self.class_id)


class TooManyHeaderProperties(InvalidParse):
    """The sequence of header properties is fixed for each AMQP class. Server sent flags indicating there's more than
    there should be."""

    def __init__(self, class_id: int):
        self.class_id = class_id

    def __str__(self) -> str:
        return 'Header frame with more properties than class {} specifies'.format(self.class_id)


class UnsupportedMethod(InvalidParse):
    """Server sent a class+method pair we don't understand"""

    def __init__(self, cam: int):
        self.cam = cam

    def __str__(self):
        class_id, method_id = divmod(self.cam, 2**16)
        return 'Method frame with unsupported method {} for class {}'.format(method_id, class_id)
