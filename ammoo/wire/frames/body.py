FRAME_TYPE_BODY = 3


class BodyFrame:
    frame_type = FRAME_TYPE_BODY
    __slots__ = 'channel_id', 'payload_size', 'payload'

    def __init__(self, channel_id: int, payload_size: int, payload: bytes):
        self.channel_id = channel_id
        self.payload_size = payload_size
        self.payload = payload

    def __str__(self):
        return 'Body frame on channel {} with {} bytes of data'.format(self.channel_id, self.payload_size)
