FRAME_TYPE_HEARTBEAT = 8


class HeartbeatFrame:
    __slots__ = ()
    frame_type = FRAME_TYPE_HEARTBEAT

    def __init__(self):
        pass

    def __str__(self):
        return 'Heartbeat frame'


HEARTBEAT_FRAME = HeartbeatFrame()  # singleton value
