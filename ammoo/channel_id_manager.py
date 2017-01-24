from bisect import bisect_left

MAX_CHANNEL_LIMIT = 2**16 - 1


class _Range:
    __slots__ = 'low', 'high'

    def __init__(self, low, high):
        assert low <= high
        self.low = low
        self.high = high

    def __len__(self):
        return self.high - self.low + 1

    def __lt__(self, other):
        return self.high < other.low

    def __iter__(self):
        return iter(range(self.low, self.high + 1))

    def __repr__(self):
        return '[{},{}]'.format(self.low, self.high)


class NoChannelsAvailable(Exception):
    def __init__(self, max_channel):
        self.max_channel = max_channel

    def __str__(self):
        return 'No channel identifier is available: {} channels in use'.format(self.max_channel)


class AlreadyFree(Exception):
    def __str__(self):
        return 'channel id is already free'


class ChannelIdManager:
    def __init__(self, max_channel):
        if max_channel is None:
            max_channel = MAX_CHANNEL_LIMIT
        elif max_channel < 1 or max_channel > MAX_CHANNEL_LIMIT:
            raise ValueError(max_channel)
        self._max_channel = max_channel
        self._ranges = [_Range(1, max_channel)]

    @property
    def channel_max(self):
        return self._max_channel

    def acquire(self):
        try:
            rng = self._ranges[0]
        except IndexError:
            raise NoChannelsAvailable(self._max_channel)
        channel_id = rng.low
        if len(rng) == 1:
            del self._ranges[0]
        else:
            rng.low += 1
        return channel_id

    def release(self, channel_id):
        rng = _Range(channel_id, channel_id)
        index = bisect_left(self._ranges, rng)
        try:
            right_neighbor = self._ranges[index]
        except IndexError:
            # no neighbors to the right
            pass
        else:
            if right_neighbor.low <= channel_id:
                raise AlreadyFree()
            if right_neighbor.low == channel_id + 1:
                # merge with it
                right_neighbor.low = rng.low
                rng = right_neighbor
        try:
            left_neighbor = self._ranges[index - 1]
        except IndexError:
            # no neighbors to the left
            pass
        else:
            if left_neighbor.high == channel_id - 1:
                # merge with it
                left_neighbor.high = rng.high
                if len(rng) > 1:
                    # also merged with right - need to remove it
                    del self._ranges[index]
                rng = left_neighbor
        if len(rng) == 1:
            # did not merge with left nor right
            self._ranges.insert(index, rng)

    def __len__(self):
        return sum(len(rng) for rng in self._ranges)

    def __iter__(self):
        for rng in self._ranges:
            yield from rng
