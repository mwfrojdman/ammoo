from asyncio import Queue
from itertools import chain


class QueueClosed(Exception):
    pass


class CloseableQueue(Queue):
    def _init(self, maxsize):
        super()._init(maxsize)
        self._closed = False

    def close(self):
        self._closed = True
        for waiter in chain(self._getters, self._putters):
            if not waiter.done():
                waiter.set_exception(QueueClosed)
        self._getters.clear()
        self._putters.clear()
        self._finished.set()

    async def put(self, item):
        if self._closed:
            raise QueueClosed
        return await super().put(item)

    def put_nowait(self, item):
        if self._closed:
            raise QueueClosed
        return super().put_nowait(item)

    async def get(self):
        if self._closed:
            raise QueueClosed
        return await super().get()

    def get_nowait(self):
        if self._closed:
            raise QueueClosed
        return super().get_nowait()

    def task_done(self):
        if self._closed:
            raise QueueClosed
        return super().task_done()

    async def join(self):
        if self._closed:
            raise QueueClosed
        await super().join()
        if self._closed:
            raise QueueClosed
