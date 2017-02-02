import asyncio
from functools import partial
from io import BytesIO

import pytest

from ammoo.wire.class_and_method import CONNECTION_TUNE_OK_CAM

from ammoo.wire.frames.method.connection import parse_connection_tune_parameters, pack_connection_tune_ok_parameters
from ammoo.auth.password import PasswordAuthentication
from ammoo.connect import connect
from ammoo.connection import Connection
from ammoo.exceptions.connection import HeartbeatTimeout, ConnectionLost

from ammoo_pytest_helpers import pytestmark, check_clean_connection_close


class _NoClientHeartbeatsConnection(Connection):
    """Used in test_miss_client_heartbeat. Skips sending heartbeats to server"""
    async def _send_heartbeats(self):
        while True:
            await asyncio.sleep(self._heartbeat_interval, loop=self._loop)


class _NoServerHeartbeatsConnection(Connection):
    """Used in test_miss_server_heartbeat. Makes client think server should send a heartbeat sooner than it does"""

    def _send_method_frame(self, channel_id: int, cam: int, parameters: bytes) -> None:
        if cam == CONNECTION_TUNE_OK_CAM:
            tune_ok_parameters = parse_connection_tune_parameters(BytesIO(parameters), 'utf-8', True)
            super()._send_method_frame(channel_id, cam, pack_connection_tune_ok_parameters(
                channel_max=tune_ok_parameters.channel_max,
                frame_max=tune_ok_parameters.frame_max,
                # this make the server think it should should send heartbeats every minute, while the client expects them every second
                heartbeat=60
            ))
        else:
            super()._send_method_frame(channel_id, cam, parameters)


@pytest.mark.timeout(20)
@pytestmark
async def test_miss_client_heartbeat(event_loop, rabbitmq_host):
    """Wait until server closes connection because the client (=we) didn't send heartbeats"""
    protocol_factory = partial(
        _NoClientHeartbeatsConnection, virtualhost='/', loop=event_loop, heartbeat_interval=1,
        auth=PasswordAuthentication('guest', 'guest'), frame_max=2*1024**2
    )

    transport, connection = await event_loop.create_connection(protocol_factory, rabbitmq_host, 5672, ssl=None)
    async with connection:
        interval = connection.heartbeat_interval
        assert interval == 1
        await asyncio.sleep(interval * 4.1, loop=event_loop)
    assert isinstance(connection._closing_exc, ConnectionLost)
    assert isinstance(connection._closing_exc.exc, ConnectionResetError)


@pytest.mark.timeout(20)
@pytestmark
async def test_miss_server_heartbeat(event_loop, rabbitmq_host):
    """Fool client to expect heartbeats sooner (every 1 sec) from server than it sends (60 secs)"""
    protocol_factory = partial(
        _NoServerHeartbeatsConnection, virtualhost='/', loop=event_loop, heartbeat_interval=1,
        auth=PasswordAuthentication('guest', 'guest'), frame_max=2*1024**2
    )

    transport, connection = await event_loop.create_connection(protocol_factory, rabbitmq_host, 5672, ssl=None)
    async with connection:
        interval = connection.heartbeat_interval
        assert interval == 1
        await asyncio.sleep(interval * 4.1, loop=event_loop)
    assert isinstance(connection._closing_exc, HeartbeatTimeout)
    assert connection._closing_exc.timeout == 2  # twice the interval


@pytest.mark.timeout(15)
@pytestmark
async def test_idle_connection(event_loop, rabbitmq_host):
    """Keep connection idle for 10 rounds of heartbeats, without disconnects"""
    async with await connect(host=rabbitmq_host, loop=event_loop, heartbeat_interval=1) as connection:
        assert connection._heartbeat_interval == 1
        async with connection.channel():
            await asyncio.sleep(10.0, loop=event_loop)
    check_clean_connection_close(connection)


@pytest.mark.timeout(15)
@pytestmark
async def test_no_heartbeats(event_loop, rabbitmq_host):
    # TODO: improve this so that there is a Connection subclass which overrides _heartbeat_subscriber and raises
    # an exception if there is a heartbeat sent. Might need a longer sleep, as Rabbit's default is 60 secs
    async with await connect(host=rabbitmq_host, loop=event_loop, heartbeat_interval=0) as connection:
        assert connection._heartbeat_interval == 0
        async with connection.channel():
            await asyncio.sleep(10.0, loop=event_loop)
    check_clean_connection_close(connection)
