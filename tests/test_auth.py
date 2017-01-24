import pytest

from ammoo.auth.mechanisms.amqplain import AMQPlainAuth
from pytest import raises

from ammoo.auth.abc import Authentication, AuthenticationMechanism
from ammoo.auth.mechanisms.sasl_plain import SaslPlainAuth
from ammoo.auth.password import PasswordAuthentication
from ammoo.connect import connect
from ammoo.exceptions.connection import ServerClosedConnection, ServerAuthMechanismsNotSupportedError
from tests.conftest import pytestmark, check_clean_channel_close, check_clean_connection_close


@pytest.mark.timeout(7)
@pytestmark
async def test_bad_user(event_loop, rabbitmq_host):
    auth = PasswordAuthentication('baduser', 'password')
    with raises(ServerClosedConnection) as ctx:
        async with await connect(host=rabbitmq_host, loop=event_loop, auth=auth):
            pass
    assert ctx.value.reply_code == 403
    assert ctx.value.reply_text.startswith('ACCESS_REFUSED')
    assert ctx.value.class_id == 0
    assert ctx.value.method_id == 0


@pytest.mark.timeout(7)
@pytestmark
async def test_bad_password(event_loop, rabbitmq_host):
    auth = PasswordAuthentication('guest', 'badpass')
    with raises(ServerClosedConnection):
        async with await connect(host=rabbitmq_host, loop=event_loop, auth=auth):
            pass


class SaslPlainOnlyAuth(Authentication):
    def __init__(self):
        pass

    def select_mechanism(self, server_mechanisms):
        if SaslPlainAuth.name() not in server_mechanisms:
            raise ServerAuthMechanismsNotSupportedError()
        return SaslPlainAuth('guest', 'guest')


@pytest.mark.timeout(7)
@pytestmark
async def test_sasl_plain_auth(event_loop, rabbitmq_host):
    auth = SaslPlainOnlyAuth()
    async with await connect(host=rabbitmq_host, loop=event_loop, auth=auth) as connection:
        async with connection.channel() as channel:
            pass
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


class AmqPlainOnlyAuth(Authentication):
    def __init__(self):
        pass

    def select_mechanism(self, server_mechanisms) -> AMQPlainAuth:
        if AMQPlainAuth.name() not in server_mechanisms:
            raise ServerAuthMechanismsNotSupportedError()
        return AMQPlainAuth('guest', 'guest')


@pytest.mark.timeout(7)
@pytestmark
async def test_amqplain_auth(event_loop, rabbitmq_host):
    auth = AmqPlainOnlyAuth()
    async with await connect(host=rabbitmq_host, loop=event_loop, auth=auth) as connection:
        async with connection.channel() as channel:
            pass
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


@pytest.mark.timeout(7)
@pytestmark
async def test_amqplain_auth_directly(event_loop, rabbitmq_host):
    """directly as in no Authentication to choose the mech first"""
    auth = AMQPlainAuth('guest', 'guest')
    async with await connect(host=rabbitmq_host, loop=event_loop, auth=auth) as connection:
        async with connection.channel() as channel:
            pass
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)


class NoSuitableMechanismsAuth(Authentication):
    def __init__(self):
        pass

    def select_mechanism(self, server_mechanisms):
        raise ServerAuthMechanismsNotSupportedError()


@pytest.mark.timeout(7)
@pytestmark
async def test_no_suitable_auth_mechanism(event_loop, rabbitmq_host):
    auth = NoSuitableMechanismsAuth()
    connection = await connect(host=rabbitmq_host, loop=event_loop, auth=auth)
    with raises(ServerAuthMechanismsNotSupportedError) as excinfo:
        await connection.__aenter__()
    assert connection._closing_exc is excinfo.value


class RabbitAuthMechanismCrAuth(AuthenticationMechanism):
    @classmethod
    def name(cls):
        return 'RABBIT-CR-DEMO'

    def build_start_ok_response(self, encoding: str, rabbitmq: bool):
        return 'guest'.encode(encoding)

    async def challenge(self, challenge: bytes) -> bytes:
        assert challenge == b'Please tell me your password'
        return b'My password is guest'


class RabbitAuthMechanismCrDemo(Authentication):
    def __init__(self):
        pass

    def select_mechanism(self, server_mechanisms) -> RabbitAuthMechanismCrAuth:
        if RabbitAuthMechanismCrAuth.name() not in server_mechanisms:
            raise ServerAuthMechanismsNotSupportedError()
        return RabbitAuthMechanismCrAuth()


@pytest.mark.rabbitmq_demoqr
@pytest.mark.timeout(7)
@pytestmark
async def test_rabbit_cr_auth_demo(event_loop, rabbitmq_host):
    auth = RabbitAuthMechanismCrDemo()
    async with await connect(host=rabbitmq_host, loop=event_loop, auth=auth) as connection:
        async with connection.channel() as channel:
            pass
        check_clean_channel_close(channel)
    check_clean_connection_close(connection)
