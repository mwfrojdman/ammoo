from io import BytesIO

from ammoo.auth.abc import AuthenticationMechanism
from ammoo.wire.low.containers import build_lengthless_field_table


class AMQPlainAuth(AuthenticationMechanism):
    @classmethod
    def name(self):
        return 'AMQPLAIN'

    def __init__(self, login, password):
        if not isinstance(login, (bytes, str)):
            raise TypeError('login must be a str or bytes object')
        if not isinstance(password, (bytes, str)):
            raise TypeError('password must be a str or bytes object')
        self.table = {
            'LOGIN': login,
            'PASSWORD': password,
        }

    def build_start_ok_response(self, encoding, rabbitmq):
        bio = BytesIO()
        build_lengthless_field_table(bio, self.table, encoding, rabbitmq)
        return bio.getvalue()
