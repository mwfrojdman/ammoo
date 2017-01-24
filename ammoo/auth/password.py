from typing import Set, Union

from ammoo.auth.mechanisms.sasl_plain import SaslPlainAuth
from .abc import Authentication
from .mechanisms.amqplain import AMQPlainAuth
from ammoo.exceptions.connection import ServerAuthMechanismsNotSupportedError


class PasswordAuthentication(Authentication):
    def __init__(self, login, password):
        self.login = login
        self.password = password

    def select_mechanism(self, server_mechanisms: Set[str]) -> Union[AMQPlainAuth, SaslPlainAuth]:
        for auth_mech in (AMQPlainAuth, SaslPlainAuth):
            if auth_mech.name() in server_mechanisms:
                return auth_mech(self.login, self.password)
        raise ServerAuthMechanismsNotSupportedError()
