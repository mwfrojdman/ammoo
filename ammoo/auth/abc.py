from abc import ABCMeta, abstractmethod
from typing import Set

from ammoo.exceptions.connection import NoChallengesAuthenticationError


class AuthenticationMechanism(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def name(cls) -> str:
        raise NotImplementedError()

    @abstractmethod
    def build_start_ok_response(self, encoding: str, rabbitmq: bool) -> bytes:
        raise NotImplementedError()

    async def challenge(self, challenge: bytes) -> bytes:
        raise NoChallengesAuthenticationError(self.name())


class Authentication(metaclass=ABCMeta):
    @abstractmethod
    def select_mechanism(self, server_mechanisms: Set[str]) -> AuthenticationMechanism:
        raise NotImplementedError()
