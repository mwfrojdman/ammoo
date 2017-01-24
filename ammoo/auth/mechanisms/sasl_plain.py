from io import StringIO
from typing import Optional

from ammoo.auth.abc import AuthenticationMechanism


class SaslPlainAuth(AuthenticationMechanism):
    """The SASL PLAIN authentication mechanism according to RFC 4616"""
    __slots__ = 'authcid', 'passwd', 'authzid'

    @classmethod
    def name(self):
        return 'PLAIN'

    def __init__(self, authcid: str, passwd: str, authzid: Optional[str]=None):
        """
        :param authcid: Authentication identity (username)
        :param passwd: Password
        :param authzid: Optional: Authorization identity
        """
        if not isinstance(authcid, str):
            raise TypeError('authcid must be a str object')
        if '\x00' in authcid:
            raise ValueError('NUL is not allowed')
        if not isinstance(passwd, str):
            raise TypeError('passwd must be a str object')
        if authzid is not None and not isinstance(authzid, str):
            raise TypeError('authzid must be a str object')
        self.authcid = authcid
        self.passwd = passwd
        self.authzid = authzid

    def build_start_ok_response(self, encoding, rabbitmq):
        if encoding != 'utf-8':
            raise AuthFail('{} only supports utf-8 encoding'.format(self.name()))
        sio = StringIO()
        authzid = self.authzid
        if authzid is not None:
            sio.write(authzid)
        sio.write('\0')
        sio.write(self.authcid)
        sio.write('\0')
        sio.write(self.passwd)
        return sio.getvalue().encode(encoding)
