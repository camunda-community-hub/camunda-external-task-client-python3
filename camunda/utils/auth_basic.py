import base64
import copy
from pydantic import BaseModel


def obfuscate_password(config: dict) -> dict:
    """Obfuscate password value in auth_basic config

    :param config: config from ExternalTaskWorker or ExternalTaskClient
    :returns: _config with obfuscated password
    """
    _config = copy.deepcopy(config)
    _auth = _config.get('auth_basic')
    if _auth is not None and 'password' in _auth.keys():
        _auth['password'] = '***'
    return _config

class AuthBasic(BaseModel):
    username: str
    password: str
    token: str = ""

    def __init__(self, **data):
        super().__init__(**data)
        token = f"{self.username}:{self.password}"
        bytemsg = base64.b64encode(token.encode('utf-8'))
        tokenb64 = str(bytemsg, "utf-8")
        self.token = f"Basic {tokenb64}"
