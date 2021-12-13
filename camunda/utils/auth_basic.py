import base64
from pydantic import BaseModel


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
