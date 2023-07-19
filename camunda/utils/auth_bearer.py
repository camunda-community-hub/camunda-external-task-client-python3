from typing import Any, Dict, Union

from pydantic import BaseModel, validator


class AuthBearer(BaseModel):
    access_token: str

    @validator('access_token', pre=True)
    @classmethod
    def get_token_from_dict(cls, value: Union[str, Dict[str, Any]]) -> str:
        if isinstance(value, str):
            return value
        if not isinstance(value, dict):
            raise ValueError('token should be dict or str')
        if not value.get('access_token'):
            raise KeyError(
                'you should pass the token inside "access_token" key')
        return value['access_token']

    @validator('access_token')
    @classmethod
    def concat_bearer(cls, value: str) -> str:
        if not any([
            value.startswith('Bearer'),
            value.startswith('bearer')
        ]):
            return f'Bearer {value}'
