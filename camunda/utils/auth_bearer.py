from pydantic import BaseModel, validator


class AuthBearer(BaseModel):
    access_token: str

    @validator('access_token')
    @classmethod
    def concat_bearer(cls, value: str) -> str:
        return f'Bearer {value}'
