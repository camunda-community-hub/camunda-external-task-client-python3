from unittest import TestCase

from camunda.utils.auth_bearer import AuthBearer


class TestAuthBasic(TestCase):
    """Can you generate a bearer token using jwt lib.

    reffer - https://pyjwt.readthedocs.io/en/stable/
    """
    def test_concat_bearer(self):
        token = ('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkZW1vIn0'
                 '.NbMsjy8QQ5nrjGTXqdTrJ6g0dqawRvZAqp4XvNt437M')
        auth_bearer = AuthBearer(access_token=token)
        self.assertEqual(auth_bearer.access_token, f'Bearer {token}')
