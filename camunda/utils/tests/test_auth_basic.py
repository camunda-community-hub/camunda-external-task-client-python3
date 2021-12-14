from unittest import TestCase

from camunda.utils.auth_basic import AuthBasic


class TestAuthBasic(TestCase):
    def test_auth_basic(self):
        auth_basic = AuthBasic(**{
            "username": "test",
            "password": "test",
        })
        self.assertEqual(auth_basic.token, 'Basic dGVzdDp0ZXN0')
