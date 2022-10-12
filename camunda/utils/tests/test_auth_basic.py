from unittest import TestCase

from camunda.utils.auth_basic import AuthBasic, obfuscate_password


class TestAuthBasic(TestCase):
    def test_auth_basic(self):
        auth_basic = AuthBasic(**{
            "username": "test",
            "password": "test",
        })
        self.assertEqual(auth_basic.token, 'Basic dGVzdDp0ZXN0')


    def test_obfuscate_password(self):
        default_config = {
            "auth_basic": {"username": "demo", "password": "demo"},
            "maxTasks": 1,
            "lockDuration": 10000,
            "asyncResponseTimeout": 0,
            "isDebug": True,
        }
        obfuscate_config = {
            "auth_basic": {"username": "demo", "password": "***"},
            "maxTasks": 1,
            "lockDuration": 10000,
            "asyncResponseTimeout": 0,
            "isDebug": True,
        }
        self.assertEqual(obfuscate_password(default_config), obfuscate_config)
