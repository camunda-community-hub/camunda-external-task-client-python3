from unittest import TestCase

from camunda.client.engine_client import ENGINE_LOCAL_BASE_URL
from camunda.client.external_task_client import ExternalTaskClient


class ExternalTaskClientTest(TestCase):

    def test_auth_basic_creation_with_no_debug_config(self):
        client = ExternalTaskClient(
            1, ENGINE_LOCAL_BASE_URL, {"auth_basic": {"username": "demo", "password": "demo"}})
        self.assertFalse(client.is_debug)
        self.assertFalse(client.config.get("isDebug"))

    def test_auth_basic_creation_with_debug_config(self):
        client = ExternalTaskClient(
            1, ENGINE_LOCAL_BASE_URL,{"auth_basic": {"username": "demo", "password": "demo"}, "isDebug": True})
        self.assertTrue(client.is_debug)
        self.assertTrue(client.config.get("isDebug"))
