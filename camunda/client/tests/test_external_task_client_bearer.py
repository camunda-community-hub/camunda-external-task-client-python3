from unittest import TestCase

from camunda.client.engine_client import ENGINE_LOCAL_BASE_URL
from camunda.client.external_task_client import ExternalTaskClient


class ExternalTaskClientTest(TestCase):

    def test_auth_bearer_creation_with_no_debug_config(self):
        token = ('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkZW1vIn0'
                 '.NbMsjy8QQ5nrjGTXqdTrJ6g0dqawRvZAqp4XvNt437M')
        client = ExternalTaskClient(
            1, ENGINE_LOCAL_BASE_URL, {"auth_bearer": {"access_token": token}})
        self.assertFalse(client.is_debug)
        self.assertFalse(client.config.get("isDebug"))

    def test_auth_bearer_creation_with_debug_config(self):
        token = ('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkZW1vIn0'
                 '.NbMsjy8QQ5nrjGTXqdTrJ6g0dqawRvZAqp4XvNt437M')
        client = ExternalTaskClient(
            1, ENGINE_LOCAL_BASE_URL,
            {"auth_bearer": {"access_token": token}, "isDebug": True})
        self.assertTrue(client.is_debug)
        self.assertTrue(client.config.get("isDebug"))
