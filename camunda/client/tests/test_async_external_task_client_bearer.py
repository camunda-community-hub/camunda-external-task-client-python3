import unittest
from http import HTTPStatus
from unittest.mock import patch, AsyncMock

from camunda.client.async_external_task_client import AsyncExternalTaskClient
from camunda.client.engine_client import ENGINE_LOCAL_BASE_URL


class AsyncExternalTaskClientAuthTest(unittest.IsolatedAsyncioTestCase):

    async def test_auth_bearer_fetch_and_lock_no_debug(self):
        token = "some.super.long.jwt"
        with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value.status_code = HTTPStatus.OK
            mock_post.return_value.json.return_value = []

            client = AsyncExternalTaskClient(
                1,
                ENGINE_LOCAL_BASE_URL,
                {"auth_bearer": {"access_token": token}}
            )
            await client.fetch_and_lock("someTopic")

            headers_used = mock_post.call_args[1]["headers"]
            self.assertIn("Authorization", headers_used)
            self.assertEqual(f"Bearer {token}", headers_used["Authorization"])

    async def test_auth_bearer_fetch_and_lock_with_debug(self):
        token = "some.super.long.jwt"
        with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value.status_code = HTTPStatus.OK
            mock_post.return_value.json.return_value = []

            client = AsyncExternalTaskClient(
                1,
                ENGINE_LOCAL_BASE_URL,
                {"auth_bearer": {"access_token": token}, "isDebug": True}
            )
            await client.fetch_and_lock("someTopic")

            headers_used = mock_post.call_args[1]["headers"]
            self.assertIn("Authorization", headers_used)
            self.assertEqual(f"Bearer {token}", headers_used["Authorization"])