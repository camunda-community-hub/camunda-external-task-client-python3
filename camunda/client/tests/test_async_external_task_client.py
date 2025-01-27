import unittest
from http import HTTPStatus
from unittest.mock import patch, AsyncMock

import httpx

# Adjust the import based on your actual module path
from camunda.client.async_external_task_client import AsyncExternalTaskClient, ENGINE_LOCAL_BASE_URL


class AsyncExternalTaskClientTest(unittest.IsolatedAsyncioTestCase):
    """
    Tests for async_external_task_client.py
    """

    def setUp(self):
        # Common setup if needed
        self.default_worker_id = 1
        self.default_engine_url = ENGINE_LOCAL_BASE_URL

    async def test_creation_with_no_debug_config(self):
        client = AsyncExternalTaskClient(self.default_worker_id, self.default_engine_url, {})
        self.assertFalse(client.is_debug)
        self.assertFalse(client.config.get("isDebug"))
        # Check default_config merges:
        self.assertEqual(client.config["maxConcurrentTasks"], 10)
        self.assertEqual(client.config["lockDuration"], 300000)

    async def test_creation_with_debug_config(self):
        client = AsyncExternalTaskClient(self.default_worker_id, self.default_engine_url, {"isDebug": True})
        self.assertTrue(client.is_debug)
        self.assertTrue(client.config.get("isDebug"))

    @patch("httpx.AsyncClient.post")
    async def test_fetch_and_lock_success(self, mock_post):
        # Provide actual JSON as bytes
        content = b'[{"id": "someExternalTaskId", "topicName": "topicA"}]'
        mock_post.return_value = httpx.Response(
            status_code=200,
            request=httpx.Request("POST", "http://example.com"),
            content=content
        )

        client = AsyncExternalTaskClient(self.default_worker_id, self.default_engine_url, {})
        # Perform call
        tasks = await client.fetch_and_lock("topicA")

        # Assertions
        expected_url = f"{ENGINE_LOCAL_BASE_URL}/external-task/fetchAndLock"
        self.assertEqual([{"id": "someExternalTaskId", "topicName": "topicA"}], tasks)
        mock_post.assert_awaited_once()  # Check post was awaited exactly once
        args, kwargs = mock_post.call_args
        self.assertEqual(expected_url, args[0], "Expected correct fetchAndLock endpoint URL")
        # You could also check the payload or headers here:
        self.assertIn("json", kwargs)
        self.assertEqual(kwargs["json"]["workerId"], "1")  # str(worker_id)

    @patch("httpx.AsyncClient.post")
    async def test_fetch_and_lock_server_error(self, mock_post):
        # Create a real httpx.Response with status=500
        server_err_resp = httpx.Response(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            request=httpx.Request("POST", "http://example.com/external-task/fetchAndLock"),
            content=b"Internal Server Error"
        )
        # Each call to mock_post() returns this real response object
        mock_post.return_value = server_err_resp

        client = AsyncExternalTaskClient(self.default_worker_id, self.default_engine_url, {})

        # Now we expect an exception
        with self.assertRaises(httpx.HTTPStatusError) as ctx:
            await client.fetch_and_lock("topicA")

        # Optional: confirm the error message
        self.assertIn("500 Internal Server Error", str(ctx.exception))

    @patch("httpx.AsyncClient.post", new_callable=AsyncMock)
    async def test_complete_success(self, mock_post):
        mock_post.return_value.status_code = HTTPStatus.NO_CONTENT

        client = AsyncExternalTaskClient(self.default_worker_id, self.default_engine_url, {})
        result = await client.complete("myTaskId", {"globalVar": 1})

        self.assertTrue(result)
        mock_post.assert_awaited_once()
        complete_url = f"{ENGINE_LOCAL_BASE_URL}/external-task/myTaskId/complete"
        self.assertEqual(complete_url, mock_post.call_args[0][0])

    @patch("httpx.AsyncClient.post", new_callable=AsyncMock)
    async def test_failure_with_error_details(self, mock_post):
        mock_post.return_value.status_code = HTTPStatus.NO_CONTENT

        client = AsyncExternalTaskClient(self.default_worker_id, self.default_engine_url, {})
        result = await client.failure(
            task_id="myTaskId",
            error_message="some error",
            error_details="stacktrace info",
            retries=3,
            retry_timeout=10000
        )

        self.assertTrue(result)
        mock_post.assert_awaited_once()
        failure_url = f"{ENGINE_LOCAL_BASE_URL}/external-task/myTaskId/failure"
        self.assertEqual(failure_url, mock_post.call_args[0][0])
        self.assertIn("errorDetails", mock_post.call_args[1]["json"])

    @patch("httpx.AsyncClient.post", new_callable=AsyncMock)
    async def test_bpmn_failure_success(self, mock_post):
        mock_post.return_value.status_code = HTTPStatus.NO_CONTENT

        client = AsyncExternalTaskClient(self.default_worker_id, self.default_engine_url, {"isDebug": True})
        result = await client.bpmn_failure(
            task_id="myTaskId",
            error_code="BPMN_ERROR",
            error_message="an example BPMN error",
            variables={"foo": "bar"}
        )

        self.assertTrue(result)
        mock_post.assert_awaited_once()
        bpmn_url = f"{ENGINE_LOCAL_BASE_URL}/external-task/myTaskId/bpmnError"
        args, kwargs = mock_post.call_args
        self.assertEqual(bpmn_url, args[0])
        self.assertEqual(kwargs["json"]["errorCode"], "BPMN_ERROR")
        self.assertTrue(client.is_debug)  # Confirm the debug flag is set

