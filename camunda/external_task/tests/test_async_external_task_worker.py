import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from camunda.client.async_external_task_client import AsyncExternalTaskClient
from camunda.external_task.async_external_task_worker import AsyncExternalTaskWorker
from camunda.external_task.external_task import ExternalTask, TaskResult


class AsyncExternalTaskWorkerTest(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        """
        Setup a worker with a mock AsyncExternalTaskClient
        """
        self.mock_client = AsyncMock(spec=AsyncExternalTaskClient)
        self.mock_client.fetch_and_lock.return_value = []

        self.config = {"maxConcurrentTasks": 2, "sleepSeconds": 0}  # faster tests
        self.worker = AsyncExternalTaskWorker("testWorker", config=self.config)
        # Replace the worker's .client with our mock
        self.worker.client = self.mock_client
        # Similarly, replace the executor's .external_task_client
        self.worker.executor.external_task_client = self.mock_client

    async def test_fetch_and_execute_no_tasks_returns_false(self):
        """
        If fetch_and_lock returns [], then fetch_and_execute should return False.
        """
        self.mock_client.fetch_and_lock.return_value = []
        result = await self.worker.fetch_and_execute(
            topic_name="myTopic",
            action=AsyncMock(return_value=None)  # doesn't matter, won't be called
        )
        self.assertFalse(result)

    async def test_fetch_and_execute_tasks_creates_execute_task_coroutines(self):
        """
        If fetch_and_lock returns multiple tasks, ensure each is passed into _execute_task
        in the background.
        """
        # 2 tasks with different variables
        resp = [
            {
                "id": "task1",
                "topicName": "myTopic",
                "workerId": "aWorkerId",
                "variables": {"foo": {"value": "bar"}}
            },
            {
                "id": "task2",
                "topicName": "myTopic",
                "workerId": "aWorkerId2",
                "variables": {"abc": {"value": 123}}
            }
        ]
        self.mock_client.fetch_and_lock.return_value = resp

        async def success_action(task: ExternalTask):
            # Return a success result for each
            return TaskResult.success(task, {"someGlobalVar": 99})

        returned = await self.worker.fetch_and_execute("myTopic", success_action)
        self.assertTrue(returned)
        # confirm 2 tasks => 2 coroutines started
        self.assertEqual(len(self.worker.running_tasks), 2)

        # Let them all finish
        await asyncio.gather(*self.worker.running_tasks, return_exceptions=True)

        # Now they should be removed from running_tasks
        self.assertEqual(len(self.worker.running_tasks), 0)

    async def test_execute_task_failure_when_action_raises_exception(self):
        """
        If an uncaught exception occurs in the user-provided action,
        the worker’s _execute_task wraps the result as a failure and tries to call
        external_task_client.failure(...)
        """
        self.mock_client.fetch_and_lock.return_value = [
            {"id": "task1", "topicName": "topicX", "workerId": "w1"}
        ]

        async def fail_action(task: ExternalTask):
            raise RuntimeError("Something went wrong")

        # We'll run fetch_and_execute => it should spawn one background task
        await self.worker.fetch_and_execute("topicX", fail_action)

        # Wait for background tasks to complete
        await asyncio.gather(*self.worker.running_tasks, return_exceptions=True)

        # Confirm the worker attempted to call 'failure(...)'
        self.mock_client.failure.assert_awaited_once()
        task_id, error_message, error_details, retries, retry_timeout = self.mock_client.failure.call_args.args
        self.assertEqual(task_id, "task1")
        self.assertEqual("Task execution failed", error_message)
        self.assertEqual("An unexpected error occurred while executing the task", error_details)
        self.assertEqual(3, retries)
        self.assertEqual(300000, retry_timeout)

    @patch.object(AsyncExternalTaskWorker, "_fetch_and_execute_safe")
    async def test_cancel_running_tasks_single_iteration(self, mock_fetch_and_execute):
        # Make _fetch_and_execute_safe run exactly once, then return
        async def one_iteration(*args, **kwargs):
            await self.worker.semaphore.acquire()
            await self.worker.fetch_and_execute(*args, **kwargs)
            # no 'while True', so it ends

        mock_fetch_and_execute.side_effect = one_iteration

        async def fake_long_action(task):
            await asyncio.sleep(9999999)

        self.mock_client.fetch_and_lock.return_value = [{"id": "taskX", "topicName": "topicA"}]

        sub_task = asyncio.create_task(
            self.worker._fetch_and_execute_safe("topicA", fake_long_action)
        )
        self.worker.subscriptions.append(sub_task)

        # Wait for that single iteration to run
        await asyncio.sleep(0.2)

        await self.worker.stop()
        await asyncio.sleep(0)  # let cancellation finish

        for t in self.worker.running_tasks:
            self.assertTrue(t.done())
