import unittest
from unittest.mock import AsyncMock

from camunda.client.async_external_task_client import AsyncExternalTaskClient
from camunda.external_task.async_external_task_executor import AsyncExternalTaskExecutor
from camunda.external_task.external_task import ExternalTask, TaskResult


class AsyncExternalTaskExecutorTest(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        """
        asyncSetUp runs before each test method in IsolatedAsyncioTestCase.
        We instantiate an AsyncExternalTaskClient and patch/mocks as needed.
        """
        self.mock_client = AsyncMock(spec=AsyncExternalTaskClient)
        self.mock_client.complete.return_value = True
        self.mock_client.failure.return_value = True
        self.mock_client.bpmn_failure.return_value = True

        self.executor = AsyncExternalTaskExecutor(
            worker_id="someWorker",
            external_task_client=self.mock_client
        )

    async def test_execute_task_success(self):
        async def success_action(task: ExternalTask):
            return TaskResult.success(task, {"globalVar": 42}, {"localVar": "foo"})

        task = ExternalTask({"id": "taskId", "topicName": "someTopic"})

        result = await self.executor.execute_task(task, success_action)

        # Assertions
        self.assertTrue(result.is_success())
        self.mock_client.complete.assert_awaited_once_with(
            "taskId", {"globalVar": 42}, {"localVar": "foo"}
        )

    async def test_execute_task_failure(self):
        async def fail_action(task: ExternalTask):
            return TaskResult.failure(
                task,
                error_message="Some error",
                error_details="Details here",
                retries=3,
                retry_timeout=1000
            )

        task = ExternalTask({"id": "taskId", "topicName": "someTopic"})
        result = await self.executor.execute_task(task, fail_action)

        # Assertions
        self.assertTrue(result.is_failure())
        self.mock_client.failure.assert_awaited_once_with(
            "taskId", "Some error", "Details here", 3, 1000
        )

    async def test_execute_task_bpmn_error(self):
        async def bpmn_error_action(task: ExternalTask):
            return TaskResult.bpmn_error(
                task,
                error_code="bpmn_err_code",
                error_message="bpmn error message",
                variables={"varA": True}
            )

        task = ExternalTask({"id": "taskId", "topicName": "someTopic"})
        result = await self.executor.execute_task(task, bpmn_error_action)

        # Assertions
        self.assertTrue(result.is_bpmn_error())
        self.mock_client.bpmn_failure.assert_awaited_once_with(
            "taskId", "bpmn_err_code", "bpmn error message", {"varA": True}
        )

    async def test_execute_task_empty_result_raises_exception(self):
        """
        If the action returns an "empty" TaskResult (not success/failure/BPMNError),
        executor should raise an exception.
        """

        async def empty_action(task: ExternalTask):
            return TaskResult.empty_task_result(task)

        task = ExternalTask({"id": "taskId", "topicName": "someTopic"})

        with self.assertRaises(Exception) as ctx:
            await self.executor.execute_task(task, empty_action)

        self.assertIn("must be either complete/failure/BPMNError", str(ctx.exception))

    async def test_handle_task_success_when_client_returns_false_raises_exception(self):
        """
        If client.complete returns False, an Exception must be raised.
        """
        self.mock_client.complete.return_value = False

        async def success_action(task: ExternalTask):
            return TaskResult.success(task, {"var": "val"})

        task = ExternalTask({"id": "taskId", "topicName": "someTopic"})

        with self.assertRaises(Exception) as ctx:
            await self.executor.execute_task(task, success_action)

        self.assertIn("Not able to mark complete for task_id=taskId", str(ctx.exception))

    async def test_handle_task_failure_when_client_returns_false_raises_exception(self):
        """
        If client.failure returns False, an Exception must be raised.
        """
        self.mock_client.failure.return_value = False

        async def fail_action(task: ExternalTask):
            return TaskResult.failure(task, "errMsg", "errDetails", 3, 2000)

        task = ExternalTask({"id": "taskId", "topicName": "someTopic"})

        with self.assertRaises(Exception) as ctx:
            await self.executor.execute_task(task, fail_action)

        self.assertIn("Not able to mark failure for task_id=taskId", str(ctx.exception))

    async def test_handle_task_bpmn_error_when_client_returns_false_raises_exception(self):
        """
        If client.bpmn_failure returns False, an Exception must be raised.
        """
        self.mock_client.bpmn_failure.return_value = False

        async def bpmn_error_action(task: ExternalTask):
            return TaskResult.bpmn_error(task, "ERR_CODE", "error message")

        task = ExternalTask({"id": "taskId", "topicName": "someTopic"})

        with self.assertRaises(Exception) as ctx:
            await self.executor.execute_task(task, bpmn_error_action)

        self.assertIn("Not able to mark BPMN Error for task_id=taskId", str(ctx.exception))
