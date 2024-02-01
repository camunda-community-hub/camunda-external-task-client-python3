import base64
import collections
from http import HTTPStatus
from unittest import IsolatedAsyncioTestCase

from aioresponses import aioresponses

from camunda.client.external_task_client import ExternalTaskClient
from camunda.external_task.external_task import TaskResult, ExternalTask
from camunda.external_task.external_task_executor import ExternalTaskExecutor


class ExternalTaskExecutorTest(IsolatedAsyncioTestCase):

    @staticmethod
    async def task_success_action(task):
        output_vars = {"var1": 1, "var2": "value", "var3": True}
        return TaskResult.success(task, output_vars)

    @aioresponses()
    async def test_task_complete(self, mocked: aioresponses):
        task = ExternalTask({"id": "1", "topicName": "my_topic"})
        output_vars = {"var1": 1, "var2": "value", "var3": True}
        expected_task_result = TaskResult.success(task, output_vars)

        external_task_client = ExternalTaskClient(worker_id=1)
        mocked.post(external_task_client.get_task_complete_url(task.get_task_id()), status=HTTPStatus.NO_CONTENT)
        executor = ExternalTaskExecutor(worker_id=1, external_task_client=external_task_client)

        actual_task_result = await executor.execute_task(task, self.task_success_action)
        self.assertEqual(str(expected_task_result), str(actual_task_result))

    @staticmethod
    async def task_failure_action(task):
        return TaskResult.failure(task, error_message="unknown task failure", error_details="unknown error",
                                  retries=3, retry_timeout=30000)

    @aioresponses()
    async def test_task_failure(self, mocked: aioresponses):
        task = ExternalTask({"id": "1", "topicName": "my_topic"})
        expected_task_result = TaskResult.failure(
            task,
            error_message="unknown task failure",
            error_details="unknown error",
            retries=3,
            retry_timeout=30000
        )

        external_task_client = ExternalTaskClient(worker_id=1)
        mocked.post(external_task_client.get_task_failure_url(task.get_task_id()), status=HTTPStatus.NO_CONTENT)
        executor = ExternalTaskExecutor(worker_id=1, external_task_client=external_task_client)

        actual_task_result = await executor.execute_task(task, self.task_failure_action)
        self.assertEqual(str(expected_task_result), str(actual_task_result))

    @staticmethod
    async def task_bpmn_error_action(task):
        return TaskResult.bpmn_error(task, error_code="bpmn_err_code_1", error_message="bpmn error")

    @aioresponses()
    async def test_task_bpmn_error(self, mocked: aioresponses):
        task = ExternalTask({"id": "1", "topicName": "my_topic"})
        expected_task_result = TaskResult.bpmn_error(task, error_code="bpmn_err_code_1", error_message="bpmn error")

        external_task_client = ExternalTaskClient(worker_id=1)
        mocked.post(external_task_client.get_task_bpmn_error_url(task.get_task_id()), status=HTTPStatus.NO_CONTENT)
        executor = ExternalTaskExecutor(worker_id=1, external_task_client=external_task_client)

        actual_task_result = await executor.execute_task(task, self.task_bpmn_error_action)
        self.assertEqual(str(expected_task_result), str(actual_task_result))

    @staticmethod
    async def task_result_not_complete_failure_bpmn_error(task):
        return TaskResult.empty_task_result(task)

    async def test_task_result_not_complete_failure_bpmn_error_raises_exception(self):
        task = ExternalTask({"id": "1", "topicName": "my_topic"})
        external_task_client = ExternalTaskClient(worker_id=1)
        executor = ExternalTaskExecutor(worker_id=1, external_task_client=external_task_client)

        with self.assertRaises(Exception) as exception_ctx:
            await executor.execute_task(task, self.task_result_not_complete_failure_bpmn_error)

        self.assertEqual(
            "task result for task_id=1 must be either complete/failure/BPMNError",
            str(exception_ctx.exception)
        )

    @aioresponses()
    async def test_execute_task_raises_exception_raised_when_updating_status_in_engine(self, mocked: aioresponses):
        client = ExternalTaskClient(worker_id=1)
        task = ExternalTask({"id": "1", "topicName": "my_topic"})
        executor = ExternalTaskExecutor(worker_id=1, external_task_client=client)

        TaskResultStatusInput = collections.namedtuple(
            'TaskResultStatusInput',
            ['task_status', 'task_action', 'task_status_url', 'error_message']
        )

        task_result_tests = [
            TaskResultStatusInput(
                "complete",
                self.task_success_action,
                client.get_task_complete_url(task.get_task_id()),
                "Not able to mark complete for task_id=1 for topic=my_topic, worker_id=1"
            ),
            TaskResultStatusInput(
                "failure",
                self.task_failure_action,
                client.get_task_failure_url(task.get_task_id()),
                "Not able to mark failure for task_id=1 for topic=my_topic, worker_id=1"
            ),
            TaskResultStatusInput(
                "bpmn_error",
                self.task_bpmn_error_action,
                client.get_task_bpmn_error_url(task.get_task_id()),
                "Not able to mark BPMN Error for task_id=1 for topic=my_topic, worker_id=1"
            )
        ]

        for task_result_test in task_result_tests:
            with self.subTest(task_result_test.task_status):
                mocked.post(task_result_test.task_status_url, payload=task_result_test.error_message)

                with self.assertRaises(Exception) as exception_ctx:
                    await executor.execute_task(task, task_result_test.task_action)

                self.assertEqual(task_result_test.error_message, str(exception_ctx.exception))

    @aioresponses()
    async def test_execute_task_raises_exception_if_engine_returns_http_status_other_than_no_content_204(self, mocked: aioresponses):
        client = ExternalTaskClient(worker_id=1)
        task = ExternalTask({"id": "1", "topicName": "my_topic"})
        executor = ExternalTaskExecutor(worker_id=1, external_task_client=client)

        TaskResultStatusInput = collections.namedtuple('TaskResultStatusInput',
                                                       ['task_status', 'task_action', 'task_status_url', 'http_status_code', 'expected_error_message'])

        task_result_tests = [
            TaskResultStatusInput(
                "complete",
                self.task_success_action,
                client.get_task_complete_url(task.get_task_id()),
                HTTPStatus.OK,
                'Not able to mark complete for task_id=1 for topic=my_topic, worker_id=1'
            ),
            TaskResultStatusInput(
                "failure",
                self.task_failure_action,
                client.get_task_failure_url(task.get_task_id()),
                HTTPStatus.CREATED,
                'Not able to mark failure for task_id=1 for topic=my_topic, worker_id=1'
            ),
            TaskResultStatusInput(
                "bpmn_error",
                self.task_bpmn_error_action,
                client.get_task_bpmn_error_url(task.get_task_id()),
                HTTPStatus.ACCEPTED,
                'Not able to mark BPMN Error for task_id=1 for topic=my_topic, worker_id=1'
            )
        ]

        for task_result_test in task_result_tests:
            with self.subTest(task_result_test.task_status):
                mocked.post(task_result_test.task_status_url, status=task_result_test.http_status_code)

                with self.assertRaises(Exception) as exception_ctx:
                    await executor.execute_task(task, task_result_test.task_action)

                self.assertEqual(task_result_test.expected_error_message, str(exception_ctx.exception))

    def test_strip_long_variables(self):
        variables = {
            "var0": "string",
            "var1": {"value": "string"},
            "var2": {"value": 1},
            "var3": {"value": "{\"key\": \"value\"}", "type": "Json"},
            "var4": {"value": base64.encodebytes(b"string"), "type": "Bytes"},
            "var5": {"value": "some file content", "type": "File"},
        }
        cleaned = ExternalTaskExecutor("worker-1", None)._strip_long_variables(variables)
        self.assertEqual({
            "var0": "string",
            "var1": {"value": "string"},
            "var2": {"value": 1},
            "var3": {"value": "{\"key\": \"value\"}", "type": "Json"},
            "var4": {"value": "...", "type": "Bytes"},
            "var5": {"value": "...", "type": "File"},
        }, cleaned)
