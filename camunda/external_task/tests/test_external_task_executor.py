from http import HTTPStatus

import aiounittest
from aioresponses import aioresponses

from camunda.client.external_task_client import ExternalTaskClient
from camunda.external_task.external_task import TaskResult, ExternalTask
from camunda.external_task.external_task_executor import ExternalTaskExecutor


class ExternalTaskExecutorTest(aiounittest.AsyncTestCase):

    async def task_success_action(self, task):
        output_vars = {"var1": 1, "var2": "value", "var3": True}
        return TaskResult.success(task, output_vars)

    @aioresponses()
    async def test_task_complete(self, http_mock):
        task = ExternalTask({"id": "1", "topicName": "my_topic"})
        output_vars = {"var1": 1, "var2": "value", "var3": True}
        expected_task_result = TaskResult.success(task, output_vars)

        external_task_client = ExternalTaskClient(worker_id=1)
        http_mock.post(external_task_client.get_task_complete_url(task.get_task_id()), status=HTTPStatus.NO_CONTENT)
        executor = ExternalTaskExecutor(worker_id=1, external_task_client=external_task_client)

        actual_task_result = await executor.execute_task(task, self.task_success_action)
        self.assertEqual(str(expected_task_result), str(actual_task_result))

    async def task_failure_action(self, task):
        return TaskResult.failure(task, error_message="unknown task failure", error_details="unknown error",
                                  retries=3, retry_timeout=30000)

    @aioresponses()
    async def test_task_failure(self, http_mock):
        task = ExternalTask({"id": "1", "topicName": "my_topic"})
        expected_task_result = TaskResult.failure(task,
                                                  error_message="unknown task failure", error_details="unknown error",
                                                  retries=3, retry_timeout=30000)

        external_task_client = ExternalTaskClient(worker_id=1)
        http_mock.post(external_task_client.get_task_failure_url(task.get_task_id()), status=HTTPStatus.NO_CONTENT)
        executor = ExternalTaskExecutor(worker_id=1, external_task_client=external_task_client)

        actual_task_result = await executor.execute_task(task, self.task_failure_action)
        self.assertEqual(str(expected_task_result), str(actual_task_result))

    async def task_bpmn_error_action(self, task):
        return TaskResult.bpmn_error(task, error_code="bpmn_err_code_1")

    @aioresponses()
    async def test_task_bpmn_error(self, http_mock):
        task = ExternalTask({"id": "1", "topicName": "my_topic"})
        expected_task_result = TaskResult.bpmn_error(task, error_code="bpmn_err_code_1")

        external_task_client = ExternalTaskClient(worker_id=1)
        http_mock.post(external_task_client.get_task_bpmn_error_url(task.get_task_id()), status=HTTPStatus.NO_CONTENT)
        executor = ExternalTaskExecutor(worker_id=1, external_task_client=external_task_client)

        actual_task_result = await executor.execute_task(task, self.task_bpmn_error_action)
        self.assertEqual(str(expected_task_result), str(actual_task_result))
