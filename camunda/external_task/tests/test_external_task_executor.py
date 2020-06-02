import collections
from http import HTTPStatus
from unittest import TestCase

import responses

from camunda.client.external_task_client import ExternalTaskClient
from camunda.external_task.external_task import TaskResult, ExternalTask
from camunda.external_task.external_task_executor import ExternalTaskExecutor


class ExternalTaskExecutorTest(TestCase):

    def task_success_action(self, task):
        output_vars = {"var1": 1, "var2": "value", "var3": True}
        return TaskResult.success(task, output_vars)

    @responses.activate
    def test_task_complete(self):
        task = ExternalTask({"id": "1", "topicName": "my_topic"})
        output_vars = {"var1": 1, "var2": "value", "var3": True}
        expected_task_result = TaskResult.success(task, output_vars)

        external_task_client = ExternalTaskClient(worker_id=1)
        responses.add(responses.POST, external_task_client.get_task_complete_url(task.get_task_id()),
                      status=HTTPStatus.NO_CONTENT)
        executor = ExternalTaskExecutor(worker_id=1, external_task_client=external_task_client)

        actual_task_result = executor.execute_task(task, self.task_success_action)
        self.assertEqual(str(expected_task_result), str(actual_task_result))

    def task_failure_action(self, task):
        return TaskResult.failure(task, error_message="unknown task failure", error_details="unknown error",
                                  retries=3, retry_timeout=30000)

    @responses.activate
    def test_task_failure(self):
        task = ExternalTask({"id": "1", "topicName": "my_topic"})
        expected_task_result = TaskResult.failure(task,
                                                  error_message="unknown task failure", error_details="unknown error",
                                                  retries=3, retry_timeout=30000)

        external_task_client = ExternalTaskClient(worker_id=1)
        responses.add(responses.POST, external_task_client.get_task_failure_url(task.get_task_id()),
                      status=HTTPStatus.NO_CONTENT)
        executor = ExternalTaskExecutor(worker_id=1, external_task_client=external_task_client)

        actual_task_result = executor.execute_task(task, self.task_failure_action)
        self.assertEqual(str(expected_task_result), str(actual_task_result))

    def task_bpmn_error_action(self, task):
        return TaskResult.bpmn_error(task, error_code="bpmn_err_code_1")

    @responses.activate
    def test_task_bpmn_error(self):
        task = ExternalTask({"id": "1", "topicName": "my_topic"})
        expected_task_result = TaskResult.bpmn_error(task, error_code="bpmn_err_code_1")

        external_task_client = ExternalTaskClient(worker_id=1)
        responses.add(responses.POST, external_task_client.get_task_bpmn_error_url(task.get_task_id()),
                      status=HTTPStatus.NO_CONTENT)
        executor = ExternalTaskExecutor(worker_id=1, external_task_client=external_task_client)

        actual_task_result = executor.execute_task(task, self.task_bpmn_error_action)
        self.assertEqual(str(expected_task_result), str(actual_task_result))

    def task_result_not_complete_failure_bpmnerror(self, task):
        return TaskResult.empty_task_result(task)

    def test_task_result_not_complete_failure_bpmnerror_raises_exception(self):
        task = ExternalTask({"id": "1", "topicName": "my_topic"})
        external_task_client = ExternalTaskClient(worker_id=1)
        executor = ExternalTaskExecutor(worker_id=1, external_task_client=external_task_client)

        with self.assertRaises(Exception) as exception_ctx:
            executor.execute_task(task, self.task_result_not_complete_failure_bpmnerror)

        self.assertEqual("task result for task_id=1 must be either complete/failure/BPMNError",
                         str(exception_ctx.exception))

    @responses.activate
    def test_execute_task_raises_exception_raised_when_updating_status_in_engine(self):
        client = ExternalTaskClient(worker_id=1)
        task = ExternalTask({"id": "1", "topicName": "my_topic"})
        executor = ExternalTaskExecutor(worker_id=1, external_task_client=client)

        TaskResultStatusInput = collections.namedtuple('TaskResultStatusInput',
                                                       ['task_status', 'task_action', 'task_status_url',
                                                        'error_message'])

        task_result_tests = [
            TaskResultStatusInput("complete", self.task_success_action,
                                  client.get_task_complete_url(task.get_task_id()),
                                  "cannot update task status to complete"),
            TaskResultStatusInput("failure", self.task_failure_action,
                                  client.get_task_failure_url(task.get_task_id()),
                                  "cannot update task status to failure"),
            TaskResultStatusInput("bpmn_error", self.task_bpmn_error_action,
                                  client.get_task_bpmn_error_url(task.get_task_id()),
                                  "cannot update task status to BPMN err")
        ]

        for task_result_test in task_result_tests:
            with self.subTest(task_result_test.task_status):
                responses.add(responses.POST, task_result_test.task_status_url,
                              body=Exception(task_result_test.error_message))

                with self.assertRaises(Exception) as exception_ctx:
                    executor.execute_task(task, task_result_test.task_action)

                self.assertEqual(task_result_test.error_message, str(exception_ctx.exception))

    @responses.activate
    def test_execute_task_raises_exception_if_engine_returns_http_status_other_than_no_content_204(self):
        client = ExternalTaskClient(worker_id=1)
        task = ExternalTask({"id": "1", "topicName": "my_topic"})
        executor = ExternalTaskExecutor(worker_id=1, external_task_client=client)

        TaskResultStatusInput = collections.namedtuple('TaskResultStatusInput',
                                                       ['task_status', 'task_action', 'task_status_url',
                                                        'http_status_code', 'expected_error_message'])

        task_result_tests = [
            TaskResultStatusInput("complete", self.task_success_action,
                                  client.get_task_complete_url(task.get_task_id()), HTTPStatus.OK,
                                  'Not able to mark complete for task_id=1 for topic=my_topic, worker_id=1'),
            TaskResultStatusInput("failure", self.task_failure_action,
                                  client.get_task_failure_url(task.get_task_id()), HTTPStatus.CREATED,
                                  'Not able to mark failure for task_id=1 for topic=my_topic, worker_id=1'),
            TaskResultStatusInput("bpmn_error", self.task_bpmn_error_action,
                                  client.get_task_bpmn_error_url(task.get_task_id()), HTTPStatus.ACCEPTED,
                                  'Not able to mark BPMN Error for task_id=1 for topic=my_topic, worker_id=1')
        ]

        for task_result_test in task_result_tests:
            with self.subTest(task_result_test.task_status):
                responses.add(responses.POST, task_result_test.task_status_url,
                              status=task_result_test.http_status_code)

                with self.assertRaises(Exception) as exception_ctx:
                    executor.execute_task(task, task_result_test.task_action)

                self.assertEqual(task_result_test.expected_error_message, str(exception_ctx.exception))
