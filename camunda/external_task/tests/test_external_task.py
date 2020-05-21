from unittest import TestCase

from camunda.external_task.external_task import ExternalTask


class ExternalTaskTest(TestCase):

    def test_external_task_creation_from_context(self):
        task = ExternalTask(context={"id": "123", "workerId": "321", "topicName": "my_topic", "tenantId": "tenant1"})

        self.assertEqual("123", task.get_task_id())
        self.assertEqual("321", task.get_worker_id())
        self.assertEqual("my_topic", task.get_topic_name())
        self.assertEqual("tenant1", task.get_tenant_id())
        self.assertEqual("empty_task_result", str(task.get_task_result()))

    def test_complete_returns_success_task_result(self):
        task = ExternalTask(context={})
        task_result = task.complete({})

        self.assertEqual(task, task_result.get_task())
        self.assertEqual(task_result, task.get_task_result())

        self.assertTrue(task_result.is_success())
        self.assertFalse(task_result.is_failure())
        self.assertFalse(task_result.is_bpmn_error())

    def test_failure_returns_failure_task_result(self):
        task = ExternalTask(context={})
        task_result = task.failure(error_message="unknown error", error_details="error details here",
                                   max_retries=3, retry_timeout=1000)

        self.assertEqual(task, task_result.get_task())
        self.assertEqual(task_result, task.get_task_result())

        self.assertFalse(task_result.is_success())
        self.assertTrue(task_result.is_failure())
        self.assertFalse(task_result.is_bpmn_error())

        self.assertEqual("unknown error", task_result.error_message)
        self.assertEqual("error details here", task_result.error_details)
        self.assertEqual(3, task_result.retries)
        self.assertEqual(1000, task_result.retry_timeout)

    def test_bpmn_error_returns_bpmn_error_task_result(self):
        task = ExternalTask(context={})
        task_result = task.bpmn_error(error_code="bpmn_error_code_1")

        self.assertEqual(task, task_result.get_task())
        self.assertEqual(task_result, task.get_task_result())

        self.assertFalse(task_result.is_success())
        self.assertFalse(task_result.is_failure())
        self.assertTrue(task_result.is_bpmn_error())

        self.assertEqual("bpmn_error_code_1", task_result.bpmn_error_code)

    def test_task_with_retries_returns_failure_task_result_with_decremented_retries(self):
        retries = 3
        task = ExternalTask(context={"retries": retries})
        task_result = task.failure(error_message="unknown error", error_details="error details here",
                                   max_retries=10, retry_timeout=1000)

        self.assertEqual(retries - 1, task_result.retries)

    def test_get_variable_returns_none_for_missing_variable(self):
        task = ExternalTask(context={})
        variable = task.get_variable("var_name")
        self.assertIsNone(variable)

    def test_get_variable_returns_value_for_variable_present(self):
        task = ExternalTask(context={"variables": {"var_name": {"value": 1}}})
        variable = task.get_variable("var_name")
        self.assertEqual(1, variable)

    def test_str(self):
        task = ExternalTask(context={"variables": {"var_name": {"value": 1}}})
        self.assertEqual("{'variables': {'var_name': {'value': 1}}}", str(task))
