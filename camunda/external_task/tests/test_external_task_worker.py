from http import HTTPStatus
from unittest import mock, TestCase
from unittest.mock import patch

import responses

from camunda.client.external_task_client import ExternalTaskClient
from camunda.external_task.external_task_worker import ExternalTaskWorker


class ExternalTaskWorkerTest(TestCase):

    @responses.activate
    def test_fetch_and_execute_calls_task_action_for_each_task_fetched(self):
        external_task_client = ExternalTaskClient(worker_id=0)
        resp_payload = [{
            "activityId": "anActivityId",
            "activityInstanceId": "anActivityInstanceId",
            "errorMessage": "anErrorMessage",
            "errorDetails": "anErrorDetails",
            "executionId": "anExecutionId",
            "id": "anExternalTaskId",
            "lockExpirationTime": "2015-10-06T16:34:42",
            "processDefinitionId": "aProcessDefinitionId",
            "processDefinitionKey": "aProcessDefinitionKey",
            "processInstanceId": "aProcessInstanceId",
            "tenantId": None,
            "retries": 3,
            "workerId": "aWorkerId",
            "priority": 4,
            "topicName": "createOrder",
            "variables": {
                "orderId": {
                    "type": "String",
                    "value": "1234",
                    "valueInfo": {}
                }
            }
        },
            {
                "activityId": "anActivityId",
                "activityInstanceId": "anActivityInstanceId",
                "errorMessage": "anErrorMessage",
                "errorDetails": "anotherErrorDetails",
                "executionId": "anExecutionId",
                "id": "anExternalTaskId",
                "lockExpirationTime": "2015-10-06T16:34:42",
                "processDefinitionId": "aProcessDefinitionId",
                "processDefinitionKey": "aProcessDefinitionKey",
                "processInstanceId": "aProcessInstanceId",
                "tenantId": None,
                "retries": 3,
                "workerId": "aWorkerId",
                "priority": 0,
                "topicName": "createOrder",
                "variables": {
                    "orderId": {
                        "type": "String",
                        "value": "3456",
                        "valueInfo": {}
                    }
                }
            }]
        responses.add(responses.POST, external_task_client.get_fetch_and_lock_url(),
                      status=HTTPStatus.OK, json=resp_payload)

        worker = ExternalTaskWorker()
        mock_action = mock.Mock()
        worker.fetch_and_execute("my_topic", mock_action)
        self.assertEqual(2, mock_action.call_count)

    @responses.activate
    @patch('time.sleep', return_value=None)
    def test_fetch_and_execute_raises_exception_sleep_is_called(self, mock_time_sleep):
        external_task_client = ExternalTaskClient(worker_id=0)
        responses.add(responses.POST, external_task_client.get_fetch_and_lock_url(),
                      status=HTTPStatus.INTERNAL_SERVER_ERROR)

        sleep_seconds = 100
        worker = ExternalTaskWorker(config={"sleepSeconds": sleep_seconds})
        mock_action = mock.Mock()

        worker.fetch_and_execute("my_topic", mock_action)

        self.assertEqual(0, mock_action.call_count)
        self.assertEqual(1, mock_time_sleep.call_count)
        mock_time_sleep.assert_called_with(sleep_seconds)
