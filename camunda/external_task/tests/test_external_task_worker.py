from unittest import mock

import aiounittest
from aioresponses import aioresponses

from camunda.client.external_task_client import ExternalTaskClient
from camunda.external_task.external_task_worker import ExternalTaskWorker
from camunda.external_task.tests.test_helper import AsyncMock


class ExternalTaskWorkerTest(aiounittest.AsyncTestCase):

    @aioresponses()
    async def test_fetch_and_execute_calls_task_action_for_each_task_fetched(self, http_mock):
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
        http_mock.post(external_task_client.get_fetch_and_lock_url(), status=200, payload=resp_payload)

        worker = ExternalTaskWorker()
        mock_action = mock.Mock()
        await worker.fetch_and_execute("my_topic", mock_action)
        self.assertEqual(2, mock_action.call_count)

    @aioresponses()
    async def test_fetch_and_execute_raises_exception_sleep_is_called(self, http_mock):
        external_task_client = ExternalTaskClient(worker_id=0)
        http_mock.post(external_task_client.get_fetch_and_lock_url(), exception=Exception("cannot fetch tasks"))

        sleep_seconds = 100
        worker = ExternalTaskWorker(config={"sleepSeconds": sleep_seconds})
        mock_action = mock.Mock()

        async_sleep_mock = AsyncMock
        with mock.patch('asyncio.sleep', new_callable=async_sleep_mock) as async_sleep_mock:
            await worker.fetch_and_execute("my_topic", mock_action)

        self.assertEqual(0, mock_action.call_count)
        self.assertEqual(1, async_sleep_mock.call_count)
        async_sleep_mock.assert_called_with(sleep_seconds)
