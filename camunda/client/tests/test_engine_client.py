from http import HTTPStatus

import aiounittest
from aioresponses import aioresponses

from camunda.client.engine_client import EngineClient


class EngineClientTest(aiounittest.AsyncTestCase):

    @aioresponses()
    async def test_start_process_success_returns_id(self, http_mock):
        client = EngineClient()
        resp_payload = {
            "links": [
                {
                    "method": "GET",
                    "href": "http://localhost:8080/engine-rest/process-instance/cb678be8-9b93-11ea-bad9-0242ac110002",
                    "rel": "self"
                }
            ],
            "id": "cb678be8-9b93-11ea-bad9-0242ac110002",
            "definitionId": "PARALLEL_STEPS_EXAMPLE:1:9b72da83-9b91-11ea-bad9-0242ac110002",
            "businessKey": None,
            "caseInstanceId": None,
            "ended": False,
            "suspended": False,
            "tenantId": None
        }
        http_mock.post(client.get_start_process_instance_url("PARALLEL_STEPS_EXAMPLE"),
                       status=HTTPStatus.OK, payload=resp_payload)
        process_id = await client.start_process("PARALLEL_STEPS_EXAMPLE", {})
        self.assertEqual("cb678be8-9b93-11ea-bad9-0242ac110002", process_id)

    @aioresponses()
    async def test_start_process_not_found_raises_exception(self, http_mock):
        client = EngineClient()
        resp_payload = {
            "type": "RestException",
            "message": "No matching process definition with key: PROCESS_KEY_NOT_EXISTS and no tenant-id"
        }
        http_mock.post(client.get_start_process_instance_url("PROCESS_KEY_NOT_EXISTS"),
                       status=HTTPStatus.NOT_FOUND, payload=resp_payload)
        with self.assertRaises(Exception) as exception_ctx:
            await client.start_process("PROCESS_KEY_NOT_EXISTS", {})

        self.assertEqual("No matching process definition with key: PROCESS_KEY_NOT_EXISTS and no tenant-id",
                         str(exception_ctx.exception))

    @aioresponses()
    async def test_start_process_bad_request_raises_exception(self, http_mock):
        client = EngineClient()
        expected_message = "Cannot instantiate process definition " \
                           "PARALLEL_STEPS_EXAMPLE:1:9b72da83-9b91-11ea-bad9-0242ac110002: " \
                           "Cannot convert value '1aa2345' of type 'Integer' to java type java.lang.Integer"
        resp_payload = {
            "type": "InvalidRequestException",
            "message": expected_message
        }
        http_mock.post(client.get_start_process_instance_url("PARALLEL_STEPS_EXAMPLE"),
                       status=HTTPStatus.BAD_REQUEST, payload=resp_payload)
        with self.assertRaises(Exception) as exception_ctx:
            await client.start_process("PARALLEL_STEPS_EXAMPLE", {"int_var": "1aa2345"})

        self.assertEqual(expected_message, str(exception_ctx.exception))
