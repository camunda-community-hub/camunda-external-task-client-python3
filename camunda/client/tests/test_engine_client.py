from http import HTTPStatus

import aiounittest
from aioresponses import aioresponses

from camunda.client.engine_client import EngineClient, ENGINE_LOCAL_BASE_URL


class EngineClientTest(aiounittest.AsyncTestCase):
    tenant_id = "6172cdf0-7b32-4460-9da0-ded5107aa977"
    process_key = "PARALLEL_STEPS_EXAMPLE"

    @aioresponses()
    async def test_start_process_success(self, http_mock):
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
        http_mock.post(client.get_start_process_instance_url(self.process_key, self.tenant_id),
                       status=HTTPStatus.OK, payload=resp_payload)
        actual_resp_payload = await client.start_process(self.process_key, {}, self.tenant_id)
        self.assertDictEqual(resp_payload, actual_resp_payload)

    @aioresponses()
    async def test_start_process_not_found_raises_exception(self, http_mock):
        client = EngineClient()
        resp_payload = {
            "type": "RestException",
            "message": "No matching process definition with key: PROCESS_KEY_NOT_EXISTS and tenant-id: tenant_123"
        }
        http_mock.post(client.get_start_process_instance_url("PROCESS_KEY_NOT_EXISTS", self.tenant_id),
                       status=HTTPStatus.NOT_FOUND, payload=resp_payload)
        with self.assertRaises(Exception) as exception_ctx:
            await client.start_process("PROCESS_KEY_NOT_EXISTS", {}, self.tenant_id)

        self.assertEqual("No matching process definition with key: PROCESS_KEY_NOT_EXISTS and tenant-id: tenant_123",
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
        http_mock.post(client.get_start_process_instance_url(self.process_key, self.tenant_id),
                       status=HTTPStatus.BAD_REQUEST, payload=resp_payload)
        with self.assertRaises(Exception) as exception_ctx:
            await client.start_process(self.process_key, {"int_var": "1aa2345"}, self.tenant_id)

        self.assertEqual(expected_message, str(exception_ctx.exception))

    @aioresponses()
    async def test_start_process_server_error_raises_exception(self, http_mock):
        client = EngineClient()
        http_mock.post(client.get_start_process_instance_url(self.process_key, self.tenant_id),
                       status=HTTPStatus.INTERNAL_SERVER_ERROR)
        with self.assertRaises(Exception) as exception_ctx:
            await client.start_process(self.process_key, {"int_var": "1aa2345"}, self.tenant_id)

        self.assertEqual("HTTPStatus.INTERNAL_SERVER_ERROR, message='Internal Server Error'",
                         str(exception_ctx.exception))

    @aioresponses()
    async def test_get_process_instance_success(self, http_mock):
        client = EngineClient()
        resp_payload = [
            {
                "links": [],
                "id": "c2c68785-9f42-11ea-a841-0242ac1c0004",
                "definitionId": "PARALLEL_STEPS_EXAMPLE:1:88613042-9f42-11ea-a841-0242ac1c0004",
                "businessKey": None,
                "caseInstanceId": None,
                "ended": False,
                "suspended": False,
                "tenantId": self.tenant_id
            }
        ]
        get_process_instance_url = f"{ENGINE_LOCAL_BASE_URL}/process-instance" \
                                   f"?processDefinitionKey={self.process_key}" \
                                   f"&tenantIdIn={self.tenant_id}" \
                                   f"&variables=intVar_eq_1,strVar_eq_hello"
        http_mock.get(get_process_instance_url, status=HTTPStatus.OK, payload=resp_payload)
        actual_resp_payload = await client.get_process_instance(process_key=self.process_key,
                                                                variables=["intVar_eq_1", "strVar_eq_hello"],
                                                                tenant_ids=[self.tenant_id])
        self.assertListEqual(resp_payload, actual_resp_payload)

    @aioresponses()
    async def test_get_process_instance_bad_request_raises_exception(self, http_mock):
        client = EngineClient()
        expected_message = "Invalid variable comparator specified: XXX"
        resp_payload = {
            "type": "InvalidRequestException",
            "message": expected_message
        }
        get_process_instance_url = f"{ENGINE_LOCAL_BASE_URL}/process-instance" \
                                   f"?processDefinitionKey={self.process_key}" \
                                   f"&tenantIdIn={self.tenant_id}" \
                                   f"&variables=intVar_XXX_1,strVar_eq_hello"
        http_mock.get(get_process_instance_url, status=HTTPStatus.BAD_REQUEST, payload=resp_payload)
        with self.assertRaises(Exception) as exception_ctx:
            await client.get_process_instance(process_key=self.process_key,
                                              variables=["intVar_XXX_1", "strVar_eq_hello"],
                                              tenant_ids=[self.tenant_id])

        self.assertEqual(expected_message, str(exception_ctx.exception))

    @aioresponses()
    async def test_get_process_instance_server_error_raises_exception(self, http_mock):
        client = EngineClient()
        get_process_instance_url = f"{ENGINE_LOCAL_BASE_URL}/process-instance" \
                                   f"?processDefinitionKey={self.process_key}" \
                                   f"&tenantIdIn={self.tenant_id}" \
                                   f"&variables=intVar_XXX_1,strVar_eq_hello"
        http_mock.get(get_process_instance_url, status=HTTPStatus.INTERNAL_SERVER_ERROR)
        with self.assertRaises(Exception) as exception_ctx:
            await client.get_process_instance(process_key=self.process_key,
                                              variables=["intVar_XXX_1", "strVar_eq_hello"],
                                              tenant_ids=[self.tenant_id])

        self.assertEqual("HTTPStatus.INTERNAL_SERVER_ERROR, message='Internal Server Error'",
                         str(exception_ctx.exception))
