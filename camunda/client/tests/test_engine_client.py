from http import HTTPStatus
from unittest import TestCase

import responses

from camunda.client.engine_client import EngineClient, ENGINE_LOCAL_BASE_URL


class EngineClientTest(TestCase):
    tenant_id = "6172cdf0-7b32-4460-9da0-ded5107aa977"
    process_key = "PARALLEL_STEPS_EXAMPLE"

    @responses.activate
    def test_start_process_success(self):
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
        responses.add(responses.POST, client.get_start_process_instance_url(self.process_key, self.tenant_id),
                      json=resp_payload, status=HTTPStatus.OK)
        actual_resp_payload = client.start_process(self.process_key, {}, self.tenant_id)
        self.assertDictEqual(resp_payload, actual_resp_payload)

    @responses.activate
    def test_start_process_not_found_raises_exception(self):
        client = EngineClient()
        resp_payload = {
            "type": "RestException",
            "message": "No matching process definition with key: PROCESS_KEY_NOT_EXISTS and tenant-id: tenant_123"
        }
        responses.add(responses.POST, client.get_start_process_instance_url("PROCESS_KEY_NOT_EXISTS", self.tenant_id),
                      status=HTTPStatus.NOT_FOUND, json=resp_payload)
        with self.assertRaises(Exception) as exception_ctx:
            client.start_process("PROCESS_KEY_NOT_EXISTS", {}, self.tenant_id)

        self.assertEqual("No matching process definition with key: PROCESS_KEY_NOT_EXISTS and tenant-id: tenant_123",
                         str(exception_ctx.exception))

    @responses.activate
    def test_start_process_bad_request_raises_exception(self):
        client = EngineClient()
        expected_message = "Cannot instantiate process definition " \
                           "PARALLEL_STEPS_EXAMPLE:1:9b72da83-9b91-11ea-bad9-0242ac110002: " \
                           "Cannot convert value '1aa2345' of type 'Integer' to java type java.lang.Integer"
        resp_payload = {
            "type": "InvalidRequestException",
            "message": expected_message
        }
        responses.add(responses.POST, client.get_start_process_instance_url(self.process_key, self.tenant_id),
                      status=HTTPStatus.BAD_REQUEST, json=resp_payload)
        with self.assertRaises(Exception) as exception_ctx:
            client.start_process(self.process_key, {"int_var": "1aa2345"}, self.tenant_id)

        self.assertEqual(expected_message, str(exception_ctx.exception))

    @responses.activate
    def test_start_process_server_error_raises_exception(self):
        client = EngineClient()
        responses.add(responses.POST, client.get_start_process_instance_url(self.process_key, self.tenant_id),
                      status=HTTPStatus.INTERNAL_SERVER_ERROR)
        with self.assertRaises(Exception) as exception_ctx:
            client.start_process(self.process_key, {"int_var": "1aa2345"}, self.tenant_id)

        self.assertTrue("HTTPStatus.INTERNAL_SERVER_ERROR Server Error: Internal Server Error"
                        in str(exception_ctx.exception))

    @responses.activate
    def test_get_process_instance_success(self):
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
        responses.add(responses.GET, get_process_instance_url, status=HTTPStatus.OK, json=resp_payload)
        actual_resp_payload = client.get_process_instance(process_key=self.process_key,
                                                          variables=["intVar_eq_1", "strVar_eq_hello"],
                                                          tenant_ids=[self.tenant_id])
        self.assertListEqual(resp_payload, actual_resp_payload)

    @responses.activate
    def test_get_process_instance_bad_request_raises_exception(self):
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
        responses.add(responses.GET, get_process_instance_url, status=HTTPStatus.BAD_REQUEST, json=resp_payload)
        with self.assertRaises(Exception) as exception_ctx:
            client.get_process_instance(process_key=self.process_key,
                                        variables=["intVar_XXX_1", "strVar_eq_hello"],
                                        tenant_ids=[self.tenant_id])

        self.assertEqual(expected_message, str(exception_ctx.exception))

    @responses.activate
    def test_get_process_instance_server_error_raises_exception(self):
        client = EngineClient()
        get_process_instance_url = f"{ENGINE_LOCAL_BASE_URL}/process-instance" \
                                   f"?processDefinitionKey={self.process_key}" \
                                   f"&tenantIdIn={self.tenant_id}" \
                                   f"&variables=intVar_XXX_1,strVar_eq_hello"
        responses.add(responses.GET, get_process_instance_url, status=HTTPStatus.INTERNAL_SERVER_ERROR)
        with self.assertRaises(Exception) as exception_ctx:
            client.get_process_instance(process_key=self.process_key,
                                        variables=["intVar_XXX_1", "strVar_eq_hello"],
                                        tenant_ids=[self.tenant_id])

        self.assertTrue("HTTPStatus.INTERNAL_SERVER_ERROR Server Error: Internal Server Error"
                        in str(exception_ctx.exception))
