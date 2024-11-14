import base64
from http import HTTPStatus
from unittest import TestCase
from unittest.mock import patch

import responses

from camunda.client.engine_client import EngineClient, ENGINE_LOCAL_BASE_URL


class EngineClientAuthTest(TestCase):
    tenant_id = "6172cdf0-7b32-4460-9da0-ded5107aa977"
    process_key = "PARALLEL_STEPS_EXAMPLE"

    def setUp(self):
        self.client = EngineClient(config={"auth_basic": {"username": "demo", "password": "demo"}})

    @responses.activate
    def test_auth_basic_start_process_success(self):
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
            "businessKey": "123456",
            "caseInstanceId": None,
            "ended": False,
            "suspended": False,
            "tenantId": None
        }
        responses.add(responses.POST, self.client.get_start_process_instance_url(self.process_key, self.tenant_id),
                      json=resp_payload, status=HTTPStatus.OK)
        actual_resp_payload = self.client.start_process(self.process_key, {}, self.tenant_id, "123456")
        self.assertDictEqual(resp_payload, actual_resp_payload)

    @responses.activate
    def test_auth_basic_start_process_not_found_raises_exception(self):
        resp_payload = {
            "type": "RestException",
            "message": "No matching process definition with key: PROCESS_KEY_NOT_EXISTS and tenant-id: tenant_123"
        }
        responses.add(responses.POST,
                      self.client.get_start_process_instance_url("PROCESS_KEY_NOT_EXISTS", self.tenant_id),
                      status=HTTPStatus.NOT_FOUND, json=resp_payload)
        with self.assertRaises(Exception) as exception_ctx:
            self.client.start_process("PROCESS_KEY_NOT_EXISTS", {}, self.tenant_id)

        self.assertEqual("received 404 : RestException : "
                         "No matching process definition with key: PROCESS_KEY_NOT_EXISTS and tenant-id: tenant_123",
                         str(exception_ctx.exception))

    @responses.activate
    def test_auth_basic_start_process_bad_request_raises_exception(self):
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

        self.assertEqual(f"received 400 : InvalidRequestException : {expected_message}", str(exception_ctx.exception))

    @responses.activate
    def test_auth_basic_start_process_server_error_raises_exception(self):
        responses.add(responses.POST, self.client.get_start_process_instance_url(self.process_key, self.tenant_id),
                      status=HTTPStatus.INTERNAL_SERVER_ERROR)
        with self.assertRaises(Exception) as exception_ctx:
            self.client.start_process(self.process_key, {"int_var": "1aa2345"}, self.tenant_id)

        self.assertTrue(f"{HTTPStatus.INTERNAL_SERVER_ERROR} Server Error: Internal Server Error"
                        in str(exception_ctx.exception))

    @responses.activate
    def test_auth_basic_get_process_instance_success(self):
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
        actual_resp_payload = self.client.get_process_instance(process_key=self.process_key,
                                                               variables=["intVar_eq_1", "strVar_eq_hello"],
                                                               tenant_ids=[self.tenant_id])
        self.assertListEqual(resp_payload, actual_resp_payload)

    @responses.activate
    def test_auth_basic_get_process_instance_bad_request_raises_exception(self):
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
            self.client.get_process_instance(process_key=self.process_key,
                                             variables=["intVar_XXX_1", "strVar_eq_hello"],
                                             tenant_ids=[self.tenant_id])

        self.assertEqual(f"received 400 : InvalidRequestException : {expected_message}", str(exception_ctx.exception))

    @responses.activate
    def test_auth_basic_get_process_instance_server_error_raises_exception(self):
        get_process_instance_url = f"{ENGINE_LOCAL_BASE_URL}/process-instance" \
                                   f"?processDefinitionKey={self.process_key}" \
                                   f"&tenantIdIn={self.tenant_id}" \
                                   f"&variables=intVar_XXX_1,strVar_eq_hello"
        responses.add(responses.GET, get_process_instance_url, status=HTTPStatus.INTERNAL_SERVER_ERROR)
        with self.assertRaises(Exception) as exception_ctx:
            self.client.get_process_instance(process_key=self.process_key,
                                             variables=["intVar_XXX_1", "strVar_eq_hello"],
                                             tenant_ids=[self.tenant_id])

        self.assertTrue(f"{HTTPStatus.INTERNAL_SERVER_ERROR} Server Error: Internal Server Error"
                        in str(exception_ctx.exception))

    @patch('requests.post')
    def test_auth_basic_correlate_message_with_only_message_name(self, mock_post):
        expected_request_payload = {
            "messageName": "CANCEL_MESSAGE",
            "withoutTenantId": True,
            "resultEnabled": True
        }

        self.client.correlate_message("CANCEL_MESSAGE")
        mock_post.assert_called_with(ENGINE_LOCAL_BASE_URL + "/message",
                                     json=expected_request_payload,
                                     headers={'Content-Type': 'application/json',
                                              'Authorization': 'Basic ZGVtbzpkZW1v'})

    @patch('requests.post')
    def test_auth_basic_correlate_message_with_business_key(self, mock_post):
        expected_request_payload = {
            "messageName": "CANCEL_MESSAGE",
            "withoutTenantId": True,
            "businessKey": "123456",
            "resultEnabled": True
        }

        self.client.correlate_message("CANCEL_MESSAGE", business_key="123456")
        mock_post.assert_called_with(ENGINE_LOCAL_BASE_URL + "/message",
                                     json=expected_request_payload,
                                     headers={'Content-Type': 'application/json',
                                              'Authorization': 'Basic ZGVtbzpkZW1v'})

    @patch('requests.post')
    def test_auth_basic_correlate_message_with_tenant_id(self, mock_post):
        expected_request_payload = {
            "messageName": "CANCEL_MESSAGE",
            "withoutTenantId": False,
            "tenantId": "123456",
            "resultEnabled": True
        }

        self.client.correlate_message("CANCEL_MESSAGE", tenant_id="123456")
        mock_post.assert_called_with(ENGINE_LOCAL_BASE_URL + "/message",
                                     json=expected_request_payload,
                                     headers={'Content-Type': 'application/json',
                                              'Authorization': 'Basic ZGVtbzpkZW1v'})

    @responses.activate
    def test_auth_basic_correlate_message_invalid_message_name_raises_exception(self):
        expected_message = "org.camunda.bpm.engine.MismatchingMessageCorrelationException: " \
                           "Cannot correlate message 'XXX': No process definition or execution matches the parameters"
        resp_payload = {
            "type": "RestException",
            "message": expected_message
        }
        correlate_msg_url = f"{ENGINE_LOCAL_BASE_URL}/message"
        responses.add(responses.POST, correlate_msg_url, status=HTTPStatus.BAD_REQUEST, json=resp_payload)
        with self.assertRaises(Exception) as exception_ctx:
            self.client.correlate_message(message_name="XXX")

        self.assertEqual(f"received 400 : RestException : {expected_message}", str(exception_ctx.exception))

    @responses.activate
    def test_auth_basic_get_process_instance_variable_without_meta(self):
        process_instance_id = "c2c68785-9f42-11ea-a841-0242ac1c0004"
        variable_name = "var1"
        process_instance_var_url = \
            f"{ENGINE_LOCAL_BASE_URL}/process-instance/{process_instance_id}/variables/{variable_name}"
        resp_frame_payload = {"value": None, "valueInfo": {}, "type": ""}
        resp_data_payload = base64.decodebytes(b"hellocamunda")
        process_instance_var_data_url = f"{process_instance_var_url}/data"

        responses.add(responses.GET, process_instance_var_url, status=HTTPStatus.OK, json=resp_frame_payload)
        responses.add(responses.GET, process_instance_var_data_url, status=HTTPStatus.OK, body=resp_data_payload)

        resp = self.client.get_process_instance_variable(process_instance_id, variable_name)
        self.assertEqual("hellocamunda\n", resp)

    @responses.activate
    def test_auth_basic_get_process_instance_variable_with_meta(self):
        process_instance_id = "c2c68785-9f42-11ea-a841-0242ac1c0004"
        variable_name = "var1"
        process_instance_var_url = \
            f"{ENGINE_LOCAL_BASE_URL}/process-instance/{process_instance_id}/variables/{variable_name}"
        resp_frame_payload = {"value": None, "valueInfo": {}, "type": ""}
        resp_data_payload = base64.decodebytes(b"hellocamunda")
        process_instance_var_data_url = f"{process_instance_var_url}/data"

        responses.add(responses.GET, process_instance_var_url, status=HTTPStatus.OK, json=resp_frame_payload)
        responses.add(responses.GET, process_instance_var_data_url, status=HTTPStatus.OK, body=resp_data_payload)

        resp = self.client.get_process_instance_variable(process_instance_id, variable_name, True)
        self.assertEqual({"value": "hellocamunda\n", "valueInfo": {}, "type": ""}, resp)
