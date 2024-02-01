import base64
from http import HTTPStatus
from unittest import IsolatedAsyncioTestCase

from aioresponses import aioresponses

from camunda.client.engine_client import EngineClient, ENGINE_LOCAL_BASE_URL


class EngineClientAuthTest(IsolatedAsyncioTestCase):
    tenant_id = "6172cdf0-7b32-4460-9da0-ded5107aa977"
    process_key = "PARALLEL_STEPS_EXAMPLE"

    def setUp(self):
        token = ('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkZW1vIn0'
                 '.NbMsjy8QQ5nrjGTXqdTrJ6g0dqawRvZAqp4XvNt437M')
        self.client = EngineClient(
            config={"auth_bearer": {"access_token": token}})

    @aioresponses()
    async def test_auth_basic_start_process_success(self, mocked: aioresponses):
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
        mocked.post(self.client.get_start_process_instance_url(self.process_key, self.tenant_id), payload=resp_payload, status=HTTPStatus.OK)
        actual_resp_payload = await self.client.start_process(self.process_key, {}, self.tenant_id, "123456")
        self.assertDictEqual(resp_payload, actual_resp_payload)

    @aioresponses()
    async def test_auth_basic_start_process_not_found_raises_exception(self, mocked: aioresponses):
        resp_payload = {
            "type": "RestException",
            "message": "No matching process definition with key: PROCESS_KEY_NOT_EXISTS and tenant-id: tenant_123"
        }
        mocked.post(self.client.get_start_process_instance_url("PROCESS_KEY_NOT_EXISTS", self.tenant_id), status=HTTPStatus.NOT_FOUND, payload=resp_payload)
        with self.assertRaises(Exception) as exception_ctx:
            await self.client.start_process("PROCESS_KEY_NOT_EXISTS", {}, self.tenant_id)

        self.assertEqual("received 404 : RestException : "
                         "No matching process definition with key: PROCESS_KEY_NOT_EXISTS and tenant-id: tenant_123",
                         str(exception_ctx.exception))

    @aioresponses()
    async def test_auth_basic_start_process_bad_request_raises_exception(self, mocked: aioresponses):
        client = EngineClient()
        expected_message = "Cannot instantiate process definition " \
                           "PARALLEL_STEPS_EXAMPLE:1:9b72da83-9b91-11ea-bad9-0242ac110002: " \
                           "Cannot convert value '1aa2345' of type 'Integer' to java type java.lang.Integer"
        resp_payload = {
            "type": "InvalidRequestException",
            "message": expected_message
        }
        mocked.post(client.get_start_process_instance_url(self.process_key, self.tenant_id), status=HTTPStatus.BAD_REQUEST, payload=resp_payload)
        with self.assertRaises(Exception) as exception_ctx:
            await client.start_process(self.process_key, {"int_var": "1aa2345"}, self.tenant_id)

        self.assertEqual(f"received 400 : InvalidRequestException : {expected_message}", str(exception_ctx.exception))

    @aioresponses()
    async def test_auth_basic_start_process_server_error_raises_exception(self, mocked: aioresponses):
        mocked.post(self.client.get_start_process_instance_url(self.process_key, self.tenant_id), status=HTTPStatus.INTERNAL_SERVER_ERROR)
        with self.assertRaises(Exception) as exception_ctx:
            await self.client.start_process(self.process_key, {"int_var": "1aa2345"}, self.tenant_id)

        self.assertEqual("received 500", str(exception_ctx.exception))
        # self.assertTrue("HTTPStatus.INTERNAL_SERVER_ERROR Server Error: Internal Server Error"
        #                 in str(exception_ctx.exception))

    @aioresponses()
    async def test_auth_basic_get_process_instance_success(self, mocked: aioresponses):
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
        mocked.get(get_process_instance_url, status=HTTPStatus.OK, payload=resp_payload)
        actual_resp_payload = await self.client.get_process_instance(process_key=self.process_key,
                                                                     variables=["intVar_eq_1", "strVar_eq_hello"],
                                                                     tenant_ids=[self.tenant_id])
        self.assertListEqual(resp_payload, actual_resp_payload)

    @aioresponses()
    async def test_auth_basic_get_process_instance_bad_request_raises_exception(self, mocked: aioresponses):
        expected_message = "Invalid variable comparator specified: XXX"
        resp_payload = {
            "type": "InvalidRequestException",
            "message": expected_message
        }
        get_process_instance_url = f"{ENGINE_LOCAL_BASE_URL}/process-instance" \
                                   f"?processDefinitionKey={self.process_key}" \
                                   f"&tenantIdIn={self.tenant_id}" \
                                   f"&variables=intVar_XXX_1,strVar_eq_hello"
        mocked.get(get_process_instance_url, status=HTTPStatus.BAD_REQUEST, payload=resp_payload)
        with self.assertRaises(Exception) as exception_ctx:
            await self.client.get_process_instance(process_key=self.process_key,
                                                   variables=["intVar_XXX_1", "strVar_eq_hello"],
                                                   tenant_ids=[self.tenant_id])

        self.assertEqual(f"received 400 : InvalidRequestException : {expected_message}", str(exception_ctx.exception))

    @aioresponses()
    async def test_auth_basic_get_process_instance_server_error_raises_exception(self, mocked: aioresponses):
        get_process_instance_url = f"{ENGINE_LOCAL_BASE_URL}/process-instance" \
                                   f"?processDefinitionKey={self.process_key}" \
                                   f"&tenantIdIn={self.tenant_id}" \
                                   f"&variables=intVar_XXX_1,strVar_eq_hello"
        mocked.get(get_process_instance_url, status=HTTPStatus.INTERNAL_SERVER_ERROR)
        with self.assertRaises(Exception) as exception_ctx:
            await self.client.get_process_instance(process_key=self.process_key,
                                                   variables=["intVar_XXX_1", "strVar_eq_hello"],
                                                   tenant_ids=[self.tenant_id])

        self.assertEqual("received 500", str(exception_ctx.exception))
        # self.assertTrue("HTTPStatus.INTERNAL_SERVER_ERROR Server Error: Internal Server Error"
        #                 in str(exception_ctx.exception))

    @aioresponses()
    async def test_auth_basic_correlate_message_with_only_message_name(self, mocked: aioresponses):
        expected_request_payload = {
            "messageName": "CANCEL_MESSAGE",
            "withoutTenantId": True,
            "resultEnabled": True
        }
        mocked.post(ENGINE_LOCAL_BASE_URL + "/message", status=HTTPStatus.OK)
        token = ('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkZW1vIn0'
                 '.NbMsjy8QQ5nrjGTXqdTrJ6g0dqawRvZAqp4XvNt437M')
        await self.client.correlate_message("CANCEL_MESSAGE")
        mocked.assert_called_with(ENGINE_LOCAL_BASE_URL + "/message",
                                  method='POST',
                                  json=expected_request_payload,
                                  headers={'Content-Type': 'application/json',
                                           'Authorization': f'Bearer {token}'})

    @aioresponses()
    async def test_auth_basic_correlate_message_with_business_key(self, mocked: aioresponses):
        expected_request_payload = {
            "messageName": "CANCEL_MESSAGE",
            "withoutTenantId": True,
            "businessKey": "123456",
            "resultEnabled": True
        }
        mocked.post(ENGINE_LOCAL_BASE_URL + "/message", status=HTTPStatus.OK)
        token = ('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkZW1vIn0'
                 '.NbMsjy8QQ5nrjGTXqdTrJ6g0dqawRvZAqp4XvNt437M')
        await self.client.correlate_message("CANCEL_MESSAGE", business_key="123456")
        mocked.assert_called_with(ENGINE_LOCAL_BASE_URL + "/message",
                                  method='POST',
                                  json=expected_request_payload,
                                  headers={'Content-Type': 'application/json',
                                           'Authorization': f'Bearer {token}'})

    @aioresponses()
    async def test_auth_basic_correlate_message_with_tenant_id(self, mocked: aioresponses):
        expected_request_payload = {
            "messageName": "CANCEL_MESSAGE",
            "withoutTenantId": False,
            "tenantId": "123456",
            "resultEnabled": True
        }
        mocked.post(ENGINE_LOCAL_BASE_URL + "/message", status=HTTPStatus.OK)
        token = ('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkZW1vIn0'
                 '.NbMsjy8QQ5nrjGTXqdTrJ6g0dqawRvZAqp4XvNt437M')
        await self.client.correlate_message("CANCEL_MESSAGE", tenant_id="123456")
        mocked.assert_called_with(ENGINE_LOCAL_BASE_URL + "/message",
                                  method='POST',
                                  json=expected_request_payload,
                                  headers={'Content-Type': 'application/json',
                                           'Authorization': f'Bearer {token}'})

    @aioresponses()
    async def test_auth_basic_correlate_message_invalid_message_name_raises_exception(self, mocked: aioresponses):
        expected_message = "org.camunda.bpm.engine.MismatchingMessageCorrelationException: " \
                           "Cannot correlate message 'XXX': No process definition or execution matches the parameters"
        resp_payload = {
            "type": "RestException",
            "message": expected_message
        }
        correlate_msg_url = f"{ENGINE_LOCAL_BASE_URL}/message"
        mocked.post(correlate_msg_url, status=HTTPStatus.BAD_REQUEST, payload=resp_payload)
        with self.assertRaises(Exception) as exception_ctx:
            await self.client.correlate_message(message_name="XXX")

        self.assertEqual(f"received 400 : RestException : {expected_message}", str(exception_ctx.exception))

    @aioresponses()
    async def test_auth_basic_get_process_instance_variable_without_meta(self, mocked: aioresponses):
        process_instance_id = "c2c68785-9f42-11ea-a841-0242ac1c0004"
        variable_name = "var1"
        process_instance_var_url = f"{ENGINE_LOCAL_BASE_URL}/process-instance/{process_instance_id}/variables/{variable_name}"
        resp_frame_payload = {"value": None, "valueInfo": {}, "type": ""}

        process_instance_var_data_url = f"{process_instance_var_url}/data"
        resp_data_payload = base64.decodebytes(b"hellocamunda")

        mocked.get(process_instance_var_url, status=HTTPStatus.OK, payload=resp_frame_payload)
        mocked.get(process_instance_var_data_url, status=HTTPStatus.OK, body=resp_data_payload)

        resp = await self.client.get_process_instance_variable(process_instance_id, variable_name)
        self.assertEqual("hellocamunda\n", resp)

    @aioresponses()
    async def test_auth_basic_get_process_instance_variable_with_meta(self, mocked: aioresponses):
        process_instance_id = "c2c68785-9f42-11ea-a841-0242ac1c0004"
        variable_name = "var1"
        process_instance_var_url = f"{ENGINE_LOCAL_BASE_URL}/process-instance/{process_instance_id}/variables/{variable_name}"
        resp_frame_payload = {"value": None, "valueInfo": {}, "type": ""}

        process_instance_var_data_url = f"{process_instance_var_url}/data"
        resp_data_payload = base64.decodebytes(b"hellocamunda")

        mocked.get(process_instance_var_url, status=HTTPStatus.OK, payload=resp_frame_payload)
        mocked.get(process_instance_var_data_url, status=HTTPStatus.OK, body=resp_data_payload)

        resp = await self.client.get_process_instance_variable(process_instance_id, variable_name, True)
        self.assertEqual({"value": "hellocamunda\n", "valueInfo": {}, "type": ""}, resp)
