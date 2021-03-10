from http import HTTPStatus
from unittest import TestCase

import responses

from camunda.process_definition.process_definition_client import ProcessDefinitionClient


class ProcessDefinitionClientTest(TestCase):

    def setUp(self):
        self.process_client = ProcessDefinitionClient()

    def test_get_process_definitions_url_params_uses_non_none_params(self):
        url_params = self.process_client.get_process_definitions_url_params(
            process_key="PROCESS_KEY",
            version_tag=None,
            tenant_ids=None,
            sort_by="version",
            sort_order="desc"
        )
        self.assertDictEqual({
            "key": 'PROCESS_KEY',
            "sortBy": "version",
            "sortOrder": "desc",
            "firstResult": 0,
            "maxResults": 1,
        }, url_params)

    def test_get_process_definitions_url_params_uses_all_specified_params(self):
        url_params = self.process_client.get_process_definitions_url_params(
            process_key="PROCESS_KEY",
            version_tag='1.2.3',
            tenant_ids=['tenant1'],
            sort_by="version",
            sort_order="asc"
        )
        self.assertDictEqual({
            "key": "PROCESS_KEY",
            "versionTagLike": "1.2.3%",
            "tenantIdIn": "tenant1",
            "sortBy": "version",
            "sortOrder": "asc",
            "firstResult": 0,
            "maxResults": 1,
        }, url_params)

    @responses.activate
    def test_start_process_by_version_raises_exception_if_no_process_definitions_found(self):
        get_process_definitions_resp = []
        responses.add(responses.GET, self.process_client.get_process_definitions_url(),
                      status=HTTPStatus.OK, json=get_process_definitions_resp)

        with self.assertRaises(Exception) as context:
            self.process_client.start_process_by_version("ORIGINATION", "3.8.3", {}, "tenant1")

        self.assertEqual(f"cannot start process because no process definitions found "
                         f"for process_key: ORIGINATION, version_tag: 3.8.3 and tenant_id: tenant1",
                         str(context.exception))

    @responses.activate
    def test_start_process_by_version_uses_first_process_definition_id_if_more_than_one_found(self):
        get_process_definitions_resp = [
            {
                "id": "process_definition_id_2",
                "key": "ORIGINATION",
                "category": "http://bpmn.io/schema/bpmn",
                "description": "- Removed unwanted external tasks",
                "name": "Origination",
                "version": 37,
                "resource": "bpmn/origination.bpmn",
                "deploymentId": "05f7527e-737e-11eb-ac5c-0a58a9feac2a",
                "diagram": None,
                "suspended": False,
                "tenantId": "tenant1",
                "versionTag": "3.8.3",
                "historyTimeToLive": None,
                "startableInTasklist": True
            },
            {
                "id": "process_definition_id_1",
                "key": "ORIGINATION",
                "category": "http://bpmn.io/schema/bpmn",
                "description": "- Added new external tasks",
                "name": "Origination",
                "version": 36,
                "resource": "bpmn/origination.bpmn",
                "deploymentId": "035d3c55-5f01-11eb-bcaf-0a58a9feac2a",
                "diagram": None,
                "suspended": False,
                "tenantId": "tenant1",
                "versionTag": "3.8.3",
                "historyTimeToLive": None,
                "startableInTasklist": True
            },
        ]
        responses.add(responses.GET, self.process_client.get_process_definitions_url(),
                      status=HTTPStatus.OK, json=get_process_definitions_resp)

        start_process_resp = {
            "links": [
                {
                    "method": "GET",
                    "href": "http://localhost:8080/engine-rest/process-instance/e07b461a-80d0-11eb-83ea-0a58a9feac2a",
                    "rel": "self"
                }
            ],
            "id": "e07b461a-80d0-11eb-83ea-0a58a9feac2a",
            "definitionId": "process_definition_id_2",
            "businessKey": "businessKey",
            "caseInstanceId": None,
            "ended": False,
            "suspended": False,
            "tenantId": "tenant1",
            "variables": {
                "applicationId": {
                    "type": "String",
                    "value": "30ea7b40-283b-4526-8979-371f4ffc9ee0",
                    "valueInfo": {}
                }
            }
        }
        responses.add(responses.POST, self.process_client.get_start_process_url('process_definition_id_2'),
                      status=HTTPStatus.OK, json=start_process_resp)

        resp_json = self.process_client.start_process_by_version("ORIGINATION", "3.8.3", {}, "tenant1")

        self.assertDictEqual(start_process_resp, resp_json)

    @responses.activate
    def test_start_process_by_version_returns_process_details_if_started_successfully(self):
        get_process_definitions_resp = [
            {
                "id": "process_definition_id",
                "key": "ORIGINATION",
                "category": "http://bpmn.io/schema/bpmn",
                "description": "- Removed unwanted external tasks",
                "name": "Origination",
                "version": 37,
                "resource": "bpmn/origination.bpmn",
                "deploymentId": "05f7527e-737e-11eb-ac5c-0a58a9feac2a",
                "diagram": None,
                "suspended": False,
                "tenantId": "tenant1",
                "versionTag": "3.8.3",
                "historyTimeToLive": None,
                "startableInTasklist": True
            }
        ]
        responses.add(responses.GET, self.process_client.get_process_definitions_url(),
                      status=HTTPStatus.OK, json=get_process_definitions_resp)

        start_process_resp = {
            "links": [
                {
                    "method": "GET",
                    "href": "http://localhost:8080/engine-rest/process-instance/e07b461a-80d0-11eb-83ea-0a58a9feac2a",
                    "rel": "self"
                }
            ],
            "id": "e07b461a-80d0-11eb-83ea-0a58a9feac2a",
            "definitionId": "89337817-75c9-11eb-84bb-0a58a9feac2a",
            "businessKey": "businessKey",
            "caseInstanceId": None,
            "ended": False,
            "suspended": False,
            "tenantId": "tenant1",
            "variables": {
                "applicationId": {
                    "type": "String",
                    "value": "30ea7b40-283b-4526-8979-371f4ffc9ee0",
                    "valueInfo": {}
                }
            }
        }
        responses.add(responses.POST, self.process_client.get_start_process_url('process_definition_id'),
                      status=HTTPStatus.OK, json=start_process_resp)

        resp_json = self.process_client.start_process_by_version("ORIGINATION", "3.8.3", {}, "tenant1")

        self.assertDictEqual(start_process_resp, resp_json)
