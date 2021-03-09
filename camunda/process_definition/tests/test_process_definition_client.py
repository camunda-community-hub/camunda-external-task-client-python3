from http import HTTPStatus
from unittest import TestCase

import responses

from camunda.process_definition.process_definition_client import ProcessDefinitionClient


class ProcessDefinitionClientTest(TestCase):

    @responses.activate
    def test_start_process_by_version_raises_exception_if_more_than_one_process_definitions_found(self):
        process_client = ProcessDefinitionClient()
        get_process_definitions_resp = [
            {
                "id": "03e3f888-5f01-11eb-bcaf-0a58a9feac2a",
                "key": "ORIGINATION",
                "category": "http://bpmn.io/schema/bpmn",
                "description": "- Added new external tasks",
                "name": "Origination",
                "version": 33,
                "resource": "bpmn/origination.bpmn",
                "deploymentId": "035d3c55-5f01-11eb-bcaf-0a58a9feac2a",
                "diagram": None,
                "suspended": False,
                "tenantId": "tenant1",
                "versionTag": "3.8.3",
                "historyTimeToLive": None,
                "startableInTasklist": True
            },
            {
                "id": "06073101-737e-11eb-ac5c-0a58a9feac2a",
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
        responses.add(responses.GET, process_client.get_process_definitions_url(),
                      status=HTTPStatus.OK, json=get_process_definitions_resp)

        with self.assertRaises(Exception) as context:
            process_client.start_process_by_version("ORIGINATION", "3.8.3", {}, "tenant1")

        self.assertEqual(f"cannot start process because more than one process definitions found "
                         f"for process_key: ORIGINATION, "
                         f"version_tag: 3.8.3 and "
                         f"tenant_ids: ['tenant1']",
                         str(context.exception))

    @responses.activate
    def test_start_process_by_version_returns_process_details_if_started_successfully(self):
        process_client = ProcessDefinitionClient()
        get_process_definitions_resp = [
            {
                "id": "06073101-737e-11eb-ac5c-0a58a9feac2a",
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
        responses.add(responses.GET, process_client.get_process_definitions_url(),
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
            "businessKey": "30ea7b40-283b-4526-8979-371f4ffc9ee0",
            "caseInstanceId": None,
            "ended": False,
            "suspended": False,
            "tenantId": "2fb74cfc-5f4b-4666-80af-3aa47e954721",
            "variables": {
                "applicationId": {
                    "type": "String",
                    "value": "30ea7b40-283b-4526-8979-371f4ffc9ee0",
                    "valueInfo": {}
                }
            }
        }
        responses.add(responses.POST, process_client.get_start_process_url('06073101-737e-11eb-ac5c-0a58a9feac2a'),
                      status=HTTPStatus.OK, json=start_process_resp)

        resp_json = process_client.start_process_by_version("ORIGINATION", "3.8.3", {}, "tenant1")

        self.assertDictEqual(start_process_resp, resp_json)
