import logging

import requests

from camunda.client.engine_client import EngineClient, ENGINE_LOCAL_BASE_URL
from camunda.utils.response_utils import raise_exception_if_not_ok
from camunda.utils.utils import join
from camunda.variables.variables import Variables

logger = logging.getLogger(__name__)


class ProcessDefinitionClient(EngineClient):

    def __init__(self, engine_base_url=ENGINE_LOCAL_BASE_URL):
        super().__init__(engine_base_url)

    def get_process_definitions(self, process_key, version_tag, tenant_ids):
        url = self.get_process_definitions_url()
        url_params = self.__get_process_definitions_url_params(process_key, version_tag, tenant_ids)
        response = requests.get(url, headers=self._get_headers(), params=url_params)
        raise_exception_if_not_ok(response)
        return response.json()

    def get_process_definitions_url(self):
        return f"{self.engine_base_url}/process-definition"

    def __get_process_definitions_url_params(self, process_key, version_tag=None, tenant_ids=None):
        url_params = {}
        if process_key:
            url_params["key"] = process_key

        if version_tag:
            url_params["versionTag"] = version_tag

        tenant_ids_filter = join(tenant_ids, ',')
        if tenant_ids_filter:
            url_params["tenantIdIn"] = tenant_ids_filter
        return url_params

    def start_process_by_version(self, process_key, version_tag, variables, tenant_id=None, business_key=None):
        """
        Start a process instance with the process_key and specified version tag and variables passed.
        :param process_key: Mandatory
        :param version_tag:
        :param variables: Mandatory - can be empty dict
        :param tenant_id: Optional
        :param business_key: Optional
        :return: response json
        """
        tenant_ids = [tenant_id] if tenant_id else []
        process_definitions = self.get_process_definitions(process_key, version_tag, tenant_ids)
        if len(process_definitions) > 1:
            raise Exception(f"cannot start process because more than one process definitions found "
                            f"for process_key: {process_key}, "
                            f"version_tag: {version_tag} and "
                            f"tenant_ids: {tenant_ids}")

        process_definition_id = process_definitions[0]['id']
        url = self.get_start_process_url(process_definition_id)
        body = {
            "variables": Variables.format(variables)
        }
        if business_key:
            body["businessKey"] = business_key

        response = requests.post(url, headers=self._get_headers(), json=body)
        raise_exception_if_not_ok(response)
        return response.json()

    def get_start_process_url(self, process_definition_id):
        return f"{self.engine_base_url}/process-definition/{process_definition_id}/start"
