import logging

import requests

from camunda.utils.response_utils import raise_exception_if_not_ok
from camunda.utils.utils import join
from camunda.variables.variables import Variables

logger = logging.getLogger(__name__)

ENGINE_LOCAL_BASE_URL = "http://localhost:8080/engine-rest"


class EngineClient:

    def __init__(self, engine_base_url=ENGINE_LOCAL_BASE_URL):
        self.engine_base_url = engine_base_url

    def get_start_process_instance_url(self, process_key, tenant_id=None):
        if tenant_id:
            return f"{self.engine_base_url}/process-definition/key/{process_key}/tenant-id/{tenant_id}/start"
        return f"{self.engine_base_url}/process-definition/key/{process_key}/start"

    def start_process(self, process_key, variables, tenant_id=None, business_key=None):
        """
        Start a process instance with the process_key and variables passed.
        :param process_key: Mandatory
        :param variables: Mandatory - can be empty dict
        :param tenant_id: Optional
        :param business_key: Optional
        :return: response json
        """
        url = self.get_start_process_instance_url(process_key, tenant_id)
        body = {
            "variables": Variables.format(variables)
        }
        if business_key:
            body["businessKey"] = business_key

        response = requests.post(url, headers=self._get_headers(), json=body)
        raise_exception_if_not_ok(response)
        return response.json()

    def get_process_instance(self, process_key=None, variables=frozenset([]), tenant_ids=frozenset([])):
        url = f"{self.engine_base_url}/process-instance"
        url_params = self.__get_process_instance_url_params(process_key, tenant_ids, variables)
        response = requests.get(url, headers=self._get_headers(), params=url_params)
        raise_exception_if_not_ok(response)
        return response.json()

    def __get_process_instance_url_params(self, process_key, tenant_ids, variables):
        url_params = {}
        if process_key:
            url_params["processDefinitionKey"] = process_key
        var_filter = join(variables, ',')
        if var_filter:
            url_params["variables"] = var_filter
        tenant_ids_filter = join(tenant_ids, ',')
        if tenant_ids_filter:
            url_params["tenantIdIn"] = tenant_ids_filter
        return url_params

    def _get_headers(self):
        return {
            "Content-Type": "application/json"
        }

    def correlate_message(self, message_name, process_instance_id=None, tenant_id=None, business_key=None,
                          process_variables=None):
        """
        Correlates a message to the process engine to either trigger a message start event or
        an intermediate message catching event.
        :param message_name:
        :param process_instance_id:
        :param tenant_id:
        :param business_key:
        :param process_variables:
        :return: response json
        """
        url = f"{self.engine_base_url}/message"
        body = {
            "messageName": message_name,
            "resultEnabled": True,
            "processVariables": Variables.format(process_variables) if process_variables else None,
            "processInstanceId": process_instance_id,
            "tenantId": tenant_id,
            "withoutTenantId": not tenant_id,
            "businessKey": business_key,
        }

        body = {k: v for k, v in body.items() if v is not None}

        response = requests.post(url, headers=self._get_headers(), json=body)
        raise_exception_if_not_ok(response)
        return response.json()
