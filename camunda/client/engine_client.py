import logging
from http import HTTPStatus

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

    def start_process(self, process_key, variables, tenant_id=None):
        url = self.get_start_process_instance_url(process_key, tenant_id)
        body = {
            "variables": Variables.format(variables)
        }

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
