import logging
import glob
from os.path import basename, splitext
from http import HTTPStatus

import requests

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
        if response.status_code == HTTPStatus.OK:
            return response.json()
        elif response.status_code == HTTPStatus.NOT_FOUND or response.status_code == HTTPStatus.BAD_REQUEST:
            raise Exception(response.json()["message"])
        else:
            response.raise_for_status()

    def get_process_instance(self, process_key=None, variables=frozenset([]), tenant_ids=frozenset([])):
        url = f"{self.engine_base_url}/process-instance"
        url_params = self.__get_process_instance_url_params(process_key, tenant_ids, variables)
        response = requests.get(url, headers=self._get_headers(), params=url_params)
        if response.status_code == HTTPStatus.OK:
            return response.json()
        elif response.status_code == HTTPStatus.BAD_REQUEST:
            raise Exception(response.json()["message"])
        else:
            response.raise_for_status()

    def upload_definition(self, path):
        if "*" in path:
            paths = glob.glob(path)
        else:
            paths = [path]

        for p in paths:
            base_name = basename(p)
            no_ext, _ = splitext(base_name)
            files =  {
                base_name: (base_name, open(p, 'rb'), "text/xml")
            }
            body = {'deployment-name': no_ext, 'deployment-source': 'bconf', 'deploy-changed-only': 'true'}
            response = requests.post(f"{self.engine_base_url}/deployment/create", files=files, data=body)
            if response.status_code == HTTPStatus.BAD_REQUEST:
                raise Exception(response.json()["message"])
            elif response.status_code != HTTPStatus.OK:
                response.raise_for_status()

    def send_message(self, message_name, correlation_keys={}, process_variables={}):
        body  = {
            "messageName": message_name,
            "correlationKeys": correlation_keys,
            "processVariables": process_variables 
        }
        response = requests.post(f"{self.engine_base_url}/message", json=body)
        if response.status_code == HTTPStatus.OK:
            return response.json()
        elif response.status_code == HTTPStatus.BAD_REQUEST:
            raise Exception(response.json()["message"])
        else:
            response.raise_for_status()

    def stop_processes(self, process_ids):
        params = dict(cascade=True, skipCustomListeners=True, skipIoMappings=True)
        process_instances_url = f"{self.engine_base_url}/process-instance"
        if not process_ids:
            r = requests.get(process_instances_url)
            processes = [elem['id'] for elem in r.json()]
        for process_id in processes:
            response = requests.delete(f"{process_instances_url}/{process_id}", params=params)
            if response.status_code == HTTPStatus.BAD_REQUEST:
                raise Exception(response.json()["message"])
            elif response.status_code != HTTPStatus.OK:
                response.raise_for_status()

    def __get_process_instance_url_params(self, process_key, tenant_ids, variables):
        url_params = {}
        if process_key:
            url_params["processDefinitionKey"] = process_key
        var_filter = self.join(variables, ',')
        if var_filter:
            url_params["variables"] = var_filter
        tenant_ids_filter = self.join(tenant_ids, ',')
        if tenant_ids_filter:
            url_params["tenantIdIn"] = tenant_ids_filter
        return url_params

    def _get_headers(self):
        return {
            "Content-Type": "application/json"
        }

    def join(self, list_of_values, separator):
        return separator.join(str(v) for v in list_of_values)
