import logging
import glob
from os.path import basename, splitext
from http import HTTPStatus

from camunda.utils.response_utils import raise_exception_if_not_ok
from camunda.utils.utils import join
from camunda.variables.variables import Variables

logger = logging.getLogger(__name__)

ENGINE_LOCAL_BASE_URL = "http://localhost:8080/engine-rest"


class EngineClient:
    def __init__(self, session, engine_base_url=ENGINE_LOCAL_BASE_URL):
        self.engine_base_url = engine_base_url
        self.session = session

    def get_start_process_instance_url(self, process_key, tenant_id=None):
        if tenant_id:
            return f"{self.engine_base_url}/process-definition/key/{process_key}/tenant-id/{tenant_id}/start"
        return f"{self.engine_base_url}/process-definition/key/{process_key}/start"

    async def start_process(self, process_key, variables, tenant_id=None):
        url = self.get_start_process_instance_url(process_key, tenant_id)
        body = {"variables": Variables.format(variables)}
        async with self.session.post(url, headers=self._get_headers(), json=body) as request:
            await raise_exception_if_not_ok(response)
            return response.json()

    async def get_process_instance(
        self, process_key=None, variables=frozenset([]), tenant_ids=frozenset([])
    ):
        url = f"{self.engine_base_url}/process-instance"
        url_params = self.__get_process_instance_url_params(process_key, tenant_ids, variables)
        async with self.session.get(
            url, headers=self._get_headers(), params=url_params
        ) as response:
            await raise_exception_if_not_ok(response)
            return await response.json()

    async def upload_definition(self, path):
        if "*" in path:
            paths = glob.glob(path)
        else:
            paths = [path]

        for p in paths:
            base_name = basename(p)
            no_ext, _ = splitext(base_name)
            files = {base_name: (base_name, open(p, "rb"), "text/xml")}
            body = {
                "deployment-name": no_ext,
                "deployment-source": "bconf",
                "deploy-changed-only": "true",
            }
            async with self.session.post(
                f"{self.engine_base_url}/deployment/create", files=files, data=body
            ) as response:
                if response.status == HTTPStatus.BAD_REQUEST:
                    raise Exception(await response.json()["message"])
                elif response.status != HTTPStatus.OK:
                    response.raise_for_status()

    async def send_message(self, message_name, correlation_keys={}, process_variables={}):
        body = {
            "messageName": message_name,
            "correlationKeys": correlation_keys,
            "processVariables": process_variables,
        }
        async with self.session.post(f"{self.engine_base_url}/message", json=body) as response:
            if response.status == HTTPStatus.OK:
                return await response.json()
            elif response.status == HTTPStatus.BAD_REQUEST:
                raise Exception(await response.json()["message"])
            else:
                response.raise_for_status()

    async def stop_processes(self, process_ids):
        params = dict(cascade="true", skipCustomListeners="true", skipIoMappings="true")
        process_instances_url = f"{self.engine_base_url}/process-instance"
        if not process_ids:
            async with self.session.get(process_instances_url) as response:
                process_ids = [elem["id"] for elem in await response.json()]
        for process_id in process_ids:
            async with self.session.delete(
                f"{process_instances_url}/{process_id}", params=params
            ) as response:
                if response.status == HTTPStatus.BAD_REQUEST:
                    raise Exception(await response.json()["message"])
                elif response.status != HTTPStatus.OK:
                    response.raise_for_status()

    def __get_process_instance_url_params(self, process_key, tenant_ids, variables):
        url_params = {}
        if process_key:
            url_params["processDefinitionKey"] = process_key
        var_filter = join(variables, ",")
        if var_filter:
            url_params["variables"] = var_filter
        tenant_ids_filter = join(tenant_ids, ",")
        if tenant_ids_filter:
            url_params["tenantIdIn"] = tenant_ids_filter
        return url_params

    def _get_headers(self):
        return {"Content-Type": "application/json"}
