import logging
from http import HTTPStatus

from aiohttp_requests import requests as req

from camunda.variables.variables import Variables

logger = logging.getLogger(__name__)

ENGINE_LOCAL_BASE_URL = "http://localhost:8080/engine-rest"


class EngineClient:

    def __init__(self, engine_base_url=ENGINE_LOCAL_BASE_URL):
        self.engine_base_url = engine_base_url

    def get_start_process_instance_url(self, process_key):
        return f"{self.engine_base_url}/process-definition/key/{process_key}/start"

    async def start_process(self, process_key, variables):
        url = self.get_start_process_instance_url(process_key)
        body = {
            "variables": Variables.format(variables)
        }

        response = await req.post(url, headers=self._get_headers(), json=body)
        resp_json = await response.json()
        if response.status == HTTPStatus.OK:
            return resp_json
        elif response.status == HTTPStatus.NOT_FOUND or response.status == HTTPStatus.BAD_REQUEST:
            raise Exception(resp_json["message"])
        else:
            response.raise_for_status()

    def _get_headers(self):
        return {
            "Content-Type": "application/json"
        }
