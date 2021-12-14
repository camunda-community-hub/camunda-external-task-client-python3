import logging

import requests

from camunda.client.engine_client import EngineClient, ENGINE_LOCAL_BASE_URL
from camunda.utils.response_utils import raise_exception_if_not_ok
from camunda.utils.utils import join
from camunda.variables.variables import Variables

logger = logging.getLogger(__name__)


class ProcessDefinitionClient(EngineClient):
    def __init__(self, engine_base_url=ENGINE_LOCAL_BASE_URL, config=None):
        super().__init__(engine_base_url, config=config)

    def get_process_definitions(
        self,
        process_key,
        version_tag,
        tenant_ids,
        sort_by="version",
        sort_order="desc",
        offset=0,
        limit=1,
    ):
        url = self.get_process_definitions_url()
        url_params = self.get_process_definitions_url_params(
            process_key, version_tag, tenant_ids, sort_by, sort_order, offset, limit
        )
        response = requests.get(url, headers=self._get_headers(), params=url_params)
        raise_exception_if_not_ok(response)
        return response.json()

    def get_process_definitions_url(self):
        return f"{self.engine_base_url}/process-definition"

    def get_process_definitions_url_params(
        self,
        process_key,
        version_tag=None,
        tenant_ids=None,
        sort_by="version",
        sort_order="desc",
        offset=0,
        limit=1,
    ):
        """
        offset starts with zero
        sort_order can be "asc" or "desc
        """
        url_params = {
            "key": process_key,
            "versionTagLike": f"{version_tag}%" if version_tag else None,
            "tenantIdIn": join(tenant_ids, ","),
            "sortBy": sort_by,
            "sortOrder": sort_order,
            "firstResult": offset,
            "maxResults": limit,
        }

        url_params = {k: v for k, v in url_params.items() if v is not None and v != ""}

        return url_params

    def start_process_by_version(
        self, process_key, version_tag, variables, tenant_id=None, business_key=None
    ):
        """
        Start a process instance with the process_key and specified version tag and variables passed.
        If multiple versions with same version tag found, it triggers the latest one
        :param process_key: Mandatory
        :param version_tag:
        :param variables: Mandatory - can be empty dict
        :param tenant_id: Optional
        :param business_key: Optional
        :return: response json
        """
        tenant_ids = [tenant_id] if tenant_id else []
        process_definitions = self.get_process_definitions(
            process_key,
            version_tag,
            tenant_ids,
            sort_by="version",
            sort_order="desc",
            offset=0,
            limit=1,
        )

        if len(process_definitions) == 0:
            raise Exception(
                f"cannot start process because no process definitions found "
                f"for process_key: {process_key}, version_tag: {version_tag} and tenant_id: {tenant_id}"
            )

        process_definition_id = process_definitions[0]["id"]
        version = process_definitions[0]["version"]
        if len(process_definitions) > 1:
            logger.info(
                f"multiple process definitions found for process_key: {process_key}, "
                f"version_tag: {version_tag} and tenant_id: {tenant_id}, "
                f"using latest process_definition_id: {process_definition_id} with version: {version}"
            )
        else:
            logger.info(
                f"exactly one process definition found for process_key: {process_key}, "
                f"version_tag: {version_tag} and tenant_id: {tenant_id}, "
                f"using process_definition_id: {process_definition_id} with version: {version}"
            )

        url = self.get_start_process_url(process_definition_id)
        body = {"variables": Variables.format(variables)}
        if business_key:
            body["businessKey"] = business_key

        response = requests.post(url, headers=self._get_headers(), json=body)
        raise_exception_if_not_ok(response)
        return response.json()

    def get_start_process_url(self, process_definition_id):
        return (
            f"{self.engine_base_url}/process-definition/{process_definition_id}/start"
        )
