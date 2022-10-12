import logging
from http import HTTPStatus

import requests

from camunda.client.engine_client import ENGINE_LOCAL_BASE_URL
from camunda.utils.log_utils import log_with_context
from camunda.utils.response_utils import raise_exception_if_not_ok
from camunda.utils.utils import str_to_list
from camunda.utils.auth_basic import AuthBasic, obfuscate_password
from camunda.variables.variables import Variables

logger = logging.getLogger(__name__)


class ExternalTaskClient:
    default_config = {
        "maxTasks": 1,
        "lockDuration": 300000,  # in milliseconds
        "asyncResponseTimeout": 30000,
        "retries": 3,
        "retryTimeout": 300000,
        "httpTimeoutMillis": 30000,
        "timeoutDeltaMillis": 5000,
        "includeExtensionProperties": True  # enables Camunda Extension Properties
    }

    def __init__(self, worker_id, engine_base_url=ENGINE_LOCAL_BASE_URL, config=None):
        config = config if config is not None else {}
        self.worker_id = worker_id
        self.external_task_base_url = engine_base_url + "/external-task"
        self.config = type(self).default_config.copy()
        self.config.update(config)
        self.is_debug = config.get('isDebug', False)
        self.http_timeout_seconds = self.config.get('httpTimeoutMillis') / 1000
        self._log_with_context(f"Created External Task client with config: {obfuscate_password(self.config)}")

    def get_fetch_and_lock_url(self):
        return f"{self.external_task_base_url}/fetchAndLock"

    def fetch_and_lock(self, topic_names, process_variables=None):
        url = self.get_fetch_and_lock_url()
        body = {
            "workerId": str(self.worker_id),  # convert to string to make it JSON serializable
            "maxTasks": self.config["maxTasks"],
            "topics": self._get_topics(topic_names, process_variables),
            "asyncResponseTimeout": self.config["asyncResponseTimeout"]
        }

        if self.is_debug:
            self._log_with_context(f"trying to fetch and lock with request payload: {body}")
        http_timeout_seconds = self.__get_fetch_and_lock_http_timeout_seconds()
        response = requests.post(url, headers=self._get_headers(), json=body, timeout=http_timeout_seconds)
        raise_exception_if_not_ok(response)

        resp_json = response.json()
        if self.is_debug:
            self._log_with_context(f"fetch and lock response json: {resp_json} for request: {body}")
        return response.json()

    def __get_fetch_and_lock_http_timeout_seconds(self):
        # use HTTP timeout slightly more than async Response / long polling timeout
        return (self.config["timeoutDeltaMillis"] + self.config["asyncResponseTimeout"]) / 1000

    def _get_topics(self, topic_names, process_variables):
        topics = []
        for topic in str_to_list(topic_names):
            topics.append({
                "topicName": topic,
                "lockDuration": self.config["lockDuration"],
                "processVariables": process_variables if process_variables else {},
                # enables Camunda Extension Properties
                "includeExtensionProperties": self.config.get("includeExtensionProperties") or False

            })
        return topics

    def complete(self, task_id, global_variables, local_variables=None):
        url = self.get_task_complete_url(task_id)

        body = {
            "workerId": self.worker_id,
            "variables": Variables.format(global_variables),
            "localVariables": Variables.format(local_variables)
        }

        response = requests.post(url, headers=self._get_headers(), json=body, timeout=self.http_timeout_seconds)
        raise_exception_if_not_ok(response)
        return response.status_code == HTTPStatus.NO_CONTENT

    def get_task_complete_url(self, task_id):
        return f"{self.external_task_base_url}/{task_id}/complete"

    def failure(self, task_id, error_message, error_details, retries, retry_timeout):
        url = self.get_task_failure_url(task_id)
        logger.info(f"setting retries to: {retries} for task: {task_id}")
        body = {
            "workerId": self.worker_id,
            "errorMessage": error_message,
            "retries": retries,
            "retryTimeout": retry_timeout,
        }
        if error_details:
            body["errorDetails"] = error_details

        response = requests.post(url, headers=self._get_headers(), json=body, timeout=self.http_timeout_seconds)
        raise_exception_if_not_ok(response)
        return response.status_code == HTTPStatus.NO_CONTENT

    def get_task_failure_url(self, task_id):
        return f"{self.external_task_base_url}/{task_id}/failure"

    def bpmn_failure(self, task_id, error_code, error_message, variables=None):
        url = self.get_task_bpmn_error_url(task_id)

        body = {
            "workerId": self.worker_id,
            "errorCode": error_code,
            "errorMessage": error_message,
            "variables": Variables.format(variables),
        }

        if self.is_debug:
            self._log_with_context(f"trying to report bpmn error with request payload: {body}")

        resp = requests.post(url, headers=self._get_headers(), json=body, timeout=self.http_timeout_seconds)
        resp.raise_for_status()
        return resp.status_code == HTTPStatus.NO_CONTENT

    def get_task_bpmn_error_url(self, task_id):
        return f"{self.external_task_base_url}/{task_id}/bpmnError"

    @property
    def auth_basic(self) -> dict:
        if not self.config.get("auth_basic") or not isinstance(self.config.get("auth_basic"), dict):
            return {}
        token = AuthBasic(**self.config.get("auth_basic").copy()).token
        return {"Authorization": token}

    def _get_headers(self):
        headers = {
            "Content-Type": "application/json"
        }
        if self.auth_basic:
            headers.update(self.auth_basic)
        return headers

    def _log_with_context(self, msg, log_level='info', **kwargs):
        context = {"WORKER_ID": self.worker_id}
        log_with_context(msg, context=context, log_level=log_level, **kwargs)
