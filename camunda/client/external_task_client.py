import logging
from http import HTTPStatus

import requests
from frozendict import frozendict

from camunda.client.engine_client import ENGINE_LOCAL_BASE_URL
from camunda.utils.utils import str_to_list
from camunda.variables.variables import Variables

logger = logging.getLogger(__name__)


class ExternalTaskClient:
    default_config = {
        "maxTasks": 1,
        "lockDuration": 300000,  # in milliseconds
        "asyncResponseTimeout": 5000,
        "retries": 3,
        "retryTimeout": 300000,
    }

    def __init__(self, worker_id, engine_base_url=ENGINE_LOCAL_BASE_URL, config=frozendict({})):
        self.worker_id = worker_id
        self.external_task_base_url = engine_base_url + "/external-task"
        self.config = type(self).default_config
        self.config.update(config)

    def get_fetch_and_lock_url(self):
        return f"{self.external_task_base_url}/fetchAndLock"

    def fetch_and_lock(self, topic_names):
        url = self.get_fetch_and_lock_url()
        body = {
            "workerId": self.worker_id,
            "maxTasks": self.config["maxTasks"],
            "topics": self._get_topics(topic_names),
            "asyncResponseTimeout": self.config["asyncResponseTimeout"]
        }

        response = requests.post(url, headers=self._get_headers(), json=body)
        response.raise_for_status()
        return response

    def _get_topics(self, topic_names):
        topics = []
        for topic in str_to_list(topic_names):
            topics.append({
                "topicName": topic,
                "lockDuration": self.config["lockDuration"],
            })
        return topics

    def complete(self, task_id, variables):
        url = self.get_task_complete_url(task_id)

        body = {
            "workerId": self.worker_id,
            "variables": Variables.format(variables),
        }

        resp = requests.post(url, headers=self._get_headers(), json=body)
        resp.raise_for_status()
        return resp.status_code == HTTPStatus.NO_CONTENT

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

        resp = requests.post(url, headers=self._get_headers(), json=body)
        resp.raise_for_status()
        return resp.status_code == HTTPStatus.NO_CONTENT

    def get_task_failure_url(self, task_id):
        return f"{self.external_task_base_url}/{task_id}/failure"

    def bpmn_failure(self, task_id, error_code):
        url = self.get_task_bpmn_error_url(task_id)

        body = {
            "workerId": self.worker_id,
            "errorCode": error_code,
        }

        resp = requests.post(url, headers=self._get_headers(), json=body)
        resp.raise_for_status()
        return resp.status_code == HTTPStatus.NO_CONTENT

    def get_task_bpmn_error_url(self, task_id):
        return f"{self.external_task_base_url}/{task_id}/bpmnError"

    def _get_headers(self):
        return {
            "Content-Type": "application/json"
        }
