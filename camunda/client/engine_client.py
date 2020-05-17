import logging

from aiohttp_requests import requests as req
from frozendict import frozendict

from camunda.utils.utils import str_to_list
from camunda.variables.variables import Variables

logger = logging.getLogger(__name__)


class ExternalTaskClient:
    default_config = {
        "maxTasks": 1,
        "lockDuration": 120000,
        "asyncResponseTimeout": 5000,
        "retries": 3,
        "retryTimeout": 300000,
    }

    def __init__(self, worker_id, engine_base_url, config=frozendict({})):
        self.worker_id = worker_id
        self.external_task_base_url = engine_base_url + "/external-task"
        self.config = type(self).default_config
        self.config.update(config)

    async def fetch_and_lock(self, topic_names):
        url = self.external_task_base_url + "/fetchAndLock"
        body = {
            "workerId": self.worker_id,
            "maxTasks": self.config["maxTasks"],
            "topics": self._get_topics(topic_names),
            "asyncResponseTimeout": self.config["asyncResponseTimeout"]
        }

        response = await req.post(url, headers=self._get_headers(), json=body)
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

    async def complete(self, task_id, variables):
        url = f"{self.external_task_base_url}/{task_id}/complete"

        body = {
            "workerId": self.worker_id,
            "variables": Variables.format(variables),
        }

        resp = await req.post(url, headers=self._get_headers(), json=body)
        resp.raise_for_status()
        return resp.status == 204

    async def failure(self, task_id, error_message, error_details, retries, retry_timeout):
        url = f"{self.external_task_base_url}/{task_id}/failure"
        logger.info(f"setting retries to: {retries} for task: {task_id}")
        body = {
            "workerId": self.worker_id,
            "errorMessage": error_message,
            "retries": retries,
            "retryTimeout": retry_timeout,
        }
        if error_details:
            body["errorDetails"] = error_details

        resp = await req.post(url, headers=self._get_headers(), json=body)
        resp.raise_for_status()
        return resp.status == 204

    async def bpmn_failure(self, task_id, error_code):
        url = f"{self.external_task_base_url}/{task_id}/bpmnError"

        body = {
            "workerId": self.worker_id,
            "errorCode": error_code,
        }

        resp = await req.post(url, headers=self._get_headers(), json=body)
        resp.raise_for_status()
        return resp.status == 204

    def _get_headers(self):
        return {
            "Content-Type": "application/json"
        }
