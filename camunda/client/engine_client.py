import logging

import requests as req

from camunda.utils.utils import str_to_list

logger = logging.getLogger(__name__)


class EngineClient:
    default_config = {
        "maxTasks": 1,
        "lockDuration": 120000,
        "asyncResponseTimeout": 5000,
        "retries": 3,
        "retryTimeout": 300000,
    }

    def __init__(self, worker_id, base_url, config={}):
        self.worker_id = worker_id
        self.base_url = base_url
        self.config = type(self).default_config
        self.config.update(config)

    async def fetchAndLock(self, topic_names):
        url = self.base_url + "/fetchAndLock"
        body = {
            "workerId": self.worker_id,
            "maxTasks": self.config["maxTasks"],
            "topics": self._get_topics(topic_names),
            "asyncResponseTimeout": self.config["asyncResponseTimeout"]
        }

        return req.post(url, headers=self._get_headers(), json=body)

    def _get_topics(self, topic_names):
        topics = []
        for topic in str_to_list(topic_names):
            topics.append({
                "topicName": topic,
                "lockDuration": self.config["lockDuration"],
            })
        return topics

    async def complete(self, task_id, variables):
        url = f"{self.base_url}/{task_id}/complete"

        body = {
            "workerId": self.worker_id,
            "variables": self.format(variables),
        }

        resp = req.post(url, headers=self._get_headers(), json=body)
        resp.raise_for_status()
        return resp.status_code == 204

    async def failure(self, task_id, error_message, error_details, retries, retry_timeout):
        url = f"{self.base_url}/{task_id}/failure"
        logger.info(f"setting retries to: {retries} for task: {task_id}")
        body = {
            "workerId": self.worker_id,
            "errorMessage": error_message,
            "retries": retries,
            "retryTimeout": retry_timeout,
        }
        if error_details:
            body["errorDetails"] = error_details

        resp = req.post(url, headers=self._get_headers(), json=body)
        return resp.status_code == 204

    async def bpmn_failure(self, task_id, error_code):
        url = f"{self.base_url}/{task_id}/bpmnError"

        body = {
            "workerId": self.worker_id,
            "errorCode": error_code,
        }

        resp = req.post(url, headers=self._get_headers(), json=body)
        return resp.status_code == 204

    def format(self, variables):
        """
        Gives the correct format to variables.
        :param variables: dict - Dictionary of variable names to values.
        :return: Dictionary of well formed variables
            {"var1": 1, "var2": True}
            ->
            {"var1": {"value": 1}, "var2": {"value": True}}
        """
        return {k: {"value": v} for k, v in variables.items()}

    def _get_headers(self):
        return {
            "Content-Type": "application/json"
        }
