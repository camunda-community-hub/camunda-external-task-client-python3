import logging

import requests as req

from camunda.utils.utils import str_to_list

logger = logging.getLogger(__name__)


class EngineClient:
    default_options = {
        "maxTasks": 1,
        "lockDuration": 120000,
        "asyncResponseTimeout": 5000,
        "retries": 3,
        "retryTimeout": 300000,
    }

    def __init__(self, worker_id, base_url, options={}):
        self.base_url = base_url
        self.worker_id = worker_id
        self.options = type(self).default_options
        self.options.update(options)

    async def fetchAndLock(self, topic_names):
        url = self.base_url + "/fetchAndLock"
        body = {
            "workerId": self.worker_id,
            "maxTasks": self.options["maxTasks"],
            "topics": self._get_topics(topic_names),
            "asyncResponseTimeout": self.options["asyncResponseTimeout"]
        }

        return req.post(url, headers=self._get_headers(), json=body)

    def _get_topics(self, topic_names):
        topics = []
        for topic in str_to_list(topic_names):
            topics.append({
                "topicName": topic,
                "lockDuration": self.options["lockDuration"],
            })
        return topics

    async def complete(self, task_id, variables):
        url = f"{self.base_url}/{task_id}/complete"

        body = {
            "workerId": self.worker_id,
            "variables": variables,
            "localVariables": {},
        }

        resp = req.post(url, headers=self._get_headers(), json=body)
        return resp.status_code == 204

    async def failure(self, task_id, retries, error_message, error_details):
        url = f"{self.base_url}/{task_id}/failure"
        logger.info(f"setting retries to: {retries} for task: {task_id}")
        body = {
            "workerId": self.worker_id,
            "errorMessage": error_message,
            "retries": retries,
            "retryTimeout": self.options["retryTimeout"],
        }
        if error_details:
            body["errorDetails"] = error_details

        resp = req.post(url, headers=self._get_headers(), json=body)
        return resp.status_code == 204

    async def bpmn_failure(self, id, error_code, error_message, variables):
        url = f"{self.base_url}/{id}/bpmnError"

        body = {
            "workerId": self.worker_id,
            "errorCode": error_code,
            "errorMessage": error_message,
            "variables": variables,
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
