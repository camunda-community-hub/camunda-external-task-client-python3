import asyncio
import logging

from camunda.client.engine_client import EngineClient

logger = logging.getLogger(__name__)

ENGINE_LOCAL_BASE_URL = "http://localhost:8080/engine-rest/external-task"


class ExternalTaskWorker:
    worker_id = 0

    def __init__(self, base_url=ENGINE_LOCAL_BASE_URL, options={}):
        self.worker_id = str(type(self).worker_id)
        type(self).worker_id += 1
        self.client = EngineClient(self.worker_id, base_url, options)
        self._log_with_context(f"Created new External Task Worker")

    def subscribe(self, topic_names, action):
        # Boilerplate code necessary to work asynchronously. All the real work is done by _subscribe
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(
            self._subscribe(topic_names, action)
        )
        loop.close()

    async def _subscribe(self, topic_names, action):
        while True:
            response = await self.client.fetchAndLock(topic_names)

            if not response.json():
                self._log_with_context(f"No external tasks found for Topics: {topic_names}")

            for context in response.json():
                topic = context["topicName"]
                self._log_with_context(f"External task found for Topic: {topic}")
                task_id = context["id"]
                variables = await action(context)
                is_error = variables.get("error", False)
                is_bpmn_error = variables.get("bpmn_error", False)
                formatted_variables = self.client.format(variables)

                if not is_error and not is_bpmn_error:
                    self._log_with_context(f"Marking task complete for Topic: {topic}", task_id)
                    if await self.client.complete(task_id, formatted_variables):
                        self._log_with_context(f"Marked task completed - Topic: {topic} "
                                               f"variables: {variables} formatted_variables: {formatted_variables}",
                                               task_id)
                elif is_bpmn_error:
                    bpmn_error_handled = await self.client.bpmn_failure(task_id, variables["errorCode"],
                                                                        variables["errorMessage"],
                                                                        formatted_variables)
                    self._log_with_context(f"BPMN Error Handled: {bpmn_error_handled} Topic: {topic} "
                                           f"variables: {variables} formatted_variables: {formatted_variables}")
                elif is_error:
                    err_msg = variables.get("errorMessage", "Task failed")
                    err_details = variables.get("errorDetails", "Failed Task details")
                    self._log_with_context(f"Marking task failed - Topic: {topic} "
                                           f"variables: {variables} formatted_variables: {formatted_variables}",
                                           task_id)
                    if await self.client.failure(task_id, 0, err_msg, err_details):
                        self._log_with_context(f"Marked task failed - Topic: {topic} "
                                               f"variables: {variables} formatted_variables: {formatted_variables}",
                                               task_id)

        logger.info(f"Stopping worker id={self.worker_id}")

    def _log_with_context(self, msg, task_id=None):
        if task_id:
            logger.info(f"[WORKER_ID:{self.worker_id}] [TASK_ID:{task_id}] {msg}")
        else:
            logger.info(f"[WORKER_ID:{self.worker_id}] {msg}")
