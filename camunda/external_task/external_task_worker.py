import asyncio
import logging

from camunda.client.engine_client import EngineClient
from camunda.external_task.external_task import ExternalTask

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
                await self._execute_task(context, action)

        logger.info(f"Stopping worker id={self.worker_id}")

    async def _execute_task(self, context, action):
        task = ExternalTask(context)
        topic = task.get_topic_name()
        task_id = task.get_task_id()
        self._log_with_context(f"External task found for Topic: {topic}", task_id=task_id)
        task_result = await action(task)
        await self._handle_task_result(task, task_result)

    async def _handle_task_result(self, task, task_result):
        topic = task.get_topic_name()
        task_id = task.get_task_id()
        if task_result.is_success():
            self._log_with_context(f"Marking task complete for Topic: {topic}", task_id)
            if await self.client.complete(task_id, task_result.variables):
                self._log_with_context(f"Marked task completed - Topic: {topic} "
                                       f"variables: {task_result.variables}", task_id)
        elif task_result.is_bpmn_error():
            bpmn_error_handled = await self.client.bpmn_failure(task_id, task_result.bpmn_error_code)
            self._log_with_context(f"BPMN Error Handled: {bpmn_error_handled} "
                                   f"Topic: {topic} task_result: {task_result}")
        elif task_result.is_failure():
            self._log_with_context(f"Marking task failed - Topic: {topic} task_result: {task_result}", task_id)
            if await self.client.failure(task_id, task_result.error_message, task_result.error_details,
                                         task_result.retries, task_result.retry_timeout):
                self._log_with_context(f"Marked task failed - Topic: {topic} task_result: {task_result}", task_id)

    def _log_with_context(self, msg, task_id=None):
        if task_id:
            logger.info(f"[WORKER_ID:{self.worker_id}] [TASK_ID:{task_id}] {msg}")
        else:
            logger.info(f"[WORKER_ID:{self.worker_id}] {msg}")
