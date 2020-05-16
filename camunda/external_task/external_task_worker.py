import asyncio
import logging

from camunda.client.engine_client import EngineClient
from camunda.external_task.external_task import ExternalTask
from camunda.external_task.external_task_executor import ExternalTaskExecutor
from camunda.utils.log_utils import log_with_context

logger = logging.getLogger(__name__)

ENGINE_LOCAL_BASE_URL = "http://localhost:8080/engine-rest/external-task"


class ExternalTaskWorker:
    worker_id = 0
    DEFAULT_SLEEP_SECONDS = 5

    def __init__(self, base_url=ENGINE_LOCAL_BASE_URL, config={}):
        self.worker_id = str(type(self).worker_id)
        type(self).worker_id += 1
        self.client = EngineClient(self.worker_id, base_url, config)
        self.task_executor = ExternalTaskExecutor(self.worker_id, self.client)
        self.config = config
        self._log_with_context(f"Created new External Task Worker")

    def subscribe(self, topic_names, action):
        # Boilerplate code necessary to work asynchronously. All the real work is done by _subscribe
        # TODO: remove creation of new_event_loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(
            self._subscribe(topic_names, action)
        )
        loop.close()

    async def _subscribe(self, topic_names, action):
        while True:
            await self._fetch_and_execute(action, topic_names)

        self._log_with_context("Stopping worker")

    async def _fetch_and_execute(self, action, topic_names):
        response = await self._fetch_and_lock(topic_names)
        if response and response.json():
            self._log_with_context(f"External task(s) found for Topics: {topic_names}")
            for context in response.json():
                task = ExternalTask(context)
                await self.task_executor.execute_task(task, action)
        else:
            self._log_with_context(f"No external tasks found for Topics: {topic_names}")

    async def _fetch_and_lock(self, topic_names):
        try:
            self._log_with_context(f"Fetching and Locking external tasks for Topics: {topic_names}")
            return await self.client.fetchAndLock(topic_names)
        except Exception as e:
            sleep_seconds = self._get_sleep_seconds()
            logger.error(f'error fetching and locking tasks. retrying after {sleep_seconds} seconds', exc_info=True)
            await asyncio.sleep(sleep_seconds)

    def _log_with_context(self, msg, task_id=None, log_level='info'):
        context = {"WORKER_ID": self.worker_id, "TASK_ID": task_id}
        log_with_context(msg, context=context, log_level=log_level)

    def _get_sleep_seconds(self):
        return self.config.get("sleep_seconds", self.DEFAULT_SLEEP_SECONDS)
