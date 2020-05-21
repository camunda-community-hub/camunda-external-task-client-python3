import asyncio
import logging

from frozendict import frozendict

from camunda.client.external_task_client import ExternalTaskClient, ENGINE_LOCAL_BASE_URL
from camunda.external_task.external_task import ExternalTask
from camunda.external_task.external_task_executor import ExternalTaskExecutor
from camunda.utils.log_utils import log_with_context
from camunda.utils.utils import get_exception_detail

logger = logging.getLogger(__name__)


class ExternalTaskWorker:
    worker_id = 0
    DEFAULT_SLEEP_SECONDS = 300

    def __init__(self, base_url=ENGINE_LOCAL_BASE_URL, config=frozendict({})):
        self.worker_id = str(type(self).worker_id)
        type(self).worker_id += 1
        self.client = ExternalTaskClient(self.worker_id, base_url, config)
        self.executor = ExternalTaskExecutor(self.worker_id, self.client)
        self.config = config
        self._log_with_context("Created new External Task Worker")

    async def subscribe(self, topic_names, action):
        while True:
            await self.fetch_and_execute(topic_names, action)

        self._log_with_context("Stopping worker")

    async def fetch_and_execute(self, topic_names, action):
        response = await self._fetch_and_lock(topic_names)
        tasks = await self._parse_response(response, topic_names)
        await self._execute_tasks(tasks, action)

    async def _fetch_and_lock(self, topic_names):
        try:
            self._log_with_context(f"Fetching and Locking external tasks for Topics: {topic_names}")
            return await self.client.fetch_and_lock(topic_names)
        except Exception as e:
            sleep_seconds = self._get_sleep_seconds()
            logger.error(f'error fetching and locking tasks: {get_exception_detail(e)}. '
                         f'retrying after {sleep_seconds} seconds', exc_info=True)
            await asyncio.sleep(sleep_seconds)

    async def _parse_response(self, response, topic_names):
        tasks = []
        resp_json = response and await response.json()
        if resp_json:
            for context in resp_json:
                task = ExternalTask(context)
                tasks.append(task)

        tasks_count = len(tasks)
        self._log_with_context(f"{tasks_count} External task(s) found for Topics: {topic_names}")
        return tasks

    async def _execute_tasks(self, tasks, action):
        for task in tasks:
            await self._execute_task(task, action)

    async def _execute_task(self, task, action):
        try:
            await self.executor.execute_task(task, action)
        except Exception as e:
            self._log_with_context(f'error when executing task: {get_exception_detail(e)}',
                                   topic=task.get_topic_name(), task_id=task.get_task_id(),
                                   log_level='error', exc_info=True)

    def _log_with_context(self, msg, topic=None, task_id=None, log_level='info', **kwargs):
        context = frozendict({"WORKER_ID": self.worker_id, "TOPIC": topic, "TASK_ID": task_id})
        log_with_context(msg, context=context, log_level=log_level, **kwargs)

    def _get_sleep_seconds(self):
        return self.config.get("sleepSeconds", self.DEFAULT_SLEEP_SECONDS)
