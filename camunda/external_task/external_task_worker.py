import asyncio
from asyncio import Task
from typing import Dict

from camunda.client.external_task_client import ExternalTaskClient, ENGINE_LOCAL_BASE_URL
from camunda.external_task.external_task import ExternalTask
from camunda.external_task.external_task_executor import ExternalTaskExecutor
from camunda.utils.auth_basic import obfuscate_password
from camunda.utils.log_utils import log_with_context
from camunda.utils.utils import get_exception_detail


class ExternalTaskWorker:
    DEFAULT_SLEEP_SECONDS = 300

    def __init__(self, worker_id, base_url=ENGINE_LOCAL_BASE_URL, config=None):
        config = config if config is not None else {}  # To avoid to have a mutable default for a parameter
        self.worker_id = worker_id
        self.client = ExternalTaskClient(self.worker_id, base_url, config)
        self.executor = ExternalTaskExecutor(self.worker_id, self.client)
        self.config = config
        self.task_dict: Dict[str, Task] = {}
        self._log_with_context(f"Created new External Task Worker with config: {obfuscate_password(self.config)}")

    async def subscribe(self, topic_names, action, process_variables=None, variables=None):
        try:
            while True:
                await self._fetch_and_execute_safe(topic_names, action, process_variables, variables)
        except asyncio.exceptions.CancelledError:
            pass

        self._log_with_context("Stopping worker")

    async def _fetch_and_execute_safe(
            self, topic_names, action, process_variables=None, variables=None
    ):
        try:
            await self.fetch_and_execute(topic_names, action, process_variables, variables)
        except NoExternalTaskFound:
            self._log_with_context(f"no External Task found for Topics: {topic_names}, "
                                   f"Process variables: {process_variables}", topic=topic_names)
        except asyncio.exceptions.CancelledError:
            raise
        except BaseException as e:
            sleep_seconds = self._get_sleep_seconds()
            self._log_with_context(f'error fetching and executing tasks: {get_exception_detail(e)} '
                                   f'for topic(s)={topic_names} with Process variables: {process_variables}. '
                                   f'retrying after {sleep_seconds} seconds', exc_info=True)
            await asyncio.sleep(sleep_seconds)

    async def fetch_and_execute(self, topic_names, action, process_variables=None, variables=None):
        self._log_with_context(f"Fetching and Executing external tasks for Topics: {topic_names} "
                               f"with Process variables: {process_variables}")
        resp_json = await self._fetch_and_lock(topic_names, process_variables, variables)
        tasks = self._parse_response(resp_json, topic_names, process_variables)
        if len(tasks) == 0:
            raise NoExternalTaskFound(f"no External Task found for Topics: {topic_names}, "
                                      f"Process variables: {process_variables}")
        await self._execute_tasks(tasks, action)

    async def _fetch_and_lock(self, topic_names, process_variables=None, variables=None):
        self._log_with_context(f"Fetching and Locking external tasks for Topics: {topic_names} "
                               f"with Process variables: {process_variables}")
        return await self.client.fetch_and_lock(topic_names, process_variables, variables)

    def _parse_response(self, resp_json, topic_names, process_variables):
        tasks = []
        if resp_json:
            for context in resp_json:
                task = ExternalTask(context)
                tasks.append(task)

        tasks_count = len(tasks)
        self._log_with_context(f"{tasks_count} External task(s) found for "
                               f"Topics: {topic_names}, Process variables: {process_variables}")
        return tasks

    async def _execute_tasks(self, tasks: list[ExternalTask], action):
        for task in tasks:
            if task.get_task_id() in self.task_dict:
                # Cancel a running task when it is offered again
                self.task_dict[task.get_task_id()].cancel()
            self.task_dict[task.get_task_id()] = asyncio.create_task(
                self._execute_task(task, action)
            )

    async def _execute_task(self, task: ExternalTask, action):
        try:
            await self.executor.execute_task(task, action)
            del self.task_dict[task.get_task_id()]
        except Exception as e:
            self._log_with_context(f'error when executing task: {get_exception_detail(e)}',
                                   topic=task.get_topic_name(), task_id=task.get_task_id(),
                                   log_level='error', exc_info=True)
            raise e

    def _log_with_context(self, msg, topic=None, task_id=None, log_level='info', **kwargs):
        context = {"WORKER_ID": str(self.worker_id), "TOPIC": topic, "TASK_ID": task_id}
        log_with_context(msg, context=context, log_level=log_level, **kwargs)

    def _get_sleep_seconds(self):
        return self.config.get("sleepSeconds", self.DEFAULT_SLEEP_SECONDS)


class NoExternalTaskFound(Exception):
    pass
