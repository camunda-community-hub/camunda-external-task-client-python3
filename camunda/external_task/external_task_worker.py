import time

from camunda.client.external_task_client import ExternalTaskClient, ENGINE_LOCAL_BASE_URL
from camunda.external_task.external_task import ExternalTask
from camunda.external_task.external_task_executor import ExternalTaskExecutor
from camunda.utils.log_utils import log_with_context
from camunda.utils.auth_basic import obfuscate_password
from camunda.utils.utils import get_exception_detail


class ExternalTaskWorker:
    DEFAULT_SLEEP_SECONDS = 300

    def __init__(self, worker_id, base_url=ENGINE_LOCAL_BASE_URL, config=None):
        config = config if config is not None else {}  # To avoid to have a mutable default for a parameter
        self.worker_id = worker_id
        self.client = ExternalTaskClient(self.worker_id, base_url, config)
        self.executor = ExternalTaskExecutor(self.worker_id, self.client)
        self.config = config
        self._log_with_context(f"Created new External Task Worker with config: {obfuscate_password(self.config)}")

    def subscribe(self, topic_names, action, process_variables=None):
        while True:
            self._fetch_and_execute_safe(topic_names, action, process_variables)

        self._log_with_context("Stopping worker")  # Fixme: This code seems to be unreachable?

    def _fetch_and_execute_safe(self, topic_names, action, process_variables=None):
        try:
            self.fetch_and_execute(topic_names, action, process_variables)
        except NoExternalTaskFound:
            self._log_with_context(f"no External Task found for Topics: {topic_names}, "
                                   f"Process variables: {process_variables}", topic=topic_names)
        except BaseException as e:
            sleep_seconds = self._get_sleep_seconds()
            self._log_with_context(f'error fetching and executing tasks: {get_exception_detail(e)} '
                                   f'for topic(s)={topic_names} with Process variables: {process_variables}. '
                                   f'retrying after {sleep_seconds} seconds', exc_info=True)
            time.sleep(sleep_seconds)

    def fetch_and_execute(self, topic_names, action, process_variables=None):
        self._log_with_context(f"Fetching and Executing external tasks for Topics: {topic_names} "
                               f"with Process variables: {process_variables}")
        resp_json = self._fetch_and_lock(topic_names, process_variables)
        tasks = self._parse_response(resp_json, topic_names, process_variables)
        if len(tasks) == 0:
            raise NoExternalTaskFound(f"no External Task found for Topics: {topic_names}, "
                                      f"Process variables: {process_variables}")
        self._execute_tasks(tasks, action)

    def _fetch_and_lock(self, topic_names, process_variables=None):
        self._log_with_context(f"Fetching and Locking external tasks for Topics: {topic_names} "
                               f"with Process variables: {process_variables}")
        return self.client.fetch_and_lock(topic_names, process_variables)

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

    def _execute_tasks(self, tasks, action):
        for task in tasks:
            self._execute_task(task, action)

    def _execute_task(self, task, action):
        try:
            self.executor.execute_task(task, action)
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
