import logging

from camunda.utils.log_utils import log_with_context

logger = logging.getLogger(__name__)


class ExternalTaskExecutor:
    def __init__(self, worker_id, engine_client):
        self.worker_id = worker_id
        self.engine_client = engine_client

    async def execute_task(self, task, action):
        try:
            topic = task.get_topic_name()
            task_id = task.get_task_id()
            self._log_with_context(f"External task found for Topic: {topic}", task_id=task_id)
            task_result = await action(task)
            await self._handle_task_result(task_result)
        except Exception as e:
            self._log_with_context(f'error when executing task: topic={task.get_topic_name()}',
                                   task_id=task.get_task_id(), log_level='error', exc_info=True)

    async def _handle_task_result(self, task_result):
        task = task_result.get_task()
        topic = task.get_topic_name()
        task_id = task.get_task_id()
        if task_result.is_success():
            await self._handle_task_success(task_id, task_result, topic)
        elif task_result.is_failure():
            await self._handle_task_failure(task_id, task_result, topic)
        elif task_result.is_bpmn_error():
            await self._handle_task_bpmn_error(task_id, task_result, topic)
        else:
            self._log_with_context("task result is unknown", 'warning')

    async def _handle_task_success(self, task_id, task_result, topic):
        self._log_with_context(f"Marking task complete for Topic: {topic}", task_id)
        if await self.engine_client.complete(task_id, task_result.variables):
            self._log_with_context(f"Marked task completed - Topic: {topic} "
                                   f"variables: {task_result.variables}", task_id)
        else:
            self._log_with_context(f"Not able to mark task completed - Topic: {topic} "
                                   f"variables: {task_result.variables}", task_id)

    async def _handle_task_failure(self, task_id, task_result, topic):
        self._log_with_context(f"Marking task failed - Topic: {topic} task_result: {task_result}", task_id)
        if await self.engine_client.failure(task_id, task_result.error_message, task_result.error_details,
                                            task_result.retries, task_result.retry_timeout):
            self._log_with_context(f"Marked task failed - Topic: {topic} task_result: {task_result}", task_id)

    async def _handle_task_bpmn_error(self, task_id, task_result, topic):
        bpmn_error_handled = await self.engine_client.bpmn_failure(task_id, task_result.bpmn_error_code)
        self._log_with_context(f"BPMN Error Handled: {bpmn_error_handled} "
                               f"Topic: {topic} task_result: {task_result}")

    def _log_with_context(self, msg, task_id=None, log_level='info', **kwargs):
        context = {"WORKER_ID": self.worker_id, "TASK_ID": task_id}
        log_with_context(msg, context=context, log_level=log_level, **kwargs)
