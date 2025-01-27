import logging

from camunda.client.async_external_task_client import AsyncExternalTaskClient
from camunda.utils.log_utils import log_with_context

logger = logging.getLogger(__name__)


class AsyncExternalTaskExecutor:

    def __init__(self, worker_id: str, external_task_client: AsyncExternalTaskClient):
        self.worker_id = worker_id
        self.external_task_client = external_task_client

    async def execute_task(self, task, action):
        topic = task.get_topic_name()
        task_id = task.get_task_id()
        self._log_with_context(f"Executing external task for Topic: {topic}", task_id=task_id)
        task_result = await action(task)
        # in case task result is not set inside action function, set it in task here
        task.set_task_result(task_result)
        await self._handle_task_result(task_result)
        return task_result

    async def _handle_task_result(self, task_result):
        task = task_result.get_task()
        topic = task.get_topic_name()
        task_id = task.get_task_id()
        if task_result.is_success():
            await self._handle_task_success(task_id, task_result, topic)
        elif task_result.is_bpmn_error():
            await self._handle_task_bpmn_error(task_id, task_result, topic)
        elif task_result.is_failure():
            await self._handle_task_failure(task_id, task_result, topic)
        else:
            err_msg = f"task result for task_id={task_id} must be either complete/failure/BPMNError"
            self._log_with_context(err_msg, task_id=task_id, log_level='warning')
            raise Exception(err_msg)

    def _strip_long_variables(self, variables):
        """remove value of complex variables for the dict"""
        if not variables:
            return variables
        cleaned = {}
        for k, v in variables.items():
            if isinstance(v, dict) and v.get("type", "") in ("File", "Bytes"):
                cleaned[k] = {**v, "value": "..."}
            else:
                cleaned[k] = v
        return cleaned

    async def _handle_task_success(self, task_id, task_result, topic):
        self._log_with_context(f"Marking task complete for Topic: {topic}", task_id)
        if await self.external_task_client.complete(task_id, task_result.global_variables, task_result.local_variables):
            self._log_with_context(f"Marked task completed - Topic: {topic} "
                                   f"global_variables: {self._strip_long_variables(task_result.global_variables)} "
                                   f"local_variables: {self._strip_long_variables(task_result.local_variables)}",
                                   task_id, log_level='debug')
        else:
            self._log_with_context(f"Not able to mark task completed - Topic: {topic} "
                                   f"global_variables: {self._strip_long_variables(task_result.global_variables)} "
                                   f"local_variables: {self._strip_long_variables(task_result.local_variables)}",
                                   task_id, log_level='error')
            raise Exception(f"Not able to mark complete for task_id={task_id} "
                            f"for topic={topic}, worker_id={self.worker_id}")

    async def _handle_task_failure(self, task_id, task_result, topic):
        self._log_with_context(f"Marking task failed - Topic: {topic} task_result: {task_result}", task_id)
        if await self.external_task_client.failure(task_id, task_result.error_message, task_result.error_details,
                                                   task_result.retries, task_result.retry_timeout):
            self._log_with_context(f"Marked task failed - Topic: {topic} task_result: {task_result}", task_id)
        else:
            self._log_with_context(f"Not able to mark task failure - Topic: {topic}", task_id=task_id)
            raise Exception(f"Not able to mark failure for task_id={task_id} "
                            f"for topic={topic}, worker_id={self.worker_id}")

    async def _handle_task_bpmn_error(self, task_id, task_result, topic):
        bpmn_error_handled = await self.external_task_client.bpmn_failure(task_id, task_result.bpmn_error_code,
                                                                          task_result.error_message,
                                                                          task_result.global_variables)
        if bpmn_error_handled:
            self._log_with_context(f"BPMN Error Handled: {bpmn_error_handled} "
                                   f"Topic: {topic} task_result: {task_result}")
        else:
            self._log_with_context(f"Not able to mark BPMN error - Topic: {topic}", task_id=task_id)
            raise Exception(f"Not able to mark BPMN Error for task_id={task_id} "
                            f"for topic={topic}, worker_id={self.worker_id}")

    def _log_with_context(self, msg, task_id=None, log_level='info', **kwargs):
        context = {"WORKER_ID": self.worker_id, "TASK_ID": task_id}
        log_with_context(msg, context=context, log_level=log_level, **kwargs)
