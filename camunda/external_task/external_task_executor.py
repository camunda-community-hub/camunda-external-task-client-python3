import logging

from frozendict import frozendict

from camunda.utils.log_utils import log_with_context

logger = logging.getLogger(__name__)


class ExternalTaskExecutor:

    def __init__(self, worker_id, external_task_client):
        self.worker_id = worker_id
        self.external_task_client = external_task_client

    def execute_task(self, task, action):
        topic = task.get_topic_name()
        task_id = task.get_task_id()
        self._log_with_context(f"Executing external task for Topic: {topic}", task_id=task_id)
        task_result = action(task)
        # in case task result is not set inside action function, set it in task here
        task.set_task_result(task_result)
        self._handle_task_result(task_result)
        return task_result

    def _handle_task_result(self, task_result):
        task = task_result.get_task()
        topic = task.get_topic_name()
        task_id = task.get_task_id()
        if task_result.is_success():
            self._handle_task_success(task_id, task_result, topic)
        elif task_result.is_failure():
            self._handle_task_failure(task_id, task_result, topic)
        elif task_result.is_bpmn_error():
            self._handle_task_bpmn_error(task_id, task_result, topic)
        else:
            err_msg = f"task result for task_id={task_id} must be either complete/failure/BPMNError"
            self._log_with_context(err_msg, task_id=task_id, log_level='warning')
            raise Exception(err_msg)

    def _handle_task_success(self, task_id, task_result, topic):
        self._log_with_context(f"Marking task complete for Topic: {topic}", task_id)
        if self.external_task_client.complete(task_id, task_result.variables):
            self._log_with_context(f"Marked task completed - Topic: {topic} "
                                   f"variables: {task_result.variables}", task_id)
        else:
            self._log_with_context(f"Not able to mark task completed - Topic: {topic} "
                                   f"variables: {task_result.variables}", task_id)
            raise Exception(f"Not able to mark complete for task_id={task_id} "
                            f"for topic={topic}, worker_id={self.worker_id}")

    def _handle_task_failure(self, task_id, task_result, topic):
        self._log_with_context(f"Marking task failed - Topic: {topic} task_result: {task_result}", task_id)
        if self.external_task_client.failure(task_id, task_result.error_message, task_result.error_details,
                                             task_result.retries, task_result.retry_timeout):
            self._log_with_context(f"Marked task failed - Topic: {topic} task_result: {task_result}", task_id)
        else:
            self._log_with_context(f"Not able to mark task failure - Topic: {topic}", task_id=task_id)
            raise Exception(f"Not able to mark failure for task_id={task_id} "
                            f"for topic={topic}, worker_id={self.worker_id}")

    def _handle_task_bpmn_error(self, task_id, task_result, topic):
        bpmn_error_handled = self.external_task_client.bpmn_failure(task_id, task_result.bpmn_error_code)
        if bpmn_error_handled:
            self._log_with_context(f"BPMN Error Handled: {bpmn_error_handled} "
                                   f"Topic: {topic} task_result: {task_result}")
        else:
            self._log_with_context(f"Not able to mark BPMN error - Topic: {topic}", task_id=task_id)
            raise Exception(f"Not able to mark BPMN Error for task_id={task_id} "
                            f"for topic={topic}, worker_id={self.worker_id}")

    def _log_with_context(self, msg, task_id=None, log_level='info', **kwargs):
        context = frozendict({"WORKER_ID": self.worker_id, "TASK_ID": task_id})
        log_with_context(msg, context=context, log_level=log_level, **kwargs)
