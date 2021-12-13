from datetime import datetime
from random import randint

import time

from camunda.external_task.external_task import ExternalTask
from camunda.utils.log_utils import log_with_context


def handle_task(task: ExternalTask):
    log_context = {"WORKER_ID": task.get_worker_id(),
                   "TASK_ID": task.get_task_id(),
                   "TOPIC": task.get_topic_name()}

    log_with_context(f"handle_task started: business key = {task.get_business_key()}", log_context)

    # simulate task execution
    execution_time = randint(0, 10)
    log_with_context(f"handle_task - business logic execution started for task: "
                     f"it will execute for {execution_time} seconds", log_context)
    time.sleep(execution_time)

    # simulate that task results randomly into failure/BPMN error/complete
    failure = random_true()
    bpmn_error = False if failure else random_true()
    # override the values to simulate success/failure/BPMN error explicitly (if needed)
    failure, bpmn_error = False, False
    log_with_context(f"handle_task - business logic executed: failure: {failure}, bpmn_error: {bpmn_error}",
                     log_context)

    return __handle_task_result(task, failure, bpmn_error)


def __handle_task_result(task, failure, bpmn_error):
    if failure:
        return task.failure("task failed", "failed task details", 3, 5000)
    elif bpmn_error:
        return task.bpmn_error("BPMN_ERROR_CODE")
    return task.complete({"success": True, "task_completed_on": str(datetime.now())})


def random_true():
    current_milli_time = int(round(time.time() * 1000))
    return current_milli_time % 2 == 0
