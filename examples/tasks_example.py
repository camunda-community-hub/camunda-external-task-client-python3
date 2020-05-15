import logging

import time

from camunda.external_task.external_task import ExternalTask
from camunda.external_task.external_task_worker import ExternalTaskWorker


def main():
    configure_logging()
    ExternalTaskWorker(options={
        "maxTasks": 1,
        "lockDuration": 60000,
        "asyncResponseTimeout": 5000,
        "retries": 3,
        "retryTimeout": 5000,
    }).subscribe(["FIRST_TOPIC", "SECOND_TOPIC"], handleTask)


def configure_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler()])


async def handleTask(task: ExternalTask):
    print(f"handleTask() - {task}")
    # error = random_true()
    # bpmn_error = False if error else random_true()
    error, bpmn_error = False, True

    if error:
        return task.failure("task failed", "failed task details", 0, 300000)
    elif bpmn_error:
        return task.bpmn_error("BPMN_ERROR_CODE")

    return task.complete({})


def random_true():
    current_milli_time = lambda: int(round(time.time() * 1000))
    return current_milli_time() % 2 == 0


if __name__ == '__main__':
    main()
