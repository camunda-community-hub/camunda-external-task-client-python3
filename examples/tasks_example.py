import logging
from datetime import datetime

import time

from camunda.external_task.external_task_worker import ExternalTaskWorker


def main():
    configure_logging()
    ExternalTaskWorker(config={
        "maxTasks": 1,
        "lockDuration": 10000,
        "asyncResponseTimeout": 5000,
        "retries": 3,
        "retryTimeout": 5000,
    }).subscribe(["FIRST_TOPIC", "SECOND_TOPIC"], handleTask)


def configure_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler()])


async def handleTask(task):
    logging.info(f"handleTask() - {task}")
    failure = random_true()
    bpmn_error = False if failure else random_true()

    # override the values to simulate success/failure/BPMN error explicitly
    # failure, bpmn_error = False, False

    if failure:
        return task.failure("task failed", "failed task details", 3, 3000)
    elif bpmn_error:
        return task.bpmn_error("BPMN_ERROR_CODE")

    return task.complete({"success": True, "task_completed_on": str(datetime.now())})


def random_true():
    current_milli_time = lambda: int(round(time.time() * 1000))
    return current_milli_time() % 2 == 0


if __name__ == '__main__':
    main()
