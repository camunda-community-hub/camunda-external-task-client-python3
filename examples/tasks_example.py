import logging

import time

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


async def handleTask(context):
    print(f"handleTask() - {context}")
    error = random_true()
    bpmn_error = False if error else random_true()
    result = {"error": error, "bpmn_error": bpmn_error}
    if error:
        result["errorMessage"] = "task failed"
        result["errorMessage"] = "failed task details"
    if bpmn_error:
        result["errorCode"] = "BPMN_ERROR_CODE"
        result["errorMessage"] = "task failed"
    return result


def random_true():
    current_milli_time = lambda: int(round(time.time() * 1000))
    return current_milli_time() % 2 == 0


if __name__ == '__main__':
    main()
