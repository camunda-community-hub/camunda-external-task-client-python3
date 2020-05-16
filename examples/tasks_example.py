import asyncio
import logging
from datetime import datetime

import time

from camunda.external_task.external_task import ExternalTask
from camunda.external_task.external_task_worker import ExternalTaskWorker

logger = logging.getLogger(__name__)


async def main():
    configure_logging()

    loop = asyncio.get_event_loop()

    first_topic = loop.create_task(ExternalTaskWorker(config={
        "maxTasks": 1,
        "lockDuration": 10000,
        "asyncResponseTimeout": 5000,
        "retries": 3,
        "retryTimeout": 5000,
        "sleepSeconds": 30
    }).subscribe(["PARALLEL_STEP_1"], handle_task))

    second_topic = loop.create_task(ExternalTaskWorker(config={
        "maxTasks": 1,
        "lockDuration": 10000,
        "asyncResponseTimeout": 5000,
        "retries": 3,
        "retryTimeout": 5000,
        "sleepSeconds": 30
    }).subscribe(["PARALLEL_STEP_2"], handle_task))

    third_topic = loop.create_task(ExternalTaskWorker(config={
        "maxTasks": 1,
        "lockDuration": 10000,
        "asyncResponseTimeout": 5000,
        "retries": 3,
        "retryTimeout": 5000,
        "sleepSeconds": 30
    }).subscribe(["COMBINE_STEP"], handle_task))

    await asyncio.gather(first_topic, second_topic, third_topic)


def configure_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler()])


async def handle_task(task: ExternalTask):
    logging.info(f"handleTask() - {task}")

    # simulate task execution
    logger.info(f"handleTask() - business logic execution started for task: {task.get_task_id()}")
    await asyncio.sleep(10)

    failure = random_true()
    bpmn_error = False if failure else random_true()

    # override the values to simulate success/failure/BPMN error explicitly
    failure, bpmn_error = False, False
    logger.info(f"handleTask() - business logic executed for topic: {task.get_topic_name()}, "
                f"task: {task.get_task_id()}, failure: {failure}, bpmn_error: {bpmn_error}")

    if failure:
        return task.failure("task failed", "failed task details", 3, 5000)
    elif bpmn_error:
        return task.bpmn_error("BPMN_ERROR_CODE")

    return task.complete({"success": True, "task_completed_on": str(datetime.now())})


def random_true():
    current_milli_time = lambda: int(round(time.time() * 1000))
    return current_milli_time() % 2 == 0


if __name__ == '__main__':
    asyncio.run(main())
