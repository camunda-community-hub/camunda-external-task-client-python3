import logging

import asyncio

from camunda.external_task.external_task import ExternalTask
from camunda.external_task.external_task_worker import ExternalTaskWorker
from camunda.utils.log_utils import log_with_context

logger = logging.getLogger(__name__)

default_config = {
    "maxTasks": 1,
    "lockDuration": 10000,
    "asyncResponseTimeout": 30000,
    "retries": 3,
    "retryTimeout": 5000,
    "sleepSeconds": 30,
    "isDebug": True,
}


async def generic_task_handler(task: ExternalTask):
    log_context = {"WORKER_ID": task.get_worker_id(),
                   "TASK_ID": task.get_task_id(),
                   "TOPIC": task.get_topic_name()}

    log_with_context("executing generic task handler", log_context)
    return await task.complete()


async def fail_task_handler(task: ExternalTask):
    log_context = {"WORKER_ID": task.get_worker_id(),
                   "TASK_ID": task.get_task_id(),
                   "TOPIC": task.get_topic_name()}

    log_with_context("executing fail_task_handler", log_context)
    return await task.failure("task failed", "task failed forced", 0, 10)


async def main():
    loop = asyncio.get_event_loop()

    configure_logging()
    topics = [
        ("TASK_1", fail_task_handler),
        ("TASK_2", fail_task_handler),
    ]
    tasks = []
    for index, topic_handler in enumerate(topics):
        topic = topic_handler[0]
        handler_func = topic_handler[1]
        tasks.append(loop.create_task(ExternalTaskWorker(worker_id=index, config=default_config).subscribe(topic, handler_func)))


def configure_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler()])


if __name__ == '__main__':
    asyncio.run(main())
