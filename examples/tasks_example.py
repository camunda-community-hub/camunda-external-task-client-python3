import logging

import asyncio

from camunda.external_task.external_task_worker import ExternalTaskWorker
from examples.task_handler_example import handle_task

logger = logging.getLogger(__name__)

default_config = {
    "maxTasks": 1,
    "lockDuration": 10000,
    "asyncResponseTimeout": 3000,
    "retries": 3,
    "retryTimeout": 5000,
    "sleepSeconds": 30,
    "isDebug": True,
    "httpTimeoutMillis": 3000,
}


async def main():
    loop = asyncio.get_event_loop()

    configure_logging()
    topics = ["PARALLEL_STEP_1", "PARALLEL_STEP_2", "COMBINE_STEP"]
    tasks = []
    for index, topic in enumerate(topics):
        tasks.append(loop.create_task(ExternalTaskWorker(worker_id=index, config=default_config).subscribe(topic, handle_task,
                        {"strVar": "hello"})))


def configure_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler()])


if __name__ == '__main__':
    asyncio.run(main())
