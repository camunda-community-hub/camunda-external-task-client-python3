import logging
from concurrent.futures.thread import ThreadPoolExecutor

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


def generic_task_handler(task: ExternalTask):
    log_context = {"WORKER_ID": task.get_worker_id(),
                   "TASK_ID": task.get_task_id(),
                   "TOPIC": task.get_topic_name()}

    log_with_context("executing generic task handler", log_context)
    return task.complete()


def main():
    configure_logging()
    topics = [
        ("STEP_1", generic_task_handler),
        # ("STEP_2", generic_task_handler),
        # ("CLEAN_UP", generic_task_handler),
    ]
    executor = ThreadPoolExecutor(max_workers=len(topics))
    for index, topic_handler in enumerate(topics):
        topic = topic_handler[0]
        handler_func = topic_handler[1]
        executor.submit(ExternalTaskWorker(worker_id=index, config=default_config).subscribe, topic, handler_func)


def configure_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler()])


if __name__ == '__main__':
    main()
