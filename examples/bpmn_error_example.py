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


def validate_image(task: ExternalTask):
    """
    To simulate BPMN/Failure/Success, this handler uses image name variable (to be passed when launching the process)
    """
    log_context = {"WORKER_ID": task.get_worker_id(),
                   "TASK_ID": task.get_task_id(),
                   "TOPIC": task.get_topic_name()}

    log_with_context("executing validate_image", log_context)
    img_name = task.get_variable('imgName')

    if "poor" in img_name:
        return task.bpmn_error("POOR_QUALITY_IMAGE", "Image quality is bad",
                               {"img_rejection_code": "POOR_QUALITY_CODE_XX",
                                "img_rejection_reason": f"Image quality must be at least GOOD"})
    elif "jpg" in img_name:
        return task.complete({"img_approved": True})
    elif "corrupt" in img_name:
        return task.failure("Cannot validate image", "image is corrupted", 0, default_config.get("retryTimeout"))
    else:
        return task.bpmn_error("INVALID_IMAGE", "Image extension must be jpg",
                               {"img_rejection_code": "INVALID_IMG_NAME",
                                "img_rejection_reason": f"Image name {img_name} is invalid"})


def generic_task_handler(task: ExternalTask):
    log_context = {"WORKER_ID": task.get_worker_id(),
                   "TASK_ID": task.get_task_id(),
                   "TOPIC": task.get_topic_name()}

    log_with_context("executing generic task handler", log_context)
    return task.complete()


def main():
    configure_logging()
    topics = [("VALIDATE_IMAGE", validate_image),
              # ("APPROVE_IMAGE", generic_task_handler),
              # ("REJECT_IMAGE", generic_task_handler),
              # ("ENHANCE_IMAGE_QUALITY", generic_task_handler),
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
