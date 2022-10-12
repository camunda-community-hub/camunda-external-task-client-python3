[![Community Extension](https://img.shields.io/badge/Community%20Extension-An%20open%20source%20community%20maintained%20project-FF4700)](https://github.com/camunda-community-hub/community)[![Lifecycle: Stable](https://img.shields.io/badge/Lifecycle-Stable-brightgreen)](https://github.com/Camunda-Community-Hub/community/blob/main/extension-lifecycle.md#stable-)

# camunda-external-task-client-python3
![camunda-external-task-client-python3](https://github.com/trustfactors/camunda-external-task-client-python3/workflows/camunda-external-task-client-python3/badge.svg)

This repository contains Camunda External Task Client written in Python3.


Implement your [BPMN Service Task](https://docs.camunda.org/manual/latest/user-guide/process-engine/external-tasks/) in Python3.

> Python >= 3.7 is required

## Installing
Add following line to `requirements.txt` of your Python project.
```
git+https://github.com/trustfactors/camunda-external-task-client-python3.git/#egg=camunda-external-task-client-python3
```

Or use pip to install as shown below:
```
pip install camunda-external-task-client-python3
```

## Running Camunda with Docker
To run the examples provided in [examples](./examples) folder, you need to have Camunda running locally or somewhere.

To run Camunda locally with Postgres DB as backend, you can use [docker-compose.yml](./docker-compose.yml) file.

```
$> docker-compose -f docker-compose.yml up
```
### Auth Basic Examples

To run the examples with Auth Basic provided in [examples/examples_auth_basic](./examples/examples_auth_basic) folder, you need to have Camunda with AuthBasic, running locally or somewhere.

To run Camunda with AuthBasic locally with Postgres DB as backend, you can use [docker-compose-auth.yml](./docker-compose-auth.yml) file.

```
$> docker-compose -f docker-compose-auth.yml up
```

## Usage

1.  Make sure to have [Camunda](https://camunda.com/download/) running.
2.  Create a simple process model with an External Service Task and define the topic as 'topicName'.
3.  Deploy the process to the Camunda BPM engine.
4.  In your Python code:

```python
import time
from camunda.external_task.external_task import ExternalTask, TaskResult
from camunda.external_task.external_task_worker import ExternalTaskWorker

# configuration for the Client
default_config = {
    "maxTasks": 1,
    "lockDuration": 10000,
    "asyncResponseTimeout": 5000,
    "retries": 3,
    "retryTimeout": 5000,
    "sleepSeconds": 30
}

def handle_task(task: ExternalTask) -> TaskResult:
    """
    This task handler you need to implement with your business logic.
    After completion of business logic call either task.complete() or task.failure() or task.bpmn_error() 
    to report status of task to Camunda
    """
    # add your business logic here
    # ...
    
    # mark task either complete/failure/bpmnError based on outcome of your business logic
    failure, bpmn_error = random_true(), random_true() # this code simulate random failure
    if failure:
        # this marks task as failed in Camunda
        return task.failure(error_message="task failed",  error_details="failed task details", 
                            max_retries=3, retry_timeout=5000)
    elif bpmn_error:
        return task.bpmn_error(error_code="BPMN_ERROR_CODE", error_message="BPMN Error occurred", 
                                variables={"var1": "value1", "success": False})
    
    # pass any output variables you may want to send to Camunda as dictionary to complete()
    return task.complete({"var1": 1, "var2": "value"}) 

def random_true():
    current_milli_time = int(round(time.time() * 1000))
    return current_milli_time % 2 == 0

if __name__ == '__main__':
   ExternalTaskWorker(worker_id="1", config=default_config).subscribe("topicName", handle_task)
```

## About External Tasks

External Tasks are service tasks whose execution differs particularly from the execution of other service tasks (e.g. Human Tasks).
The execution works in a way that units of work are polled from the engine before being completed.

**camunda-external-task-client-python** allows you to create easily such client in Python3.

## Features

### [Start process](https://docs.camunda.org/manual/latest/reference/rest/process-definition/post-start-process-instance/)
Camunda provides functionality to start a process instance for a given process definition.

To start a process instance, we can use `start_process()` from [engine_client.py](./camunda/client/engine_client.py#L24)

You can find a usage example [here](./examples/start_process.py).

```python
client = EngineClient()
resp_json = client.start_process(process_key="PARALLEL_STEPS_EXAMPLE", variables={"intVar": "1", "strVar": "hello"},
                                 tenant_id="6172cdf0-7b32-4460-9da0-ded5107aa977", business_key=str(uuid.uuid1()))
```

### [Fetch and Lock](https://docs.camunda.org/manual/latest/reference/rest/external-task/fetch/)

`ExternalTaskWorker(worker_id="1").subscribe("topicName", handle_task)` starts long polling of the Camunda engine for external tasks.

* Polling tasks from the engine works by performing a fetch & lock operation of tasks that have subscriptions. It then calls the handler function passed to `subscribe()` function. i.e. `handle_task` in above example.
* Long Polling is done periodically based on the `asyncResponseTimeout` configuration. Read more about [Long Polling](https://docs.camunda.org/manual/latest/user-guide/process-engine/external-tasks/#long-polling-to-fetch-and-lock-external-tasks).

### [Complete](https://docs.camunda.org/manual/latest/reference/rest/external-task/post-complete/)
```python
from camunda.external_task.external_task import ExternalTask, TaskResult
from camunda.external_task.external_task_worker import ExternalTaskWorker
def handle_task(task: ExternalTask) -> TaskResult:
    # add your business logic here
    
    # Complete the task
    # pass any output variables you may want to send to Camunda as dictionary to complete()
    return task.complete({"var1": 1, "var2": "value"})

ExternalTaskWorker(worker_id="1").subscribe("topicName", handle_task)
```

### [Handle Failure](https://docs.camunda.org/manual/latest/reference/rest/external-task/post-failure/)
```python
from camunda.external_task.external_task import ExternalTask, TaskResult
from camunda.external_task.external_task_worker import ExternalTaskWorker
def handle_task(task: ExternalTask) -> TaskResult:
    # add your business logic here
    
    # Handle task Failure
    return task.failure(error_message="task failed",  error_details="failed task details", 
                        max_retries=3, retry_timeout=5000)
    # This client/worker uses max_retries if no retries are previously set in the task
    # if retries are previously set then it just decrements that count by one before reporting failure to Camunda
    # when retries are zero, Camunda creates an incident which then manually needs to be looked into on Camunda Cockpit            

ExternalTaskWorker(worker_id="1").subscribe("topicName", handle_task)
```

### [Handle BPMN Error](https://docs.camunda.org/manual/latest/reference/rest/external-task/post-bpmn-error/)
```python
from camunda.external_task.external_task import ExternalTask, TaskResult
from camunda.external_task.external_task_worker import ExternalTaskWorker
def handle_task(task: ExternalTask) -> TaskResult:
    # add your business logic here
    
    # Handle a BPMN Failure
    return task.bpmn_error(error_code="BPMN_ERROR", error_message="BPMN error occurred")

ExternalTaskWorker(worker_id="1" ).subscribe("topicName", handle_task)
```

### Access Process Variables
```python
from camunda.external_task.external_task import ExternalTask, TaskResult
from camunda.external_task.external_task_worker import ExternalTaskWorker
def handle_task(task: ExternalTask) -> TaskResult:
    # add your business logic here
    # get the process variable 'score'
    score = task.get_variable("score")
    if int(score) >= 100:
        return task.complete(...)
    else:
        return task.failure(...)        

ExternalTaskWorker().subscribe("topicName", handle_task)
```

### [Correlate message](https://docs.camunda.org/manual/7.13/reference/bpmn20/events/message-events/)
Camunda provides functionality to send a message event to a running process instance.

You can read more about the message events here: https://docs.camunda.org/manual/7.13/reference/bpmn20/events/message-events/

In our to send a message event to a process instance, a new function called `correlate_message()` is added to [engine_client.py](./camunda/client/engine_client.py#L60)

We can correlate the message by:
- process_instance_id
- tenant_id
- business_key
- process_variables

You can find a usage example [here](./examples/correlate_message.py).

```python
client = EngineClient()
resp_json = client.correlate_message("CANCEL_MESSAGE", business_key="b4a6f392-12ab-11eb-80ef-acde48001122")
```
## AuthBasic Usage

To create an EngineClient with AuthBasic simple

```python
client = EngineClient(config={"auth_basic": {"username": "demo", "password": "demo"}})
resp_json = client.start_process(process_key="PARALLEL_STEPS_EXAMPLE", variables={"intVar": "1", "strVar": "hello"},
                                 tenant_id="6172cdf0-7b32-4460-9da0-ded5107aa977", business_key=str(uuid.uuid1()))
```

To create an ExternalTaskWorker with AuthBasic simple

```python
from camunda.external_task.external_task import ExternalTask, TaskResult
from camunda.external_task.external_task_worker import ExternalTaskWorker

config = {"auth_basic": {"username": "demo", "password": "demo"}}

def handle_task(task: ExternalTask) -> TaskResult:
    # add your business logic here
    
    # Complete the task
    # pass any output variables you may want to send to Camunda as dictionary to complete()
    return task.complete({"var1": 1, "var2": "value"})

ExternalTaskWorker(worker_id="1", config=config).subscribe("topicName", handle_task)
```

## License
The source files in this repository are made available under the [Apache License Version 2.0](./LICENSE).
