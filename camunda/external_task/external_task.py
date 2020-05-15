from camunda.variables.variables import Variables


class ExternalTask:

    def __init__(self, context):
        self._context = context
        self._local_variables = Variables(context["variables"])
        self._task_result = TaskResult.empty_task_result(task=self)

    def get_worker_id(self):
        return self._context["workerId"]

    def get_task_id(self):
        return self._context["id"]

    def get_topic_name(self):
        return self._context["topicName"]

    def get_local_variable(self, variable_name):
        return self._local_variables.get_variable(variable_name)

    def get_tenant_id(self):
        return self._context.get("tenantId", None)

    def complete(self, variables):
        self._task_result = TaskResult.success(self, variables)
        return self._task_result

    def failure(self, error_message, error_details, retries, retry_timeout):
        self._task_result = TaskResult.failure(self, error_message=error_message, error_details=error_details,
                                               retries=retries, retry_timeout=retry_timeout)
        return self._task_result

    def bpmn_error(self, error_code):
        self._task_result = TaskResult.bpmn_error(self, error_code=error_code)
        return self._task_result

    def __str__(self):
        return f"{self._context}"


class TaskResult:

    def __init__(self, task, success=False, variables={}, bpmn_error_code=None,
                 error_message=None, error_details={}, retries=0, retry_timeout=300000):
        self.task = task
        self.success = success
        self.variables = variables
        self.bpmn_error_code = bpmn_error_code
        self.error_message = error_message
        self.error_details = error_details
        self.retries = retries
        self.retry_timeout = retry_timeout

    @classmethod
    def success(cls, task, variables):
        return TaskResult(task, success=True, variables=variables)

    @classmethod
    def failure(cls, task, error_message, error_details, retries, retry_timeout):
        return TaskResult(task, success=False, error_message=error_message, error_details=error_details,
                          retries=retries, retry_timeout=retry_timeout)

    @classmethod
    def bpmn_error(cls, task, error_code):
        return TaskResult(task, success=False, bpmn_error_code=error_code)

    @classmethod
    def empty_task_result(cls, task):
        return TaskResult(task, success=False)

    def is_success(self):
        return self.success and self.bpmn_error_code is None and self.error_message is None

    def is_failure(self):
        return not self.success and self.error_message

    def is_bpmn_error(self):
        return not self.success and self.bpmn_error_code

    def __str__(self):
        if self.is_success():
            return f"success: task_id={self.task.get_task_id()}, variable={self.variables}"
        elif self.is_failure():
            return f"failure: task_id={self.task.get_task_id()}, " \
                   f"error_message={self.error_message}, error_details={self.error_details}, " \
                   f"retries={self.retries}, retry_timeout={self.retry_timeout}"

        return f"bpmn_error: task_id={self.task.get_task_id()}, error_code={self.bpmn_error_code}"
