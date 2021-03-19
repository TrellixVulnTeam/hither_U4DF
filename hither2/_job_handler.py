import uuid
from abc import abstractmethod
from ._job import Job

class JobHandler:
    def __init__(self):
        self._internal_id = 'jh-' + str(uuid.uuid4())[-12:]
    @abstractmethod
    def queue_job(self, job: Job):
        pass
    @abstractmethod
    def iterate(self):
        pass
    def _get_internal_id(self):
        return self._internal_id

class DefaultJobHandler(JobHandler):
    def __init__(self):
        super().__init__()
    def queue_job(self, job: Job):
        try:
            return_value = job.get_function()(**job.get_kwargs())
            error = None
        except Exception as e:
            error = e
            return_value = None
        if error is None:
            job._set_finished(return_value=return_value)
        else:
            job._set_error(error)
    def iterate(self):
        pass