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