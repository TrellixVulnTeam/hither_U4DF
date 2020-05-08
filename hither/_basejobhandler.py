from abc import ABC, abstractmethod

from ._enums import JobStatus

class BaseJobHandler(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def handle_job(self, job):
        if job._status != JobStatus.QUEUED:
            return # job is already handled
        # TODO: SHOULD LOG THIS
        print(f"\nHandling job: {job._label}")
        job._status = JobStatus.RUNNING

    @abstractmethod
    def cancel_job(self, job_id):
        raise NotImplementedError

    @abstractmethod
    def iterate(self):
        raise NotImplementedError
