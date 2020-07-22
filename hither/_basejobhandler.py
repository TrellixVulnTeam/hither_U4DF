from abc import ABC, abstractmethod
from types import SimpleNamespace

from ._enums import JobStatus

class BaseJobHandler(ABC):
    def __init__(self):
        self.is_remote = False
        self._internal_counts = SimpleNamespace(
            num_jobs = 0,
            num_run_jobs = 0,
            num_finished_jobs = 0,
            num_errored_jobs = 0,
            num_skipped_jobs = 0
        )
    
    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        self.cleanup()

    @abstractmethod
    def cleanup(self):
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
