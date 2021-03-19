import time
from typing import Any, Dict, List, Union
from ._job import Job
from ._job_handler import JobHandler, _run_job_directly

class JobManager:
    def __init__(self):
        self._jobs: Dict[str, Job] = {}
    def _add_job(self, job: Job):
        self._jobs[job.get_job_id()] = job
    def _iterate(self):
        deletion_job_ids: List[str] = []
        for job_id, job in self._jobs.items():
            if job.get_status() == 'pending':
                if _job_is_ready_to_run(job):
                    jh = job.get_config().job_handler
                    job._set_queued()
                    if jh is not None:
                        jh.queue_job(job)
                    else:
                        _run_job_directly(job)
                else:
                    e = _get_job_input_error(job)
                    if e is not None:
                        job._set_error(e)
            elif job.get_status() == 'running':
                pass
            elif job.get_status() == 'finished':
                deletion_job_ids.append(job_id)
            elif job.get_status() == 'error':
                deletion_job_ids.append(job_id)
        for job_id in deletion_job_ids:
            del self._jobs[job_id]
        job_handlers_to_iterate: Dict[str, JobHandler] = dict()
        for job_id, job in self._jobs.items():
            if job.get_status() in ['queued', 'running']:
                jh = job.get_config().job_handler
                if jh is not None:
                    job_handlers_to_iterate[jh._get_internal_id()] = jh
        for jh in job_handlers_to_iterate.values():
            jh.iterate()
    def wait(self, timeout_sec: Union[float, None]):
        timer = time.time()
        while True:
            self._iterate()
            if len(self._jobs.keys()) == 0:
                return
            else:
                time.sleep(0.05)
            if timeout_sec is not None:
                elaped = time.time() - timer
                if elaped > timeout_sec:
                    return

def _job_is_ready_to_run(job: Job):
    return _kwargs_all_resolved(job.get_kwargs())

def _kwargs_all_resolved(x: Any):
    if isinstance(x, Job):
        return False
    elif isinstance(x, dict):
        for k, v in x.items():
            if not _kwargs_all_resolved(v):
                return False
    elif isinstance(x, list):
        for a in x:
            if not _kwargs_all_resolved(a):
                return False
    elif isinstance(x, tuple):
        for a in x:
            if not _kwargs_all_resolved(a):
                return False
    else:
        pass
    return True

def _get_job_input_error(job: Job):
    return _get_kwargs_job_error(job.get_kwargs())

def _get_kwargs_job_error(x: Any):
    if isinstance(x, Job):
        if x.get_status() == 'error':
            e = x.get_result().get_error()
            assert e is not None
            return e
    elif isinstance(x, dict):
        for k, v in x.items():
            e = _get_kwargs_job_error(v)
            if e is not None:
                return e
    elif isinstance(x, list):
        for a in x:
            e = _get_kwargs_job_error(a)
            if e is not None:
                return e
    elif isinstance(x, tuple):
        for a in x:
            e = _get_kwargs_job_error(a)
            if e is not None:
                return e
    else:
        pass
    return None

global_job_manager = JobManager()
def wait(timeout_sec: Union[float, None]):
    global_job_manager.wait(timeout_sec)