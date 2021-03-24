import time
from typing import Any, Dict, List, Union
from ._job import Job
from ._job_handler import JobHandler
from ._config import UseConfig
from .function import _get_hither_function_wrapper
from ._check_job_cache import _check_job_cache, _write_result_to_job_cache, _batch_check_job_cache
from ._run_function import _run_function

class JobManager:
    def __init__(self):
        self._jobs: Dict[str, Job] = {}
    def _add_job(self, job: Job):
        self._jobs[job.job_id] = job
    def _iterate(self):
        deletion_job_ids: List[str] = []

        # check job cache for the pending jobs that are ready to run
        # important to do this in a single batch (instead of individual checks)
        jobs_to_check = [job for job in self._jobs.values() if (job.status == 'pending') and (job.config.job_cache is not None) and (_job_is_ready_to_run(job))]
        if len(jobs_to_check) > 0:
            _batch_check_job_cache(jobs_to_check)

        for job_id, job in self._jobs.items():
            f = job.function
            fw = _get_hither_function_wrapper(f)
            if fw is None:
                raise Exception('Unexpected: no function wrapper')
            if job.status == 'pending':
                if job.cancel_pending:
                    job._set_error(Exception('Job cancelled while pending.'))
                elif _job_is_ready_to_run(job):
                    jh = job.config.job_handler
                    if jh is not None:
                        # we have a job handler
                        job._set_queued()
                        jh.queue_job(job)
                    else:
                        job._set_running()
                        try:
                            return_value = _run_function(
                                function_wrapper=fw,
                                kwargs=job.get_resolved_kwargs(),
                                use_container=job.config.use_container
                            )
                            error = None
                        except Exception as e:
                            error = e
                            return_value = None
                        if error is None:
                            job._set_finished(return_value=return_value)
                        else:
                            job._set_error(error)
                else:
                    e = _get_job_input_error(job)
                    if e is not None:
                        job._set_error(e)
            elif job.status in ['queued', 'running']:
                if job.cancel_pending:
                    jh = job.config.job_handler
                    if jh is not None:
                        jh.cancel_job(job.job_id)
            elif job.status == 'finished':
                if not job.result_is_from_cache:
                    jc = job.config.job_cache
                    if jc is not None:
                        jr = job.result
                        if jr is not None:
                            _write_result_to_job_cache(job_result=jr, function_name=fw.name, function_version=fw.version, kwargs=job.get_resolved_kwargs(), job_cache=jc)
                deletion_job_ids.append(job_id)
            elif job.status == 'error':
                deletion_job_ids.append(job_id)
        for job_id in deletion_job_ids:
            del self._jobs[job_id]
        job_handlers_to_iterate: Dict[str, JobHandler] = dict()
        for job_id, job in self._jobs.items():
            if job.status in ['queued', 'running']:
                jh = job.config.job_handler
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
    return _kwargs_are_all_resolved(job.get_resolved_kwargs())

def _kwargs_are_all_resolved(x: Any):
    if isinstance(x, Job):
        return False
    elif isinstance(x, dict):
        for k, v in x.items():
            if not _kwargs_are_all_resolved(v):
                return False
    elif isinstance(x, list):
        for a in x:
            if not _kwargs_are_all_resolved(a):
                return False
    elif isinstance(x, tuple):
        for a in x:
            if not _kwargs_are_all_resolved(a):
                return False
    else:
        pass
    return True

def _get_job_input_error(job: Job):
    return _get_kwargs_job_error(job.get_resolved_kwargs())

def _get_kwargs_job_error(x: Any):
    if isinstance(x, Job):
        if x.status == 'error':
            e = x.result.error
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
def wait(timeout_sec: Union[float, None]=None):
    global_job_manager.wait(timeout_sec)