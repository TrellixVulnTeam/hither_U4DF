import time
from typing import Any, Union, Dict, List

from ._containermanager import ContainerManager
from ._enums import JobStatus
from .job import Job

class _JobManager:
    def __init__(self) -> None:
        self._unsubmitted_jobs = dict()
        self._submitted_jobs = dict()

    def queue_job(self, job):
        job._set_status(JobStatus.QUEUED)
        job.load_required_files_if_needed()
        job.prepare_container_if_needed()
        self._unsubmitted_jobs[job._job_id] = job
    
    def reset(self):
        self._unsubmitted_jobs = dict()
        self._submitted_jobs = dict()
    
    def wait(self, timeout: Union[float, None]=None):
        timer = time.time()
        while True:
            self.iterate()
            if self._unsubmitted_jobs == {} and self._submitted_jobs == {}:
                return
            if timeout == 0:
                return
            time.sleep(0.02)
            elapsed = time.time() - timer
            if timeout is not None and elapsed > timeout:
                return

    def iterate(self):
        # Called periodically during wait()
        self._prune_unsubmitted_jobs()
        self._submit_unsubmitted_jobs()
        self._review_submitted_jobs()

    def _prune_unsubmitted_jobs(self):
        for _id, job in list(self._unsubmitted_jobs.items()):
            if job._status != JobStatus.QUEUED:
                del self._unsubmitted_jobs[_id]

    def _submit_unsubmitted_jobs(self):
        unsubmitted_job_ids = list(self._unsubmitted_jobs.keys())
        for _id in unsubmitted_job_ids:
            job: Job = self._unsubmitted_jobs[_id]
            if not job.is_ready_to_run(): continue

            del self._unsubmitted_jobs[_id]
            if job._status == JobStatus.ERROR: continue

            job._job_handler._internal_counts.num_jobs += 1

            self._submitted_jobs[_id] = job
            job.resolve_dependent_job_values()
            if job._job_cache is not None:
                job._job_cache.fetch_cached_job_results(job)
                if job._status in JobStatus.complete_statuses():
                    job._job_handler._internal_counts.num_skipped_jobs += 1
                    return

            job._job_handler._internal_counts.num_submitted_jobs += 1
            job._job_handler.handle_job(job)

    def _review_submitted_jobs(self):
        # Check which submitted jobs are finished and iterate job handlers of submitted jobs
        job_handlers_to_iterate = dict()
        submitted_job_ids = list(self._submitted_jobs.keys())
        for _id in submitted_job_ids:
            job: Job = self._submitted_jobs[_id]
            jh = job._job_handler
            job_handlers_to_iterate[jh._internal_id] = jh
            if job._status == JobStatus.QUEUED:
                pass
            elif job._status == JobStatus.RUNNING:
                pass    
            elif job._status in JobStatus.complete_statuses():
                del self._submitted_jobs[job._job_id]
                if job._job_cache is not None:
                    job._job_cache.cache_job_result(job)
                if job._status == JobStatus.ERROR:
                    job._job_handler._internal_counts.num_errored_jobs += 1
                elif job._status == JobStatus.FINISHED:
                    job._job_handler._internal_counts.num_finished_jobs += 1
        for jh in job_handlers_to_iterate.values():
            jh.iterate()