import time
from typing import Any, Union, Dict, List

from ._containermanager import ContainerManager
from ._enums import JobStatus
from .job import Job

class _JobManager:
    def __init__(self) -> None:
        self._queued_jobs = dict()
        self._running_jobs = dict()

    def queue_job(self, job):
        job._status = JobStatus.QUEUED
        self._queued_jobs[job._job_id] = job

    def process_job_queues(self):
        # Called periodically during wait()
        self.prune_job_queue()
        self.prepare_containers_for_queued_jobs()
        self.run_queued_jobs()
        self.review_running_jobs()

    def prune_job_queue(self):
        for _id, job in list(self._queued_jobs.items()):
            if job._status != JobStatus.QUEUED:
                del self._queued_jobs[_id]

    def prepare_containers_for_queued_jobs(self):
        job:Job
        for job in self._queued_jobs.values():
            job.prepare_container_if_needed()

    def run_queued_jobs(self):
        queued_job_ids = list(self._queued_jobs.keys())
        for _id in queued_job_ids:
            job: Job = self._queued_jobs[_id]
            if not job.is_ready_to_run(): continue

            del self._queued_jobs[_id]
            if job._status == JobStatus.ERROR: continue

            job._job_handler._internal_counts.num_jobs += 1

            self._running_jobs[_id] = job
            job.resolve_wrapped_job_values()
            if job._job_cache is not None:
                job._job_cache.fetch_cached_job_results(job)
                if job._status in JobStatus.complete_statuses():
                    job._job_handler._internal_counts.num_skipped_jobs += 1
                    return

            job._job_handler._internal_counts.num_run_jobs += 1
            job._job_handler.handle_job(job)

    def review_running_jobs(self):
        # Check which running jobs are finished and iterate job handlers of running or preparing jobs
        running_job_ids = list(self._running_jobs.keys())
        for _id in running_job_ids:
            job: Job = self._running_jobs[_id]
            if job._status == JobStatus.RUNNING:
                # Note: we effectively iterate the same job handler potentially many times here -- I think that's okay but not 100% sure.
                # NOTE: I think it's okay, but any reason not to just move on to the next Job?
                job._job_handler.iterate()
            if job._status in JobStatus.complete_statuses():
                self.finish_completed_job(job)
                if job._status == JobStatus.ERROR:
                    job._job_handler._internal_counts.num_errored_jobs += 1
                elif job._status == JobStatus.FINISHED:
                    job._job_handler._internal_counts.num_finished_jobs += 1


    def finish_completed_job(self, job:Job) -> None:
        del self._running_jobs[job._job_id]
        if job._download_results:
            job.download_results_if_needed()
        if job._job_cache is not None:
            job._job_cache.cache_job_result(job)

    def reset(self):
        self._queued_jobs = dict()
        self._running_jobs = dict()
    
    def wait(self, timeout: Union[float, None]=None):
        timer = time.time()
        while True:
            self.process_job_queues()
            if self._queued_jobs == {} and self._running_jobs == {}:
                return
            if timeout == 0:
                return
            time.sleep(0.02)
            elapsed = time.time() - timer
            if timeout is not None and elapsed > timeout:
                return

