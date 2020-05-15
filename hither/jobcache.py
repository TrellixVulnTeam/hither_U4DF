from typing import Dict, List, Union, Any

from .database import Database
from ._util import _deserialize_item
from ._enums import JobStatus, JobKeys
from .job import Job

class JobCache:
    def __init__(self, database: Database, cache_failing:bool=False,
                    rerun_failing:bool=False, force_run:bool=False):
        self._database = database
        self._cache_failing = cache_failing
        self._rerun_failing = rerun_failing
        self._force_run = force_run

    def fetch_cached_job_results(self, job: Job) -> bool:
        """Replaces completed Jobs with their result from cache, and returns whether the cache
        hit or missed.

        Arguments:
            job {Job} -- Job to look for in the job cache.

        Returns:
            bool -- True if an acceptable cached result was found. False if the Job has not run,
            is unknown, or returned an error (and we're set to rerun errored Jobs).
        """
        if self._force_run:
            return False
        job_dict = self._database._fetch_cached_job(job._compute_hash())
        if job_dict is None:
            return False

        status:JobStatus = JobStatus(job_dict[JobKeys.STATUS])
        if status not in JobStatus.complete_statuses():
            return False

        job_description = f"{job._label} ({job._function_name} {job._function_version})"
        if status == JobStatus.FINISHED:
            result = _deserialize_item(job_dict[JobKeys.RESULT])
            if not job._result_files_are_available_locally():
                print(f'Found result in cache, but files do not exist locally: {job_description}')  # TODO: Make log
                return False
            job._result = result
            job._exception = None
            print(f'Using cached result for job: {job_description}') # TODO: Make log
        elif status == JobStatus.ERROR:
            exception = job_dict[JobKeys.EXCEPTION]
            if self._cache_failing and (not self._rerun_failing):
                job._result = None
                job._exception = Exception(exception)
                print(f'Using cached error for job: {job_description}') # TODO: Make log
            else:
                return False
        job._status = status
        job._runtime_info = job_dict[JobKeys.RUNTIME_INFO]
        return True

    def cache_job_result(self, job:Job):
        if job._status == JobStatus.ERROR and not self._cache_failing:
            return 
        self._database._cache_job_result(job)
        