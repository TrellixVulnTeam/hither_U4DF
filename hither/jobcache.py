from typing import Dict, List, Union, Any

import kachery as ka
from .database import Database, JobKeys
from ._util import _deserialize_item, _flatten_nested_collection
from ._enums import JobStatus
from .file import File
from .job import Job

class JobCache:
    def __init__(self, database: Database, cache_failing=False, rerun_failing=False, force_run=False):
        self._database = database
        self._cache_failing = cache_failing
        self._rerun_failing = rerun_failing
        self._force_run = force_run

    def fetch_cached_job_results(self, job: Job) -> bool:
        if self._force_run:
            return False
        job_dict = self._database._fetch_cached_job(job._compute_hash())
        if job_dict is None:
            return False

        status = job_dict[JobKeys.STATUS]
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

    def cache_job_result(self, job):
        assert isinstance(job._status, JobStatus)
        if job._status == JobStatus.ERROR:
            if not self._cache_failing:
                return
        self._database._cache_job_result(job)
        