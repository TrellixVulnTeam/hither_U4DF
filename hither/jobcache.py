from typing import Dict, List, Union, Any
import tempfile
import os
import json

from .database import Database
from ._util import _deserialize_item
from ._enums import JobStatus, JobKeys
from .job import Job
from ._filelock import FileLock

class JobCache:
    def __init__(self,
        database: Union[Database, None]=None,
        use_tempdir: Union[bool, None]=None,
        path: Union[str, None]=None,
        cache_failing:bool=False,
        rerun_failing:bool=False,
        force_run:bool=False
    ):
        """Cache for storing a retrieving results of hither jobs.

        Provide one of the following arguments:
            database, use_tempdir, path

        Keyword Arguments:
            database {Union[Database, None]} -- A Mongo database object (default: {None})
            use_tempdir {Union[bool, None]} -- Whether to use a directory inside /tmp (or wherever tempdir is configured) (default: {None})
            path {Union[str, None]} -- Path to directory on local disk (default: {None})
            cache_failing {bool} -- Whether to cache failing jobs (default: {False})
            rerun_failing {bool} -- Whether to rerun jobs that had previously failed and been cached (default: {False})
            force_run {bool} -- Whether to force run jobs, even if results had previously been ached (default: {False})
        """
        self._database = database
        self._path = path
        self._use_tempdir = use_tempdir
        self._cache_failing = cache_failing
        self._rerun_failing = rerun_failing
        self._force_run = force_run
        
        errmsg = "You must provide exactly one of: database, use_tempdir, path"
        if self._database is not None:
            assert self._path is None, errmsg
            assert self._use_tempdir is None, errmsg
        
        if self._path is not None:
            assert self._database is None, errmsg
            assert self._use_tempdir is None, errmsg
        
        if self._use_tempdir is not None:
            assert self._database is None, errmsg
            assert self._path is None, errmsg
            self._path = f'{tempfile.gettempdir()}/hither_job_cache'
            if not os.path.exists(self._path):
                os.makedirs(self._path)
        
        if self._path:
            self._disk_cache = DiskJobCache(self._path)
        else:
            self._disk_cache = None
        
        assert self._database is not None or self._disk_cache is not None, errmsg

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
        job_dict = self._fetch_cached_job_result(job._compute_hash())
        if job_dict is None:
            return False

        status:JobStatus = JobStatus(job_dict[JobKeys.STATUS])
        if status not in JobStatus.complete_statuses():
            return False

        job_description = f"{job._label} ({job._function_name} {job._function_version})"
        if status == JobStatus.FINISHED:
            result = _deserialize_item(job_dict[JobKeys.RESULT])
            if not job._result_files_are_available_locally(results=result):
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
        job_hash = job._compute_hash()
        if self._database is not None:
            self._database._cache_job_result(job_hash, job)
        elif self._disk_cache is not None:
            self._disk_cache._cache_job_result(job_hash, job)
        else:
            raise Exception('Unexpected error.')
    
    def _fetch_cached_job_result(self, job_hash) -> Union[Dict[str, Any], None]:
        if self._database is not None:
            return self._database._fetch_cached_job_result(job_hash)
        elif self._path is not None:
            return self._disk_cache._fetch_cached_job_result(job_hash)
        else:
            raise Exception('Unexpected error.')

class DiskJobCache:
    def __init__(self, path):
        self._path = path
    
    def _cache_job_result(self, job_hash, job):
        obj = {
            JobKeys.JOB_HASH: job_hash,
            JobKeys.STATUS: job._status.value,
            JobKeys.RESULT: job._serialized_result(),
            JobKeys.RUNTIME_INFO: job._runtime_info,
            JobKeys.EXCEPTION: '{}'.format(job._exception)
        }
        p = self._get_cache_file_path(job_hash=job_hash, create_dir_if_needed=True)
        with FileLock(p + '.lock', exclusive=True):
            with open(p, 'w') as f:
                json.dump(obj, f)

    def _fetch_cached_job_result(self, job_hash):
        p = self._get_cache_file_path(job_hash=job_hash, create_dir_if_needed=False)
        if not os.path.exists(p):
            return None
        with FileLock(p + '.lock', exclusive=False):
            with open(p, 'r') as f:
                return json.load(f)

    def _get_cache_file_path(self, job_hash, create_dir_if_needed):
        dirpath = f'{self._path}/{job_hash[0]}{job_hash[1]}/{job_hash[2]}{job_hash[3]}/{job_hash[4]}{job_hash[5]}'
        if create_dir_if_needed:
            if not os.path.exists(dirpath):
                os.makedirs(dirpath)
        return f'{dirpath}/{job_hash}.json'


        